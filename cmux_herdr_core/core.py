"""Constants, plugin paths, configuration, state files and shared locks."""

import contextvars
import fcntl
import hashlib
import json
import os
import re
import shlex
import stat
import sys
import threading
import tomllib
import uuid
from contextlib import contextmanager, suppress
from pathlib import Path

from cmux_herdr_core import cmux, remote_bridge

ATTENTION_STATUSES = {"blocked", "done"}


TRACKED_STATUSES = ATTENTION_STATUSES | {"working"}


DEFAULT_CMUX_BIN = "/Applications/cmux.app/Contents/Resources/bin/cmux"


STATUS_COLOR_WAITING = "#ff9500"


STATUS_COLOR_DONE = "#30d158"


STATUS_COLOR_WORKING = "#0a84ff"


ICON_WAITING = "hourglass"


ICON_DONE = "checkmark.circle"


ICON_WORKING = "bolt"


PANE_STATUS_WORDS = {"blocked": "waiting", "done": "finished", "working": "working"}


PANE_STATUS_ICONS = {
    "blocked": ICON_WAITING,
    "done": ICON_DONE,
    "working": ICON_WORKING,
}


PANE_STATUS_COLORS = {
    "blocked": STATUS_COLOR_WAITING,
    "done": STATUS_COLOR_DONE,
    "working": STATUS_COLOR_WORKING,
}


PANE_STATUS_PRIORITIES = {"blocked": "15", "done": "14", "working": "5"}


DETECT_TTL_SECONDS = 300


NOTIFICATION_SUBTITLE = "herdr"


NOTIFICATION_TITLE_RE = re.compile(r": (waiting for input|finished)$")


# Interval between iterations of the long-running supervisor, watch and
# client loops.
LOOP_TICK_SECONDS = 1.0


REMOTE_KEY_PREFIX = "herdr.remote."


REMOTE_REF_TTL_SECONDS = 60


REMOTE_STALE_REF_TTL_SECONDS = 600


REMOTE_DEFAULT_POLL_SECONDS = 2.0


REMOTE_ERROR_BACKOFF_SECONDS = 10.0


# Set when the remote daemon is shutting down; workers must not spawn new
# SSH forwards after this point (the main thread is tearing them down).
DAEMON_SHUTDOWN = threading.Event()


LEGACY_REPLAY_CONTEXT = contextvars.ContextVar("legacy_replay_context", default=None)


ACTIVATION_DIRECTORY = contextvars.ContextVar("activation_directory", default=None)


HERDR_SOURCE_FENCE = contextvars.ContextVar("herdr_source_fence", default=None)


state_dir = Path(os.environ.get("HERDR_PLUGIN_STATE_DIR", "."))


config_dir = Path(os.environ.get("HERDR_PLUGIN_CONFIG_DIR", "."))


STATE_PATH = state_dir / "state.json"


def v1_plugin_paths(env):
    """Resolve the Herdr plugin directories for a shell-launched observer."""
    user_home = Path(env.get("HOME") or str(Path.home()))
    state_base = Path(env.get("XDG_STATE_HOME") or user_home / ".local/state")
    config_base = Path(env.get("XDG_CONFIG_HOME") or user_home / ".config")
    state = Path(
        env.get("HERDR_PLUGIN_STATE_DIR") or state_base / "herdr/plugins/cmux-herdr"
    )
    config = Path(
        env.get("HERDR_PLUGIN_CONFIG_DIR")
        or config_base / "herdr/plugins/config/cmux-herdr"
    )
    return state, config


def log(message):
    print(f"[cmux-herdr] {message}", file=sys.stderr)


def load_config():
    path = config_dir / "config.toml"
    if not path.exists():
        return {}
    try:
        with path.open("rb") as f:
            return tomllib.load(f)
    except Exception as e:
        log(f"failed to parse {path}: {e}")
        return {}


def load_config_strict():
    """Like load_config, but None on parse errors so callers can keep the
    last valid config instead of treating it as empty."""
    path = config_dir / "config.toml"
    if not path.exists():
        return {}
    try:
        with path.open("rb") as f:
            return tomllib.load(f)
    except Exception as e:
        log(f"failed to parse {path}: {e}; keeping previous config")
        return None


def load_state():
    try:
        state = json.loads(STATE_PATH.read_text())
    except Exception:
        return {"sessions": {}}
    if "workspaces" in state or "session_ref" in state:
        # Pre-multi-session format: entries cannot be attributed to the
        # session that owned them, so drop them. Each session rebuilds its
        # own bucket from events/reconcile and the orphan sweep clears any
        # pills the dropped entries leave behind.
        state.pop("workspaces", None)
        state.pop("session_ref", None)
        log("dropped pre-multi-session state; pills will be rebuilt")
    state.setdefault("sessions", {})
    return state


def save_state(state):
    data = json.dumps(state)
    # Long-running loops save every tick; rewriting identical content would
    # only churn the file.
    with suppress(OSError, ValueError):
        if STATE_PATH.read_text() == data:
            return
    state_dir.mkdir(parents=True, exist_ok=True)
    # Per-process tmp name: concurrent hooks from different sessions share
    # this file and must not move each other's tmp away mid-save.
    tmp = STATE_PATH.with_suffix(f".{os.getpid()}.tmp")
    tmp.write_text(data)
    tmp.replace(STATE_PATH)


def _v1_check_private_file(info, message, *, max_size=None):
    """Require a regular file owned by this user and closed to everyone else."""
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or info.st_mode & 0o077
        or (max_size is not None and info.st_size > max_size)
    ):
        raise ValueError(message)


@contextmanager
def _v1_private_temporary(directory_fd, prefix, data):
    """Durably write data to a new private file and yield its name and stat.

    The caller installs the file (link or replace); whatever is left under the
    temporary name is removed afterwards.
    """
    temporary = f".{prefix}-{uuid.uuid4()}"
    fd = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
        0o600,
        dir_fd=directory_fd,
    )
    try:
        with os.fdopen(fd, "wb") as file:
            file.write(data)
            file.flush()
            os.fsync(file.fileno())
            info = os.fstat(file.fileno())
        yield temporary, info
    finally:
        with suppress(FileNotFoundError):
            os.unlink(temporary, dir_fd=directory_fd)


class state_lock:
    """Serialize load/modify/save of state.json across concurrent hooks.

    Hooks from every herdr session share one state file; without an
    exclusive lock a later save silently drops another hook's concurrent
    update.
    """

    def __enter__(self):
        state_dir.mkdir(parents=True, exist_ok=True)
        self._lock = open(state_dir / "state.lock", "w")
        fcntl.flock(self._lock, fcntl.LOCK_EX)
        return self

    def __exit__(self, *_exc_info):
        self._lock.close()
        return False


class v1_source_leader:
    """Allow one process to consume a source's ordered event stream."""

    def __init__(self, source_id, *, blocking=False):
        digest = hashlib.sha256(source_id.encode()).hexdigest()
        self.path = state_dir / f"v1-source-{digest}.lock"
        self._lock = None
        self.blocking = blocking

    def __enter__(self):
        state_dir.mkdir(parents=True, exist_ok=True)
        self._lock = self.path.open("a")
        try:
            fcntl.flock(
                self._lock, fcntl.LOCK_EX | (0 if self.blocking else fcntl.LOCK_NB)
            )
        except BlockingIOError:
            self._lock.close()
            self._lock = None
            return False
        return True

    def __exit__(self, *_exc_info):
        if self._lock is not None:
            self._lock.close()
        return False


def current_session():
    # herdr injects HERDR_SESSION into every hook; "" when run outside one.
    context = LEGACY_REPLAY_CONTEXT.get()
    return (
        context["session"]
        if context is not None
        else os.environ.get("HERDR_SESSION", "")
    )


def interactive_client_target(args):
    """Identify supported interactive Herdr invocations, not API commands."""
    session = os.environ.get("HERDR_SESSION", "")
    remote = None
    explicit_session = False
    remaining = list(args)
    while remaining:
        arg = remaining.pop(0)
        if arg in ("--session", "--remote", "--remote-keybindings"):
            if not remaining:
                return None
            value = remaining.pop(0)
            if arg == "--session":
                explicit_session = True
                session = value
            elif arg == "--remote":
                remote = value
        elif arg.startswith("--session="):
            explicit_session = True
            session = arg.partition("=")[2]
            if not session:
                return None
        elif arg.startswith("--remote="):
            remote = arg.partition("=")[2]
        elif arg in ("--handoff",):
            continue
        elif arg == "session" and len(remaining) == 2 and remaining[0] == "attach":
            explicit_session = True
            session = remaining[1]
            remaining.clear()
        else:
            return None
    if remote == "" or ("--session" in args and not session):
        return None
    target = {"session": session, "remote": remote}
    socket_override = os.environ.get("HERDR_SOCKET_PATH")
    if not remote and not explicit_session and socket_override:
        target["socket_path"] = socket_override
    return target


def v1_source_key(target):
    """Identify a local or remote session without ambiguous delimiters."""
    if target.get("socket_path"):
        identity = ["local-socket", os.path.realpath(target["socket_path"])]
    else:
        identity = [target.get("remote"), target.get("session") or "default"]
    return json.dumps(identity, separators=(",", ":"))


def v1_supervised_source_key(target):
    """Separate source incarnations so handoff can prepare both safely."""
    return json.dumps(
        [
            v1_source_key(target),
            target.get("server_boot_id") or target.get("socket_generation"),
            target.get("server_pid"),
            target.get("server_start_token"),
        ],
        separators=(",", ":"),
    )


def v1_target_with_remote_config(cfg, target):
    """Keep remote executable configuration without using title as identity."""
    if not target.get("remote"):
        return target.copy()
    remotes = remote_bridge.remote_configs(cfg)
    if remotes is None:
        return None
    matches = [
        entry
        for entry in remotes
        if entry["ssh_target"] == target["remote"]
        and entry["session"] == (target.get("session") or "default")
    ]
    binaries = {entry.get("herdr_bin") for entry in matches if entry.get("herdr_bin")}
    if len(binaries) > 1:
        log("conflicting herdr_bin settings for remote session")
        return None
    return {**target, **({"herdr_bin": binaries.pop()} if binaries else {})}


def v1_remote_config(target):
    rcfg = {
        "ssh_target": target["remote"],
        "session": target.get("session") or "default",
    }
    if target.get("herdr_bin"):
        rcfg["herdr_bin"] = target["herdr_bin"]
    return rcfg


def v1_local_generation(socket_path):
    """Identify the local server's socket incarnation."""
    stat = os.stat(socket_path)
    return f"{stat.st_dev}:{stat.st_ino}"


def v1_remote_generation(ssh_target, socket_path):
    """Identify a remote server socket without interpolating shell syntax."""
    script = "import os,sys; s=os.stat(sys.argv[1]); print(f'{s.st_dev}:{s.st_ino}')"
    command = " ".join(
        shlex.quote(value) for value in ("python3", "-c", script, socket_path)
    )
    proc = cmux.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", ssh_target, command],
        timeout=20,
    )
    if proc.returncode:
        raise remote_bridge.SocketError(
            f"cannot inspect remote socket: {proc.stderr.strip()}"
        )
    value = proc.stdout.strip()
    if not re.fullmatch(r"\d+:\d+", value):
        raise remote_bridge.SocketError("invalid remote socket identity")
    return value


def session_bucket(state, session=None):
    """This session's slice of the shared plugin state.

    One plugin state directory is shared by every herdr session on the
    machine, and workspace ids (w1, w2, ...) are only unique within a
    session, so all per-session data must live under the session's own
    bucket to keep sessions from clobbering each other.
    """
    return state.setdefault("sessions", {}).setdefault(
        current_session() if session is None else session, {"workspaces": {}}
    )


def all_workspaces(state):
    """Iterate (bucket, ws_id, ws) across every session bucket."""
    for bucket in state.get("sessions", {}).values():
        for ws_id, ws in bucket.get("workspaces", {}).items():
            yield bucket, ws_id, ws


def remote_state_path():
    return state_dir / "remote-state.json"


def load_remote_state():
    try:
        return json.loads(remote_state_path().read_text())
    except Exception:
        return {"remotes": {}}


def save_remote_state(rstate):
    state_dir.mkdir(parents=True, exist_ok=True)
    tmp = remote_state_path().with_suffix(".tmp")
    tmp.write_text(json.dumps(rstate))
    tmp.replace(remote_state_path())


# cmux attributes CLI notifications to the caller's pane via these inherited
# env vars, overriding --workspace targeting (hooks and the remote daemon
# inherit them from wherever the herdr server was started). Scrub them from
# child processes so explicit --workspace refs win.
CMUX_CALLER_IDENTITY_VARS = (
    "CMUX_PANEL_ID",
    "CMUX_SURFACE_ID",
    "CMUX_TAB_ID",
    "CMUX_WORKSPACE_ID",
    "CMUX_TERMINAL_LIFECYCLE_ID",
)
