#!/usr/bin/env python3
import contextvars
import copy
import ctypes
import errno
import fcntl
import hashlib
import json
import math
import os
import re
import select
import selectors
import shlex
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
import uuid
from contextlib import contextmanager, suppress
from pathlib import Path

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

REMOTE_KEY_PREFIX = "herdr.remote."
REMOTE_REF_TTL_SECONDS = 60
REMOTE_STALE_REF_TTL_SECONDS = 600
REMOTE_DEFAULT_POLL_SECONDS = 2.0
REMOTE_ERROR_BACKOFF_SECONDS = 10.0

# Set when the remote daemon is shutting down; workers must not spawn new
# SSH forwards after this point (the main thread is tearing them down).
DAEMON_SHUTDOWN = threading.Event()
LEGACY_REPLAY_CONTEXT = contextvars.ContextVar("legacy_replay_context", default=None)

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
    state_dir.mkdir(parents=True, exist_ok=True)
    # Per-process tmp name: concurrent hooks from different sessions share
    # this file and must not move each other's tmp away mid-save.
    tmp = STATE_PATH.with_suffix(f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(state))
    tmp.replace(STATE_PATH)


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

    def __exit__(self, *exc):
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

    def __exit__(self, *exc):
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
        [v1_source_key(target), target["server_boot_id"]], separators=(",", ":")
    )


def v1_target_with_remote_config(cfg, target):
    """Keep remote executable configuration without using title as identity."""
    if not target.get("remote"):
        return target.copy()
    remotes = remote_configs(cfg)
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
    proc = run(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", ssh_target, command],
        timeout=20,
    )
    if proc.returncode:
        raise SocketError(f"cannot inspect remote socket: {proc.stderr.strip()}")
    value = proc.stdout.strip()
    if not re.fullmatch(r"\d+:\d+", value):
        raise SocketError("invalid remote socket identity")
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


def run(cmd, timeout=10):
    env = {k: v for k, v in os.environ.items() if k not in CMUX_CALLER_IDENTITY_VARS}
    context = LEGACY_REPLAY_CONTEXT.get()
    if context is not None:
        env["HERDR_SESSION"] = context["session"]
        if context.get("socket_path") is None:
            env.pop("HERDR_SOCKET_PATH", None)
        else:
            env["HERDR_SOCKET_PATH"] = context["socket_path"]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env)


def herdr_cli(args):
    binary = os.environ.get("HERDR_BIN_PATH", "herdr")
    proc = run([binary, *args])
    if proc.returncode != 0:
        raise RuntimeError(f"herdr {' '.join(args)} failed: {proc.stderr.strip()}")
    return json.loads(proc.stdout)["result"]


def cmux_cli(cfg, args):
    binary = cfg.get("cmux_bin") or DEFAULT_CMUX_BIN
    try:
        proc = run([binary, *args])
    except FileNotFoundError:
        log(f"cmux binary not found: {binary}")
        return False
    except subprocess.TimeoutExpired:
        log(f"cmux {' '.join(args)} timed out")
        return False
    if proc.returncode != 0:
        log(f"cmux {' '.join(args)} failed: {proc.stderr.strip()}")
        return False
    return True


def cmux_workspaces_by_title(cfg):
    binary = cfg.get("cmux_bin") or DEFAULT_CMUX_BIN
    try:
        proc = run([binary, "list-workspaces"])
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return {}
    if proc.returncode != 0:
        return {}
    result = {}
    for line in proc.stdout.splitlines():
        # The selected workspace row is prefixed with "*" and suffixed with
        # "[selected]"; it must still resolve by its bare title.
        m = re.match(r"^[\s*]*(workspace:\S+)\s+(.+?)\s*$", line)
        if m:
            title = re.sub(r"\s*\[selected\]$", "", m.group(2))
            result[title.strip().lower()] = m.group(1)
    return result


def cmux_workspace_refs(cfg):
    """List workspace references without collapsing duplicate titles."""
    binary = cfg.get("cmux_bin") or DEFAULT_CMUX_BIN
    try:
        proc = run([binary, "list-workspaces"])
        if proc.returncode:
            return set()
        return {
            match.group(1)
            for line in proc.stdout.splitlines()
            if (match := re.match(r"^[\s*]*(workspace:\S+)\s+", line))
        }
    except (OSError, subprocess.TimeoutExpired):
        return set()


def cmux_herdr_status_keys_checked(cfg, ref):
    binary = cfg.get("cmux_bin") or DEFAULT_CMUX_BIN
    try:
        proc = run([binary, "list-status", "--workspace", ref])
    except (OSError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0:
        return None
    keys = []
    for line in proc.stdout.splitlines():
        m = re.match(r"^(herdr\.\S+?)=", line.strip())
        if m:
            keys.append(m.group(1))
    return keys


def cmux_herdr_status_keys(cfg, ref):
    return cmux_herdr_status_keys_checked(cfg, ref) or []


def cmux_notifications(cfg):
    binary = cfg.get("cmux_bin") or DEFAULT_CMUX_BIN
    try:
        proc = run([binary, "list-notifications", "--json"])
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return []
    if proc.returncode != 0:
        return []
    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return []
    return data if isinstance(data, list) else []


def cmux_notifications_checked(cfg):
    """Return None on failure so an unknown notification list is not empty."""
    binary = cfg.get("cmux_bin") or DEFAULT_CMUX_BIN
    try:
        proc = run([binary, "list-notifications", "--json"])
        if proc.returncode:
            return None
        result = json.loads(proc.stdout)
        return result if isinstance(result, list) else None
    except (OSError, subprocess.TimeoutExpired, ValueError):
        return None


def herdr_session_pids():
    session = current_session()
    try:
        proc = subprocess.run(
            ["ps", "-eo", "pid=,args="], capture_output=True, text=True, timeout=10
        )
    except subprocess.TimeoutExpired:
        return []
    pids = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        m = re.match(r"^(\d+)\s+(\S.*)$", line)
        if not m:
            continue
        pid, args = m.group(1), m.group(2)
        if "herdr" not in args:
            continue
        if re.search(r"\b(server|remote-client-bridge)\b", args):
            continue
        if re.search(r"(^|\s)--session(=|\s)", args):
            if session and not re.search(
                rf"--session(=|\s+){re.escape(session)}(\s|$)", args
            ):
                continue
            pids.append(int(pid))
        elif re.search(r"(^|\s)session\s+attach(\s|$)", args):
            if session and not re.search(
                rf"session\s+attach\s+{re.escape(session)}(\s|$)", args
            ):
                continue
            pids.append(int(pid))
        elif re.match(r"^\S*herdr\s*$", args):
            pids.append(int(pid))
    return pids


def find_pid_workspace(node, pids, workspace_ref=None):
    if isinstance(node, dict):
        ref = workspace_ref
        for key in ("workspace_ref", "workspace_id", "workspace"):
            v = node.get(key)
            if isinstance(v, str) and v.startswith("workspace:"):
                ref = v
        node_type = str(node.get("type") or node.get("kind") or "")
        if node_type.startswith("workspace") and isinstance(node.get("ref"), str):
            ref = node["ref"]
        pid = node.get("pid")
        if isinstance(pid, int) and pid in pids and ref:
            return ref
        for value in node.values():
            found = find_pid_workspace(value, pids, ref)
            if found:
                return found
    elif isinstance(node, list):
        for item in node:
            found = find_pid_workspace(item, pids, workspace_ref)
            if found:
                return found
    return None


def detect_session_workspace(cfg, bucket):
    context = LEGACY_REPLAY_CONTEXT.get()
    env_ref = (
        context.get("workspace")
        if context is not None
        else os.environ.get("CMUX_WORKSPACE_ID")
    )
    if env_ref:
        return env_ref

    cached = bucket.get("session_ref") or {}
    if cached.get("ref") and time.time() - cached.get("at", 0) < DETECT_TTL_SECONDS:
        return cached["ref"]

    ref = None
    pids = herdr_session_pids()
    if pids:
        binary = cfg.get("cmux_bin") or DEFAULT_CMUX_BIN
        try:
            proc = run([binary, "top", "--all", "--processes", "--json"], timeout=20)
            if proc.returncode == 0:
                ref = find_pid_workspace(json.loads(proc.stdout), set(pids))
        except (
            FileNotFoundError,
            subprocess.TimeoutExpired,
            json.JSONDecodeError,
        ) as e:
            log(f"session workspace detection failed: {e}")
    if ref:
        bucket["session_ref"] = {"ref": ref, "at": time.time()}
        log(f"detected cmux workspace {ref} for this herdr session")
    elif cached.get("ref"):
        return cached["ref"]
    return ref


def workspace_label(ws_id):
    try:
        for ws in herdr_cli(["workspace", "list"]).get("workspaces", []):
            if ws.get("workspace_id") == ws_id or ws.get("id") == ws_id:
                return ws.get("label") or ""
    except Exception as e:
        log(f"workspace list failed: {e}")
    return ""


def resolve_cmux_workspace(cfg, bucket, ws_id):
    entry = bucket["workspaces"].get(ws_id, {})
    label = entry.get("label") or workspace_label(ws_id)
    if label and not entry.get("label"):
        entry["label"] = label
        bucket["workspaces"][ws_id] = entry

    ref = cfg.get("workspaces", {}).get(ws_id) or cfg.get("labels", {}).get(label)
    if not ref:
        ref = detect_session_workspace(cfg, bucket)
    if not ref and cfg.get("match_by_label", True) and label:
        ref = cmux_workspaces_by_title(cfg).get(label.strip().lower())
    if not ref:
        log(
            f"no cmux workspace mapping for herdr workspace {ws_id} "
            f"({label!r}); skipping"
        )
    return ref, label


def status_key(ws_id):
    return f"herdr.{ws_id}"


def sanitize_key(part):
    """Pane ids contain characters like ':'; keep status keys simple."""
    return re.sub(r"[^A-Za-z0-9_-]", "_", part)


def pane_key(base, pane_id):
    return f"{base}.{sanitize_key(pane_id)}"


def push_pill(cfg, ref, key, panes):
    """Publish the current state of one herdr workspace to the cmux sidebar.

    Everything here is keyed set-status state, never log history: entries
    update in place while a status holds and are cleared as soon as the
    pane leaves it, so the sidebar always reflects *now*. "finished" is an
    unread-result signal — herdr flips done to idle when the user views the
    pane, which clears the entry.
    """
    waiting = sum(1 for p in panes.values() if p["status"] == "blocked")
    done = sum(1 for p in panes.values() if p["status"] == "done")
    working = sum(1 for p in panes.values() if p["status"] == "working")
    if not panes:
        cmux_cli(cfg, ["clear-status", key, "--workspace", ref])
    else:
        parts = []
        if waiting:
            parts.append(f"{waiting} waiting")
        if done:
            parts.append(f"{done} finished")
        if working:
            parts.append(f"{working} working")
        # The aggregate takes the look of the most action-worthy state.
        if waiting:
            color, icon, priority = STATUS_COLOR_WAITING, ICON_WAITING, "20"
        elif done:
            color, icon, priority = STATUS_COLOR_DONE, ICON_DONE, "18"
        else:
            color, icon, priority = STATUS_COLOR_WORKING, ICON_WORKING, "10"
        cmux_cli(
            cfg,
            [
                "set-status",
                key,
                " · ".join(parts),
                "--workspace",
                ref,
                "--color",
                color,
                "--priority",
                priority,
                "--icon",
                icon,
            ],
        )
    # Per-agent entries are only ever (re)set here; removals are cleared
    # explicitly by the callers that observe them (events, snapshot diffs,
    # teardown) and by the orphan sweeps.
    for pane_id, pane in panes.items():
        status = pane["status"]
        cmux_cli(
            cfg,
            [
                "set-status",
                pane_key(key, pane_id),
                f"{pane['agent']}: {PANE_STATUS_WORDS.get(status, status)}",
                "--workspace",
                ref,
                "--color",
                PANE_STATUS_COLORS.get(status, STATUS_COLOR_WORKING),
                "--priority",
                PANE_STATUS_PRIORITIES.get(status, "5"),
                "--icon",
                PANE_STATUS_ICONS.get(status, ICON_WORKING),
            ],
        )


def clear_pane_pill(cfg, bucket, ws_id, pane_id):
    ref, _ = resolve_cmux_workspace(cfg, bucket, ws_id)
    if ref:
        cmux_cli(
            cfg,
            ["clear-status", pane_key(status_key(ws_id), pane_id), "--workspace", ref],
        )


def update_pill(cfg, bucket, ws_id):
    ref, _ = resolve_cmux_workspace(cfg, bucket, ws_id)
    if not ref:
        return
    panes = bucket["workspaces"].get(ws_id, {}).get("panes", {})
    push_pill(cfg, ref, status_key(ws_id), panes)


def notify(cfg, bucket, ws_id, title, body):
    ref, _ = resolve_cmux_workspace(cfg, bucket, ws_id)
    if not ref:
        return
    cmux_cli(
        cfg,
        [
            "notify",
            "--title",
            title,
            "--subtitle",
            NOTIFICATION_SUBTITLE,
            "--body",
            body,
            "--workspace",
            ref,
        ],
    )


def mark_read(cfg, bucket, ws_id):
    ref, _ = resolve_cmux_workspace(cfg, bucket, ws_id)
    if not ref:
        return
    # mark-notification-read is workspace-wide; do not hide notifications
    # for a remote that still needs attention on the same workspace.
    for r in load_remote_state().get("remotes", {}).values():
        if r.get("ref") == ref and has_attention_panes(r.get("panes", {})):
            return
    cmux_cli(cfg, ["mark-notification-read", "--workspace", ref])


def has_attention_panes(panes):
    return any(p["status"] in ATTENTION_STATUSES for p in panes.values())


def v1_completion_sequence(entry):
    """Recover the exact completed episode without treating its watermark as its ID."""
    generation, terminal = entry.get("generation"), entry.get("terminal_id")
    episode, watermark = entry.get("episode"), entry.get("sequence")
    if not all(
        isinstance(value, str) and value for value in (generation, terminal, episode)
    ):
        return None
    prefix = f"{generation}:{terminal}:"
    if not episode.startswith(prefix):
        return None
    suffix = episode[len(prefix) :]
    if not re.fullmatch(r"0|[1-9][0-9]{0,19}", suffix):
        return None
    sequence = int(suffix)
    if (
        sequence > 0xFFFFFFFFFFFFFFFF
        or type(watermark) is not int
        or sequence > watermark
    ):
        return None
    saved = entry.get("completion_sequence", sequence)
    return sequence if type(saved) is int and saved == sequence else None


def apply_attention_status(attention, pane_id, status, episode, *, sequence=None):
    """Update a pane's attention without treating a seen result as resolved."""
    current = attention.get(pane_id, {})
    previous_sequence = current.get("sequence")
    if (
        sequence is not None
        and previous_sequence is not None
        and sequence <= previous_sequence
    ):
        return
    if status in ATTENTION_STATUSES:
        if current.get("acknowledged") == episode:
            return
        if current.get("status") != status or current.get("episode") != episode:
            attention[pane_id] = {"status": status, "episode": episode}
    elif status == "working" or (
        status == "idle" and current.get("status") == "blocked"
    ):
        attention.pop(pane_id, None)
    elif status == "unknown" and current.get("status") == "done":
        completion_sequence = v1_completion_sequence(current)
        if completion_sequence is not None:
            current.setdefault("completion_sequence", completion_sequence)
    watermark = sequence if sequence is not None else previous_sequence
    if watermark is not None:
        attention.setdefault(pane_id, {})["sequence"] = watermark
    for key in ("generation", "terminal_id", "observed_changes"):
        if key in current:
            attention.setdefault(pane_id, {})[key] = current[key]


def force_acknowledge_attention(attention, pane_id, episode):
    """Acknowledge only the specific completed episode named by the caller."""
    current = attention.get(pane_id, {})
    if (
        current.get("status") != "done"
        or current.get("episode") != episode
        or current.get("provisional")
    ):
        return False
    attention[pane_id] = {"acknowledged": episode}
    for key in (
        "sequence",
        "generation",
        "terminal_id",
        "provisional",
        "observed_changes",
        "predicted_sequence",
        "release_provenance_seq",
        "released_completion_seq",
    ):
        if key in current:
            attention[pane_id][key] = current[key]
    return True


def has_pending_attention(attention, pane_id):
    return attention.get(pane_id, {}).get("status") in ATTENTION_STATUSES


def reconcile_agent_snapshot(attention, agents, generation):
    """Apply a successful agent snapshot without inferring missing panes closed."""
    for agent in agents:
        pane_id = agent.get("pane_id")
        terminal_id = agent.get("terminal_id")
        sequence = agent.get("state_change_seq")
        if not pane_id or not terminal_id or not isinstance(sequence, int):
            continue
        current = attention.get(pane_id, {})
        if current.get("terminal_id") not in (None, terminal_id):
            attention.pop(pane_id, None)
            current = {}
        if current.get("generation") not in (None, generation):
            if current.get("status") == "blocked":
                attention.pop(pane_id, None)
                current = {}
            else:
                current.pop("sequence", None)
        episode = f"{generation}:{terminal_id}:{sequence}"
        apply_attention_status(
            attention, pane_id, agent.get("agent_status"), episode, sequence=sequence
        )
        attention.setdefault(pane_id, {})["generation"] = generation
        attention[pane_id]["terminal_id"] = terminal_id


def apply_v1_snapshot(
    state,
    source_id,
    generation,
    agents,
    pane_ids,
    pane_records=None,
    *,
    reconnect=False,
    boot_id=None,
):
    """Persist observed state, retiring panes only with an authoritative list."""
    if agents is None:
        return
    source = (
        state.setdefault("v1", {})
        .setdefault("sources", {})
        .setdefault(source_id, {"attention": {}, "panes": {}})
    )
    attention = source.setdefault("attention", {})
    panes = source.setdefault("panes", {})
    if source.get("cursor_generation") not in (None, generation):
        panes.clear()
        for pane_id, entry in list(attention.items()):
            if entry.get("status") == "blocked":
                attention.pop(pane_id, None)
    for pane in pane_records or []:
        pane_id = pane["pane_id"]
        panes.setdefault(
            pane_id,
            {
                "workspace_id": pane.get("workspace_id", ""),
                "terminal_id": pane.get("terminal_id", ""),
                "status": pane.get("agent_status", "unknown"),
                "agent": pane.get("display_agent") or pane.get("agent") or "agent",
                "title": pane.get("terminal_title_stripped") or pane.get("title") or "",
            },
        )
    accepted = []
    for agent in agents:
        current = attention.get(agent.get("pane_id"), {})
        sequence = agent.get("state_change_seq")
        if (
            source.get("server_boot_id")
            and agent.get("agent_status") == "idle"
            and current.get("status") == "done"
            and current.get("generation") == generation
            and current.get("terminal_id") == agent.get("terminal_id")
            and type(sequence) is int
            and type(current.get("sequence")) is int
            and sequence > current["sequence"]
        ):
            if agent.get("completion_seq") == sequence:
                agent = {**agent, "agent_status": "done"}
            elif reconnect:
                attention.pop(agent.get("pane_id"), None)
                current = {}
            else:
                continue
        if (
            current.get("generation") == generation
            and current.get("terminal_id") == agent.get("terminal_id")
            and isinstance(sequence, int)
            and isinstance(current.get("sequence"), int)
            and sequence < current["sequence"]
        ):
            continue
        accepted.append(agent)
    snapshot_agents = []
    for agent in accepted:
        pane_id = agent.get("pane_id")
        current = attention.get(pane_id, {})
        pane = panes.get(pane_id, {})
        status = agent.get("agent_status")
        if (
            current.get("provisional")
            and current.get("generation") == generation
            and current.get("terminal_id") == agent.get("terminal_id")
            and (
                pane.get("status") == status
                and status in ATTENTION_STATUSES
                or pane.get("status") in ("done", "idle")
                and status == "idle"
                and current.get("status") == "done"
            )
            and isinstance(agent.get("state_change_seq"), int)
        ):
            current["episode"] = (
                f"{generation}:{agent['terminal_id']}:{agent['state_change_seq']}"
            )
            current["sequence"] = agent["state_change_seq"]
            current.pop("provisional", None)
            current.pop("predicted_sequence", None)
        else:
            if (
                (
                    boot_id
                    or source.get("server_boot_id")
                    and source.get("cursor_generation") == generation
                )
                and status == "idle"
                and type(agent.get("state_change_seq")) is int
                and agent.get("completion_seq") == agent["state_change_seq"]
            ):
                snapshot_agents.append({**agent, "agent_status": "done"})
            else:
                snapshot_agents.append(agent)
    reconcile_agent_snapshot(attention, snapshot_agents, generation)
    for agent in accepted:
        attention.get(agent.get("pane_id"), {}).pop("observed_changes", None)
    sequences = [
        agent["state_change_seq"]
        for agent in accepted
        if isinstance(agent.get("state_change_seq"), int)
    ]
    if source.get("cursor_generation") != generation:
        source.pop("published_tokens", None)
        source["event_cursor"] = max(sequences, default=0)
        source["cursor_generation"] = generation
    elif sequences:
        source["event_cursor"] = max(source.get("event_cursor", 0), *sequences)
    for agent in accepted:
        pane_id = agent.get("pane_id")
        if not pane_id:
            continue
        panes[pane_id] = {
            "workspace_id": agent.get("workspace_id", ""),
            "terminal_id": agent.get("terminal_id", ""),
            "status": agent.get("agent_status"),
            "completion_seq": agent.get("completion_seq"),
            "agent": agent.get("display_agent") or agent.get("agent") or "agent",
            "title": agent.get("terminal_title_stripped") or agent.get("title") or "",
        }
    for pane in pane_records or []:
        apply_v1_ack_event(state, source_id, {"pane": pane})
    if pane_ids is not None:
        live = set(pane_ids)
        for pane_id in set(attention) | set(panes):
            if pane_id not in live:
                attention.pop(pane_id, None)
                panes.pop(pane_id, None)
                source.get("retired_blocked", {}).pop(pane_id, None)


def new_v1_episode_id():
    return uuid.uuid4().hex


def v1_reconcile_absent_agents(
    state, source_id, generation, before, agents, panes, after
):
    """Retire blocked attention only with a stable, sequenced absence proof."""
    source = state.get("v1", {}).get("sources", {}).get(source_id, {})
    attention, tracked = source.get("attention", {}), source.get("panes", {})
    if any(
        not isinstance(snapshot.get("agents"), list)
        or not isinstance(snapshot.get("panes"), list)
        for snapshot in (before, after)
    ):
        return
    if any(
        not isinstance(item, dict) or not isinstance(item.get("pane_id"), str)
        for collection in (
            before["agents"],
            after["agents"],
            before["panes"],
            after["panes"],
            agents,
            panes,
        )
        for item in collection
    ):
        return

    def unique_panes(items):
        result = {}
        for item in items:
            pane_id = item["pane_id"]
            if pane_id in result:
                return None
            result[pane_id] = item
        return result

    snapshots = [
        unique_panes(items) for items in (before["panes"], panes, after["panes"])
    ]
    if any(snapshot is None for snapshot in snapshots):
        return
    active = {
        item["pane_id"]
        for collection in (before["agents"], agents, after["agents"])
        for item in collection
    }
    retired = source.setdefault("retired_blocked", {})
    for pane_id in snapshots[-1]:
        if pane_id in active:
            continue
        observations = [snapshot.get(pane_id) for snapshot in snapshots]
        if any(not isinstance(pane, dict) for pane in observations):
            continue
        terminal = observations[0].get("terminal_id")
        sequence = observations[0].get("state_change_seq")
        if (
            not isinstance(terminal, str)
            or not terminal
            or type(sequence) is not int
            or sequence < 0
            or any(
                pane.get("terminal_id") != terminal
                or pane.get("state_change_seq") != sequence
                or pane.get("agent") is not None
                or pane.get("agent_status") not in ("idle", "unknown")
                for pane in observations
            )
        ):
            continue
        prior = retired.get(pane_id, {})
        if (
            prior.get("generation") == generation
            and prior.get("terminal_id") == terminal
            and type(prior.get("sequence")) is int
            and prior["sequence"] > sequence
        ):
            continue
        retired[pane_id] = {
            "generation": generation,
            "terminal_id": terminal,
            "sequence": sequence,
        }
        current = attention.get(pane_id, {})
        observed_unknown_at_absence = (
            current.get("status") == "blocked"
            and tracked.get(pane_id, {}).get("status") == "unknown"
            and current.get("sequence") == sequence
        )
        if (
            current.get("generation") == generation
            and current.get("terminal_id") == terminal
            and type(current.get("sequence")) is int
            and current["sequence"] >= sequence
            and not observed_unknown_at_absence
        ):
            continue
        if current.get("terminal_id") not in (None, terminal):
            attention.pop(pane_id, None)
        elif (
            current.get("status") == "blocked"
            and current.get("generation") == generation
            and current.get("terminal_id") == terminal
            and type(current.get("sequence")) is int
            and (current["sequence"] < sequence or observed_unknown_at_absence)
        ):
            attention[pane_id] = {
                "sequence": current["sequence"],
                "generation": generation,
                "terminal_id": terminal,
            }
        tracked[pane_id] = {
            "workspace_id": observations[-1].get("workspace_id", ""),
            "terminal_id": terminal,
            "status": observations[-1]["agent_status"],
            "completion_seq": None,
            "agent": tracked.get(pane_id, {}).get("agent", "agent")
            if current.get("status") == "done"
            else "agent",
            "title": tracked.get(pane_id, {}).get("title", "")
            if current.get("status") == "done"
            else observations[-1].get("terminal_title_stripped")
            or observations[-1].get("title")
            or "",
        }


def v1_preserve_released_completion(
    source, pane_id, terminal, sequence, released, boot
):
    """Advance a watermark only for an already known, exact released completion."""
    generation = source.get("cursor_generation")
    entry = source.get("attention", {}).get(pane_id, {})
    if (
        not boot
        or source.get("server_boot_id") != boot
        or not isinstance(generation, str)
        or not generation
        or not isinstance(terminal, str)
        or not terminal
        or type(sequence) is not int
        or not 0 <= sequence <= 0xFFFFFFFFFFFFFFFF
        or type(released) is not int
        or not 0 <= released <= sequence
        or entry.get("generation") != generation
        or entry.get("terminal_id") != terminal
        or type(entry.get("sequence")) is not int
        or entry["sequence"] > sequence
    ):
        return False
    episode = f"{generation}:{terminal}:{released}"
    pending = (
        entry.get("status") == "done" and v1_completion_sequence(entry) == released
    )
    acknowledged = entry.get("acknowledged") == episode
    if not pending and not acknowledged:
        return False
    if pending:
        entry["completion_sequence"] = released
    entry["release_provenance_seq"] = released
    entry["sequence"] = sequence
    source["event_cursor"] = max(source.get("event_cursor", 0), sequence)
    return True


def apply_v1_status_event(state, source_id, data, event_boot=None):
    """Apply a status event in stream order without collapsing quick transitions."""
    source = state.get("v1", {}).get("sources", {}).get(source_id)
    pane_id = data.get("pane_id")
    status = data.get("agent_status")
    if not source or pane_id not in source.get("panes", {}):
        return False
    if source.get("server_boot_id") != event_boot and (
        source.get("server_boot_id") is not None or event_boot is not None
    ):
        return False
    if status not in TRACKED_STATUSES | {"idle", "unknown"}:
        return False
    pane = source["panes"][pane_id]
    previous = pane.get("status")
    attention = source.setdefault("attention", {})
    current = attention.get(pane_id, {})
    sequence, terminal = data.get("state_change_seq"), data.get("terminal_id")
    if event_boot:
        if (
            type(sequence) is not int
            or sequence < 0
            or not isinstance(terminal, str)
            or not terminal
            or terminal != pane.get("terminal_id")
            or not source.get("cursor_generation")
        ):
            return False
        released = data.get("released_completion_seq")
        if released is not None and status in ("done", "idle", "unknown"):
            if v1_preserve_released_completion(
                source, pane_id, terminal, sequence, released, event_boot
            ):
                pane["status"] = status
                return True
            if data.get("completion_seq") != sequence or released != sequence:
                return False
        retired = source.get("retired_blocked", {}).get(pane_id, {})
        if (
            status == "blocked"
            and retired.get("generation") == source["cursor_generation"]
            and retired.get("terminal_id") == terminal
            and type(retired.get("sequence")) is int
            and sequence <= retired["sequence"]
        ):
            return True
        watermark = current.get("sequence")
        if type(watermark) is int and sequence <= watermark:
            generation = source["cursor_generation"]
            episode = f"{generation}:{terminal}:{sequence}"
            if sequence == watermark and released is None:
                current.pop("released_completion_seq", None)
                if (
                    current.get("status") == "done"
                    and "release_provenance_seq" in current
                    and type(v1_completion_sequence(current)) is int
                    and v1_completion_sequence(current) < watermark
                ):
                    attention[pane_id] = current = {
                        "sequence": watermark,
                        "generation": generation,
                        "terminal_id": terminal,
                    }
                current.pop("release_provenance_seq", None)
            if (
                sequence < watermark
                and status in ("done", "idle")
                and data.get("completion_seq") == sequence
                and current.get("released_completion_seq") == sequence
                and current.get("generation") == generation
                and current.get("terminal_id") == terminal
                and current.get("status") not in ATTENTION_STATUSES
                and current.get("acknowledged") != episode
            ):
                attention[pane_id] = {
                    "status": "done",
                    "episode": episode,
                    "sequence": watermark,
                    "completion_sequence": sequence,
                    "release_provenance_seq": sequence,
                    "generation": generation,
                    "terminal_id": terminal,
                    "server_boot_id": event_boot,
                }
                return True
            if (
                sequence == watermark
                and status in ("idle", "done")
                and previous == "idle"
                and type(data.get("completion_seq")) is int
                and data["completion_seq"] == sequence
                and pane.get("completion_seq") == sequence
                and current.get("status") not in ATTENTION_STATUSES
                and current.get("acknowledged") != episode
            ):
                attention[pane_id] = {
                    "status": "done",
                    "episode": episode,
                    "sequence": sequence,
                    "generation": generation,
                    "terminal_id": terminal,
                    "server_boot_id": event_boot,
                }
            if (
                sequence == watermark
                and previous in ("done", "idle")
                and status in ("done", "idle")
            ):
                pane["status"] = status
            return True
        current.pop("released_completion_seq", None)
        if (
            released is None
            and current.get("status") == "done"
            and "release_provenance_seq" in current
            and type(v1_completion_sequence(current)) is int
            and type(watermark) is int
            and v1_completion_sequence(current) < watermark
        ):
            attention[pane_id] = {
                "sequence": watermark,
                "generation": source["cursor_generation"],
                "terminal_id": terminal,
            }
        elif released is None:
            current.pop("release_provenance_seq", None)
        generation = source["cursor_generation"]
        episode = f"{generation}:{terminal}:{sequence}"
        effective_status = (
            "done"
            if status == "idle"
            and type(data.get("completion_seq")) is int
            and data["completion_seq"] == sequence
            else status
        )
        if status == "idle" and effective_status == "idle":
            attention.pop(pane_id, None)
        apply_attention_status(
            attention, pane_id, effective_status, episode, sequence=sequence
        )
        entry = attention.setdefault(pane_id, {})
        entry.update(generation=generation, terminal_id=terminal)
        entry.pop("provisional", None)
        entry.pop("predicted_sequence", None)
        if effective_status == "done":
            entry["server_boot_id"] = event_boot
        source["event_cursor"] = max(source.get("event_cursor", 0), sequence)
        pane["status"] = status
        if data.get("display_agent") or data.get("agent"):
            pane["agent"] = data.get("display_agent") or data.get("agent")
        if data.get("title"):
            pane["title"] = data["title"]
        return True
    episode = ""
    if status in ATTENTION_STATUSES:
        same_episode = status == previous or (
            status == "done" and previous == "idle" and bool(current)
        )
        episode = (
            current.get("episode") or current.get("acknowledged")
            if same_episode
            else None
        ) or new_v1_episode_id()
    apply_attention_status(attention, pane_id, status, episode)
    entry = attention.setdefault(pane_id, {})
    if source.get("cursor_generation") is not None:
        entry.setdefault("generation", source["cursor_generation"])
    if pane.get("terminal_id"):
        entry.setdefault("terminal_id", pane["terminal_id"])
    effective_previous = "idle" if previous in ("idle", "done") else previous
    effective_status = "idle" if status in ("idle", "done") else status
    if effective_previous is not None and effective_previous != effective_status:
        entry = attention.setdefault(pane_id, {})
        entry["observed_changes"] = entry.get("observed_changes", 0) + 1
        source["event_cursor"] = source.get("event_cursor", 0) + 1
        if status in ATTENTION_STATUSES:
            entry["predicted_sequence"] = source["event_cursor"]
    if status in ATTENTION_STATUSES and not same_episode:
        attention[pane_id]["provisional"] = True
    if status == "done" and event_boot and source.get("server_boot_id") == event_boot:
        attention[pane_id]["server_boot_id"] = event_boot
    pane["status"] = status
    if data.get("display_agent") or data.get("agent"):
        pane["agent"] = data.get("display_agent") or data.get("agent")
    if data.get("title"):
        pane["title"] = data["title"]
    return True


def apply_v1_ack_event(state, source_id, data):
    """Apply a pane metadata acknowledgment only to its matching completion."""
    source = state.get("v1", {}).get("sources", {}).get(source_id)
    pane = data.get("pane") or {}
    token = (pane.get("tokens") or {}).get("cmux_herdr_ack")
    if not source or not isinstance(token, str) or not token:
        return False
    entry = source.get("attention", {}).get(pane.get("pane_id"), {})
    released = entry.get("released_completion_seq")
    if (
        type(released) is int
        and entry.get("generation") == source.get("cursor_generation")
        and entry.get("terminal_id") == pane.get("terminal_id")
        and token == f"{entry['generation']}:{entry['terminal_id']}:{released}"
        and entry.get("status") not in ATTENTION_STATUSES
    ):
        entry["acknowledged"] = token
        return True
    return force_acknowledge_attention(
        source.get("attention", {}), pane.get("pane_id"), token
    )


def v1_frontmost_app():
    """Read the macOS frontmost application without changing app focus."""
    if sys.platform != "darwin":
        return None, None
    try:
        objc = ctypes.CDLL("/usr/lib/libobjc.A.dylib")
        ctypes.CDLL("/System/Library/Frameworks/AppKit.framework/AppKit")
        objc.objc_getClass.argtypes = [ctypes.c_char_p]
        objc.objc_getClass.restype = ctypes.c_void_p
        objc.sel_registerName.argtypes = [ctypes.c_char_p]
        objc.sel_registerName.restype = ctypes.c_void_p
        send = ctypes.CFUNCTYPE(ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p)(
            ("objc_msgSend", objc)
        )
        send_string = ctypes.CFUNCTYPE(
            ctypes.c_char_p, ctypes.c_void_p, ctypes.c_void_p
        )(("objc_msgSend", objc))
        send_int = ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)(
            ("objc_msgSend", objc)
        )

        def selector(name):
            return objc.sel_registerName(name.encode())

        pool = send(objc.objc_getClass(b"NSAutoreleasePool"), selector("new"))
        try:
            workspace = send(
                objc.objc_getClass(b"NSWorkspace"), selector("sharedWorkspace")
            )
            app = send(workspace, selector("frontmostApplication"))
            bundle = send(app, selector("bundleIdentifier"))
            result = send_string(bundle, selector("UTF8String")) if bundle else None
            pid = send_int(app, selector("processIdentifier")) if app else None
            return result.decode("utf-8") if result else None, pid
        finally:
            if pool:
                send(pool, selector("drain"))
    except (OSError, AttributeError, UnicodeDecodeError, TypeError, ValueError):
        return None, None


def valid_v1_client_envelope(response, pid):
    return isinstance(response, dict) and not (
        type(response.get("version")) is not int
        or response["version"] != 1
        or type(response.get("pid")) is not int
        or response["pid"] != pid
        or response.get("status") != "ok"
        or not isinstance(response.get("instance"), str)
        or not response["instance"]
    )


def valid_v1_client_observation(response, pid):
    """Reject incomplete or incompatible focus evidence."""
    if not valid_v1_client_envelope(response, pid):
        return False
    observation = response.get("observation")
    if not isinstance(observation, dict):
        return False
    if any(
        not isinstance(observation.get(key), str) or not observation[key]
        for key in ("endpoint", "pane_id", "boot_id")
    ):
        return False
    if not re.fullmatch(r"[0-9a-f]{32}", observation["boot_id"]):
        return False
    if any(
        type(observation.get(key)) is not int or observation[key] < 0
        for key in ("snapshot_revision", "connection_generation")
    ):
        return False
    agent_sequence = observation.get("agent_state_change_seq")
    if "agent_state_change_seq" not in observation or (
        agent_sequence is not None
        and (type(agent_sequence) is not int or agent_sequence < 0)
    ):
        return False
    outer = observation.get("outer_focused")
    return "outer_focused" in observation and (outer is None or type(outer) is bool)


def valid_v1_source_inventory(response, pid):
    """Validate source identity independently of the current input target."""
    if not valid_v1_client_envelope(response, pid):
        return False
    sources = response.get("sources")
    if type(response.get("sources_complete")) is not bool or not isinstance(
        sources, list
    ):
        return False
    if len(sources) > 65:
        return False
    seen = set()
    for source in sources:
        if not isinstance(source, dict):
            return False
        endpoint, boot = source.get("endpoint"), source.get("boot_id")
        if (
            not isinstance(endpoint, str)
            or not re.fullmatch(r"local|ssh:[0-9a-f]{32}", endpoint)
            or endpoint in seen
            or not isinstance(boot, str)
            or not re.fullmatch(r"[0-9a-f]{32}", boot)
            or type(source.get("connection_generation")) is not int
            or source["connection_generation"] < 0
        ):
            return False
        seen.add(endpoint)
        if "api_socket_path" in source:
            path = source.get("api_socket_path")
            if (
                endpoint != "local"
                or not isinstance(path, str)
                or not os.path.isabs(path)
                or "\x00" in path
            ):
                return False
            if "ssh_target" in source or "session" in source:
                return False
        else:
            target, session = source.get("ssh_target"), source.get("session")
            if (
                not isinstance(target, str)
                or not target
                or target.startswith("-")
                or "\x00" in target
                or not isinstance(session, str)
                or not session
                or "\x00" in session
                or "api_socket_path" in source
            ):
                return False
    return True


def v1_read_client_response(path, expected_peer=None, *, timeout=2):
    """Read one bounded response and optionally verify its socket peer PID."""

    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate observer response key")
            result[key] = value
        return result

    try:
        deadline = time.monotonic() + timeout
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
            conn.settimeout(timeout)
            conn.connect(str(path))
            if expected_peer is not None and (
                sys.platform != "darwin" or conn.getsockopt(0, 2) != expected_peer
            ):
                return None
            data = b""
            while b"\n" not in data and len(data) <= 65536:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                conn.settimeout(remaining)
                chunk = conn.recv(min(4096, 65537 - len(data)))
                if not chunk:
                    return None
                data += chunk
            if b"\n" not in data or len(data) > 65536:
                return None
            response = json.loads(
                data.split(b"\n", 1)[0], object_pairs_hook=unique_object
            )
        return response if isinstance(response, dict) else None
    except (OSError, ValueError, TypeError, RecursionError):
        return None


def v1_client_observation(path, pid):
    response = v1_read_client_response(path, expected_peer=pid)
    return response if valid_v1_client_observation(response, pid) else None


def v1_client_source_inventory(path, pid, *, timeout=2):
    response = v1_read_client_response(path, expected_peer=pid, timeout=timeout)
    return response if valid_v1_source_inventory(response, pid) else None


def v1_tty_device(tty):
    if not isinstance(tty, str) or not tty.strip() or tty.strip() == "not a tty":
        return None
    tty = tty.strip()
    path = tty if tty.startswith("/dev/") else f"/dev/{tty}"
    try:
        info = os.stat(path)
        return info.st_rdev if stat.S_ISCHR(info.st_mode) else None
    except OSError:
        return None


def v1_native_process_info(pid):
    """Read a live Darwin process incarnation and its kernel relationships."""
    if sys.platform != "darwin" or type(pid) is not int or not 0 < pid < 2**31:
        return None

    class ProcBsdInfo(ctypes.Structure):
        _fields_ = [
            ("flags", ctypes.c_uint32),
            ("status", ctypes.c_uint32),
            ("xstatus", ctypes.c_uint32),
            ("pid", ctypes.c_uint32),
            ("ppid", ctypes.c_uint32),
            ("uid", ctypes.c_uint32),
            ("gid", ctypes.c_uint32),
            ("ruid", ctypes.c_uint32),
            ("rgid", ctypes.c_uint32),
            ("svuid", ctypes.c_uint32),
            ("svgid", ctypes.c_uint32),
            ("reserved", ctypes.c_uint32),
            ("comm", ctypes.c_char * 16),
            ("name", ctypes.c_char * 32),
            ("nfiles", ctypes.c_uint32),
            ("pgid", ctypes.c_uint32),
            ("pjobc", ctypes.c_uint32),
            ("tty_device", ctypes.c_uint32),
            ("tty_pgid", ctypes.c_uint32),
            ("nice", ctypes.c_int32),
            ("start_sec", ctypes.c_uint64),
            ("start_usec", ctypes.c_uint64),
        ]

    try:
        lib = ctypes.CDLL("/usr/lib/libSystem.B.dylib", use_errno=True)
        proc_pidinfo = lib.proc_pidinfo
        proc_pidinfo.argtypes = [
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_uint64,
            ctypes.c_void_p,
            ctypes.c_int,
        ]
        proc_pidinfo.restype = ctypes.c_int
        info = ProcBsdInfo()
        size = ctypes.sizeof(info)
        if proc_pidinfo(pid, 3, 0, ctypes.byref(info), size) != size:
            return None
        if info.pid != pid or info.status == 5 or info.start_usec >= 1_000_000:
            return None
        start_token = info.start_sec * 1_000_000 + info.start_usec
        if not 0 < start_token < 2**64:
            return None
        proc_pidpath = lib.proc_pidpath
        proc_pidpath.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_uint32]
        proc_pidpath.restype = ctypes.c_int
        path = ctypes.create_string_buffer(4096)
        if proc_pidpath(pid, path, len(path)) <= 0:
            return None
        executable = os.fsdecode(path.value)
        if not os.path.isabs(executable):
            return None
        return {
            "pid": pid,
            "ppid": info.ppid,
            "pgid": info.pgid,
            "tty_device": info.tty_device,
            "tty_pgid": info.tty_pgid,
            "start_token": start_token,
            "path": executable,
        }
    except (OSError, AttributeError, ValueError, OverflowError):
        return None


def v1_native_transport_envelope(path, pid, *, timeout=2):
    """Read an observer envelope from the same live launcher incarnation.

    Its bridge inventory is untrusted until separately validated.
    """
    before = v1_native_process_info(pid)
    if before is None:
        return None
    response = v1_read_client_response(path, expected_peer=pid, timeout=timeout)
    after = v1_native_process_info(pid)
    if (
        before != after
        or not isinstance(response, dict)
        or type(response.get("version")) is not int
        or response["version"] != 1
        or type(response.get("owner_pid")) is not int
        or response["owner_pid"] != pid
        or type(response.get("owner_start_token")) is not int
        or response["owner_start_token"] != before["start_token"]
        or not isinstance(response.get("instance"), str)
        or not re.fullmatch(r"[0-9a-f]{32}", response["instance"])
        or type(response.get("revision")) is not int
        or not 0 <= response["revision"] < 2**64
    ):
        return None
    return response


def v1_native_transport_claims(response):
    """Validate launcher-scoped claims; callers must verify peer and topology."""

    def uint(value, *, positive=False, maximum=2**64):
        return type(value) is int and int(positive) <= value < maximum

    def identity(value):
        return (
            isinstance(value, dict)
            and uint(value.get("pid"), positive=True, maximum=2**31)
            and uint(value.get("start_token"), positive=True)
        )

    def identifier(value):
        return isinstance(value, str) and bool(value) and "\x00" not in value

    if (
        not isinstance(response, dict)
        or type(response.get("version")) is not int
        or response["version"] != 1
        or response.get("status") != "ok"
        or response.get("ownership_complete") is not True
        or not uint(response.get("owner_pid"), positive=True, maximum=2**31)
        or not uint(response.get("owner_start_token"), positive=True)
        or not isinstance(response.get("instance"), str)
        or not re.fullmatch(r"[0-9a-f]{32}", response["instance"])
        or not uint(response.get("revision"))
        or response["revision"] == 2**64 - 1
    ):
        return None
    bridges, bounds = response.get("bridges"), response.get("bound_observations")
    if (
        not isinstance(bridges, list)
        or not 0 < len(bridges) <= 64
        or not isinstance(bounds, list)
        or len(bridges) != len(bounds)
    ):
        return None
    indexed = {}
    for bound in bounds:
        if (
            not isinstance(bound, dict)
            or not identifier(bound.get("bridge_id"))
            or bound["bridge_id"] in indexed
        ):
            return None
        indexed[bound["bridge_id"]] = bound
    claims, seen_processes, seen_connections = [], {response["owner_pid"]}, set()
    for bridge in bridges:
        if not isinstance(bridge, dict) or not identifier(bridge.get("id")):
            return None
        bound = indexed.pop(bridge["id"], None)
        connection, client, source = (
            bridge.get("connection"),
            bridge.get("client"),
            bridge.get("source"),
        )
        if (
            not isinstance(bound, dict)
            or not isinstance(connection, dict)
            or not isinstance(source, dict)
            or not identifier(source.get("target"))
            or source["target"].startswith("-")
            or not identifier(source.get("session"))
            or not identity(client)
            or not identity(connection.get("consumer"))
            or client != connection["consumer"]
            or not identity(connection.get("ssh_child"))
            or not identifier(connection.get("id"))
            or type(connection.get("peer_identity_verified")) is not bool
            or connection["id"] in seen_connections
            or bound.get("connection_id") != connection["id"]
            or not uint(bound.get("generation"))
            or not isinstance(bound.get("instance"), str)
            or not re.fullmatch(r"[0-9a-f]{32}", bound["instance"])
        ):
            return None
        ssh = connection["ssh_child"]
        if (
            client["pid"] in seen_processes
            or ssh["pid"] in seen_processes
            or ssh["pid"] == client["pid"]
        ):
            return None
        seen_processes.update((client["pid"], ssh["pid"]))
        seen_connections.add(connection["id"])
        nested = bound.get("observation")
        if (
            not valid_v1_source_inventory(nested, client["pid"])
            or nested["instance"] != bound["instance"]
            or nested["sources_complete"] is not True
            or "observation" not in nested
            or not all(
                uint(item["connection_generation"]) for item in nested["sources"]
            )
        ):
            return None
        local = [item for item in nested["sources"] if item["endpoint"] == "local"]
        if (
            len(local) != 1
            or local[0].get("ssh_target") != source.get("target")
            or local[0].get("session") != source.get("session")
            or local[0]["connection_generation"] != bound["generation"]
        ):
            return None
        pane = nested["observation"]
        if pane is not None and (
            not valid_v1_client_observation(nested, client["pid"])
            or not uint(pane["snapshot_revision"])
            or (
                pane["agent_state_change_seq"] is not None
                and not uint(pane["agent_state_change_seq"])
            )
            or not any(
                pane["endpoint"] == item["endpoint"]
                and pane["boot_id"] == item["boot_id"]
                and pane["connection_generation"] == item["connection_generation"]
                for item in nested["sources"]
            )
        ):
            return None
        claims.append(
            {
                "bridge_id": bridge["id"],
                "connection_id": connection["id"],
                "client": client,
                "ssh_child": ssh,
                "source": source,
                "generation": bound["generation"],
                "observation": nested,
            }
        )
    return claims if not indexed else None


def v1_native_transport_topology(response, claims, nodes, tty_device):
    """Check validated claims against a single surface and live kernel identities."""
    try:
        owner_pid = response["owner_pid"]
        owner_node = nodes[owner_pid]
        if type(tty_device) is not int or not isinstance(claims, list) or not claims:
            return None

        def role(node, name):
            path = node.get("path")
            return (
                node.get("name") == name
                or isinstance(path, str)
                and Path(path).name == name
            )

        def process(pid, token, name, parent=None):
            node = nodes.get(pid)
            if (
                not isinstance(node, dict)
                or node.get("pid") != pid
                or not role(node, name)
                or node.get("tty_device") != tty_device
            ):
                return None
            info = v1_native_process_info(pid)
            if (
                not info
                or info.get("pid") != pid
                or info.get("start_token") != token
                or info.get("tty_device") != tty_device
                or not role(info, name)
            ):
                return None
            if node.get("ppid") != info.get("ppid") or (
                parent is not None and info.get("ppid") != parent
            ):
                return None
            return info

        owner = process(owner_pid, response["owner_start_token"], "herdr")
        if owner is None or not isinstance(owner_node.get("children"), list):
            return None
        child_nodes = owner_node["children"]
        if any(not isinstance(node, dict) for node in child_nodes):
            return None
        observations = {owner_pid: owner}
        records = []
        for claim in claims:
            client, ssh = claim["client"], claim["ssh_child"]
            for identity, name in ((client, "herdr"), (ssh, "ssh")):
                pid = identity["pid"]
                matching = [node for node in child_nodes if node.get("pid") == pid]
                if (
                    len(matching) != 1
                    or matching[0] != nodes.get(pid)
                    or pid in observations
                ):
                    return None
                info = process(pid, identity["start_token"], name, owner_pid)
                if info is None:
                    return None
                observations[pid] = info
            nested = claim["observation"]
            records.append(
                {
                    "owner_pid": owner_pid,
                    "owner_start_token": response["owner_start_token"],
                    "owner_instance": response["instance"],
                    "revision": response["revision"],
                    "bridge_id": claim["bridge_id"],
                    "connection_id": claim["connection_id"],
                    "client": client.copy(),
                    "ssh_child": ssh.copy(),
                    "source": claim["source"].copy(),
                    "generation": claim["generation"],
                    "client_instance": nested["instance"],
                    "sources": sorted(
                        nested["sources"], key=lambda item: item["endpoint"]
                    ),
                    "tty_device": tty_device,
                }
            )
        if any(
            v1_native_process_info(pid) != info for pid, info in observations.items()
        ):
            return None
        return records
    except (KeyError, TypeError, AttributeError, ValueError, OSError):
        return None


def v1_foreground_surface_processes(payload, workspace_id, surface_id):
    """Find foreground processes in one unambiguous cmux surface tree."""

    def matches(node, identity):
        return isinstance(identity, str) and any(
            isinstance(node.get(key), str)
            and node[key].casefold() == identity.casefold()
            for key in ("id", "ref")
        )

    try:
        surfaces = [
            surface
            for window in payload["windows"]
            for workspace in window["workspaces"]
            if matches(workspace, workspace_id)
            for pane in workspace["panes"]
            for surface in pane["surfaces"]
            if matches(surface, surface_id)
        ]
        if len(surfaces) != 1 or not isinstance(surfaces[0]["processes"], list):
            return None
        tty_device = v1_tty_device(surfaces[0].get("tty"))
        if tty_device is None:
            return None
        stack = list(surfaces[0]["processes"])
        seen, foreground, foreground_groups = set(), {}, set()
        while stack:
            process = stack.pop()
            if not isinstance(process, dict) or len(seen) >= 16384:
                return None
            pid = process.get("pid")
            if type(pid) is not int or pid <= 0 or pid in seen:
                return None
            seen.add(pid)
            children = process.get("children")
            if not isinstance(children, list):
                return None
            stack.extend(children)
            pgid, tpgid = process.get("pgid"), process.get("tpgid")
            if (
                type(process.get("tty_device")) is int
                and process["tty_device"] == tty_device
                and type(tpgid) is int
                and tpgid > 0
            ):
                foreground_groups.add(tpgid)
            if (
                type(pgid) is int
                and type(tpgid) is int
                and pgid > 0
                and pgid == tpgid
                and type(process.get("tty_device")) is int
                and process["tty_device"] == tty_device
            ):
                foreground[pid] = process
        return foreground if len(foreground_groups) <= 1 else None
    except (KeyError, TypeError, AttributeError):
        return None


def v1_foreground_surface_pids(payload, workspace_id, surface_id):
    processes = v1_foreground_surface_processes(payload, workspace_id, surface_id)
    return set(processes) if processes is not None else None


def v1_observer_socket_exists(path):
    try:
        return stat.S_ISSOCK(path.lstat().st_mode)
    except FileNotFoundError:
        return False


def v1_native_surface_ownership(nodes, directories, tty_device, deadline):
    """Find validated native bridges without exempting unrelated processes."""
    owners, transports, clients, used = set(), set(), {}, set()
    complete = True
    for pid, node in nodes.items():
        if time.monotonic() >= deadline:
            return owners, transports, clients, False
        if type(node.get("tty_device")) is int and node["tty_device"] != tty_device:
            continue
        path = node.get("path")
        if not (
            node.get("name") == "herdr"
            or isinstance(path, str)
            and Path(path).name == "herdr"
        ):
            continue
        sockets = [
            directory.parent / "transport-observers" / f"{pid}.sock"
            for directory in directories
        ]
        sockets = [
            candidate for candidate in sockets if v1_observer_socket_exists(candidate)
        ]
        if not sockets:
            continue
        if len(sockets) != 1:
            complete = False
            continue
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return owners, transports, clients, False
        envelope = v1_native_transport_envelope(
            sockets[0], pid, timeout=min(2, remaining)
        )
        claims = v1_native_transport_claims(envelope)
        records = (
            v1_native_transport_topology(envelope, claims, nodes, tty_device)
            if claims
            else None
        )
        if not records or len(records) != len(claims):
            complete = False
            continue
        claimed = {pid}
        for claim in claims:
            claimed.update((claim["client"]["pid"], claim["ssh_child"]["pid"]))
        if len(claimed) != 1 + 2 * len(claims) or claimed & used:
            complete = False
            continue
        used.update(claimed)
        owners.add(pid)
        for claim, record in zip(claims, records, strict=True):
            client_pid = claim["client"]["pid"]
            transports.add(claim["ssh_child"]["pid"])
            clients[client_pid] = (claim["observation"], record)
    return owners, transports, clients, complete


def v1_discover_clients_in_tree(payload, directories, *, deadline=None):
    """Collect candidates from one tree; recheck them before accepting ownership."""
    clients, seen, surfaces = [], set(), set()
    complete = True
    if deadline is None:
        deadline = time.monotonic() + 20
    try:
        if not isinstance(payload.get("windows"), list):
            return None
        for window in payload["windows"]:
            if time.monotonic() >= deadline:
                return {"clients": clients, "complete": False}
            if not isinstance(window.get("workspaces"), list):
                return None
            for workspace in window["workspaces"]:
                if time.monotonic() >= deadline:
                    return {"clients": clients, "complete": False}
                workspace_id = workspace["id"]
                if not isinstance(workspace_id, str) or not workspace_id:
                    return None
                if not isinstance(workspace.get("panes"), list):
                    return None
                for pane in workspace["panes"]:
                    if time.monotonic() >= deadline:
                        return {"clients": clients, "complete": False}
                    if not isinstance(pane.get("surfaces"), list):
                        return None
                    for surface in pane["surfaces"]:
                        if time.monotonic() >= deadline:
                            return {"clients": clients, "complete": False}
                        if not isinstance(surface.get("type"), str):
                            return None
                        if surface.get("type") != "terminal":
                            if surface["type"] != "browser":
                                return None
                            continue
                        surface_id = surface["id"]
                        if not isinstance(surface_id, str) or not surface_id:
                            return None
                        key = (workspace_id.casefold(), surface_id.casefold())
                        if key in surfaces:
                            return None
                        surfaces.add(key)
                        tty_device = v1_tty_device(surface.get("tty"))
                        if tty_device is None:
                            complete = False
                            continue
                        if not isinstance(surface.get("processes"), list):
                            return None
                        stack = list(surface["processes"])
                        nodes = {}
                        while stack:
                            if time.monotonic() >= deadline:
                                return {"clients": clients, "complete": False}
                            process = stack.pop()
                            if not isinstance(process, dict) or len(seen) >= 16384:
                                return None
                            pid = process.get("pid")
                            children = process.get("children")
                            if (
                                type(pid) is not int
                                or pid <= 0
                                or pid in seen
                                or not isinstance(children, list)
                            ):
                                return None
                            seen.add(pid)
                            stack.extend(children)
                            nodes[pid] = process
                        owners, transports, native_clients, native_complete = (
                            v1_native_surface_ownership(
                                nodes, directories, tty_device, deadline
                            )
                        )
                        complete &= native_complete
                        for pid, process in nodes.items():
                            if time.monotonic() >= deadline:
                                return {"clients": clients, "complete": False}
                            name, path = process.get("name"), process.get("path")
                            if not (isinstance(name, str) and name) and not (
                                isinstance(path, str) and path
                            ):
                                complete = False
                                continue
                            if pid in owners or pid in transports:
                                continue
                            if (
                                name == "ssh"
                                or isinstance(path, str)
                                and Path(path).name == "ssh"
                            ) and (
                                type(process.get("tty_device")) is not int
                                or process["tty_device"] == tty_device
                            ):
                                # A remote shell may hide a Herdr client.
                                complete = False
                            if not (
                                name == "herdr"
                                or isinstance(path, str)
                                and Path(path).name == "herdr"
                            ):
                                continue
                            if type(process.get("tty_device")) is not int:
                                complete = False
                                continue
                            if process["tty_device"] != tty_device:
                                continue
                            if pid in native_clients:
                                inventory, ownership = native_clients[pid]
                                clients.append(
                                    {
                                        "pid": pid,
                                        "workspace": workspace_id,
                                        "surface": surface_id,
                                        "instance": inventory["instance"],
                                        "sources": inventory["sources"],
                                        "sources_complete": inventory[
                                            "sources_complete"
                                        ],
                                        "native_ownership": ownership,
                                    }
                                )
                                continue
                            sockets = [
                                candidate
                                for directory in directories
                                if v1_observer_socket_exists(
                                    candidate := directory / f"{pid}.sock"
                                )
                            ]
                            if len(sockets) != 1:
                                complete = False
                                continue
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                return {"clients": clients, "complete": False}
                            inventory = v1_client_source_inventory(
                                sockets[0], pid, timeout=min(2, remaining)
                            )
                            if not inventory:
                                complete = False
                                continue
                            complete &= inventory["sources_complete"]
                            clients.append(
                                {
                                    "pid": pid,
                                    "workspace": workspace_id,
                                    "surface": surface_id,
                                    "instance": inventory["instance"],
                                    "sources": inventory["sources"],
                                    "sources_complete": inventory["sources_complete"],
                                }
                            )
        return {
            "clients": clients,
            "complete": complete and time.monotonic() < deadline,
        }
    except (KeyError, TypeError, OSError, ValueError, AttributeError):
        return None


def v1_wait_child_state(pid):
    """Inspect an exited child without releasing its process-group identity."""
    options = os.WEXITED | os.WNOHANG | os.WNOWAIT
    if hasattr(os, "waitid"):
        status = os.waitid(os.P_PID, pid, options)
        return (status.si_code, status.si_status) if status else None
    if sys.platform != "darwin":
        raise OSError("non-reaping child status unavailable")

    class DarwinSiginfo(ctypes.Structure):
        _fields_ = [
            ("signo", ctypes.c_int),
            ("error", ctypes.c_int),
            ("code", ctypes.c_int),
            ("pid", ctypes.c_int),
            ("uid", ctypes.c_uint),
            ("status", ctypes.c_int),
            ("remainder", ctypes.c_byte * 128),
        ]

    libc = ctypes.CDLL(None, use_errno=True)
    waitid = libc.waitid
    waitid.argtypes = [
        ctypes.c_int,
        ctypes.c_uint,
        ctypes.POINTER(DarwinSiginfo),
        ctypes.c_int,
    ]
    waitid.restype = ctypes.c_int
    info = DarwinSiginfo()
    if waitid(os.P_PID, pid, ctypes.byref(info), options) != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    return (info.code, info.status) if info.pid else None


def v1_bounded_command_output(cmd, *, max_bytes=32 * 1024 * 1024, timeout=20):
    """Read a command's stdout with a total deadline and memory limit."""
    proc = None
    handlers, pending = {}, []
    try:
        if threading.current_thread() is threading.main_thread():
            blocked = {signal.SIGTERM, signal.SIGINT}
            previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, blocked)
            try:
                for signum in (signal.SIGTERM, signal.SIGINT):
                    handler = signal.getsignal(signum)
                    if callable(handler) or handler == signal.SIG_DFL:
                        handlers[signum] = handler
                        signal.signal(
                            signum, lambda sig, frame: pending.append((sig, frame))
                        )
            finally:
                signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
        if pending:
            return None
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        if pending:
            return None
        deadline = time.monotonic() + timeout
        data = bytearray()
        with selectors.DefaultSelector() as selector:
            selector.register(proc.stdout, selectors.EVENT_READ)
            while True:
                if pending:
                    return None
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                if not selector.select(min(remaining, 0.1)):
                    continue
                chunk = os.read(
                    proc.stdout.fileno(), min(65536, max_bytes + 1 - len(data))
                )
                if not chunk:
                    break
                data.extend(chunk)
                if len(data) > max_bytes:
                    return None
        while True:
            if pending:
                return None
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            status = v1_wait_child_state(proc.pid)
            if status is not None:
                break
            time.sleep(min(0.01, remaining))
        if status != (os.CLD_EXITED, 0):
            return None
        result = data.decode("utf-8")
        return result
    except (OSError, UnicodeDecodeError, subprocess.TimeoutExpired):
        return None
    finally:
        try:
            if proc is not None:
                with suppress(ProcessLookupError, PermissionError):
                    os.killpg(proc.pid, signal.SIGKILL)
                try:
                    proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                if proc.stdout is not None:
                    proc.stdout.close()
        finally:
            restoration_error = None
            if handlers:
                previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set(handlers))
                try:
                    while True:
                        try:
                            for signum, handler in handlers.items():
                                signal.signal(signum, handler)
                            break
                        except (KeyboardInterrupt, SystemExit) as error:
                            restoration_error = restoration_error or error
                finally:
                    try:
                        signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
                    except (KeyboardInterrupt, SystemExit) as error:
                        restoration_error = restoration_error or error
            defaults = [sig for sig, _ in pending if handlers[sig] == signal.SIG_DFL]
            if defaults:
                os.kill(os.getpid(), defaults[0])
            elif restoration_error:
                raise restoration_error
            elif pending:
                signum, frame = pending[0]
                handlers[signum](signum, frame)


def v1_discover_clients(cfg):
    """Bracket client observations with two cmux surface process inventories."""
    binary = cfg.get("cmux_bin") or DEFAULT_CMUX_BIN
    state_home = Path(os.environ.get("XDG_STATE_HOME") or Path.home() / ".local/state")
    directories = [
        state_home / name / "client-observers" for name in ("herdr", "herdr-dev")
    ]
    scans = []
    deadline = time.monotonic() + 20
    try:
        for _ in range(2):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            output = v1_bounded_command_output(
                [
                    binary,
                    "--id-format",
                    "both",
                    "top",
                    "--all",
                    "--processes",
                    "--json",
                ],
                timeout=remaining,
            )
            if output is None:
                return None
            scan = v1_discover_clients_in_tree(
                json.loads(output), directories, deadline=deadline
            )
            if scan is None:
                return None
            scans.append(scan)
        before, after = scans

        def identity(client):
            return (
                client["pid"],
                client["workspace"],
                client["surface"],
                client["instance"],
            )

        index = {}
        for client in before["clients"]:
            if time.monotonic() >= deadline:
                return None
            key = identity(client)
            if key in index:
                return None
            index[key] = client
        stable = []
        for client in after["clients"]:
            if time.monotonic() >= deadline:
                return None
            if index.get(identity(client)) == client:
                stable.append(client)
        complete = (
            before["complete"]
            and after["complete"]
            and len(stable) == len(before["clients"]) == len(after["clients"])
            and time.monotonic() < deadline
        )
        return {"clients": stable, "complete": complete}
    except (OSError, ValueError, TypeError, RecursionError, subprocess.TimeoutExpired):
        return None


def v1_discovered_targets(inventory):
    """Build per-client source obligations from verified discovery results."""
    targets = {}
    for client in inventory["clients"]:
        for source in client["sources"]:
            identity = [
                client["pid"],
                client["instance"],
                client["workspace"],
                client["surface"],
                source["endpoint"],
            ]
            key = hashlib.sha256(
                json.dumps(identity, separators=(",", ":")).encode()
            ).hexdigest()
            target = {"server_boot_id": source["boot_id"]}
            if "api_socket_path" in source:
                target.update(remote=None, socket_path=source["api_socket_path"])
            else:
                target.update(remote=source["ssh_target"], session=source["session"])
            targets[key] = {
                "workspace": client["workspace"],
                "surface": client["surface"],
                "observed_client_pid": client["pid"],
                "observed_client_instance": client["instance"],
                "endpoint": source["endpoint"],
                "connection_generation": source["connection_generation"],
                "target": target,
            }
            if "native_ownership" in client:
                targets[key]["native_ownership"] = client["native_ownership"]
    return targets


def v1_foreground_observation(processes, directories):
    """Require exactly one Herdr client with one working observation socket."""
    if not isinstance(processes, dict):
        return None
    candidates = []
    for pid, process in processes.items():
        name, path = process.get("name"), process.get("path")
        if name == "herdr" or isinstance(path, str) and Path(path).name == "herdr":
            candidates.append(pid)
    if not candidates:
        return None
    try:
        existing = [
            (pid, path)
            for pid in candidates
            for directory in directories
            if v1_observer_socket_exists(path := directory / f"{pid}.sock")
        ]
    except (OSError, TypeError, AttributeError):
        return None
    if len(existing) != 1:
        return None
    pid, path = existing[0]
    ancestors, current = set(), pid
    while current in processes and current not in ancestors:
        ancestors.add(current)
        current = processes[current].get("ppid")
    if not set(candidates).issubset(ancestors):
        return None
    return v1_client_observation(path, pid)


def v1_cmux_identity(cfg, workspace_id, surface_id):
    """Check whether the calling terminal is cmux's focused surface."""
    try:
        if not workspace_id or not surface_id:
            return None
        binary = cfg.get("cmux_bin") or DEFAULT_CMUX_BIN
        proc = run(
            [
                binary,
                "--json",
                "identify",
                "--workspace",
                workspace_id,
                "--surface",
                surface_id,
            ]
        )
        if proc.returncode:
            return None
        result = json.loads(proc.stdout)
        caller, focused = result.get("caller", {}), result.get("focused", {})
        keys = ("window_ref", "workspace_ref", "surface_ref")
        bundle = result.get("bundle_identifier")
        if not bundle or any(not caller.get(key) for key in keys):
            return None
        matches = all(caller.get(key) == focused.get(key) for key in keys)
        return {
            "focused": bool(matches),
            "bundle": bundle,
            "socket_path": result.get("socket_path"),
        }
    except (OSError, subprocess.TimeoutExpired, ValueError, TypeError, AttributeError):
        return None


def v1_cmux_focus(cfg, workspace_id, surface_id):
    identity = v1_cmux_identity(cfg, workspace_id, surface_id)
    return (identity["focused"], identity["bundle"]) if identity else (False, None)


def v1_unix_peer_pid(path):
    """Read the owner PID of a macOS Unix socket connection."""
    if sys.platform != "darwin" or not isinstance(path, str) or not path:
        return None
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
            conn.settimeout(1)
            conn.connect(path)
            pid = conn.getsockopt(0, 2)  # Darwin SOL_LOCAL, LOCAL_PEERPID
            return pid if pid > 0 else None
    except OSError:
        return None


def v1_cmux_terminal_focus(cfg, workspace_id, surface_id):
    """Verify the actual key window and terminal first responder."""
    try:
        binary = cfg.get("cmux_bin") or DEFAULT_CMUX_BIN
        proc = run([binary, "rpc", "debug.terminals", "{}"])
        if proc.returncode:
            return False
        terminals = json.loads(proc.stdout).get("terminals")
        if not isinstance(terminals, list):
            return False

        def matches(node, prefix, identity):
            return isinstance(identity, str) and any(
                isinstance(node.get(f"{prefix}_{key}"), str)
                and node[f"{prefix}_{key}"].casefold() == identity.casefold()
                for key in ("id", "ref")
            )

        candidates = [
            terminal
            for terminal in terminals
            if isinstance(terminal, dict)
            and matches(terminal, "workspace", workspace_id)
            and matches(terminal, "surface", surface_id)
        ]
        return len(candidates) == 1 and all(
            candidates[0].get(key) is True
            for key in (
                "mapped",
                "workspace_selected",
                "window_key",
                "window_visible",
                "surface_view_first_responder",
            )
        )
    except (OSError, ValueError, TypeError, AttributeError, subprocess.TimeoutExpired):
        return False


def v1_cmux_is_foreground(cfg, workspace_id, surface_id):
    identity = v1_cmux_identity(cfg, workspace_id, surface_id)
    if not identity or not identity["focused"]:
        return False
    bundle, pid = v1_frontmost_app()
    return (
        bundle == identity["bundle"]
        and type(pid) is int
        and pid > 0
        and v1_unix_peer_pid(identity.get("socket_path")) == pid
        and v1_cmux_terminal_focus(cfg, workspace_id, surface_id)
    )


def apply_v1_focus_ack(state, source_id, evidence):
    """Acknowledge a verified episode only with unambiguous focus evidence."""
    if not all(
        evidence.get(key) is True
        for key in ("single_client", "foreground", "surface_focused")
    ):
        return False
    pane_id, episode = evidence.get("pane_id"), evidence.get("episode")
    if not isinstance(pane_id, str) or not isinstance(episode, str):
        return False
    source = state.get("v1", {}).get("sources", {}).get(source_id, {})
    return force_acknowledge_attention(source.get("attention", {}), pane_id, episode)


def v1_verified_focus_episode(state, source_id, observation, snapshot):
    """Match a client's input target to a current, exact server completion."""
    if not isinstance(observation, dict) or not isinstance(snapshot, dict):
        return None
    boot = observation.get("boot_id")
    pane_id = observation.get("pane_id")
    if (
        not isinstance(boot, str)
        or not re.fullmatch(r"[0-9a-f]{32}", boot)
        or snapshot.get("server_boot_id") != boot
        or not isinstance(pane_id, str)
        or not pane_id
        or observation.get("outer_focused") is False
    ):
        return None
    agents = snapshot.get("agents")
    if not isinstance(agents, list) or any(not isinstance(a, dict) for a in agents):
        return None
    matches = [agent for agent in agents if agent.get("pane_id") == pane_id]
    if len(matches) > 1:
        return None
    entry = (
        state.get("v1", {})
        .get("sources", {})
        .get(source_id, {})
        .get("attention", {})
        .get(pane_id, {})
    )
    generation = entry.get("generation")
    terminal = entry.get("terminal_id")
    sequence = v1_completion_sequence(entry)
    if (
        entry.get("status") != "done"
        or entry.get("provisional")
        or not isinstance(generation, str)
        or not generation
        or entry.get("server_boot_id") != boot
        or not isinstance(terminal, str)
        or not terminal
        or type(sequence) is not int
        or sequence < 0
    ):
        return None
    if matches:
        agent = matches[0]
        if (
            agent.get("agent_status") not in ("done", "idle")
            or agent.get("terminal_id") != terminal
            or type(agent.get("state_change_seq")) is not int
            or type(observation.get("agent_state_change_seq")) is not int
            or observation["agent_state_change_seq"] != agent["state_change_seq"]
        ):
            return None
    if not matches or matches[0]["state_change_seq"] != sequence:
        panes = snapshot.get("panes")
        presented = observation.get("pane_state")
        if not isinstance(panes, list) or any(not isinstance(p, dict) for p in panes):
            return None
        pane_matches = [pane for pane in panes if pane.get("pane_id") == pane_id]
        if len(pane_matches) != 1 or not isinstance(presented, dict):
            return None
        pane = pane_matches[0]
        release_sequence = pane.get("state_change_seq")
        if (
            (not matches and observation.get("agent_state_change_seq") is not None)
            or presented.get("agent_present") is not bool(matches)
            or pane.get("terminal_id") != terminal
            or presented.get("terminal_id") != terminal
            or type(release_sequence) is not int
            or release_sequence < sequence
            or type(presented.get("released_completion_seq")) is not int
            or presented["released_completion_seq"] != sequence
            or type(presented.get("state_change_seq")) is not int
            or presented["state_change_seq"] != release_sequence
            or (matches and matches[0]["state_change_seq"] != release_sequence)
            or (matches and pane.get("released_completion_seq") != sequence)
        ):
            return None
    episode = f"{generation}:{terminal}:{sequence}"
    return episode if entry.get("episode") == episode else None


def v1_surface_observation(cfg, workspace_id, surface_id):
    """Read the sole foreground Herdr client in the given cmux surface."""
    binary = cfg.get("cmux_bin") or DEFAULT_CMUX_BIN
    try:
        proc = run(
            [binary, "--id-format", "both", "top", "--all", "--processes", "--json"],
            timeout=20,
        )
        if proc.returncode:
            return None
        tree = json.loads(proc.stdout)
        processes = v1_foreground_surface_processes(tree, workspace_id, surface_id)
        state_home = Path(
            os.environ.get("XDG_STATE_HOME") or Path.home() / ".local/state"
        )
        directories = [
            state_home / name / "client-observers" for name in ("herdr", "herdr-dev")
        ]
        observation = v1_foreground_observation(processes, directories)
        if not observation:
            return None

        def matches(node, identity):
            return (
                isinstance(node, dict)
                and isinstance(identity, str)
                and any(
                    isinstance(node.get(key), str)
                    and node[key].casefold() == identity.casefold()
                    for key in ("id", "ref")
                )
            )

        surfaces = [
            surface
            for window in tree["windows"]
            for workspace in window["workspaces"]
            if matches(workspace, workspace_id)
            for pane in workspace["panes"]
            for surface in pane["surfaces"]
            if matches(surface, surface_id)
        ]
        if len(surfaces) != 1:
            return None
        scoped = {
            "windows": [
                {
                    "workspaces": [
                        {
                            "id": workspace_id,
                            "panes": [
                                {"surfaces": [{**surfaces[0], "id": surface_id}]}
                            ],
                        }
                    ]
                }
            ]
        }
        scan = v1_discover_clients_in_tree(scoped, directories)
        if not scan or not scan["complete"]:
            return None
        matches = [
            client
            for client in scan["clients"]
            if client["pid"] == observation.get("pid")
            and client["instance"] == observation.get("instance")
            and client["workspace"].casefold() == workspace_id.casefold()
            and client["surface"].casefold() == surface_id.casefold()
        ]
        if len(matches) != 1:
            return None
        if "native_ownership" in matches[0]:
            observation = {
                **observation,
                "native_ownership": matches[0]["native_ownership"],
            }
        return observation
    except (
        OSError,
        ValueError,
        TypeError,
        KeyError,
        AttributeError,
        RecursionError,
        subprocess.TimeoutExpired,
    ):
        return None


def v1_focus_observations_match(before, after):
    """Compare focused identity while allowing unrelated projection updates."""
    if not isinstance(before, dict) or not isinstance(after, dict):
        return False
    first, second = before.get("observation"), after.get("observation")
    if not isinstance(first, dict) or not isinstance(second, dict):
        return False
    old_revision, new_revision = (
        first.get("snapshot_revision"),
        second.get("snapshot_revision"),
    )
    if (old_revision is not None or new_revision is not None) and (
        type(old_revision) is not int
        or type(new_revision) is not int
        or not 0 <= old_revision <= new_revision < 2**64
    ):
        return False
    first = {key: value for key, value in first.items() if key != "snapshot_revision"}
    second = {key: value for key, value in second.items() if key != "snapshot_revision"}
    return {**before, "observation": first} == {**after, "observation": second}


def v1_reconcile_focus(cfg, client_id, source_id, workspace_id, surface_id, sock_path):
    """Acknowledge a completion only after two matching focus observations."""
    try:
        if not v1_cmux_is_foreground(cfg, workspace_id, surface_id):
            return False
        before = v1_surface_observation(cfg, workspace_id, surface_id)
        if not before:
            return False
        snapshot = socket_rpc(sock_path, "session.snapshot", {}, timeout=3).get(
            "snapshot"
        )
        after = v1_surface_observation(cfg, workspace_id, surface_id)
        if not v1_focus_observations_match(before, after) or not v1_cmux_is_foreground(
            cfg, workspace_id, surface_id
        ):
            return False
        observation = after["observation"]
        with state_lock():
            state = load_state()
            prune_v1_clients(state)
            client = state.get("v1", {}).get("clients", {}).get(client_id, {})
            if any(
                client.get(key) != value
                for key, value in (
                    ("source", source_id),
                    ("workspace", workspace_id),
                    ("surface", surface_id),
                )
            ):
                return False
            if (
                type(client.get("observed_client_pid")) is not int
                or not isinstance(client.get("observed_client_instance"), str)
                or not client["observed_client_instance"]
                or not isinstance(client.get("endpoint"), str)
                or not client["endpoint"]
                or type(client.get("connection_generation")) is not int
                or not isinstance(client.get("server_boot_id"), str)
                or after.get("pid") != client["observed_client_pid"]
                or after.get("instance") != client["observed_client_instance"]
                or observation.get("endpoint") != client["endpoint"]
                or observation.get("connection_generation")
                != client["connection_generation"]
                or observation.get("boot_id") != client["server_boot_id"]
                or after.get("native_ownership") != client.get("native_ownership")
            ):
                return False
            episode = v1_verified_focus_episode(state, source_id, observation, snapshot)
            if not episode:
                return False
            evidence = {
                "single_client": True,
                "foreground": True,
                "surface_focused": True,
                "pane_id": observation["pane_id"],
                "episode": episode,
            }
            if not apply_v1_focus_ack(state, source_id, evidence):
                return False
            save_state(state)
            return True
    except (SocketError, OSError, ValueError, KeyError, TypeError, AttributeError):
        return False


def v1_force_read_action():
    """Send the invoking pane's verified completion ID through Herdr metadata."""
    try:
        context = json.loads(os.environ.get("HERDR_PLUGIN_CONTEXT_JSON", "{}"))
        pane_id = context.get("focused_pane_id")
        sock_path = os.environ.get("HERDR_SOCKET_PATH")
        if not isinstance(pane_id, str) or not pane_id or not sock_path:
            return False
        snapshot = socket_rpc(sock_path, "session.snapshot", {}).get("snapshot") or {}
        boot = snapshot.get("server_boot_id")
        if not isinstance(boot, str) or not re.fullmatch(r"[0-9a-f]{32}", boot):
            return False
        pane = socket_rpc(sock_path, "pane.get", {"pane_id": pane_id}).get("pane", {})
        status = pane.get("agent_status")
        if pane.get("pane_id") != pane_id or not (
            status in ("done", "idle")
            or status == "unknown"
            and pane.get("agent") is None
        ):
            return False
        token = (pane.get("tokens") or {}).get("cmux_herdr_done")
        if not isinstance(token, str) or not token:
            return False
        socket_rpc(
            sock_path,
            "pane.report_metadata_if_boot",
            {
                "pane_id": pane_id,
                "expected_server_boot_id": boot,
                "source": "cmux-herdr.v1",
                "tokens": {"cmux_herdr_ack": token},
                "ttl_ms": 30000,
            },
        )
        return True
    except (SocketError, OSError, ValueError, TypeError, AttributeError) as e:
        log(f"v1 force read failed: {e}")
        return False


class V1SnapshotRace(ValueError):
    """A transient release raced with snapshot collection."""


def v1_preserve_stable_snapshot_releases(
    source, generation, boot, before, panes, after
):
    """Preserve release provenance only when all snapshots agree on the same pane."""
    same_source = (
        source.get("cursor_generation") == generation
        and source.get("server_boot_id") == boot
    )
    collections = (before.get("panes"), panes, after.get("panes"))
    indexed = []
    for items in collections:
        if not isinstance(items, list):
            return {}
        mapping = {}
        for pane in items:
            if not isinstance(pane, dict) or not isinstance(pane.get("pane_id"), str):
                return {}
            if pane["pane_id"] in mapping:
                return {}
            mapping[pane["pane_id"]] = pane
        indexed.append(mapping)
    for pane_id in set().union(*(mapping.keys() for mapping in indexed)):
        records = [mapping.get(pane_id) for mapping in indexed]
        if any(
            isinstance(record, dict)
            and record.get("released_completion_seq") is not None
            for record in records
        ):
            identity = ("terminal_id", "state_change_seq", "released_completion_seq")
            if any(not isinstance(record, dict) for record in records) or any(
                any(record.get(key) != records[0].get(key) for key in identity)
                for record in records[1:]
            ):
                raise V1SnapshotRace("Herdr release changed during snapshot")
    releases = {}
    for pane_id, pane in indexed[1].items():
        records = [mapping.get(pane_id) for mapping in indexed]
        if any(not isinstance(record, dict) for record in records):
            continue
        terminal = pane.get("terminal_id")
        sequence = pane.get("state_change_seq")
        released = pane.get("released_completion_seq")
        if (
            not isinstance(terminal, str)
            or not terminal
            or type(sequence) is not int
            or type(released) is not int
            or not 0 <= released <= sequence <= 0xFFFFFFFFFFFFFFFF
        ):
            continue
        if any(
            record.get("terminal_id") != terminal
            or record.get("state_change_seq") != sequence
            or record.get("released_completion_seq") != released
            or record.get("agent_status") not in ("done", "idle", "unknown")
            for record in records
        ):
            continue
        preserved = same_source and v1_preserve_released_completion(
            source, pane_id, terminal, sequence, released, boot
        )
        releases[pane_id] = (terminal, sequence, released, preserved)
    return releases


def refresh_v1_source(
    state,
    source_id,
    generation,
    socket_path,
    *,
    reconnect=False,
    expected_boot=None,
    retry_snapshot_race=False,
):
    """Refresh only after both Herdr snapshots have been obtained successfully."""
    try:
        before = socket_rpc(socket_path, "session.snapshot", {}).get("snapshot") or {}
        agents = socket_rpc(socket_path, "agent.list", {}).get("agents")
        panes = socket_rpc(socket_path, "pane.list", {}).get("panes")
        after = socket_rpc(socket_path, "session.snapshot", {}).get("snapshot") or {}
        before_boot, after_boot = (
            before.get("server_boot_id"),
            after.get("server_boot_id"),
        )
        if before_boot and after_boot and before_boot != after_boot:
            raise ValueError("Herdr server changed during snapshot")
        if not isinstance(agents, list) or not isinstance(panes, list):
            raise ValueError("invalid Herdr snapshot")
        if any(not isinstance(agent, dict) for agent in agents) or any(
            not isinstance(pane, dict) or not isinstance(pane.get("pane_id"), str)
            for pane in panes
        ):
            raise ValueError("invalid Herdr snapshot entry")
        pane_ids = [pane["pane_id"] for pane in panes]
        candidate = copy.deepcopy(state)
        verified_boot = (
            isinstance(before_boot, str)
            and re.fullmatch(r"[0-9a-f]{32}", before_boot)
            and before_boot == after_boot
        )
        if expected_boot is not None and (
            not verified_boot or before_boot != expected_boot
        ):
            raise ValueError("Herdr snapshot does not match expected server")
        previous_source = state.get("v1", {}).get("sources", {}).get(source_id, {})
        if (
            previous_source.get("server_boot_id")
            and previous_source.get("socket_generation") == generation
            and not verified_boot
        ):
            raise ValueError("Herdr server identity unavailable")
        episode_generation = (
            json.dumps([generation, before_boot], separators=(",", ":"))
            if verified_boot
            else generation
        )
        if verified_boot:
            source = candidate.get("v1", {}).get("sources", {}).get(source_id, {})
            releases = v1_preserve_stable_snapshot_releases(
                source, episode_generation, before_boot, before, panes, after
            )
            adjusted = []
            for agent in agents:
                release = releases.get(agent.get("pane_id"))
                if (
                    release
                    and not release[3]
                    and agent.get("terminal_id") == release[0]
                    and agent.get("state_change_seq") == release[1]
                    and agent.get("agent_status") == "done"
                    and agent.get("completion_seq") != release[1]
                ):
                    entry = source.get("attention", {}).get(agent["pane_id"], {})
                    if (
                        entry.get("generation") == episode_generation
                        and entry.get("terminal_id") == release[0]
                        and type(entry.get("sequence")) is int
                        and entry["sequence"] <= release[1]
                    ):
                        source["attention"].pop(agent["pane_id"], None)
                    agent = {**agent, "agent_status": "idle"}
                adjusted.append(agent)
            agents = adjusted
        apply_v1_snapshot(
            candidate,
            source_id,
            episode_generation,
            agents,
            pane_ids,
            panes,
            reconnect=reconnect,
            boot_id=before_boot if verified_boot else None,
        )
        if verified_boot:
            v1_reconcile_absent_agents(
                candidate, source_id, episode_generation, before, agents, panes, after
            )
            source = candidate["v1"]["sources"][source_id]
            for pane in panes:
                pane_id = pane["pane_id"]
                entry = source.get("attention", {}).get(pane_id, {})
                if (
                    "released_completion_seq" not in entry
                    and "release_provenance_seq" not in entry
                    or pane_id in releases
                ):
                    continue
                records = [
                    [
                        item
                        for item in snapshot.get("panes", [])
                        if isinstance(item, dict) and item.get("pane_id") == pane_id
                    ]
                    for snapshot in (before, after)
                ]
                sequence = pane.get("state_change_seq")
                terminal = pane.get("terminal_id")
                if (
                    entry.get("generation") == episode_generation
                    and entry.get("terminal_id") == terminal
                    and type(entry.get("sequence")) is int
                    and type(sequence) is int
                    and entry["sequence"] <= sequence <= 0xFFFFFFFFFFFFFFFF
                    and pane.get("released_completion_seq") is None
                    and all(
                        len(matches) == 1
                        and matches[0].get("terminal_id") == terminal
                        and matches[0].get("state_change_seq") == sequence
                        and matches[0].get("released_completion_seq") is None
                        for matches in records
                    )
                ):
                    entry.pop("released_completion_seq", None)
                    entry.pop("release_provenance_seq", None)
                    entry["sequence"] = sequence
                    if (
                        entry.get("status") == "done"
                        and type(v1_completion_sequence(entry)) is int
                        and v1_completion_sequence(entry) < sequence
                    ):
                        source["attention"][pane_id] = {
                            "sequence": sequence,
                            "generation": episode_generation,
                            "terminal_id": terminal,
                        }
            for pane_id, (terminal, sequence, released, preserved) in releases.items():
                entry = source.get("attention", {}).get(pane_id, {})
                tracked = source.get("panes", {}).get(pane_id, {})
                if (
                    not preserved
                    and not entry
                    and tracked.get("terminal_id") == terminal
                ):
                    entry = source.setdefault("attention", {}).setdefault(
                        pane_id,
                        {
                            "generation": episode_generation,
                            "terminal_id": terminal,
                            "sequence": sequence,
                        },
                    )
                if (
                    not preserved
                    and entry.get("status") == "blocked"
                    and entry.get("generation") == episode_generation
                    and entry.get("terminal_id") == terminal
                    and type(entry.get("sequence")) is int
                    and entry["sequence"] <= sequence
                ):
                    entry = {
                        "generation": episode_generation,
                        "terminal_id": terminal,
                        "sequence": sequence,
                    }
                    source["attention"][pane_id] = entry
                if (
                    not preserved
                    and entry.get("generation") == episode_generation
                    and entry.get("terminal_id") == terminal
                    and type(entry.get("sequence")) is int
                    and entry["sequence"] <= sequence
                    and entry.get("status") not in ATTENTION_STATUSES
                ):
                    entry["sequence"] = sequence
                    entry["released_completion_seq"] = released
                    entry["release_provenance_seq"] = released
                    pane = next((p for p in panes if p.get("pane_id") == pane_id), {})
                    apply_v1_ack_event(candidate, source_id, {"pane": pane})
        if verified_boot:
            candidate["v1"]["sources"][source_id]["server_boot_id"] = before_boot
        else:
            candidate["v1"]["sources"][source_id].pop("server_boot_id", None)
        candidate["v1"]["sources"][source_id]["socket_generation"] = generation
        if verified_boot and isinstance(after.get("agents"), list):
            source = candidate["v1"]["sources"][source_id]
            snapshot_agents = after["agents"]
            for agent in agents:
                pane_id = agent.get("pane_id")
                entry = source.get("attention", {}).get(pane_id, {})
                matches = [
                    current
                    for current in snapshot_agents
                    if isinstance(current, dict) and current.get("pane_id") == pane_id
                ]
                if (
                    len(matches) == 1
                    and (
                        agent.get("agent_status") == "done"
                        and (
                            matches[0].get("agent_status") == "done"
                            or matches[0].get("agent_status") == "idle"
                            and matches[0].get("completion_seq")
                            == matches[0].get("state_change_seq")
                        )
                        or agent.get("agent_status") == "idle"
                        and matches[0].get("agent_status") == "idle"
                        and (
                            entry.get("server_boot_id") == before_boot
                            or agent.get("completion_seq")
                            == agent.get("state_change_seq")
                            and matches[0].get("completion_seq")
                            == matches[0].get("state_change_seq")
                        )
                    )
                    and agent.get("terminal_id") == matches[0].get("terminal_id")
                    and agent.get("state_change_seq")
                    == matches[0].get("state_change_seq")
                    and entry.get("status") == "done"
                    and entry.get("terminal_id") == agent.get("terminal_id")
                    and entry.get("sequence") == agent.get("state_change_seq")
                    and not entry.get("provisional")
                ):
                    entry["server_boot_id"] = before_boot
        state.clear()
        state.update(candidate)
        return True
    except V1SnapshotRace:
        if retry_snapshot_race:
            raise
        return False
    except (SocketError, OSError, ValueError, KeyError, TypeError, AttributeError) as e:
        log(f"v1 source refresh failed: {e}")
        return False


def sync_v1_source(
    source_id,
    generation,
    socket_path,
    *,
    reset_predictions=False,
    expected_boot=None,
    retry_snapshot_race=False,
):
    """Commit a remote snapshot only if its source state has not changed."""
    with state_lock():
        state = load_state()
        baseline = copy.deepcopy(state.get("v1", {}).get("sources", {}).get(source_id))
    if reset_predictions:
        source = state.get("v1", {}).get("sources", {}).get(source_id, {})
        for entry in source.get("attention", {}).values():
            entry.pop("provisional", None)
            entry.pop("predicted_sequence", None)
            entry.pop("observed_changes", None)
    if not refresh_v1_source(
        state,
        source_id,
        generation,
        socket_path,
        reconnect=reset_predictions,
        expected_boot=expected_boot,
        retry_snapshot_race=retry_snapshot_race,
    ):
        return False
    updated = state.get("v1", {}).get("sources", {}).get(source_id)
    if updated is None:
        return False
    with state_lock():
        current = load_state()
        current_source = current.get("v1", {}).get("sources", {}).get(source_id)

        def snapshot_state(source):
            if not isinstance(source, dict):
                return source
            return {
                key: value for key, value in source.items() if key != "published_tokens"
            }

        if snapshot_state(current_source) != snapshot_state(baseline):
            if retry_snapshot_race:
                raise V1SnapshotRace("source changed during remote snapshot")
            return False
        if (
            isinstance(baseline, dict)
            and isinstance(current_source, dict)
            and all(
                updated.get(key) == baseline.get(key)
                for key in ("cursor_generation", "socket_generation", "server_boot_id")
            )
        ):
            if "published_tokens" in current_source:
                updated["published_tokens"] = current_source["published_tokens"]
            else:
                updated.pop("published_tokens", None)
        current.setdefault("v1", {}).setdefault("sources", {})[source_id] = updated
        save_state(current)
    return True


def persist_v1_status_event(source_id, data, event_boot=None):
    """Persist one ordered status event without overwriting another source."""
    with state_lock():
        state = load_state()
        if not apply_v1_status_event(state, source_id, data, event_boot):
            return False
        save_state(state)
        return True


def persist_v1_ack_event(source_id, data, event_boot=None):
    with state_lock():
        state = load_state()
        source = state.get("v1", {}).get("sources", {}).get(source_id, {})
        if source.get("server_boot_id") != event_boot and (
            source.get("server_boot_id") is not None or event_boot is not None
        ):
            return False
        if not apply_v1_ack_event(state, source_id, data):
            return False
        save_state(state)
        return True


def sync_v1_episode_tokens(source_id, socket_path, *, expected_boot=None):
    """Expose only verified pending completions to a Herdr action."""

    def desired_token(source, pane_id):
        entry = source.get("attention", {}).get(pane_id, {})
        return (
            entry.get("episode")
            if entry.get("status") == "done" and not entry.get("provisional")
            else None
        )

    with v1_source_leader("episode-token:" + source_id) as leader:
        if not leader:
            return
        with state_lock():
            state = load_state()
            source = state.get("v1", {}).get("sources", {}).get(source_id)
            if (
                not source
                or expected_boot is not None
                and source.get("server_boot_id") != expected_boot
            ):
                return
            if expected_boot is None:
                expected_boot = source.get("server_boot_id")
            pane_ids = sorted(
                set(source.get("attention", {}))
                | set(source.get("published_tokens", {}))
            )
        for pane_id in pane_ids:
            with state_lock():
                state = load_state()
                source = state.get("v1", {}).get("sources", {}).get(source_id)
                if not source or source.get("server_boot_id") != expected_boot:
                    return
                published = source.setdefault("published_tokens", {})
                if pane_id not in source.get("panes", {}):
                    if pane_id in published:
                        published.pop(pane_id)
                        save_state(state)
                    continue
                desired = desired_token(source, pane_id)
                if published.get(pane_id) == desired:
                    continue
                generation = source.get("socket_generation")
                pending = {"pending": True, "id": uuid.uuid4().hex}
                published[pane_id] = pending
                save_state(state)
            try:
                params = {
                    "pane_id": pane_id,
                    "source": "cmux-herdr.v1",
                    "tokens": {"cmux_herdr_done": desired},
                }
                if expected_boot is not None:
                    params["expected_server_boot_id"] = expected_boot
                socket_rpc(
                    socket_path,
                    "pane.report_metadata_if_boot"
                    if expected_boot is not None
                    else "pane.report_metadata",
                    params,
                )
            except (SocketError, OSError, ValueError) as e:
                log(f"v1 episode token update failed: {e}")
                continue
            with state_lock():
                state = load_state()
                source = state.get("v1", {}).get("sources", {}).get(source_id)
                if (
                    not source
                    or source.get("server_boot_id") != expected_boot
                    or source.get("socket_generation") != generation
                ):
                    continue
                published = source.get("published_tokens", {})
                if (
                    published.get(pane_id) != pending
                    or pane_id not in source.get("panes", {})
                    or desired_token(source, pane_id) != desired
                ):
                    continue
                if desired is None:
                    published.pop(pane_id, None)
                else:
                    published[pane_id] = desired
                save_state(state)


def sidebar_panes(panes, attention):
    """Combine live pane state with pending attention."""
    result = {}
    for pane_id, pane in panes.items():
        status = pane.get("status")
        if status == "done" and not has_pending_attention(attention, pane_id):
            continue
        if status not in TRACKED_STATUSES:
            pending_status = attention.get(pane_id, {}).get("status")
            if pending_status not in ATTENTION_STATUSES:
                continue
            status = pending_status
        result[pane_id] = {**pane, "status": status}
    return result


def reconcile_aggregate_notification(
    cfg, workspace_id, surface_id, attention, notifications, notification_id=None
):
    """Reconcile one surface without knowingly replacing another source's unread."""
    pending = [
        entry
        for entry in attention.values()
        if entry.get("status") in ATTENTION_STATUSES
    ]
    relevant = [
        item
        for item in notifications
        if item.get("workspace_id") == workspace_id
        and item.get("surface_id") == surface_id
    ]
    own = next((item for item in relevant if item.get("id") == notification_id), None)
    if not pending:
        if own and not own.get("is_read"):
            cmux_cli(cfg, ["mark-notification-read", "--id", notification_id])
        return
    if any(
        not item.get("is_read") and item.get("id") != notification_id
        for item in relevant
    ):
        return
    waiting = sum(entry["status"] == "blocked" for entry in pending)
    done = len(pending) - waiting
    parts = [
        f"{count} {label}"
        for count, label in ((waiting, "waiting"), (done, "finished"))
        if count
    ]
    title = "herdr: " + " · ".join(parts)
    if own and not own.get("is_read") and own.get("title") == title:
        return notification_id
    payload = {
        "workspace_id": workspace_id,
        "surface_id": surface_id,
        "title": title,
        "subtitle": NOTIFICATION_SUBTITLE,
        "effects": {
            "markUnread": True,
            "desktop": False,
            "sound": False,
            "command": False,
        },
    }
    binary = cfg.get("cmux_bin") or DEFAULT_CMUX_BIN
    try:
        proc = run(
            [binary, "rpc", "notification.create_for_target", json.dumps(payload)]
        )
        if proc.returncode:
            log(f"cmux notification.create_for_target failed: {proc.stderr.strip()}")
            return None
        return json.loads(proc.stdout).get("id")
    except (OSError, subprocess.TimeoutExpired, ValueError) as e:
        log(f"cmux notification.create_for_target failed: {e}")
        return None


def v1_destination_key(workspace_id, surface_id):
    return json.dumps([workspace_id, surface_id], separators=(",", ":"))


def register_v1_client(
    state, client_id, source_id, workspace_id, surface_id, pid, legacy_target=None
):
    clients = state.setdefault("v1", {}).setdefault("clients", {})
    clients[client_id] = {
        "source": source_id,
        "workspace": workspace_id,
        "surface": surface_id,
        "pid": pid,
    }
    if legacy_target is not None:
        clients[client_id]["legacy_target"] = legacy_target


def unregister_v1_client(state, client_id):
    state.get("v1", {}).get("clients", {}).pop(client_id, None)


def reconcile_v1_supervised_bindings(
    state, desired, ready, *, complete, supervisor_pid
):
    """Apply prepared bindings without retiring unknown or unready sources.

    The caller holds each prepared client's lease and serializes this update
    under the state lock. ``ready`` maps IDs to the exact prepared specification.
    """
    clients = state.setdefault("v1", {}).setdefault("clients", {})
    changed = False
    all_ready = True
    for client_id, spec in desired.items():
        existing = clients.get(client_id)
        if ready.get(client_id) != spec or existing and not existing.get("supervised"):
            all_ready = False
            continue
        record = {
            "source": v1_supervised_source_key(spec["target"]),
            "workspace": spec["workspace"],
            "surface": spec["surface"],
            "pid": supervisor_pid,
            "supervised": True,
            "server_boot_id": spec["target"]["server_boot_id"],
            "connection_generation": spec["connection_generation"],
            "observed_client_pid": spec.get("observed_client_pid"),
            "observed_client_instance": spec.get("observed_client_instance"),
            "endpoint": spec.get("endpoint"),
        }
        if "native_ownership" in spec:
            record["native_ownership"] = spec["native_ownership"]
        target = spec["target"]
        if target.get("remote") and target.get("session"):
            record["legacy_target"] = {
                "remote": target["remote"],
                "session": target["session"],
            }
        elif spec.get("legacy_target"):
            record["legacy_target"] = spec["legacy_target"].copy()
        if existing != record:
            clients[client_id] = record
            changed = True
    if complete and all_ready:
        for client_id, client in list(clients.items()):
            if client.get("supervised") and client_id not in desired:
                del clients[client_id]
                changed = True
    return changed


class V1SupervisedLeases:
    """Hold client leases while committing verified supervisor bindings."""

    def __init__(self):
        self._leases = {}
        self._leader = None

    def acquire_leader(self):
        if self._leader is None:
            leader = v1_source_leader("supervisor")
            if not leader.__enter__():
                return False
            self._leader = leader
        return True

    def reconcile(self, desired, ready, *, complete):
        if not self.acquire_leader():
            return False
        newly_acquired = set()
        available = {}
        committed = False
        try:
            for client_id, spec in desired.items():
                if ready.get(client_id) != spec:
                    continue
                if client_id not in self._leases:
                    lease = v1_source_leader("client:" + client_id)
                    if not lease.__enter__():
                        continue
                    self._leases[client_id] = lease
                    newly_acquired.add(client_id)
                available[client_id] = spec
            with state_lock():
                state = load_state()
                changed = False
                blocked = False
                for client_id, record in state.get("v1", {}).get("clients", {}).items():
                    if not record.get("supervised"):
                        continue
                    if client_id not in self._leases:
                        lease = v1_source_leader("client:" + client_id)
                        if not lease.__enter__():
                            blocked = True
                            continue
                        self._leases[client_id] = lease
                        newly_acquired.add(client_id)
                    if record.get("pid") != os.getpid():
                        record["pid"] = os.getpid()
                        changed = True
                changed |= reconcile_v1_supervised_bindings(
                    state,
                    desired,
                    available,
                    complete=complete and not blocked,
                    supervisor_pid=os.getpid(),
                )
                if changed:
                    save_state(state)
                retained = {
                    key
                    for key, value in state.get("v1", {}).get("clients", {}).items()
                    if value.get("supervised") and value.get("pid") == os.getpid()
                }
                committed = True
            for client_id in set(self._leases) - retained:
                self._leases.pop(client_id).__exit__(None, None, None)
            return changed
        finally:
            if not committed:
                for client_id in newly_acquired:
                    self._leases.pop(client_id).__exit__(None, None, None)

    def close(self):
        for lease in self._leases.values():
            lease.__exit__(None, None, None)
        self._leases.clear()
        if self._leader is not None:
            self._leader.__exit__(None, None, None)
            self._leader = None


class V1SupervisedSourceProcess:
    """Own one source worker and its bounded readiness channel."""

    def __init__(self, source_id, target):
        env = os.environ.copy()
        env["HERDR_PLUGIN_STATE_DIR"] = str(state_dir)
        env["HERDR_PLUGIN_CONFIG_DIR"] = str(config_dir)
        self._process = subprocess.Popen(
            [
                sys.executable,
                os.path.abspath(__file__),
                "supervisor-source",
                source_id,
                json.dumps(target),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=env,
            start_new_session=True,
        )
        self._deadline = time.monotonic() + 20
        self._buffer = b""
        self._ready = False
        self._closed = False
        self._protocol_failed = False

    @property
    def alive(self):
        if self._closed:
            return False
        ready = self.ready
        if getattr(self, "_protocol_failed", False):
            return False
        if not ready and time.monotonic() >= self._deadline:
            return False
        try:
            return v1_wait_child_state(self._process.pid) is None
        except OSError:
            return False

    @property
    def ready(self):
        if self._closed or getattr(self, "_protocol_failed", False):
            return False
        stream = self._process.stdout
        try:
            for _ in range(64):
                if not select.select([stream], [], [], 0)[0]:
                    break
                data = os.read(stream.fileno(), 1024)
                if not data:
                    self._protocol_failed = True
                    return False
                self._buffer += data
                while b"\n" in self._buffer:
                    frame, self._buffer = self._buffer.split(b"\n", 1)
                    if frame == b"ready":
                        self._ready = True
                    elif frame == b"not-ready":
                        if self._ready:
                            self._deadline = time.monotonic() + 20
                        self._ready = False
                    else:
                        self._protocol_failed = True
                        return False
                if len(self._buffer) > 16:
                    self._protocol_failed = True
                    return False
            else:
                self._protocol_failed = True
                return False
            return self._ready
        except OSError:
            self._protocol_failed = True
            return False

    def stop(self):
        if self._closed:
            return
        with suppress(ProcessLookupError, PermissionError):
            os.killpg(self._process.pid, signal.SIGTERM)
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            try:
                if v1_wait_child_state(self._process.pid) is not None:
                    break
            except OSError:
                break
            time.sleep(0.01)
        with suppress(ProcessLookupError, PermissionError):
            os.killpg(self._process.pid, signal.SIGKILL)
        self._process.wait(timeout=2)
        self._process.stdout.close()
        self._closed = True


class V1SupervisedSources:
    """Prepare one live subscription for each distinct discovered source."""

    def __init__(self, start):
        self._start = start
        self._workers = {}

    def reconcile(self, desired, *, complete):
        grouped = {}
        for client_id, spec in desired.items():
            target = spec["target"]
            source_id = v1_supervised_source_key(target)
            grouped.setdefault(source_id, []).append((client_id, spec))

        ready = {}
        all_ready = True
        for source_id, bindings in grouped.items():
            target = bindings[0][1]["target"].copy()
            if not target.get("remote"):
                target["socket_path"] = os.path.realpath(target["socket_path"])
            existing = self._workers.get(source_id)
            if existing and (
                not existing["worker"].alive or existing["target"] != target
            ):
                existing["worker"].stop()
                self._workers.pop(source_id)
                existing = None
            if existing is None:
                try:
                    worker = self._start(source_id, target)
                except OSError as error:
                    log(f"v1 source worker failed to start: {error}")
                    worker = None
                if worker is None:
                    all_ready = False
                    continue
                existing = {"target": target.copy(), "worker": worker}
                self._workers[source_id] = existing
            if not existing["worker"].alive or not existing["worker"].ready:
                all_ready = False
                continue
            ready.update(bindings)
        if complete and all_ready:
            self.retire_obsolete(set(grouped))
        return ready

    def retire_obsolete(self, active):
        for source_id in set(self._workers) - active:
            self._workers[source_id]["worker"].stop()
            self._workers.pop(source_id)

    def prepared_target(self, target):
        """Return the immutable endpoint prepared for this live incarnation."""
        entry = self._workers.get(v1_supervised_source_key(target))
        if not entry or not entry["worker"].alive or not entry["worker"].ready:
            return None
        prepared = entry["target"]
        candidate = target.copy()
        if not candidate.get("remote"):
            candidate["socket_path"] = os.path.realpath(candidate["socket_path"])
        return prepared.copy() if prepared == candidate else None

    def close(self):
        for entry in self._workers.values():
            entry["worker"].stop()
        self._workers.clear()


def v1_supervisor_bindings_match(desired, *, exact=False):
    """Verify all destinations belong to this supervisor and prepared source."""
    with state_lock():
        clients = load_state().get("v1", {}).get("clients", {})
        if exact and {
            key for key, client in clients.items() if client.get("supervised")
        } != set(desired):
            return False
        return all(
            clients.get(key, {}).get("supervised")
            and clients[key].get("pid") == os.getpid()
            and clients[key].get("workspace") == spec["workspace"]
            and clients[key].get("surface") == spec["surface"]
            and clients[key].get("source") == v1_supervised_source_key(spec["target"])
            and clients[key].get("server_boot_id") == spec["target"]["server_boot_id"]
            and clients[key].get("connection_generation")
            == spec["connection_generation"]
            and clients[key].get("observed_client_pid")
            == spec.get("observed_client_pid")
            and clients[key].get("observed_client_instance")
            == spec.get("observed_client_instance")
            and clients[key].get("endpoint") == spec.get("endpoint")
            and clients[key].get("native_ownership") == spec.get("native_ownership")
            for key, spec in desired.items()
        )


def v1_reconcile_supervisor_inventory(inventory, sources, leases):
    """Prepare sources before committing bindings; retire old sources afterward."""
    if inventory is None or not leases.acquire_leader():
        return False
    desired = v1_discovered_targets(inventory)
    ready = sources.reconcile(desired, complete=False)
    changed = leases.reconcile(desired, ready, complete=inventory["complete"])
    if (
        inventory["complete"]
        and len(ready) == len(desired)
        and v1_supervisor_bindings_match(desired)
    ):
        sources.retire_obsolete(
            {v1_supervised_source_key(spec["target"]) for spec in desired.values()}
        )
    return changed


def v1_handoff_has_coverage(
    discovery_complete, desired, ready, legacy_sources, sources
):
    """Require every observed legacy source to have a prepared replacement."""
    if (
        not discovery_complete
        or not desired
        or any(ready.get(key) != spec for key, spec in desired.items())
    ):
        return False
    prepared_sources = set()
    for spec in desired.values():
        target = sources.prepared_target(spec["target"])
        if target is None:
            return False
        prepared_sources.add(v1_source_key(target))
    return legacy_sources is not None and set(legacy_sources).issubset(prepared_sources)


def v1_load_legacy_journal_state():
    try:
        state = json.loads(STATE_PATH.read_text())
    except FileNotFoundError:
        state = {"sessions": {}}
    if not isinstance(state, dict):
        raise ValueError("invalid legacy state")
    return state


def v1_replay_pane_status(sock_path, pane_id):
    """Return the current state only after a validated agent-list response."""
    result = socket_rpc(sock_path, "agent.list", {})
    agents = result["agents"]
    if not isinstance(agents, list) or any(
        not isinstance(agent, dict) for agent in agents
    ):
        raise ValueError("invalid agent list")
    if any(
        agent.get("pane_id") == pane_id
        and not isinstance(agent.get("agent_status"), str)
        for agent in agents
    ):
        raise ValueError("invalid agent status")
    return next(
        (
            agent.get("agent_status")
            for agent in agents
            if agent.get("pane_id") == pane_id
        ),
        None,
    )


def v1_rollback_legacy_events_and_marker():
    """Replay pending hooks before letting new legacy hooks run."""
    marker = state_dir / "v1-enabled"
    with state_lock():
        state = v1_load_legacy_journal_state()
        pending = state.get("v1_pending_legacy_events", [])
        if not isinstance(pending, list):
            raise ValueError("invalid pending legacy event journal")
        barriers = state.setdefault("v1_pending_watch_barriers", {})
        if not isinstance(barriers, dict):
            raise ValueError("invalid pending watcher barriers")
        if pending:
            cfg = load_config_strict()
            if cfg is None:
                return False
            while pending:
                item = pending[0]
                if (
                    not isinstance(item, dict)
                    or not isinstance(item.get("session"), str)
                    or not isinstance(item.get("event"), dict)
                ):
                    raise ValueError("invalid pending legacy event")
                token = LEGACY_REPLAY_CONTEXT.set(
                    {
                        "session": item["session"],
                        "workspace": item.get("workspace"),
                        "socket_path": item.get("herdr_socket_path"),
                    }
                )
                try:
                    if item.get("kind") == "watch":
                        data = item["event"]
                        key = json.dumps(
                            [
                                item["session"],
                                data.get("workspace_id"),
                                data.get("pane_id"),
                            ]
                        )
                        sock_path = item.get("socket_path")
                        if not isinstance(sock_path, str) or not sock_path:
                            return False
                        try:
                            current = v1_replay_pane_status(
                                sock_path, data.get("pane_id")
                            )
                        except (SocketError, OSError, ValueError, KeyError, TypeError):
                            return False
                        barriers[key] = sock_path
                        apply_watch_event(
                            cfg,
                            state,
                            item.get("socket_path"),
                            item["event"],
                            session=item["session"],
                            tracked=current in TRACKED_STATUSES,
                        )
                    else:
                        data = item["event"].get("data", {})
                        key = json.dumps(
                            [
                                item["session"],
                                data.get("workspace_id"),
                                data.get("pane_id"),
                            ]
                        )
                        sock_path = barriers.get(key)
                        if (
                            sock_path
                            and data.get("type") == "pane_agent_status_changed"
                        ):
                            try:
                                current = v1_replay_pane_status(
                                    sock_path, data.get("pane_id")
                                )
                            except (
                                SocketError,
                                OSError,
                                ValueError,
                                KeyError,
                                TypeError,
                            ):
                                return False
                            if current != data.get("agent_status"):
                                pending.pop(0)
                                if not pending:
                                    state.pop("v1_pending_legacy_events", None)
                                    state.pop("v1_pending_watch_barriers", None)
                                save_state(state)
                                continue
                        handle_event(cfg, state, item["event"], session=item["session"])
                finally:
                    LEGACY_REPLAY_CONTEXT.reset(token)
                pending.pop(0)
                if not pending:
                    state.pop("v1_pending_legacy_events", None)
                    state.pop("v1_pending_watch_barriers", None)
                save_state(state)
        else:
            state.pop("v1_pending_watch_barriers", None)
        marker.unlink(missing_ok=True)
        return True


def v1_recover_legacy_handoff():
    """Retry restoration even if a prior recovery already removed the marker."""
    state_dir.mkdir(parents=True, exist_ok=True)
    marker = state_dir / "v1-enabled"
    with open(state_dir / "v1-activation.lock", "w") as activation:
        fcntl.flock(activation, fcntl.LOCK_EX)
        try:
            marker_value = marker.read_text()
        except FileNotFoundError:
            marker_value = None
        if marker_value == "enabled":
            return True
        if marker_value is not None and not v1_rollback_legacy_events_and_marker():
            return False
        plan = pending_v1_legacy_restores()
        if plan:
            restart_legacy_daemons(plan)
            return False
        return True


def v1_try_supervisor_handoff(cfg, sources, stop):
    """Fence legacy writers and commit only after repeated complete coverage."""
    state_dir.mkdir(parents=True, exist_ok=True)
    marker = state_dir / "v1-enabled"
    with open(state_dir / "v1-activation.lock", "w") as activation:
        fcntl.flock(activation, fcntl.LOCK_EX)
        with open(state_dir / "remote-transition.lock", "w") as transition:
            fcntl.flock(transition, fcntl.LOCK_EX)
            try:
                marker_value = marker.read_text()
            except FileNotFoundError:
                marker_value = None
            if marker_value == "enabled":
                return True
            if marker_value is not None:
                if not v1_rollback_legacy_events_and_marker():
                    return False
                restart_legacy_daemons(pending_v1_legacy_restores())
                return False
            inventory = v1_discover_clients(cfg)
            if inventory is None or stop.is_set():
                return False
            desired = v1_discovered_targets(inventory)
            ready = sources.reconcile(desired, complete=False)
            legacy = v1_legacy_source_inventory(cfg)
            if not v1_handoff_has_coverage(
                inventory["complete"], desired, ready, legacy, sources
            ) or not v1_supervisor_bindings_match(desired):
                return False
            restore_plan = pending_v1_legacy_restores()
            committed = False
            try:
                with state_lock():
                    tmp = marker.with_suffix(f".{os.getpid()}.tmp")
                    tmp.write_text("pending")
                    tmp.replace(marker)
                for before_retirement in (True, False):
                    if stop.is_set():
                        return False
                    current = v1_discover_clients(cfg)
                    if current is None or v1_discovered_targets(current) != desired:
                        return False
                    ready = sources.reconcile(desired, complete=False)
                    if (
                        not v1_handoff_has_coverage(
                            current["complete"], desired, ready, legacy, sources
                        )
                        or not v1_supervisor_bindings_match(desired)
                        or not all(
                            v1_probe_source(spec["target"]) for spec in desired.values()
                        )
                    ):
                        return False
                    if before_retirement and not retire_v1_legacy_daemons(restore_plan):
                        return False
                with state_lock():
                    if v1_load_legacy_journal_state().get("v1_pending_legacy_events"):
                        return False
                    tmp = marker.with_suffix(f".{os.getpid()}.tmp")
                    tmp.write_text("enabled")
                    tmp.replace(marker)
                committed = True
                update_v1_legacy_restores(lambda entries: [])
                return True
            finally:
                if not committed and v1_rollback_legacy_events_and_marker():
                    restart_legacy_daemons(pending_v1_legacy_restores())


def write_v1_legacy_remote_inventory(pid, epoch, identities):
    """Publish the daemon's conservative set of active remote identities."""
    path = state_dir / "remote-active-sources.json"
    tmp = path.with_suffix(f".{pid}.tmp")
    tmp.write_text(
        json.dumps({"pid": pid, "epoch": epoch, "sources": sorted(identities)})
    )
    tmp.replace(path)


def read_v1_legacy_remote_inventory(pid):
    try:
        pid_record = read_pidfile(state_dir / "remote.pid")
        epoch = pid_record.get("inventory_epoch")
        if pid_record.get("pid") != pid or not isinstance(epoch, str) or not epoch:
            return None
        data = json.loads((state_dir / "remote-active-sources.json").read_text())
        identities = data["sources"]
        if (
            data["pid"] != pid
            or data["epoch"] != epoch
            or not isinstance(identities, list)
        ):
            return None
        if any(
            not isinstance(item, list)
            or len(item) != 2
            or any(not isinstance(value, str) or not value for value in item)
            for item in identities
        ):
            return None
        return {(target, session) for target, session in identities}
    except (OSError, ValueError, KeyError, TypeError):
        return None


def v1_legacy_remote_next_config(current, watched, epoch):
    """Publish old and candidate sources before adopting a configuration."""
    try:
        with open(state_dir / "remote-transition.lock", "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            return v1_legacy_remote_next_config_locked(current, watched, epoch)
    except OSError as error:
        log(f"remote transition lock failed; retaining configuration: {error}")
        return current


def v1_legacy_remote_next_config_locked(current, watched, epoch):
    """Apply a source transition while holding the handoff barrier."""
    candidate = load_config_strict()
    if candidate is None or remote_configs(candidate) is None:
        candidate = current
    remotes = remote_configs(candidate)
    identities = {(target, session) for target, session, _ in watched.values()}
    identities.update((entry["ssh_target"], entry["session"]) for entry in remotes)
    try:
        write_v1_legacy_remote_inventory(os.getpid(), epoch, identities)
    except OSError as error:
        log(f"remote inventory publication failed; retaining configuration: {error}")
        return current
    return candidate


def v1_remove_confirmed_dead_pidfile_locked(path):
    """Remove a conclusively dead PID while holding its daemon lock."""
    if not path.exists():
        return True
    try:
        raw = path.read_text()
    except FileNotFoundError:
        return True
    info = read_pidfile(path)
    pid = info.get("pid")
    if type(pid) is not int or not 1 < pid <= 0x7FFFFFFF:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        path.unlink(missing_ok=True)
        return True
    except OSError:
        return False
    token = info.get("audit_token")
    if token is not None and v1_daemon_incarnation_exited(pid, token):
        if path.read_text() != raw:
            return False
        path.unlink(missing_ok=True)
        return True
    return False


def v1_remove_confirmed_dead_pidfile(path):
    """Remove only a conclusively dead PID, serialized with daemon startup."""
    with open(state_dir / f"{path.stem}.lock", "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        return v1_remove_confirmed_dead_pidfile_locked(path)


@contextmanager
def v1_daemon_startup_lock(pid_path):
    """Do not hold the global handoff lock while a daemon lock is busy."""
    while True:
        with (
            open(state_dir / "v1-activation.lock", "w") as activation,
            open(state_dir / f"{pid_path.stem}.lock", "w") as daemon_lock,
        ):
            fcntl.flock(activation, fcntl.LOCK_EX)
            try:
                fcntl.flock(daemon_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                pass
            else:
                yield activation
                return
        time.sleep(0.05)


def v1_legacy_source_inventory(cfg):
    """Identify live legacy writers; ambiguous state cannot authorize handoff."""
    sources = set()
    try:
        if load_config_strict() != cfg:
            return None
        remotes = remote_configs(cfg)
        if remotes is None:
            return None
        sources.update(
            v1_source_key({"remote": entry["ssh_target"], "session": entry["session"]})
            for entry in remotes
        )
        running = known_sessions(running_only=True)
        if running is None:
            return None
        with state_lock():
            try:
                legacy_state = json.loads(STATE_PATH.read_text())
            except FileNotFoundError:
                legacy_state = {}
        if not isinstance(legacy_state, dict) or any(
            key in legacy_state for key in ("workspaces", "session_ref")
        ):
            return None
        buckets = legacy_state.get("sessions", {})
        if not isinstance(buckets, dict):
            return None
        if any(
            not isinstance(name, str)
            or not isinstance(bucket, dict)
            or not isinstance(bucket.get("workspaces", {}), dict)
            for name, bucket in buckets.items()
        ):
            return None
        retained = {
            name
            for name, bucket in buckets.items()
            if isinstance(bucket, dict)
            and any(
                entry.get("panes")
                for entry in bucket.get("workspaces", {}).values()
                if isinstance(entry, dict)
            )
        }
        for name in running | retained:
            socket_path = v1_local_socket("" if name == "default" else name)
            if not socket_path:
                return None
            sources.add(v1_source_key({"socket_path": socket_path}))
        for mode, session, *_ in pending_v1_legacy_restores():
            if mode == "remote":
                # The journal identifies a daemon but not its last active targets.
                return None
            socket_path = v1_local_socket(session or "")
            if not socket_path:
                return None
            sources.add(v1_source_key({"socket_path": socket_path}))
        watcher_paths = sorted(state_dir.glob("watch-*.pid"))
        names = known_sessions() if watcher_paths else set()
        if names is None:
            return None
        names = {"" if name == "default" else name for name in names}
        for path in watcher_paths:
            pid = live_daemon_pid(path, "watch")
            if pid is None:
                if v1_remove_confirmed_dead_pidfile(path):
                    continue
                return None
            info = read_pidfile(path)
            if info.get("pid") != pid or info.get("handoff_protocol") != 1:
                return None
            matches = [name for name in names if watch_pid_path(name) == path]
            if len(matches) != 1:
                recovered = legacy_watcher_session(pid, path)
                if recovered is None:
                    return None
                matches = ["" if recovered == "default" else recovered]
            socket_path = v1_local_socket(matches[0])
            if not socket_path:
                return None
            sources.add(v1_source_key({"socket_path": socket_path}))
        remote_path = state_dir / "remote.pid"
        remote_pid = live_daemon_pid(remote_path, "remote")
        if (
            remote_pid is None
            and remote_path.exists()
            and not v1_remove_confirmed_dead_pidfile(remote_path)
        ):
            return None
        if remote_pid is not None:
            active = read_v1_legacy_remote_inventory(remote_pid)
            if active is None:
                return None
            sources.update(
                v1_source_key({"remote": target, "session": session})
                for target, session in active
            )
        return sources
    except (OSError, ValueError, TypeError, subprocess.TimeoutExpired):
        return None


def v1_supervisor_focus_tick(cfg, desired, ready):
    """Apply focus evidence only to currently prepared client sources."""
    for client_id, spec in desired.items():
        if ready.get(client_id) != spec:
            continue
        target = spec["target"]
        source_id = v1_supervised_source_key(target)
        if target.get("remote"):
            name = "v1-" + hashlib.sha256(source_id.encode()).hexdigest()[:16]
            sock_path = remote_sock_path(name)
        else:
            sock_path = target["socket_path"]
        v1_reconcile_focus(
            cfg, client_id, source_id, spec["workspace"], spec["surface"], sock_path
        )


def v1_supervised_publish_loop(cfg, stop, verified):
    """Publish committed state independently of slow discovery and focus RPCs."""
    while not stop.is_set():
        try:
            if (
                verified.is_set()
                and (state_dir / "v1-enabled").read_text() == "enabled"
            ):
                publish_v1_clients(cfg)
        except FileNotFoundError:
            pass
        except (
            OSError,
            ValueError,
            KeyError,
            TypeError,
            subprocess.TimeoutExpired,
        ) as error:
            log(f"v1 supervisor publication failed: {error}")
        stop.wait(1)


def v1_run_supervisor(cfg, stop):
    """Maintain discovered bindings; publish only after migration commits."""
    leases = V1SupervisedLeases()
    if not leases.acquire_leader():
        return False

    def start_source(source_id, target):
        resolved = v1_target_with_remote_config(cfg, target)
        return V1SupervisedSourceProcess(source_id, resolved) if resolved else None

    sources = V1SupervisedSources(start_source)
    publisher_stop = threading.Event()
    publisher_verified = threading.Event()
    publisher = None
    try:
        leases.reconcile({}, {}, complete=False)
        publisher = threading.Thread(
            target=v1_supervised_publish_loop,
            args=(cfg, publisher_stop, publisher_verified),
            daemon=True,
        )
        publisher.start()
        next_recovery = 0
        while not stop.is_set():
            try:
                if time.monotonic() >= next_recovery:
                    v1_recover_legacy_handoff()
                    next_recovery = time.monotonic() + 5
                inventory = v1_discover_clients(cfg)
                desired = ready = None
                if inventory is not None:
                    v1_reconcile_supervisor_inventory(inventory, sources, leases)
                    desired = v1_discovered_targets(inventory)
                    ready = sources.reconcile(desired, complete=False)
                    if (
                        inventory["complete"]
                        and ready == desired
                        and v1_supervisor_bindings_match(desired, exact=True)
                    ):
                        publisher_verified.set()
                try:
                    enabled = (state_dir / "v1-enabled").read_text() == "enabled"
                except FileNotFoundError:
                    enabled = False
                if enabled and desired is not None:
                    v1_supervisor_focus_tick(cfg, desired, ready)
            except (
                OSError,
                ValueError,
                KeyError,
                TypeError,
                subprocess.TimeoutExpired,
            ) as error:
                log(f"v1 supervisor reconciliation failed: {error}")
            stop.wait(1)
        return True
    finally:
        publisher_stop.set()
        if publisher is not None:
            publisher.join()
        try:
            sources.close()
        finally:
            leases.close()


def prune_v1_clients(state):
    """Remove registrations whose client process no longer holds its lease."""
    clients = state.get("v1", {}).get("clients", {})
    for client_id in list(clients):
        with v1_source_leader("client:" + client_id) as stale:
            if stale:
                clients.pop(client_id, None)


def persist_v1_client(
    client_id, source_id, workspace_id, surface_id, pid, legacy_target=None
):
    with state_lock():
        state = load_state()
        register_v1_client(
            state, client_id, source_id, workspace_id, surface_id, pid, legacy_target
        )
        save_state(state)


def remove_v1_client(client_id):
    with state_lock():
        state = load_state()
        unregister_v1_client(state, client_id)
        save_state(state)


def find_v1_legacy_notification(state, source_ids, workspace, surface, notifications):
    """Recognize old notifications only when old and live agent states agree."""
    words = {"blocked": "waiting for input", "done": "finished"}
    v1 = state.get("v1", {})
    clients = v1.get("clients", {}).values()
    sources = v1.get("sources", {})
    remote_state = load_remote_state().get("remotes", {})
    legacy_counts = {}
    for _, _, workspace_entry in all_workspaces(state):
        for pane in workspace_entry.get("panes", {}).values():
            key = (pane.get("agent"), pane.get("status"))
            legacy_counts[key] = legacy_counts.get(key, 0) + 1
    for remote_entry in remote_state.values():
        for pane in remote_entry.get("panes", {}).values():
            key = (pane.get("agent"), pane.get("status"))
            legacy_counts[key] = legacy_counts.get(key, 0) + 1
    expected = set()
    for client in clients:
        source_id = client.get("source")
        target = client.get("legacy_target") or {}
        if (
            source_id not in source_ids
            or client.get("workspace") != workspace
            or client.get("surface") != surface
            or "session" not in target
        ):
            continue
        session = target["session"] or "default"
        old_panes = {}
        if target.get("remote"):
            identity = [target["remote"], session]
            for entry in remote_state.values():
                if entry.get("identity") == identity:
                    old_panes.update(entry.get("panes", {}))
        else:
            names = {session} if session != "default" else {"", "default"}
            for name in names:
                bucket = state.get("sessions", {}).get(name, {})
                for entry in bucket.get("workspaces", {}).values():
                    old_panes.update(entry.get("panes", {}))
        source = sources.get(source_id, {})
        for pane_id, attention in source.get("attention", {}).items():
            status = attention.get("status")
            agent = source.get("panes", {}).get(pane_id, {}).get("agent")
            old = old_panes.get(pane_id, {})
            if (
                status in words
                and agent
                and old.get("status") == status
                and old.get("agent") == agent
                and legacy_counts.get((agent, status)) == 1
            ):
                expected.add(f"{agent}: {words[status]}")
    matches = [
        item.get("id")
        for item in notifications
        if item.get("workspace_id") == workspace
        and item.get("surface_id") == surface
        and item.get("subtitle") == NOTIFICATION_SUBTITLE
        and item.get("title") in expected
        and not item.get("is_read")
        and item.get("id")
    ]
    return matches[0] if len(matches) == 1 else None


def reconcile_v1_destination(
    cfg, state, workspace_id, surface_id, source_ids, *, is_current=lambda: True
):
    """Combine attention for a destination and track the aggregate's ID."""
    notifications = cmux_notifications_checked(cfg)
    if notifications is None or not is_current():
        return False
    v1 = state.setdefault("v1", {})
    sources = v1.setdefault("sources", {})
    attention = {}
    for source_id in source_ids:
        for pane_id, entry in sources.get(source_id, {}).get("attention", {}).items():
            attention[json.dumps([source_id, pane_id])] = entry
    destinations = v1.setdefault("destinations", {})
    destination = destinations.setdefault(
        v1_destination_key(workspace_id, surface_id), {}
    )
    notification_id = destination.get("notification_id")
    if not notification_id:
        notification_id = find_v1_legacy_notification(
            state, source_ids, workspace_id, surface_id, notifications
        )
        if notification_id:
            destination["notification_id"] = notification_id
    created_id = reconcile_aggregate_notification(
        cfg,
        workspace_id,
        surface_id,
        attention,
        notifications,
        notification_id,
    )
    if created_id:
        destination["notification_id"] = created_id
    return True


def retire_v1_destination(cfg, state, key, *, is_current=lambda: True):
    destinations = state.get("v1", {}).get("destinations", {})
    destination = destinations.get(key)
    if destination is None:
        return True
    try:
        workspace, surface = json.loads(key)
    except (TypeError, ValueError):
        return False
    notifications = cmux_notifications_checked(cfg)
    if notifications is None or not is_current():
        return False
    owned = next(
        (
            item
            for item in notifications
            if item.get("id") == destination.get("notification_id")
            and item.get("workspace_id") == workspace
            and item.get("surface_id") == surface
        ),
        None,
    )
    if (
        owned
        and not owned.get("is_read")
        and not cmux_cli(cfg, ["mark-notification-read", "--id", owned["id"]])
    ):
        return False
    destinations.pop(key, None)
    return True


def reconcile_v1_legacy_sidebar(cfg, state, remote_state):
    """Retire known legacy status keys, retrying failed cmux requests."""
    targets = set()
    clients = state.get("v1", {}).get("clients", {}).values()
    active_refs = {
        client.get("workspace") for client in clients if client.get("workspace")
    }
    active_refs |= cmux_workspace_refs(cfg)
    for bucket in state.get("sessions", {}).values():
        cached = (bucket.get("session_ref") or {}).get("ref")
        for ws_id, entry in bucket.get("workspaces", {}).items():
            label = entry.get("label") or ""
            refs = active_refs | {
                cfg.get("workspaces", {}).get(ws_id),
                cfg.get("labels", {}).get(label),
                cached,
            }
            base = status_key(ws_id)
            for ref in refs - {None, ""}:
                targets.add((ref, base))
                targets.update(
                    (ref, pane_key(base, pid)) for pid in entry.get("panes", {})
                )
    for name, entry in remote_state.get("remotes", {}).items():
        pending = entry.get("pending_clear") or []
        if isinstance(pending, str):
            pending = [pending]
        refs = active_refs | {ref for ref in [entry.get("ref"), *pending] if ref}
        base = remote_key(name)
        for ref in refs:
            targets.add((ref, base))
            targets.update((ref, pane_key(base, pid)) for pid in entry.get("panes", {}))
    refs = {ref for ref, _ in targets}
    for ref in sorted(refs):
        visible = cmux_herdr_status_keys_checked(cfg, ref)
        if visible is None:
            continue
        for key in sorted(
            set(visible) & {key for target, key in targets if target == ref}
        ):
            cmux_cli(cfg, ["clear-status", key, "--workspace", ref])


def reconcile_v1_sidebar(cfg, state, workspaces):
    """Maintain only sidebar keys owned by the active v1 clients."""
    v1 = state.setdefault("v1", {})
    sources = v1.setdefault("sources", {})
    tracked = v1.setdefault("sidebar_keys", {})
    for workspace in set(workspaces) | set(tracked):
        desired = set()
        for source_id in sorted(workspaces.get(workspace, ())):
            source = sources.get(source_id, {})
            visible = sidebar_panes(
                source.get("panes", {}), source.get("attention", {})
            )
            groups = {}
            for pane_id, pane in visible.items():
                group = pane.get("workspace_id", "")
                digest = hashlib.sha256(pane_id.encode()).hexdigest()
                groups.setdefault(group, {})[digest] = pane
            for group, panes in groups.items():
                identity = json.dumps([source_id, group], separators=(",", ":"))
                base = "herdr.v1." + hashlib.sha256(identity.encode()).hexdigest()
                push_pill(cfg, workspace, base, panes)
                desired.add(base)
                desired.update(pane_key(base, pane_id) for pane_id in panes)
        previous = set(tracked.get(workspace, []))
        for key in previous - desired:
            if not cmux_cli(cfg, ["clear-status", key, "--workspace", workspace]):
                desired.add(key)
        if desired:
            tracked[workspace] = sorted(desired)
        else:
            tracked.pop(workspace, None)


def v1_destination_inputs_current(snapshot, workspace, surface):
    """Reject a stale destination plan before sending its cmux notification."""

    def relevant(state):
        v1 = state.get("v1", {})
        clients = {
            key: value
            for key, value in v1.get("clients", {}).items()
            if value.get("workspace") == workspace and value.get("surface") == surface
        }
        sources = {}
        for client in clients.values():
            source_id = client.get("source")
            source = v1.get("sources", {}).get(source_id)
            sources[source_id] = (
                {
                    key: value
                    for key, value in source.items()
                    if key != "published_tokens"
                }
                if isinstance(source, dict)
                else source
            )
        return (
            clients,
            sources,
            v1.get("destinations", {}).get(v1_destination_key(workspace, surface)),
        )

    with state_lock():
        return relevant(snapshot) == relevant(load_state())


def publish_v1_clients(cfg, *, wait=False):
    """Reconcile each registered surface once using all of its sources."""
    with v1_source_leader("publication", blocking=wait) as leader:
        if not leader or not retire_v1_legacy_daemons():
            return
        for _ in range(3):
            if not publish_v1_clients_once(cfg):
                break


def v1_publication_inputs(state):
    """Capture the client and source data used to produce notifications."""
    v1 = state.get("v1", {})
    sources = copy.deepcopy(v1.get("sources", {}))
    for source in sources.values():
        if isinstance(source, dict):
            source.pop("published_tokens", None)
    return copy.deepcopy(v1.get("clients", {})), sources


def publish_v1_clients_once(cfg):
    with state_lock():
        state = load_state()
        prune_v1_clients(state)
        save_state(state)
    baseline = copy.deepcopy(state.get("v1", {}))
    inputs = v1_publication_inputs(state)
    destinations = {}
    for client in state.get("v1", {}).get("clients", {}).values():
        workspace = client.get("workspace")
        surface = client.get("surface")
        source = client.get("source")
        if workspace and surface and source:
            destinations.setdefault((workspace, surface), set()).add(source)
    workspaces = {}
    for (workspace, _surface), sources in destinations.items():
        workspaces.setdefault(workspace, set()).update(sources)
    reconcile_v1_legacy_sidebar(cfg, state, load_remote_state())
    reconcile_v1_sidebar(cfg, state, workspaces)
    for (workspace, surface), sources in destinations.items():
        reconcile_v1_destination(
            cfg,
            state,
            workspace,
            surface,
            sorted(sources),
            is_current=lambda workspace=workspace, surface=surface: (
                v1_destination_inputs_current(state, workspace, surface)
            ),
        )
    active = {
        v1_destination_key(workspace, surface) for workspace, surface in destinations
    }
    for key in list(state.get("v1", {}).get("destinations", {})):
        if key not in active:
            try:
                workspace, surface = json.loads(key)
            except (TypeError, ValueError):
                continue
            retire_v1_destination(
                cfg,
                state,
                key,
                is_current=lambda workspace=workspace, surface=surface: (
                    v1_destination_inputs_current(state, workspace, surface)
                ),
            )
    with state_lock():
        current = load_state()
        live_v1 = current.setdefault("v1", {})
        planned_v1 = state.get("v1", {})
        missing = object()
        for field in ("sidebar_keys", "destinations"):
            before = baseline.get(field, {})
            after = planned_v1.get(field, {})
            live = live_v1.setdefault(field, {})
            for key in set(before) | set(after):
                if live.get(key, missing) != before.get(key, missing):
                    continue
                if key in after:
                    live[key] = after[key]
                else:
                    live.pop(key, None)
        save_state(current)
        return inputs != v1_publication_inputs(current)


def sweep_orphan_pills(cfg, state):
    """Clear herdr.* status pills that no active herdr workspace owns.

    Orphans appear when the plugin state was lost (e.g. the herdr server was
    reinstalled) while cmux kept the sidebar entries. Active keys are the
    union over all session buckets, so one session's reconcile never sweeps
    another session's pills.
    """
    active_keys = set()
    for _, ws_id, ws in all_workspaces(state):
        if not ws.get("panes"):
            continue
        active_keys.add(status_key(ws_id))
        for pane_id in ws["panes"]:
            active_keys.add(pane_key(status_key(ws_id), pane_id))
    for ref in cmux_workspaces_by_title(cfg).values():
        for key in cmux_herdr_status_keys(cfg, ref):
            if key.startswith(REMOTE_KEY_PREFIX):
                continue  # owned by the remote daemon
            if key not in active_keys:
                log(f"clearing orphaned status {key} on {ref}")
                cmux_cli(cfg, ["clear-status", key, "--workspace", ref])


def is_our_notification(item):
    if not isinstance(item, dict):
        return False
    if item.get("subtitle") == NOTIFICATION_SUBTITLE:
        return True
    # Notifications sent before the subtitle tag existed
    return bool(NOTIFICATION_TITLE_RE.search(str(item.get("title") or "")))


def sweep_notifications(cfg, state):
    """Mark our unread notifications read once nothing needs attention.

    Skipped entirely while any pane is blocked/done, since unread
    notifications may still be legitimate then.
    """
    if any(
        has_attention_panes(ws.get("panes", {})) for _, _, ws in all_workspaces(state)
    ):
        return
    remote = load_remote_state()
    if any(
        has_attention_panes(r.get("panes", {}))
        for r in remote.get("remotes", {}).values()
    ):
        return
    for item in cmux_notifications(cfg):
        if item.get("is_read") or not is_our_notification(item):
            continue
        notif_id = item.get("id")
        if notif_id:
            cmux_cli(cfg, ["mark-notification-read", "--id", notif_id])


def clear_legacy_sidebar_logs(cfg, state):
    """One-time migration: drop sidebar log entries written by old versions.

    Versions before 0.5.0 appended every status transition to the cmux
    sidebar log, where entries pile up forever. The sidebar now shows
    current state only (keyed status entries), so the accumulated history
    is cleared once from every workspace this plugin ever wrote to. Log
    entries are workspace-wide and cannot be filtered by source, so this
    also drops unrelated entries on those workspaces.
    """
    if state.get("legacy_log_cleared"):
        return
    titles = cmux_workspaces_by_title(cfg)
    if not titles:
        return  # cmux unreachable; retry on the next reconcile
    refs = set()
    # Workspaces still carrying herdr status keys.
    for ref in titles.values():
        if cmux_herdr_status_keys(cfg, ref):
            refs.add(ref)
    # Workspaces known to the plugin state, by explicit mapping or label.
    for _, ws_id, ws in all_workspaces(state):
        ref = cfg.get("workspaces", {}).get(ws_id)
        label = ws.get("label") or ""
        if not ref:
            ref = cfg.get("labels", {}).get(label)
        if not ref and label:
            ref = titles.get(label.strip().lower())
        if ref:
            refs.add(ref)
    # Workspaces the remote daemon wrote to.
    for r in load_remote_state().get("remotes", {}).values():
        ref = r.get("ref") or titles.get((r.get("cmux_title") or "").strip().lower())
        if ref:
            refs.add(ref)
    for ref in sorted(refs):
        cmux_cli(cfg, ["clear-log", "--workspace", ref])
    state["legacy_log_cleared"] = True


def get_workspace(bucket, ws_id):
    ws = bucket["workspaces"].setdefault(ws_id, {"label": "", "panes": {}})
    ws.setdefault("panes", {})  # entries from older versions may lack it
    return ws


def forget_pane(cfg, bucket, ws_id, pane_id):
    """Retire a pane that left the tracked statuses.

    Shared by the status hook and the socket subscriber, so a pane leaves the
    sidebar the same way whichever of the two observed it going away.
    """
    panes = bucket["workspaces"].get(ws_id, {}).get("panes", {})
    removed = panes.pop(pane_id, None)
    if removed is None:
        return False
    if removed["status"] in ATTENTION_STATUSES and not has_attention_panes(panes):
        # Leaving blocked/done means the prompt was answered or the finished
        # pane was viewed; the notifications for this workspace are actioned.
        mark_read(cfg, bucket, ws_id)
    clear_pane_pill(cfg, bucket, ws_id, pane_id)
    update_pill(cfg, bucket, ws_id)
    return True


def on_agent_status_changed(cfg, bucket, data):
    ws_id = data["workspace_id"]
    pane_id = data["pane_id"]
    status = data["agent_status"]
    ws = get_workspace(bucket, ws_id)
    panes = ws["panes"]
    was_attention = panes.get(pane_id, {}).get("status") in ATTENTION_STATUSES

    if status not in TRACKED_STATUSES:
        # forget_pane republishes the pill itself when it removes something.
        if not forget_pane(cfg, bucket, ws_id, pane_id):
            update_pill(cfg, bucket, ws_id)
        return

    panes[pane_id] = {
        "status": status,
        "agent": data.get("display_agent") or data.get("agent") or "agent",
        "title": data.get("title") or "",
    }

    if status in ATTENTION_STATUSES:
        pane = panes[pane_id]
        agent = pane["agent"]
        label = ws.get("label") or ws_id
        if status == "blocked":
            notify(
                cfg,
                bucket,
                ws_id,
                f"{agent}: waiting for input",
                f"{label} · {pane['title'] or pane_id}",
            )
        else:
            notify(
                cfg,
                bucket,
                ws_id,
                f"{agent}: finished",
                f"{label} · {pane['title'] or pane_id}",
            )
    elif was_attention and not has_attention_panes(panes):
        # Answering the prompt retires the notification just as viewing the
        # finished pane does; forget_pane covers the leaving-tracked case.
        mark_read(cfg, bucket, ws_id)

    update_pill(cfg, bucket, ws_id)


def on_pane_closed(cfg, bucket, data):
    ws_id = data.get("workspace_id")
    pane_id = data.get("pane_id")
    if not ws_id or not pane_id:
        return
    ws = bucket["workspaces"].get(ws_id)
    if not ws:
        return
    pane = ws.setdefault("panes", {}).pop(pane_id, None)
    if pane is None:
        return
    clear_pane_pill(cfg, bucket, ws_id, pane_id)
    if pane["status"] in ATTENTION_STATUSES and not has_attention_panes(ws["panes"]):
        mark_read(cfg, bucket, ws_id)
    update_pill(cfg, bucket, ws_id)


def on_workspace_closed(cfg, bucket, data):
    ws_id = data.get("workspace_id")
    if not ws_id:
        return
    ws = bucket["workspaces"].pop(ws_id, None)
    if ws:
        ref, _ = resolve_cmux_workspace(cfg, bucket, ws_id)
        if ref:
            cmux_cli(cfg, ["clear-status", status_key(ws_id), "--workspace", ref])
            for pane_id in ws["panes"]:
                cmux_cli(
                    cfg,
                    [
                        "clear-status",
                        pane_key(status_key(ws_id), pane_id),
                        "--workspace",
                        ref,
                    ],
                )


def on_workspace_focused(cfg, bucket, data):
    ws_id = data.get("workspace_id")
    if not ws_id:
        return
    mark_read(cfg, bucket, ws_id)


def on_workspace_renamed(cfg, bucket, data):
    ws_id = data.get("workspace_id")
    if ws_id and ws_id in bucket["workspaces"]:
        bucket["workspaces"][ws_id]["label"] = data.get("label") or ""


def handle_event(cfg, state, event, *, session=None):
    bucket = session_bucket(state, session)
    data = event.get("data", {})
    kind = data.get("type")
    if kind == "pane_agent_status_changed":
        on_agent_status_changed(cfg, bucket, data)
    elif kind == "pane_closed":
        on_pane_closed(cfg, bucket, data)
    elif kind == "workspace_closed":
        on_workspace_closed(cfg, bucket, data)
    elif kind == "workspace_focused":
        on_workspace_focused(cfg, bucket, data)
    elif kind == "workspace_renamed":
        on_workspace_renamed(cfg, bucket, data)


def retire_workspace(cfg, bucket, ws_id, entry):
    """Clear pills/notifications for a workspace leaving the tracked set."""
    panes = entry.get("panes", {})
    if not panes:
        return
    for pane_id in panes:
        clear_pane_pill(cfg, bucket, ws_id, pane_id)
    bucket["workspaces"][ws_id] = {"label": entry.get("label", ""), "panes": {}}
    update_pill(cfg, bucket, ws_id)
    if has_attention_panes(panes):
        mark_read(cfg, bucket, ws_id)
    bucket["workspaces"].pop(ws_id, None)


def known_sessions(*, running_only=False):
    """Names of known Herdr sessions, or None when discovery fails."""
    binary = os.environ.get("HERDR_BIN_PATH", "herdr")
    try:
        proc = run([binary, "session", "list", "--json"])
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0:
        return None
    try:
        return {
            s["name"]
            for s in json.loads(proc.stdout).get("sessions", [])
            if not running_only or s.get("running")
        }
    except (json.JSONDecodeError, AttributeError, TypeError, KeyError):
        return None


def running_sessions():
    """Names of herdr sessions with a live server, or None when unknown."""
    return known_sessions(running_only=True)


def prune_dead_sessions(cfg, state):
    """Clear pills and state of herdr sessions that are gone for good.

    Reconcile only sees the current session's agents, so without this a
    session that ended and never restarted would keep its pills forever.
    Live sessions' buckets are left untouched; their own hooks keep them
    fresh.
    """
    sessions = state.get("sessions", {})
    names = [n for n in sessions if n]
    if not names:
        return
    running = running_sessions()
    if running is None:
        return
    for name in names:
        if name in running:
            continue
        bucket = sessions[name]
        for ws_id, entry in list(bucket.get("workspaces", {}).items()):
            retire_workspace(cfg, bucket, ws_id, entry)
        del sessions[name]
        log(f"cleared state of ended herdr session {name!r}")


def reconcile(cfg, state):
    try:
        agents = herdr_cli(["agent", "list"]).get("agents", [])
    except Exception as e:
        log(f"reconcile: agent list failed: {e}")
        return
    clear_legacy_sidebar_logs(cfg, state)
    bucket = session_bucket(state)
    previous = dict(bucket["workspaces"])
    bucket["workspaces"].clear()
    ws_ids = set()
    for a in agents:
        status = a.get("agent_status")
        if status not in TRACKED_STATUSES:
            continue
        ws_id = a["workspace_id"]
        ws_ids.add(ws_id)
        ws = get_workspace(bucket, ws_id)
        ws["panes"][a["pane_id"]] = {
            "status": status,
            "agent": a.get("display_agent") or a.get("agent") or "agent",
            "title": a.get("terminal_title_stripped") or "",
        }
    for ws_id in ws_ids:
        update_pill(cfg, bucket, ws_id)
    # Clear pills/notifications left behind by workspaces that no longer have
    # tracked agents (e.g. the session ended while the server was down). Only
    # this session's bucket is rebuilt: agent list is session-scoped, so
    # other sessions' workspaces must be left alone.
    for ws_id, entry in previous.items():
        panes = entry.get("panes", {})
        if ws_id in bucket["workspaces"]:
            # Panes that vanished since the last run lose their pills.
            for pane_id in set(panes) - set(bucket["workspaces"][ws_id]["panes"]):
                clear_pane_pill(cfg, bucket, ws_id, pane_id)
            continue
        retire_workspace(cfg, bucket, ws_id, entry)
    # Sweep sessions that ended without restarting, then cmux-side leftovers
    # that the plugin state no longer knows about.
    prune_dead_sessions(cfg, state)
    sweep_orphan_pills(cfg, state)
    sweep_notifications(cfg, state)


# --- Remote session support (SSH-polled daemon) -----------------------------


class SocketError(Exception):
    pass


def socket_rpc(sock_path, method, params, timeout=10):
    """Single NDJSON request/response against a herdr API socket."""
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        s.settimeout(timeout)
        s.connect(str(sock_path))
        s.sendall(
            json.dumps({"id": "1", "method": method, "params": params}).encode() + b"\n"
        )
        buf = b""
        while b"\n" not in buf:
            chunk = s.recv(1 << 16)
            if not chunk:
                raise SocketError(f"{method}: connection closed")
            buf += chunk
        msg = json.loads(buf.split(b"\n", 1)[0])
    finally:
        s.close()
    if "error" in msg:
        raise SocketError(f"{method}: {msg['error'].get('message')}")
    return msg.get("result") or {}


def remote_name(rcfg):
    if rcfg.get("name"):
        return rcfg["name"]
    target, session = rcfg["ssh_target"], rcfg["session"]
    if ":" in target or ":" in session:
        # IPv6 / ssh:// forms would make "target:session" ambiguous.
        digest = hashlib.sha1(f"{target}\0{session}".encode()).hexdigest()[:8]
        return f"remote-{digest}"
    return f"{target}:{session}"


def remote_key(name):
    return f"{REMOTE_KEY_PREFIX}{name}"


def remote_sock_path(name):
    # AF_UNIX paths are limited to ~104 bytes on macOS, so keep this short
    # and independent of the (unbounded) configured remote name.
    digest = hashlib.sha1(name.encode()).hexdigest()[:8]
    return Path(tempfile.gettempdir()) / f"cmux-herdr-{os.getuid()}-{digest}.sock"


def clear_remote_pill_keys(cfg, name, entry, ref):
    """Clear the aggregate pill and any per-pane pills; True when cmux
    accepted every clear."""
    base = remote_key(name)
    keys = {base}
    keys |= {pane_key(base, pid) for pid in entry.get("panes", {})}
    # Also catch per-pane pills left behind by older state (e.g. written by
    # a version where per-pane pills were opt-in) so teardown never
    # strands them.
    for k in cmux_herdr_status_keys(cfg, ref):
        if k == base or k.startswith(base + "."):
            keys.add(k)
    ok = True
    for k in sorted(keys):
        if not cmux_cli(cfg, ["clear-status", k, "--workspace", ref]):
            ok = False
    return ok


def clear_remote_pill(cfg, name, entry, ref):
    """Clear a pill, remembering it for retry when cmux rejects the call."""
    if clear_remote_pill_keys(cfg, name, entry, ref):
        return
    pending = entry.get("pending_clear")
    if isinstance(pending, str):  # scalar format from an earlier version
        pending = [pending]
    elif not isinstance(pending, list):
        pending = []
    if ref not in pending:
        pending.append(ref)
    entry["pending_clear"] = pending


def retry_pending_clears(cfg, name, entry):
    pending = entry.get("pending_clear")
    if isinstance(pending, str):  # scalar format from an earlier version
        pending = [pending]
    remaining = []
    for ref in pending or []:
        if not clear_remote_pill_keys(cfg, name, entry, ref):
            remaining.append(ref)
    if remaining:
        entry["pending_clear"] = remaining
    else:
        entry.pop("pending_clear", None)


def local_attention_refs(cfg):
    """Refs of cmux workspaces with local attention panes, resolved only via
    side-effect-free mappings (explicit config, env, cached session ref, and
    label-title matching)."""
    state = load_state()
    refs = set()
    env_ref = os.environ.get("CMUX_WORKSPACE_ID")
    titles = None
    for bucket, ws_id, ws in all_workspaces(state):
        if not has_attention_panes(ws.get("panes", {})):
            continue
        label = ws.get("label") or ""
        ref = cfg.get("workspaces", {}).get(ws_id) or cfg.get("labels", {}).get(label)
        if not ref:
            ref = env_ref
        if not ref:
            ref = (bucket.get("session_ref") or {}).get("ref")
        if not ref and cfg.get("match_by_label", True) and label:
            if titles is None:
                titles = cmux_workspaces_by_title(cfg)
            ref = titles.get(label.strip().lower())
        if ref:
            refs.add(ref)
    return refs


def remote_ref(cfg, rstate, name, entry):
    """Resolve the cmux workspace ref for a remote via its configured title."""
    if entry.get("pending_clear"):
        retry_pending_clears(cfg, name, entry)
    title = entry.get("cmux_title") or ""
    cached = entry.get("ref")
    if cached and time.time() - entry.get("ref_at", 0) < REMOTE_REF_TTL_SECONDS:
        return cached
    ref = None
    if title:
        ref = cmux_workspaces_by_title(cfg).get(title.strip().lower())
    if ref:
        if cached and ref != cached:
            # The title now resolves elsewhere; don't leave anything behind.
            clear_remote_pill(cfg, name, entry, cached)
            remote_mark_read_ref(cfg, rstate, name, cached)
        entry["ref"] = ref
        entry["ref_at"] = time.time()
        return ref
    if cached:
        age = time.time() - entry.get("ref_at", 0)
        if age < REMOTE_STALE_REF_TTL_SECONDS:
            return cached  # tolerate transient cmux lookup failures
        # Unresolvable for too long: drop the pill from the stale workspace.
        clear_remote_pill(cfg, name, entry, cached)
        entry.pop("ref", None)
        entry.pop("ref_at", None)
        entry["pill_published"] = False
        return None
    log(f"no cmux workspace titled {title!r} for remote {name}; skipping")
    return None


def remote_notify(cfg, rstate, name, entry, title, body):
    ref = remote_ref(cfg, rstate, name, entry)
    if not ref:
        return
    cmux_cli(
        cfg,
        [
            "notify",
            "--title",
            title,
            "--subtitle",
            NOTIFICATION_SUBTITLE,
            "--body",
            body,
            "--workspace",
            ref,
        ],
    )


def remote_mark_read_ref(cfg, rstate, name, ref):
    """Mark a workspace read, unless another herdr source with attention
    panes still targets it (mark-notification-read is workspace-wide)."""
    for other_name, other in rstate.get("remotes", {}).items():
        if other_name == name:
            continue
        if other.get("ref") == ref and has_attention_panes(other.get("panes", {})):
            return
    if ref in local_attention_refs(cfg):
        return
    cmux_cli(cfg, ["mark-notification-read", "--workspace", ref])


def remote_mark_read(cfg, rstate, name, entry):
    ref = remote_ref(cfg, rstate, name, entry)
    if ref:
        remote_mark_read_ref(cfg, rstate, name, ref)


def update_remote_pill(cfg, rstate, name, entry):
    ref = remote_ref(cfg, rstate, name, entry)
    entry["pill_at"] = time.time()
    if not ref:
        entry["pill_published"] = False
        return
    push_pill(cfg, ref, remote_key(name), entry.get("panes", {}))
    entry["pill_published"] = True


def reroute_remote(cfg, rstate, name, entry, cmux_title):
    """Move a remote to a new cmux workspace title, clearing the old one."""
    old_ref = entry.pop("ref", None)
    entry.pop("ref_at", None)
    if old_ref:
        clear_remote_pill(cfg, name, entry, old_ref)
        remote_mark_read_ref(cfg, rstate, name, old_ref)
    entry["cmux_title"] = cmux_title
    entry["pill_published"] = False


def apply_remote_snapshot(cfg, rstate, name, rcfg, agents, labels):
    """Diff one remote agent.list snapshot against state and drive cmux.

    Returns True when the tracked pane set changed.
    """
    entry = rstate["remotes"].setdefault(name, {"panes": {}})
    changed = False
    identity = [rcfg["ssh_target"], rcfg["session"]]
    if entry.get("identity") != identity:
        # Retargeted: the cached socket path belongs to the previous target.
        entry["identity"] = identity
        entry.pop("socket_path", None)
        changed = True
    cmux_title = rcfg.get("cmux_title") or ""
    if entry.get("cmux_title") != cmux_title:
        reroute_remote(cfg, rstate, name, entry, cmux_title)
        changed = True

    old = entry["panes"]
    old_statuses = {pid: p["status"] for pid, p in old.items()}
    new = {}
    for a in agents:
        status = a.get("agent_status")
        if status not in TRACKED_STATUSES:
            continue
        new[a["pane_id"]] = {
            "status": status,
            "agent": a.get("display_agent") or a.get("agent") or "agent",
            "title": a.get("terminal_title_stripped") or a.get("terminal_title") or "",
            "workspace_id": a.get("workspace_id") or "",
        }
    if new == old:
        # Republish when the pill was never pushed (e.g. the cmux workspace
        # did not exist yet, or the title just changed) or may have been lost
        # (e.g. a cmux restart).
        stale = time.time() - entry.get("pill_at", 0) > REMOTE_REF_TTL_SECONDS
        if stale or not entry.get("pill_published"):
            update_remote_pill(cfg, rstate, name, entry)
        return changed

    label = cmux_title or name
    for pid, p in new.items():
        prev = old_statuses.get(pid)
        if prev == p["status"]:
            continue
        what = p["title"] or labels.get(p["workspace_id"]) or pid
        if p["status"] in ATTENTION_STATUSES:
            body = f"{label} · {what}"
            if p["status"] == "blocked":
                remote_notify(
                    cfg, rstate, name, entry, f"{p['agent']}: waiting for input", body
                )
            else:
                remote_notify(cfg, rstate, name, entry, f"{p['agent']}: finished", body)

    removed = [pid for pid in old if pid not in new]
    if removed:
        ref = remote_ref(cfg, rstate, name, entry)
        if ref:
            for pid in removed:
                cmux_cli(
                    cfg,
                    [
                        "clear-status",
                        pane_key(remote_key(name), pid),
                        "--workspace",
                        ref,
                    ],
                )

    was_attention = has_attention_panes(old)
    entry["panes"] = new
    changed = True
    if was_attention and not has_attention_panes(new):
        # The prompt was answered or dismissed on the remote side.
        remote_mark_read(cfg, rstate, name, entry)
    update_remote_pill(cfg, rstate, name, entry)
    return changed


def resolve_remote_socket(rcfg):
    ssh_target = rcfg["ssh_target"]
    session = rcfg["session"]
    # Note: non-interactive ssh gets a minimal PATH on the remote, so
    # herdr_bin may need to be an absolute path in the config.
    herdr_bin = rcfg.get("herdr_bin") or "herdr"
    proc = run(
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            ssh_target,
            herdr_bin,
            "session",
            "list",
            "--json",
        ],
        timeout=20,
    )
    if proc.returncode != 0:
        raise SocketError(
            f"ssh {ssh_target} {herdr_bin} session list failed: {proc.stderr.strip()}"
        )
    for s in json.loads(proc.stdout).get("sessions", []):
        if s.get("name") == session:
            path = s.get("socket_path")
            if path:
                return path
    raise SocketError(f"session {session!r} not found on {ssh_target}")


def drop_forward(name, entry, procs):
    proc = procs.pop(name, None)
    if proc is not None and proc.poll() is None:
        proc.terminate()
    entry.pop("socket_path", None)
    remote_sock_path(name).unlink(missing_ok=True)


def ensure_forward(name, rcfg, entry, procs):
    """Return the local end of an ssh unix-socket forward, (re)spawning it."""
    if DAEMON_SHUTDOWN.is_set():
        raise SocketError("daemon is shutting down")
    local_sock = remote_sock_path(name)
    proc = procs.get(name)
    if proc is not None and proc.poll() is None and local_sock.exists():
        return local_sock
    if proc is not None:
        drop_forward(name, entry, procs)
    remote_path = entry.get("socket_path")
    if not remote_path:
        remote_path = resolve_remote_socket(rcfg)
        entry["socket_path"] = remote_path
    if DAEMON_SHUTDOWN.is_set():
        raise SocketError("daemon is shutting down")
    local_sock.unlink(missing_ok=True)
    previous_handler = None
    pending = []
    if threading.current_thread() is threading.main_thread():
        previous_handler = signal.getsignal(signal.SIGTERM)
        if callable(previous_handler):
            signal.signal(
                signal.SIGTERM, lambda sig, frame: pending.append((sig, frame))
            )
    try:
        procs[name] = subprocess.Popen(
            [
                "ssh",
                "-N",
                "-T",
                "-o",
                "BatchMode=yes",
                "-o",
                "ExitOnForwardFailure=yes",
                "-o",
                "ServerAliveInterval=15",
                "-o",
                "ServerAliveCountMax=2",
                "-L",
                f"{local_sock}:{remote_path}",
                rcfg["ssh_target"],
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    finally:
        if callable(previous_handler):
            signal.signal(signal.SIGTERM, previous_handler)
            if pending:
                previous_handler(*pending[-1])
    deadline = time.time() + 10
    while time.time() < deadline:
        if procs[name].poll() is not None:
            drop_forward(name, entry, procs)
            raise SocketError(
                f"ssh forward to {rcfg['ssh_target']} failed to establish; "
                "non-interactive (key-based) SSH auth is required"
            )
        if local_sock.exists():
            return local_sock
        time.sleep(0.2)
    drop_forward(name, entry, procs)
    raise SocketError(f"timed out waiting for ssh forward to {rcfg['ssh_target']}")


def poll_interval(rcfg):
    try:
        value = float(rcfg.get("poll_seconds") or REMOTE_DEFAULT_POLL_SECONDS)
    except (TypeError, ValueError):
        return REMOTE_DEFAULT_POLL_SECONDS
    if not math.isfinite(value) or value <= 0:
        return REMOTE_DEFAULT_POLL_SECONDS
    return max(value, 0.5)


def fetch_remote(name, rcfg, entry, procs):
    """Network-only part of a poll; safe to run in a worker thread."""
    sock = ensure_forward(name, rcfg, entry, procs)
    try:
        agents = socket_rpc(sock, "agent.list", {}).get("agents", [])
        workspaces = socket_rpc(sock, "workspace.list", {}).get("workspaces", [])
    except (SocketError, OSError, json.JSONDecodeError):
        # Remote server restarted or tunnel went stale; rebuild it next time.
        drop_forward(name, entry, procs)
        raise
    labels = {}
    for w in workspaces:
        ws_id = w.get("workspace_id") or w.get("id")
        if ws_id:
            labels[ws_id] = w.get("label") or ""
    return agents, labels


def sweep_remote_orphan_pills(cfg, remotes):
    """Clear herdr.remote.* pills for remotes no longer in the config."""
    active = {remote_key(remote_name(r)) for r in remotes}
    # Per-pane pills of active remotes belong to them; everything else with
    # the remote prefix is orphaned.
    prefixes = tuple(k + "." for k in active)
    for ref in cmux_workspaces_by_title(cfg).values():
        for key in cmux_herdr_status_keys(cfg, ref):
            if (
                key.startswith(REMOTE_KEY_PREFIX)
                and key not in active
                and not key.startswith(prefixes)
            ):
                log(f"clearing orphaned remote status {key} on {ref}")
                cmux_cli(cfg, ["clear-status", key, "--workspace", ref])


def remote_configs(cfg):
    """Validated [[remotes]] entries. Returns None when any entry is
    unusable, so callers can keep the previous config instead of tearing
    remotes down over a typo."""
    raw = cfg.get("remotes", [])
    if not isinstance(raw, list):
        log("[[remotes]] must be an array of tables")
        return None
    remotes = []
    seen = set()
    for r in raw:
        if not isinstance(r, dict):
            log(f"invalid [[remotes]] entry {r!r}; expected a table")
            return None
        if not (
            isinstance(r.get("ssh_target"), str)
            and isinstance(r.get("session"), str)
            and r["ssh_target"]
            and r["session"]
        ):
            log(f"invalid [[remotes]] entry {r!r}; ssh_target/session required")
            return None
        if r.get("name") is not None and not isinstance(r["name"], str):
            log(f"remote {r!r}: name must be a string")
            return None
        if r.get("herdr_bin") is not None and not isinstance(r["herdr_bin"], str):
            log(f"remote {r!r}: herdr_bin must be a string")
            return None
        if not isinstance(r.get("cmux_title"), str) or not r.get("cmux_title"):
            log(f"remote {remote_name(r)!r}: cmux_title is required")
            return None
        name = remote_name(r)
        if name in seen:
            log(f"duplicate remote name {name!r}; set a unique `name` in [[remotes]]")
            return None
        seen.add(name)
        remotes.append(r)
    return remotes


def retire_remote(cfg, rstate, name, procs, next_due):
    """Tear down a remote that was removed from the config or retargeted.

    Keeps a minimal entry behind when cmux rejects a pill clear, so a later
    daemon start retries the cleanup instead of stranding the pill.
    """
    entry = rstate["remotes"].get(name, {})
    drop_forward(name, entry, procs)
    next_due.pop(name, None)
    retry_pending_clears(cfg, name, entry)
    ref = entry.get("ref")
    if not ref:
        title = (entry.get("cmux_title") or "").strip().lower()
        if title:
            ref = cmux_workspaces_by_title(cfg).get(title)
    if ref:
        clear_remote_pill(cfg, name, entry, ref)
        remote_mark_read_ref(cfg, rstate, name, ref)
    if entry.get("pending_clear"):
        entry["panes"] = {}
        entry.pop("ref", None)
        log(f"remote {name}: retired, but pill cleanup is still pending")
    else:
        rstate["remotes"].pop(name, None)
    save_remote_state(rstate)


def v1_process_audit_token(pid):
    """Obtain a kernel-issued macOS process identity, or None if unavailable."""
    if sys.platform != "darwin" or type(pid) is not int or not 0 < pid <= 0x7FFFFFFF:
        return None
    try:
        lib = ctypes.CDLL("/usr/lib/libSystem.B.dylib", use_errno=True)
        lib.task_name_for_pid.argtypes = [
            ctypes.c_uint,
            ctypes.c_int,
            ctypes.POINTER(ctypes.c_uint),
        ]
        lib.task_name_for_pid.restype = ctypes.c_int
        lib.task_info.argtypes = [
            ctypes.c_uint,
            ctypes.c_int,
            ctypes.POINTER(ctypes.c_uint32),
            ctypes.POINTER(ctypes.c_uint),
        ]
        lib.task_info.restype = ctypes.c_int
        lib.mach_port_deallocate.argtypes = [ctypes.c_uint, ctypes.c_uint]
        lib.mach_port_deallocate.restype = ctypes.c_int
        own_task = ctypes.c_uint.in_dll(lib, "mach_task_self_").value
        task = ctypes.c_uint()
        if lib.task_name_for_pid(own_task, pid, ctypes.byref(task)) != 0:
            return None
        try:
            token = (ctypes.c_uint32 * 8)()
            count = ctypes.c_uint(8)
            if (
                lib.task_info(task, 15, token, ctypes.byref(count)) != 0
                or count.value != 8
            ):
                return None
            return list(token)
        finally:
            lib.mach_port_deallocate(own_task, task)
    except (OSError, AttributeError, ValueError):
        return None


def v1_signal_audit_token(token, signum):
    """Ask the kernel to signal exactly the process incarnation in token."""
    if sys.platform != "darwin":
        raise OSError(errno.ENOTSUP, "process audit signaling unavailable")
    if (
        not isinstance(token, list)
        or len(token) != 8
        or any(type(word) is not int or not 0 <= word <= 0xFFFFFFFF for word in token)
        or isinstance(signum, bool)
        or not isinstance(signum, int)
        or signum not in signal.valid_signals()
    ):
        raise ValueError("invalid process audit token")
    try:
        lib = ctypes.CDLL("/usr/lib/libSystem.B.dylib", use_errno=True)
        lib.proc_signal_with_audittoken.argtypes = [
            ctypes.POINTER(ctypes.c_uint32),
            ctypes.c_int,
        ]
        lib.proc_signal_with_audittoken.restype = ctypes.c_int
    except (OSError, AttributeError) as error:
        raise OSError(errno.ENOTSUP, "process audit signaling unavailable") from error
    raw = (ctypes.c_uint32 * 8)(*token)
    result = lib.proc_signal_with_audittoken(raw, signum)
    if result:
        raise OSError(result, os.strerror(result))


class V1AuditToken(ctypes.Structure):
    _fields_ = [("words", ctypes.c_uint32 * 8)]


def v1_audit_token_pidversion(token):
    """Decode the OS-defined generation without depending on token layout."""
    if sys.platform != "darwin" or not isinstance(token, list) or len(token) != 8:
        return None
    if any(type(word) is not int or not 0 <= word <= 0xFFFFFFFF for word in token):
        return None
    try:
        lib = ctypes.CDLL("/usr/lib/libbsm.0.dylib", use_errno=True)
        lib.audit_token_to_pidversion.argtypes = [V1AuditToken]
        lib.audit_token_to_pidversion.restype = ctypes.c_int
        return lib.audit_token_to_pidversion(
            V1AuditToken((ctypes.c_uint32 * 8)(*token))
        )
    except (OSError, AttributeError, ValueError):
        return None


def v1_daemon_incarnation_exited(pid, token):
    """Treat unavailable identity as unknown, never as a confirmed exit."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except OSError:
        return False
    current = v1_process_audit_token(pid)
    if current is None:
        return False
    previous_version = v1_audit_token_pidversion(token)
    current_version = v1_audit_token_pidversion(current)
    return (
        previous_version is not None
        and current_version is not None
        and previous_version != current_version
    )


def daemon_self_info():
    script = os.path.abspath(__file__)
    try:
        mtime = os.path.getmtime(script)
    except OSError:
        mtime = 0
    info = {"pid": os.getpid(), "script": script, "mtime": mtime}
    token = v1_process_audit_token(info["pid"])
    if token is not None:
        info["audit_token"] = token
    return info


def read_pidfile(pid_path):
    try:
        data = json.loads(pid_path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    if isinstance(data, int):  # pre-JSON format
        return {"pid": data}
    return data if isinstance(data, dict) else {}


def v1_verified_daemon_audit_token(pid_path, pid):
    """Require the pidfile and the live process to identify the same incarnation."""
    info = read_pidfile(pid_path)
    recorded = info.get("audit_token")
    if (
        info.get("pid") != pid
        or not isinstance(recorded, list)
        or len(recorded) != 8
        or any(
            type(word) is not int or not 0 <= word <= 0xFFFFFFFF for word in recorded
        )
    ):
        return None
    current = v1_process_audit_token(pid)
    return recorded if current is not None and current == recorded else None


def pid_is_daemon(pid, mode):
    """Guard against signaling an unrelated process after PID reuse.

    `mode` keeps the daemons apart: a live watcher must not read as a stale
    remote daemon, which would let a second one start.
    """
    try:
        proc = subprocess.run(
            ["ps", "-p", str(pid), "-o", "args="],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except subprocess.TimeoutExpired:
        return False
    parts = proc.stdout.strip().split()
    return any(p.endswith("cmux_herdr.py") for p in parts) and mode in parts


def live_daemon_pid(pid_path, mode):
    """PID of the running remote daemon, or None (stale/absent pidfile)."""
    if not pid_path.exists():
        return None
    pid = read_pidfile(pid_path).get("pid")
    if type(pid) is not int or not 1 < pid <= 0x7FFFFFFF:
        return None
    try:
        os.kill(pid, 0)
    except OSError:
        return None
    if not pid_is_daemon(pid, mode):
        log(f"ignoring stale pidfile: pid {pid} is not a cmux-herdr {mode} daemon")
        return None
    return pid


def wait_for_v1_legacy_restore(pid_path, mode):
    """Wait outside the startup lock for the previous writer to exit."""
    old_pid = live_daemon_pid(pid_path, mode)
    restore_pid = os.environ.get("CMUX_HERDR_RESTORE_PID", "")
    try:
        restore_token = json.loads(os.environ.get("CMUX_HERDR_RESTORE_TOKEN", "null"))
    except ValueError:
        restore_token = None
    while old_pid is not None and str(old_pid) == restore_pid:
        if (state_dir / "v1-enabled").exists():
            raise SystemExit(0)
        if restore_token is not None and v1_daemon_incarnation_exited(
            old_pid, restore_token
        ):
            return
        time.sleep(0.1)
        old_pid = live_daemon_pid(pid_path, mode)


def take_over_daemon(pid_path, mode, activation=None):
    """Ensure this is the only (and latest-code) daemon of `mode` running."""
    old_pid = live_daemon_pid(pid_path, mode)
    if old_pid is None:
        return
    info = read_pidfile(pid_path)
    self = daemon_self_info()
    if info.get("script") == self["script"] and info.get("mtime") == self["mtime"]:
        log(f"{mode} daemon already running (pid {old_pid})")
        raise SystemExit(0)
    token = v1_verified_daemon_audit_token(pid_path, old_pid)
    if token is None:
        log(f"cannot verify outdated {mode} daemon incarnation {old_pid}")
        raise SystemExit(1)
    # The plugin was reinstalled or edited since the old daemon started.
    log(f"replacing outdated {mode} daemon (pid {old_pid})")
    try:
        v1_signal_audit_token(token, signal.SIGTERM)
    except ProcessLookupError:
        return
    except OSError as error:
        log(f"cannot stop outdated {mode} daemon {old_pid}: {error}")
        raise SystemExit(1) from error
    if activation is not None:
        fcntl.flock(activation, fcntl.LOCK_UN)
    try:
        # Must exceed the old daemon's worst-case shutdown (worker quiesce).
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if v1_daemon_incarnation_exited(old_pid, token):
                return
            time.sleep(0.1)
    finally:
        if activation is not None:
            fcntl.flock(activation, fcntl.LOCK_EX)
    # The old daemon refuses to die; starting another one would fight over
    # forwarded sockets and state. Retry on the next startup/refresh.
    log(f"outdated {mode} daemon (pid {old_pid}) did not exit; aborting")
    raise SystemExit(1)


def update_v1_legacy_restores(update):
    """Serialize and atomically persist pending legacy restoration obligations."""
    state_dir.mkdir(parents=True, exist_ok=True)
    path = state_dir / "v1-legacy-restores.json"
    with open(state_dir / "v1-legacy-restores.lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            entries = json.loads(path.read_text())
        except FileNotFoundError:
            entries = []
        if not isinstance(entries, list) or any(
            not isinstance(item, list)
            or len(item) not in (3, 4)
            or item[0] not in ("watch", "remote")
            or not (item[1] is None or isinstance(item[1], str))
            or type(item[2]) is not int
            or not 1 < item[2] <= 0x7FFFFFFF
            or (
                len(item) == 4
                and (
                    not isinstance(item[3], list)
                    or len(item[3]) != 8
                    or any(
                        type(word) is not int or not 0 <= word <= 0xFFFFFFFF
                        for word in item[3]
                    )
                )
            )
            for item in entries
        ):
            raise ValueError("invalid legacy restoration journal")
        entries = update([tuple(item) for item in entries])
        if entries:
            tmp = path.with_suffix(f".{os.getpid()}.tmp")
            tmp.write_text(json.dumps(entries))
            tmp.replace(path)
        else:
            path.unlink(missing_ok=True)
        return entries


def pending_v1_legacy_restores():
    return update_v1_legacy_restores(lambda entries: entries)


def record_v1_legacy_restore(plan, item):
    added = False

    def add(entries):
        nonlocal added
        if item in entries:
            return entries
        added = True
        return [*entries, item]

    update_v1_legacy_restores(add)
    if item not in plan:
        plan.append(item)
    return added


def ack_v1_legacy_restore(mode, session, restore_pid):
    if not restore_pid or not restore_pid.isdecimal():
        return
    item = (mode, session, int(restore_pid))
    update_v1_legacy_restores(
        lambda entries: [entry for entry in entries if entry[:3] != item]
    )


def ack_v1_legacy_ready(mode, session):
    """A ready replacement satisfies any older restore for the same source."""

    def still_needed(entry):
        if (entry[0], entry[1]) != (mode, session):
            return True
        if len(entry) == 4:
            return not v1_daemon_incarnation_exited(entry[2], entry[3])
        try:
            os.kill(entry[2], 0)
        except ProcessLookupError:
            return False
        except OSError:
            return True
        return True

    update_v1_legacy_restores(
        lambda entries: [entry for entry in entries if still_needed(entry)]
    )


def legacy_watcher_session(pid, pid_path):
    """Recover the session of a verified watcher, including deleted sessions."""
    try:
        proc = subprocess.run(
            ["ps", "-eww", "-p", str(pid)],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if proc.returncode:
        return None
    names = re.findall(r"(?:^|\s)HERDR_SESSION=([A-Za-z0-9_.-]+)(?=\s|$)", proc.stdout)
    if len(names) != 1 or watch_pid_path(names[0]) != pid_path:
        return None
    return names[0]


def stop_v1_legacy_daemon(pid_path, mode, restore_plan=None, *, deadline=None):
    """Stop one verified writer while holding its legacy startup lock."""
    with open(state_dir / f"{pid_path.stem}.lock", "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        pid = live_daemon_pid(pid_path, mode)
        if pid is None:
            return v1_remove_confirmed_dead_pidfile_locked(pid_path)
        token = v1_verified_daemon_audit_token(pid_path, pid)
        if token is None:
            log(f"cannot verify legacy {mode} daemon incarnation {pid}")
            return v1_remove_confirmed_dead_pidfile_locked(pid_path)
        if deadline is not None and time.monotonic() >= deadline:
            return False
        if restore_plan is not None:
            session = None
            if mode == "watch":
                sessions = known_sessions()
                if sessions is None:
                    return False
                names = {"" if name == "default" else name for name in sessions}
                matches = [name for name in names if watch_pid_path(name) == pid_path]
                if len(matches) != 1:
                    recovered = legacy_watcher_session(pid, pid_path)
                    if recovered is None:
                        log(f"cannot safely restore legacy watcher {pid_path.name}")
                        return False
                    matches = ["" if recovered == "default" else recovered]
                session = matches[0]
            if deadline is not None and time.monotonic() >= deadline:
                return False
            item = (mode, session, pid, token)
            added = record_v1_legacy_restore(restore_plan, item)
            if deadline is not None and time.monotonic() >= deadline:
                if added:
                    update_v1_legacy_restores(
                        lambda entries: [entry for entry in entries if entry != item]
                    )
                    restore_plan.remove(item)
                return False
        try:
            v1_signal_audit_token(token, signal.SIGTERM)
        except ProcessLookupError:
            return True
        except OSError as e:
            log(f"cannot stop legacy {mode} daemon {pid}: {e}")
            return False
        stop_deadline = time.monotonic() + 15
        if deadline is not None:
            stop_deadline = min(stop_deadline, deadline)
        while time.monotonic() < stop_deadline:
            if v1_daemon_incarnation_exited(pid, token):
                return True
            time.sleep(0.1)
        log(f"legacy {mode} daemon {pid} did not exit")
        return False


def retire_v1_legacy_daemons(restore_plan=None):
    """Stop verified legacy writers before enabling v1 publication."""
    state_dir.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + 30
    success = stop_v1_legacy_daemon(
        state_dir / "remote.pid", "remote", restore_plan, deadline=deadline
    )
    seen = set()
    while True:
        if time.monotonic() >= deadline or len(seen) >= 4096:
            log("legacy watcher inventory did not stabilize")
            return False
        unseen = sorted(set(state_dir.glob("watch-*.pid")) - seen)
        if not unseen:
            for path in [state_dir / "remote.pid", *seen]:
                if path.exists() and not v1_remove_confirmed_dead_pidfile(path):
                    return False
            break
        for path in unseen:
            if time.monotonic() >= deadline or len(seen) >= 4096:
                return False
            seen.add(path)
            if not stop_v1_legacy_daemon(
                path, "watch", restore_plan, deadline=deadline
            ):
                success = False
    return success


def poll_remote_worker(rcfg, procs, results):
    """Fetch one remote's snapshot in the background. Shared state is not
    touched: the result carries everything the main thread needs to apply
    (or safely discard) it."""
    name = remote_name(rcfg)
    identity = (rcfg["ssh_target"], rcfg["session"], rcfg.get("cmux_title") or "")
    fctx = {}  # forward context (socket_path cache), owned by this worker
    try:
        agents, labels = fetch_remote(name, rcfg, fctx, procs)
        results[name] = {
            "ok": True,
            "identity": identity,
            "socket_path": fctx.get("socket_path"),
            "agents": agents,
            "labels": labels,
        }
    except Exception as e:
        results[name] = {"ok": False, "identity": identity, "error": e}


def tombstone_worker(cfg, rstate, names, done, cancel):
    """Retry retired remotes' pill clears off the scheduling thread.

    Reads shared state but never mutates it; the main thread applies the
    results only for remotes that are still retired. Names in cancel
    (remotes that became active again) are skipped before any cmux call.
    """
    for name in names:
        if name in cancel:
            done[name] = None
            continue
        entry = rstate["remotes"].get(name)
        if not entry:
            done[name] = None
            continue
        pending = entry.get("pending_clear")
        if isinstance(pending, str):  # scalar format from an earlier version
            pending = [pending]
        remaining = []
        for ref in pending or []:
            if name in cancel:
                break
            if not clear_remote_pill_keys(cfg, name, entry, ref):
                remaining.append(ref)
        done[name] = remaining if name not in cancel else None


def remote_daemon(cfg):
    # A malformed config must not look like "no remotes" (which triggers
    # cleanup); keep the previous behavior of doing nothing instead.
    strict = load_config_strict()
    if strict is None:
        return 1
    cfg = strict
    remotes = remote_configs(cfg)
    if remotes is None:
        # Remotes are present but unusable; refuse to start (and crucially,
        # do not run the empty-config cleanup against persisted state).
        return 1
    state_dir.mkdir(parents=True, exist_ok=True)
    restore_pid = os.environ.get("CMUX_HERDR_RESTORE_PID", "")
    pid_path = state_dir / "remote.pid"
    if restore_pid:
        wait_for_v1_legacy_restore(pid_path, "remote")
        os.environ.pop("CMUX_HERDR_RESTORE_PID", None)
        os.environ.pop("CMUX_HERDR_RESTORE_TOKEN", None)
    if not remotes:
        # No remotes configured: retire anything a previous daemon left
        # behind (pills, unread notifications, stale state), then exit.
        # A new daemon is spawned by each startup hook and refresh action.
        with v1_daemon_startup_lock(pid_path) as activation:
            if (state_dir / "v1-enabled").exists():
                return 0
            if live_daemon_pid(pid_path, "remote") is not None:
                # A current daemon hot-reloads the empty config and retires
                # its remotes itself (this raises SystemExit in that case);
                # an outdated one is replaced so upgrades still take effect.
                take_over_daemon(pid_path, "remote", activation)
                if (state_dir / "v1-enabled").exists():
                    return 0
                log("replaced outdated remote daemon before cleanup")
            rstate = load_remote_state()
            for name in list(rstate.get("remotes", {})):
                retire_remote(cfg, rstate, name, {}, {})
            sweep_remote_orphan_pills(cfg, [])
            save_remote_state(rstate)
            ack_v1_legacy_restore("remote", None, restore_pid)
        log("no [[remotes]] configured; remote daemon exiting")
        return 0
    # Serialize concurrent startup/refresh invocations: only one daemon
    # may take over and write the pidfile.
    inventory_epoch = uuid.uuid4().hex
    with v1_daemon_startup_lock(pid_path) as activation:
        if (state_dir / "v1-enabled").exists():
            return 0
        take_over_daemon(pid_path, "remote", activation)
        if (state_dir / "v1-enabled").exists():
            return 0
        pid_path.write_text(
            json.dumps({**daemon_self_info(), "inventory_epoch": inventory_epoch})
        )
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    rstate = load_remote_state()
    procs = {}
    next_due = {}
    inflight = {}  # name -> worker thread (fetch only; main thread applies)
    results = {}
    tombstone = {"thread": None, "done": {}, "cancel": set()}
    tombstone_due = {}
    # Seed watched from the startup config so a config change between
    # startup reconciliation and the first loop iteration is still caught.
    watched = {
        remote_name(r): (r["ssh_target"], r["session"], r.get("cmux_title") or "")
        for r in remotes
    }
    restore_seen = {}
    configured = {remote_name(r): r for r in remotes}
    for name in list(rstate["remotes"]):
        entry = rstate["remotes"][name]
        rcfg = configured.get(name)
        if rcfg is None or entry.get("identity") != [
            rcfg["ssh_target"],
            rcfg["session"],
        ]:
            # Persisted while a previous daemon was running, or the entry
            # was retargeted (or written by a version without identity
            # tracking) while no daemon was alive; never retired.
            retire_remote(cfg, rstate, name, procs, next_due)
            continue
        title = rcfg.get("cmux_title") or ""
        if entry.get("cmux_title") != title:
            # Title changed while no daemon was running; reroute now so the
            # old workspace is cleaned even if the remote is unreachable.
            reroute_remote(cfg, rstate, name, entry, title)
    sweep_remote_orphan_pills(cfg, remotes)
    names = [remote_name(r) for r in remotes]
    log(f"remote daemon started (pid {os.getpid()}); watching: {', '.join(names)}")
    try:
        while True:
            cfg = v1_legacy_remote_next_config(cfg, watched, inventory_epoch)
            remotes = remote_configs(cfg) or []
            active = {
                remote_name(r): (
                    r["ssh_target"],
                    r["session"],
                    r.get("cmux_title") or "",
                )
                for r in remotes
            }
            # Reap finished fetch workers and apply their snapshots on this
            # thread, keeping all state and cmux mutations serialized.
            for name, t in list(inflight.items()):
                if t.is_alive():
                    continue
                del inflight[name]
                res = results.pop(name, {"ok": False, "error": "poll missing"})
                rcfg = next((r for r in remotes if remote_name(r) == name), None)
                identity = (
                    (rcfg["ssh_target"], rcfg["session"], rcfg.get("cmux_title") or "")
                    if rcfg
                    else None
                )
                if identity is None or res.get("identity") != identity:
                    # Retired or retargeted while the fetch was in flight;
                    # discard the stale snapshot and kill any forward the
                    # orphaned worker may have (re)created.
                    drop_forward(name, rstate["remotes"].get(name, {}), procs)
                    continue
                if res["ok"]:
                    entry = rstate["remotes"].setdefault(name, {"panes": {}})
                    if res.get("socket_path"):
                        entry["socket_path"] = res["socket_path"]
                    if apply_remote_snapshot(
                        cfg, rstate, name, rcfg, res["agents"], res["labels"]
                    ):
                        save_remote_state(rstate)
                    restore_seen[name] = res["identity"]
                    next_due[name] = time.time() + poll_interval(rcfg)
                else:
                    log(f"remote {name}: {res['error']}")
                    next_due[name] = time.time() + REMOTE_ERROR_BACKOFF_SECONDS
            for name, identity in list(watched.items()):
                if active.get(name) != identity:
                    log(f"remote {name}: removed or retargeted; tearing down")
                    retire_remote(cfg, rstate, name, procs, next_due)
                    del watched[name]
            for name, identity in active.items():
                watched.setdefault(name, identity)
            if (state_dir / "v1-legacy-restores.json").exists() and all(
                restore_seen.get(name) == identity for name, identity in active.items()
            ):
                ack_v1_legacy_ready("remote", None)
            # Spawn fetches for due remotes without waiting for them, so one
            # unreachable remote cannot starve the others.
            for rcfg in remotes:
                name = remote_name(rcfg)
                if name in inflight:
                    continue
                # Clamp deadlines far in the future so a decreased
                # poll_seconds applies promptly, without undoing the error
                # backoff (which is at most REMOTE_ERROR_BACKOFF_SECONDS).
                cap = time.time() + max(
                    poll_interval(rcfg), REMOTE_ERROR_BACKOFF_SECONDS
                )
                if next_due.get(name, 0) > cap:
                    next_due[name] = cap
                if time.time() < next_due.get(name, 0):
                    continue
                t = threading.Thread(
                    target=poll_remote_worker,
                    args=(rcfg, procs, results),
                    daemon=True,
                )
                inflight[name] = t
                t.start()
            # Retry pill cleanups left behind by retired remotes, backed off
            # and off this thread (cmux calls can block for seconds).
            # Re-added remotes cancel their in-flight cleanup so a late
            # clear cannot wipe a freshly republished pill.
            tombstone["cancel"].update(active)
            finished = tombstone["thread"]
            if finished is not None and not finished.is_alive():
                for name, remaining in tombstone["done"].items():
                    if remaining is None or name in active:
                        # Cleanup was cancelled (or a clear-status was still
                        # in flight) when the remote became active again; a
                        # late clear may have removed the new pill, so force
                        # a republish on the next poll.
                        if name in active:
                            entry = rstate["remotes"].get(name)
                            if entry is not None:
                                entry["pill_published"] = False
                                save_remote_state(rstate)
                        continue
                    entry = rstate["remotes"].get(name)
                    if entry is None:
                        continue
                    if remaining:
                        entry["pending_clear"] = remaining
                        tombstone_due[name] = time.time() + REMOTE_ERROR_BACKOFF_SECONDS
                    else:
                        tombstone_due.pop(name, None)
                        rstate["remotes"].pop(name, None)
                        save_remote_state(rstate)
                tombstone["thread"] = None
                tombstone["done"] = {}
            if tombstone["thread"] is None:
                pending_names = [
                    name
                    for name in list(rstate["remotes"])
                    if name not in active
                    and rstate["remotes"][name].get("pending_clear")
                    and time.time() >= tombstone_due.get(name, 0)
                ]
                if pending_names:
                    # This generation must not inherit cancellations from
                    # when these names were active in the past.
                    tombstone["cancel"] -= set(pending_names)
                    tombstone["thread"] = threading.Thread(
                        target=tombstone_worker,
                        args=(
                            cfg,
                            rstate,
                            pending_names,
                            tombstone["done"],
                            tombstone["cancel"],
                        ),
                        daemon=True,
                    )
                    tombstone["thread"].start()
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        DAEMON_SHUTDOWN.set()
        # Quiesce fetch workers before tearing down their SSH forwards, so
        # no worker can spawn a new forward after this loop has run.
        deadline = time.time() + 10
        for t in inflight.values():
            t.join(max(0, deadline - time.time()))
        for name in list(procs):
            drop_forward(name, rstate["remotes"].get(name, {}), procs)
        # Only unlink our own pidfile; a replacement daemon may have
        # already written its own.
        try:
            if read_pidfile(pid_path).get("pid") == os.getpid():
                pid_path.unlink(missing_ok=True)
        except OSError:
            pass
    return 0


# --- Watch subscriber (recovers transitions herdr never dispatches) ---------

# herdr does not run plugin hooks for the `done` -> `idle` transition, the one
# that is supposed to retire a "finished" pill once the user has looked at the
# pane. The same transition *is* published on the API socket, so a subscriber
# consumes it there and retires the pane the hook never heard about.

WATCH_BACKOFF_SECONDS = 2.0
WATCH_MAX_BACKOFF_SECONDS = 60.0


def watch_pid_path(session):
    return state_dir / f"watch-{sanitize_key(session or 'default')}.pid"


def watch_subscriptions(pane_ids):
    """One agent-status subscription per pane, plus the lifecycle wildcards.

    pane.agent_status_changed takes no wildcard, so the set has to be rebuilt
    whenever a pane appears or vanishes; pane.created and pane.closed are what
    tell the subscriber to do that.
    """
    subs = [
        {"type": "pane.agent_status_changed", "pane_id": pane_id}
        for pane_id in pane_ids
    ]
    subs.append({"type": "pane.created"})
    subs.append({"type": "pane.closed"})
    return subs


def v1_subscriptions(pane_ids):
    """Observe agent changes, pane lifecycle, metadata and focus changes."""
    subs = watch_subscriptions(pane_ids)
    subs.extend(
        {"type": event}
        for event in (
            "pane.agent_detected",
            "pane.updated",
            "pane.focused",
            "pane.moved",
            "tab.focused",
            "workspace.focused",
        )
    )
    return subs


def pane_is_tracked(sock_path, pane_id):
    """Whether herdr reports the pane in a status the sidebar shows.

    Answers True when the server cannot be asked: an unverified event must
    never retire a pill.
    """
    try:
        agents = socket_rpc(sock_path, "agent.list", {}).get("agents", [])
    except (SocketError, OSError, ValueError) as e:
        log(f"watch: agent list failed: {e}")
        return True
    for agent in agents:
        if agent.get("pane_id") == pane_id:
            return agent.get("agent_status") in TRACKED_STATUSES
    return False


def apply_watch_event(cfg, state, sock_path, data, *, session=None, tracked=None):
    """Apply a watcher transition while the caller holds the state lock."""
    ws_id, pane_id = data.get("workspace_id"), data.get("pane_id")
    if not ws_id or not pane_id:
        return
    bucket = session_bucket(state, session)
    if pane_id not in bucket["workspaces"].get(ws_id, {}).get("panes", {}):
        return
    if tracked if tracked is not None else pane_is_tracked(sock_path, pane_id):
        return
    forget_pane(cfg, bucket, ws_id, pane_id)


def handle_watch_event(cfg, sock_path, data):
    """Apply one status transition the plugin's event hook never received."""
    if data.get("agent_status") in TRACKED_STATUSES:
        # Delivered to hooks correctly; they own every addition, notification
        # and log entry, so acting here would duplicate them.
        return
    with state_lock():
        state = load_state()
        marker = state_dir / "v1-enabled"
        if marker.exists():
            if marker.read_text() == "pending":
                state = v1_load_legacy_journal_state()
                state.setdefault("v1_pending_legacy_events", []).append(
                    {
                        "kind": "watch",
                        "session": current_session(),
                        "workspace": os.environ.get("CMUX_WORKSPACE_ID"),
                        "herdr_socket_path": os.environ.get("HERDR_SOCKET_PATH"),
                        "socket_path": str(sock_path),
                        "event": data,
                    }
                )
                save_state(state)
            return
        apply_watch_event(cfg, state, sock_path, data)
        save_state(state)


def session_socket_path():
    """API socket of the herdr session this process was spawned from."""
    name = current_session()
    binary = os.environ.get("HERDR_BIN_PATH", "herdr")
    try:
        proc = run([binary, "session", "list", "--json"])
    except (FileNotFoundError, subprocess.TimeoutExpired) as e:
        log(f"watch: session list failed: {e}")
        return None
    if proc.returncode != 0:
        log(f"watch: session list failed: {proc.stderr.strip()}")
        return None
    try:
        sessions = json.loads(proc.stdout).get("sessions", [])
    except ValueError as e:
        log(f"watch: session list unreadable: {e}")
        return None
    for session in sessions:
        if session.get("name") == name or (not name and session.get("default")):
            return session.get("socket_path")
    log(f"watch: no API socket for herdr session {name!r}")
    return None


def v1_local_socket(session):
    """Resolve a running local session; never guess a stale socket path."""
    binary = os.environ.get("HERDR_BIN_PATH", "herdr")
    try:
        proc = run([binary, "session", "list", "--json"])
        if proc.returncode:
            return None
        for item in json.loads(proc.stdout).get("sessions", []):
            if (
                item.get("name") == session if session else item.get("default")
            ) and item.get("running"):
                return item.get("socket_path")
    except (OSError, subprocess.TimeoutExpired, ValueError, TypeError):
        return None
    return None


def open_subscription(sock_path, subscriptions, *, timeout=None):
    """Connected socket streaming the subscribed events as NDJSON."""
    conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        if timeout is not None:
            conn.settimeout(timeout)
        conn.connect(str(sock_path))
        conn.sendall(
            json.dumps(
                {
                    "id": "watch",
                    "method": "events.subscribe",
                    "params": {"subscriptions": subscriptions},
                }
            ).encode()
            + b"\n"
        )
    except BaseException:
        conn.close()
        raise
    return conn


V1_SUBSCRIPTION_IDLE_TIMEOUT = 10


def open_v1_subscription(sock_path, subscriptions):
    return open_subscription(
        sock_path, subscriptions, timeout=V1_SUBSCRIPTION_IDLE_TIMEOUT
    )


def list_pane_ids(sock_path):
    panes = socket_rpc(sock_path, "pane.list", {}).get("panes", [])
    return [p["pane_id"] for p in panes if p.get("pane_id")]


def v1_subscription_matches_source(source_id, event_boot):
    with state_lock():
        source = load_state().get("v1", {}).get("sources", {}).get(source_id, {})
        return source.get("server_boot_id") == event_boot


def v1_watch_connection(
    source_id, generation, sock_path, *, on_change=None, expected_boot=None
):
    """Observe status transitions in order and rebuild when panes change."""
    pane_ids = list_pane_ids(sock_path)
    conn = open_v1_subscription(sock_path, v1_subscriptions(pane_ids))
    try:
        stream = conn.makefile("rb")
        ack = json.loads(stream.readline())
        if ack.get("result", {}).get("type") != "subscription_started":
            raise SocketError("Herdr subscription was not acknowledged")
        event_boot = ack["result"].get("server_boot_id")
        if not isinstance(event_boot, str) or not re.fullmatch(
            r"[0-9a-f]{32}", event_boot
        ):
            event_boot = None
        if expected_boot is not None and event_boot != expected_boot:
            raise SocketError("Herdr subscription server does not match client source")
        sync_options = (
            {"expected_boot": expected_boot} if expected_boot is not None else {}
        )

        def sync_with_retry(**options):
            while True:
                try:
                    return sync_v1_source(
                        source_id,
                        generation,
                        sock_path,
                        retry_snapshot_race=True,
                        **options,
                    )
                except V1SnapshotRace:
                    time.sleep(0.05)

        if not sync_with_retry(reset_predictions=True, **sync_options):
            raise SocketError("Herdr snapshot failed after subscription")
        if event_boot and not v1_subscription_matches_source(source_id, event_boot):
            raise SocketError("Herdr subscription server changed")
        if list_pane_ids(sock_path) != pane_ids:
            return
        if on_change is not None:
            on_change()
        for line in stream:
            try:
                msg = json.loads(line)
            except ValueError:
                continue
            event = str(msg.get("event") or "").replace(".", "_")
            if event in ("pane_created", "pane_closed", "pane_moved"):
                return
            if event == "pane_agent_status_changed":
                data = msg.get("data") or {}
                updated = persist_v1_status_event(source_id, data, event_boot)
                if not updated or data.get("agent_status") == "done":
                    if not sync_with_retry(**sync_options):
                        raise SocketError("Herdr snapshot failed after status event")
                    if event_boot and not v1_subscription_matches_source(
                        source_id, event_boot
                    ):
                        raise SocketError("Herdr subscription server changed")
                if on_change is not None:
                    on_change()
            elif event == "pane_updated":
                if (
                    persist_v1_ack_event(source_id, msg.get("data") or {}, event_boot)
                    and on_change
                ):
                    on_change()
            elif event == "pane_agent_detected":
                if not sync_with_retry(**sync_options):
                    raise SocketError(
                        "Herdr snapshot failed after agent lifecycle event"
                    )
                if event_boot and not v1_subscription_matches_source(
                    source_id, event_boot
                ):
                    raise SocketError("Herdr subscription server changed")
                if on_change is not None:
                    on_change()
    finally:
        conn.close()


def publish_v1_source(cfg, source_id, socket_path):
    sync_v1_episode_tokens(source_id, socket_path)
    publish_v1_clients(cfg)


def v1_token_retry_loop(source_id, socket_path, stop):
    while not stop.is_set():
        try:
            sync_v1_episode_tokens(source_id, socket_path)
        except OSError as e:
            log(f"v1 token retry failed: {e}")
        stop.wait(1)


def watch_v1_with_token_retry(cfg, source_id, generation, socket_path):
    stop = threading.Event()
    worker = threading.Thread(
        target=v1_token_retry_loop, args=(source_id, socket_path, stop), daemon=True
    )
    started = False

    def on_change():
        nonlocal started
        if not started:
            worker.start()
            started = True
        publish_v1_source(cfg, source_id, socket_path)

    try:
        v1_watch_connection(source_id, generation, socket_path, on_change=on_change)
    finally:
        stop.set()
        if started:
            worker.join()


def observe_v1_source(cfg, source_id, generation, sock_path):
    """Let one elected observer publish changes from a Herdr source."""
    with v1_source_leader(source_id) as leader:
        if not leader:
            return False
        watch_v1_with_token_retry(cfg, source_id, generation, sock_path)
        return True


def v1_watch_supervised_source(source_id, target, on_ready, *, on_not_ready=None):
    """Prepare one source without publishing to cmux before handoff."""
    name = "v1-" + hashlib.sha256(source_id.encode()).hexdigest()[:16]
    entry, procs = {}, {}
    with v1_source_leader(source_id) as leader:
        if not leader:
            return False
        token_stop = threading.Event()
        token_worker = None
        notified = False
        try:
            if target.get("remote"):
                sock = ensure_forward(name, v1_remote_config(target), entry, procs)
                generation = v1_remote_generation(
                    target["remote"], entry["socket_path"]
                )
            else:
                sock = target["socket_path"]
                generation = v1_local_generation(sock)

            def on_change():
                nonlocal notified, token_worker
                if not notified:
                    on_ready()
                    notified = True
                    token_worker = threading.Thread(
                        target=v1_supervised_token_loop,
                        args=(source_id, sock, target["server_boot_id"], token_stop),
                        daemon=True,
                    )
                    token_worker.start()

            v1_watch_connection(
                source_id,
                generation,
                sock,
                on_change=on_change,
                expected_boot=target["server_boot_id"],
            )
            return True
        finally:
            if notified and on_not_ready is not None:
                on_not_ready()
            token_stop.set()
            if token_worker is not None:
                token_worker.join()
            if target.get("remote"):
                drop_forward(name, entry, procs)


def v1_supervised_token_loop(source_id, socket_path, expected_boot, stop):
    """Synchronize force-read tokens without waiting for discovery."""
    while not stop.is_set():
        try:
            if (state_dir / "v1-enabled").read_text() == "enabled":
                sync_v1_episode_tokens(
                    source_id, socket_path, expected_boot=expected_boot
                )
        except FileNotFoundError:
            pass
        except (OSError, ValueError, KeyError, TypeError) as error:
            log(f"v1 supervised token sync failed: {error}")
        stop.wait(1)


def run_v1_supervised_source(
    source_id, target, on_ready, stop, *, on_not_ready=lambda: None
):
    """Rebuild silent or disconnected subscriptions without waiting for discovery."""
    ready = False

    def ready_once():
        nonlocal ready
        if not ready:
            on_ready()
            ready = True

    def not_ready():
        nonlocal ready
        if ready:
            ready = False
            on_not_ready()

    while not stop.is_set():
        try:
            if not v1_watch_supervised_source(
                source_id, target, ready_once, on_not_ready=not_ready
            ):
                return False
        except (
            SocketError,
            OSError,
            ValueError,
            KeyError,
            subprocess.TimeoutExpired,
        ) as error:
            log(f"v1 supervised source reconnecting: {error}")
        finally:
            not_ready()
        stop.wait(1)
    return True


def observe_v1_remote(cfg, source_id, target, entry, procs):
    """Forward a remote socket only while this process owns the source."""
    name = "v1-" + hashlib.sha256(source_id.encode()).hexdigest()[:16]
    rcfg = v1_remote_config(target)
    with v1_source_leader(source_id) as leader:
        if not leader:
            return False
        try:
            sock = ensure_forward(name, rcfg, entry, procs)
            generation = v1_remote_generation(target["remote"], entry["socket_path"])
            watch_v1_with_token_retry(cfg, source_id, generation, sock)
            return True
        finally:
            drop_forward(name, entry, procs)


def v1_publish_loop(cfg, stop):
    while not stop.is_set():
        try:
            publish_v1_clients(cfg)
        except OSError as e:
            log(f"v1 publication failed: {e}")
        stop.wait(1)


def v1_watch_parent(parent_pid, stop):
    """Retire a shell-launched observer if its owning shell disappears."""
    while not stop.wait(1):
        if os.getppid() != parent_pid:
            os.kill(os.getpid(), signal.SIGTERM)
            return


def v1_focus_loop(cfg, client_id, source_id, workspace_id, surface_id, target, stop):
    if target.get("remote"):
        name = "v1-" + hashlib.sha256(source_id.encode()).hexdigest()[:16]
        sock_path = remote_sock_path(name)
    else:
        sock_path = target.get("socket_path")
    if not sock_path:
        return
    while not stop.is_set():
        v1_reconcile_focus(
            cfg, client_id, source_id, workspace_id, surface_id, sock_path
        )
        stop.wait(1)


def run_v1_client(cfg, target, workspace_id, surface_id, stop):
    """Register a cmux-hosted client and follow its Herdr session."""
    target = v1_target_with_remote_config(cfg, target)
    if target is None:
        return
    client_id = uuid.uuid4().hex
    with v1_source_leader("client:" + client_id) as acquired:
        if not acquired:
            raise RuntimeError("cannot acquire client lease")
        run_v1_client_with_lease(cfg, target, workspace_id, surface_id, stop, client_id)


def resolve_v1_source_target(target):
    """Resolve local session aliases to the socket used by that server."""
    if target.get("remote"):
        rcfg = v1_remote_config(target)
        try:
            remote_socket = resolve_remote_socket(rcfg)
            v1_remote_generation(target["remote"], remote_socket)
        except (SocketError, OSError, ValueError, subprocess.TimeoutExpired):
            return None
        return target.copy()
    if target.get("socket_path"):
        try:
            v1_local_generation(target["socket_path"])
        except OSError:
            return None
        return target.copy()
    sock = v1_local_socket(target.get("session") or "")
    return {**target, "socket_path": sock} if sock else None


def v1_probe_source(target):
    """Require a live Herdr API before retiring the legacy writer."""
    entry, procs = {}, {}
    name = "v1-probe-" + uuid.uuid4().hex[:16]
    try:
        if target.get("remote"):
            rcfg = v1_remote_config(target)
            sock = ensure_forward(name, rcfg, entry, procs)
        else:
            sock = target["socket_path"]
        result = socket_rpc(sock, "session.snapshot", {})
        snapshot = result.get("snapshot")
        expected_boot = target.get("server_boot_id")
        return isinstance(snapshot, dict) and (
            expected_boot is None or snapshot.get("server_boot_id") == expected_boot
        )
    except (
        SocketError,
        OSError,
        ValueError,
        KeyError,
        TypeError,
        subprocess.TimeoutExpired,
    ):
        return False
    finally:
        if target.get("remote"):
            drop_forward(name, entry, procs)


def restart_legacy_daemons(plan):
    """Restore writers that were live before an interrupted first cutover."""
    for entry in plan:
        mode, session, pid = entry[:3]
        env = {
            key: value
            for key, value in os.environ.items()
            if key not in (*CMUX_CALLER_IDENTITY_VARS, "HERDR_SOCKET_PATH")
        }
        env["HERDR_PLUGIN_STATE_DIR"] = str(state_dir)
        env["HERDR_PLUGIN_CONFIG_DIR"] = str(config_dir)
        env["CMUX_HERDR_RESTORE_PID"] = str(pid)
        if len(entry) == 4:
            env["CMUX_HERDR_RESTORE_TOKEN"] = json.dumps(entry[3])
        else:
            env.pop("CMUX_HERDR_RESTORE_TOKEN", None)
        if mode == "watch":
            env["HERDR_SESSION"] = session or ""
        try:
            pid_path = (
                watch_pid_path(session) if mode == "watch" else state_dir / "remote.pid"
            )
            with (
                open(state_dir / f"{pid_path.stem}.restore.lock", "w") as restore_lock,
                open(state_dir / f"{mode}.log", "a") as log_file,
            ):
                try:
                    fcntl.flock(restore_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    continue
                subprocess.Popen(
                    [sys.executable, os.path.abspath(__file__), mode],
                    stdin=subprocess.DEVNULL,
                    stdout=log_file,
                    stderr=log_file,
                    env=env,
                    start_new_session=True,
                    pass_fds=(restore_lock.fileno(),),
                )
        except OSError as e:
            log(f"failed to restore legacy {mode} daemon: {e}")


def activate_v1_client(
    client_id, source_id, workspace_id, surface_id, legacy_target, stop
):
    """Serialize cutover and roll back a new marker if startup is interrupted."""
    state_dir.mkdir(parents=True, exist_ok=True)
    with open(state_dir / "v1-activation.lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        marker = state_dir / "v1-enabled"
        try:
            already_enabled = marker.read_text() != "pending"
        except FileNotFoundError:
            already_enabled = False
        restore_plan = pending_v1_legacy_restores() if not already_enabled else []
        registered = False
        try:
            with state_lock():
                if not already_enabled:
                    tmp = marker.with_suffix(f".{os.getpid()}.tmp")
                    tmp.write_text("pending")
                    tmp.replace(marker)
            while not stop.is_set():
                if retire_v1_legacy_daemons(None if already_enabled else restore_plan):
                    persist_v1_client(
                        client_id,
                        source_id,
                        workspace_id,
                        surface_id,
                        os.getpid(),
                        legacy_target,
                    )
                    if not already_enabled:
                        tmp = marker.with_suffix(f".{os.getpid()}.tmp")
                        tmp.write_text("enabled")
                        tmp.replace(marker)
                    registered = True
                    update_v1_legacy_restores(lambda entries: [])
                    return True
                stop.wait(1)
            return False
        finally:
            if not registered and not already_enabled:
                with state_lock():
                    marker.unlink(missing_ok=True)
                restart_legacy_daemons(pending_v1_legacy_restores())


def run_v1_client_with_lease(cfg, target, workspace_id, surface_id, stop, client_id):
    original = target.copy()
    while not stop.is_set():
        resolved = resolve_v1_source_target(target)
        if resolved and v1_probe_source(resolved):
            target = resolved
            break
        stop.wait(1)
    else:
        return
    source_id = v1_source_key(target)
    legacy_target = {
        "session": target.get("session") or "default",
        "remote": target.get("remote"),
    }
    if original.get("socket_path") and not target.get("remote"):
        named_socket = v1_local_socket(target.get("session") or "")
        if not named_socket or os.path.realpath(named_socket) != os.path.realpath(
            target["socket_path"]
        ):
            legacy_target = None
    publisher = focus = None
    activated = False
    try:
        if not activate_v1_client(
            client_id, source_id, workspace_id, surface_id, legacy_target, stop
        ):
            return
        activated = True
        publisher = threading.Thread(
            target=v1_publish_loop, args=(cfg, stop), daemon=True
        )
        publisher.start()
        focus = threading.Thread(
            target=v1_focus_loop,
            args=(cfg, client_id, source_id, workspace_id, surface_id, target, stop),
            daemon=True,
        )
        focus.start()
        entry, procs = {}, {}
        while not stop.is_set():
            try:
                if target.get("remote"):
                    observe_v1_remote(cfg, source_id, target, entry, procs)
                else:
                    sock = target.get("socket_path") or v1_local_socket(
                        target.get("session") or ""
                    )
                    if sock:
                        observe_v1_source(
                            cfg, source_id, v1_local_generation(sock), sock
                        )
            except (SocketError, OSError, ValueError, subprocess.TimeoutExpired) as e:
                log(f"v1 observer disconnected: {e}")
            stop.wait(1)
    finally:
        stop.set()
        for worker in (publisher, focus):
            if worker is not None and worker.ident is not None:
                worker.join(timeout=2)
        remove_v1_client(client_id)
        if activated:
            publish_v1_clients(cfg, wait=True)


def watch_connection(cfg, sock_path, *, on_ready=None):
    """Subscribe to every pane and dispatch until the pane set goes stale."""
    pane_ids = list_pane_ids(sock_path)
    conn = open_subscription(sock_path, watch_subscriptions(pane_ids))
    try:
        stream = conn.makefile("rb")
        try:
            ack = json.loads(next(stream, b""))
        except ValueError as error:
            raise SocketError("invalid Herdr subscription acknowledgement") from error
        result = ack.get("result") if isinstance(ack, dict) else None
        if not isinstance(result, dict) or result.get("type") != "subscription_started":
            raise SocketError("Herdr subscription was not acknowledged")
        # herdr 0.9 starts a subscription with live events instead of replaying
        # history, so a pane created between the listing and the subscribe would
        # be invisible. Re-listing after subscribing catches exactly that.
        if list_pane_ids(sock_path) != pane_ids:
            return
        if on_ready is not None:
            on_ready()
        for line in stream:
            try:
                msg = json.loads(line)
            except ValueError:
                continue
            # herdr spells event names with dots in some streams and
            # underscores in others.
            event = str(msg.get("event") or "").replace(".", "_")
            if event in ("pane_created", "pane_closed"):
                return  # the subscription set is stale; rebuild it
            if event == "pane_agent_status_changed":
                handle_watch_event(cfg, sock_path, msg.get("data") or {})
    finally:
        conn.close()


def watch_loop(cfg, *, on_ready=None):
    backoff = WATCH_BACKOFF_SECONDS
    while True:
        sock_path = session_socket_path()
        if sock_path:
            try:
                watch_connection(cfg, sock_path, on_ready=on_ready)
                backoff = WATCH_BACKOFF_SECONDS
                continue  # a stale pane set reconnects immediately
            except (SocketError, OSError, ValueError) as e:
                log(f"watch: connection lost: {e}")
        time.sleep(backoff)
        backoff = min(backoff * 2, WATCH_MAX_BACKOFF_SECONDS)


def watch_daemon(cfg):
    session = current_session()
    restore_pid = os.environ.get("CMUX_HERDR_RESTORE_PID", "")
    state_dir.mkdir(parents=True, exist_ok=True)
    pid_path = watch_pid_path(session)
    if restore_pid:
        wait_for_v1_legacy_restore(pid_path, "watch")
        os.environ.pop("CMUX_HERDR_RESTORE_PID", None)
        os.environ.pop("CMUX_HERDR_RESTORE_TOKEN", None)
    # Serialize concurrent startup/refresh invocations: only one watcher per
    # session may take over and write the pidfile.
    with v1_daemon_startup_lock(pid_path) as activation:
        if (state_dir / "v1-enabled").exists():
            return 0
        take_over_daemon(pid_path, "watch", activation)
        if (state_dir / "v1-enabled").exists():
            return 0
        pid_path.write_text(json.dumps({**daemon_self_info(), "handoff_protocol": 1}))
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    log(f"watch daemon started (pid {os.getpid()}) for session {session or 'default'}")
    try:
        watch_loop(
            cfg,
            on_ready=lambda: ack_v1_legacy_ready("watch", session or ""),
        )
    finally:
        # Only unlink our own pidfile; a replacement may already own it.
        try:
            if read_pidfile(pid_path).get("pid") == os.getpid():
                pid_path.unlink(missing_ok=True)
        except OSError:
            pass
    return 0


def main():
    global state_dir, config_dir, STATE_PATH
    if len(sys.argv) < 2 or sys.argv[1] not in {
        "event",
        "reconcile",
        "detect",
        "remote",
        "watch",
        "client",
        "supervisor",
        "supervisor-source",
        "force-read",
    }:
        print(
            "usage: cmux_herdr.py "
            "<event|reconcile|detect|remote|watch|client|supervisor|force-read>",
            file=sys.stderr,
        )
        return 2
    mode = sys.argv[1]
    if mode == "force-read":
        return 0 if v1_force_read_action() else 1
    if mode == "supervisor":
        if (
            not interactive_client_target(sys.argv[2:])
            or not os.environ.get("CMUX_WORKSPACE_ID")
            or not os.environ.get("CMUX_SURFACE_ID")
        ):
            return 0
        state_dir, config_dir = v1_plugin_paths(os.environ)
        STATE_PATH = state_dir / "state.json"
        stop = threading.Event()
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        v1_run_supervisor(load_config(), stop)
        return 0
    if mode == "supervisor-source":
        if len(sys.argv) != 4:
            return 2
        try:
            source_id, target = sys.argv[2], json.loads(sys.argv[3])
            boot = target.get("server_boot_id")
            if (
                not isinstance(target, dict)
                or not isinstance(boot, str)
                or not re.fullmatch(r"[0-9a-f]{32}", boot)
                or source_id != v1_supervised_source_key(target)
            ):
                return 2
        except (ValueError, TypeError, AttributeError):
            return 2
        state_dir, config_dir = v1_plugin_paths(os.environ)
        STATE_PATH = state_dir / "state.json"
        stop = threading.Event()

        def stop_source(*_args):
            stop.set()
            raise SystemExit(0)

        signal.signal(signal.SIGTERM, stop_source)
        parent_pid = os.getppid()
        if parent_pid <= 1:
            return 1
        threading.Thread(
            target=v1_watch_parent, args=(parent_pid, stop), daemon=True
        ).start()
        try:
            watched = run_v1_supervised_source(
                source_id,
                target,
                lambda: print("ready", flush=True),
                stop,
                on_not_ready=lambda: print("not-ready", flush=True),
            )
            return 0 if watched else 1
        except (
            SocketError,
            OSError,
            ValueError,
            KeyError,
            subprocess.TimeoutExpired,
        ) as error:
            log(f"v1 supervised source failed: {error}")
            return 1
        finally:
            stop.set()
    if mode == "client":
        log("the per-client observer cannot safely migrate existing sessions")
        return 1
    if mode == "reconcile" and (state_dir / "v1-enabled").exists():
        return 0
    cfg = load_config()
    try:
        if mode == "remote":
            return remote_daemon(cfg)
        if mode == "watch":
            return watch_daemon(cfg)
        with state_lock():
            if mode in {"event", "reconcile"}:
                try:
                    marker = (state_dir / "v1-enabled").read_text()
                except FileNotFoundError:
                    marker = None
                if marker is not None:
                    if mode == "event" and marker == "pending":
                        raw = os.environ.get("HERDR_PLUGIN_EVENT_JSON")
                        if raw:
                            state = v1_load_legacy_journal_state()
                            state.setdefault("v1_pending_legacy_events", []).append(
                                {
                                    "session": current_session(),
                                    "workspace": os.environ.get("CMUX_WORKSPACE_ID"),
                                    "herdr_socket_path": os.environ.get(
                                        "HERDR_SOCKET_PATH"
                                    ),
                                    "event": json.loads(raw),
                                }
                            )
                            save_state(state)
                    return 0
            state = load_state()
            if mode == "event":
                raw = os.environ.get("HERDR_PLUGIN_EVENT_JSON")
                if raw:
                    handle_event(cfg, state, json.loads(raw))
            elif mode == "reconcile":
                reconcile(cfg, state)
            else:
                bucket = session_bucket(state)
                bucket.pop("session_ref", None)
                ref = detect_session_workspace(cfg, bucket)
                print(
                    json.dumps(
                        {
                            "cmux_workspace": ref,
                            "herdr_session": os.environ.get("HERDR_SESSION"),
                            "candidate_pids": herdr_session_pids(),
                        }
                    )
                )
            save_state(state)
    except Exception as e:
        log(f"unhandled error in {mode}: {e!r}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
