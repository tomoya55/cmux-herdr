#!/usr/bin/env python3
import fcntl
import hashlib
import json
import math
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
from pathlib import Path

ATTENTION_STATUSES = {"blocked", "done"}
TRACKED_STATUSES = ATTENTION_STATUSES | {"working"}

DEFAULT_CMUX_BIN = "/Applications/cmux.app/Contents/Resources/bin/cmux"
STATUS_COLOR_WAITING = "#ff9500"
STATUS_COLOR_WORKING = "#0a84ff"
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

state_dir = Path(os.environ.get("HERDR_PLUGIN_STATE_DIR", "."))
config_dir = Path(os.environ.get("HERDR_PLUGIN_CONFIG_DIR", "."))
STATE_PATH = state_dir / "state.json"


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
        return json.loads(STATE_PATH.read_text())
    except Exception:
        return {"workspaces": {}}


def save_state(state):
    state_dir.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state))
    tmp.replace(STATE_PATH)


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


def run(cmd, timeout=10):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


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
        m = re.match(r"^\s*(workspace:\S+)\s+(.+?)\s*$", line)
        if m:
            result[m.group(2).strip().lower()] = m.group(1)
    return result


def cmux_herdr_status_keys(cfg, ref):
    binary = cfg.get("cmux_bin") or DEFAULT_CMUX_BIN
    try:
        proc = run([binary, "list-status", "--workspace", ref])
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return []
    if proc.returncode != 0:
        return []
    keys = []
    for line in proc.stdout.splitlines():
        m = re.match(r"^(herdr\.\S+?)=", line.strip())
        if m:
            keys.append(m.group(1))
    return keys


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


def herdr_session_pids():
    session = os.environ.get("HERDR_SESSION", "")
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


def detect_session_workspace(cfg, state):
    env_ref = os.environ.get("CMUX_WORKSPACE_ID")
    if env_ref:
        return env_ref

    cached = state.get("session_ref") or {}
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
        state["session_ref"] = {"ref": ref, "at": time.time()}
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


def resolve_cmux_workspace(cfg, state, ws_id):
    entry = state["workspaces"].get(ws_id, {})
    label = entry.get("label") or workspace_label(ws_id)
    if label and not entry.get("label"):
        entry["label"] = label
        state["workspaces"][ws_id] = entry

    ref = cfg.get("workspaces", {}).get(ws_id) or cfg.get("labels", {}).get(label)
    if not ref:
        ref = detect_session_workspace(cfg, state)
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


def push_pill(cfg, ref, key, panes):
    waiting = sum(1 for p in panes.values() if p["status"] in ATTENTION_STATUSES)
    working = sum(1 for p in panes.values() if p["status"] == "working")
    if waiting:
        parts = [f"{waiting} waiting"]
        if working:
            parts.append(f"{working} working")
        cmux_cli(
            cfg,
            [
                "set-status",
                key,
                " · ".join(parts),
                "--workspace",
                ref,
                "--color",
                STATUS_COLOR_WAITING,
                "--priority",
                "20",
            ],
        )
    elif working:
        cmux_cli(
            cfg,
            [
                "set-status",
                key,
                f"{working} working",
                "--workspace",
                ref,
                "--color",
                STATUS_COLOR_WORKING,
                "--priority",
                "10",
            ],
        )
    else:
        cmux_cli(cfg, ["clear-status", key, "--workspace", ref])


def update_pill(cfg, state, ws_id):
    ref, _ = resolve_cmux_workspace(cfg, state, ws_id)
    if not ref:
        return
    panes = state["workspaces"].get(ws_id, {}).get("panes", {})
    push_pill(cfg, ref, status_key(ws_id), panes)


def notify(cfg, state, ws_id, title, body):
    ref, _ = resolve_cmux_workspace(cfg, state, ws_id)
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


def mark_read(cfg, state, ws_id):
    ref, _ = resolve_cmux_workspace(cfg, state, ws_id)
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


def sweep_orphan_pills(cfg, state):
    """Clear herdr.* status pills that no active herdr workspace owns.

    Orphans appear when the plugin state was lost (e.g. the herdr server was
    reinstalled) while cmux kept the sidebar entries.
    """
    active_keys = {
        status_key(ws_id)
        for ws_id, ws in state["workspaces"].items()
        if ws.get("panes")
    }
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
        has_attention_panes(ws.get("panes", {})) for ws in state["workspaces"].values()
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


def get_workspace(state, ws_id):
    return state["workspaces"].setdefault(ws_id, {"label": "", "panes": {}})


def on_agent_status_changed(cfg, state, data):
    ws_id = data["workspace_id"]
    pane_id = data["pane_id"]
    status = data["agent_status"]
    ws = get_workspace(state, ws_id)
    panes = ws["panes"]
    was_attention = panes.get(pane_id, {}).get("status") in ATTENTION_STATUSES

    if status in TRACKED_STATUSES:
        panes[pane_id] = {
            "status": status,
            "agent": data.get("display_agent") or data.get("agent") or "agent",
            "title": data.get("title") or "",
        }
    else:
        panes.pop(pane_id, None)

    if status in ATTENTION_STATUSES:
        pane = panes[pane_id]
        agent = pane["agent"]
        label = ws.get("label") or ws_id
        if status == "blocked":
            notify(
                cfg,
                state,
                ws_id,
                f"{agent}: waiting for input",
                f"{label} · {pane['title'] or pane_id}",
            )
        else:
            notify(
                cfg,
                state,
                ws_id,
                f"{agent}: finished",
                f"{label} · {pane['title'] or pane_id}",
            )
    elif was_attention and not has_attention_panes(panes):
        # Leaving blocked/done means the prompt was answered or dismissed;
        # the notifications for this workspace have been actioned.
        mark_read(cfg, state, ws_id)

    update_pill(cfg, state, ws_id)


def on_pane_closed(cfg, state, data):
    ws_id = data.get("workspace_id")
    pane_id = data.get("pane_id")
    if not ws_id or not pane_id:
        return
    ws = state["workspaces"].get(ws_id)
    if not ws:
        return
    pane = ws["panes"].pop(pane_id, None)
    if pane is None:
        return
    if pane["status"] in ATTENTION_STATUSES and not has_attention_panes(ws["panes"]):
        mark_read(cfg, state, ws_id)
    update_pill(cfg, state, ws_id)


def on_workspace_closed(cfg, state, data):
    ws_id = data.get("workspace_id")
    if not ws_id:
        return
    ws = state["workspaces"].pop(ws_id, None)
    if ws:
        ref, _ = resolve_cmux_workspace(cfg, state, ws_id)
        if ref:
            cmux_cli(cfg, ["clear-status", status_key(ws_id), "--workspace", ref])


def on_workspace_focused(cfg, state, data):
    ws_id = data.get("workspace_id")
    if not ws_id:
        return
    mark_read(cfg, state, ws_id)


def on_workspace_renamed(cfg, state, data):
    ws_id = data.get("workspace_id")
    if ws_id and ws_id in state["workspaces"]:
        state["workspaces"][ws_id]["label"] = data.get("label") or ""


def handle_event(cfg, state, event):
    data = event.get("data", {})
    kind = data.get("type")
    if kind == "pane_agent_status_changed":
        on_agent_status_changed(cfg, state, data)
    elif kind == "pane_closed":
        on_pane_closed(cfg, state, data)
    elif kind == "workspace_closed":
        on_workspace_closed(cfg, state, data)
    elif kind == "workspace_focused":
        on_workspace_focused(cfg, state, data)
    elif kind == "workspace_renamed":
        on_workspace_renamed(cfg, state, data)


def reconcile(cfg, state):
    previous = state["workspaces"]
    state["workspaces"] = {}
    try:
        agents = herdr_cli(["agent", "list"]).get("agents", [])
    except Exception as e:
        log(f"reconcile: agent list failed: {e}")
        agents = []
    ws_ids = set()
    for a in agents:
        status = a.get("agent_status")
        if status not in TRACKED_STATUSES:
            continue
        ws_id = a["workspace_id"]
        ws_ids.add(ws_id)
        ws = get_workspace(state, ws_id)
        ws["panes"][a["pane_id"]] = {
            "status": status,
            "agent": a.get("display_agent") or a.get("agent") or "agent",
            "title": a.get("terminal_title_stripped") or "",
        }
    for ws_id in ws_ids:
        update_pill(cfg, state, ws_id)
    # Clear pills/notifications left behind by workspaces that no longer have
    # tracked agents (e.g. the session ended while the server was down).
    for ws_id, entry in previous.items():
        panes = entry.get("panes", {})
        if ws_id in state["workspaces"] or not panes:
            continue
        state["workspaces"][ws_id] = {"label": entry.get("label", ""), "panes": {}}
        update_pill(cfg, state, ws_id)
        if has_attention_panes(panes):
            mark_read(cfg, state, ws_id)
        state["workspaces"].pop(ws_id, None)
    # Sweep cmux-side leftovers that the plugin state no longer knows about.
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
    # ":" keeps the pair unambiguous ("a-b", "c" vs "a", "b-c").
    return f"{rcfg['ssh_target']}:{rcfg['session']}"


def remote_key(name):
    return f"{REMOTE_KEY_PREFIX}{name}"


def remote_sock_path(name):
    # AF_UNIX paths are limited to ~104 bytes on macOS, so keep this short
    # and independent of the (unbounded) configured remote name.
    digest = hashlib.sha1(name.encode()).hexdigest()[:8]
    return Path(tempfile.gettempdir()) / f"cmux-herdr-{os.getuid()}-{digest}.sock"


def clear_remote_pill(cfg, name, entry, ref):
    """Clear a pill, remembering it for retry when cmux rejects the call."""
    if cmux_cli(cfg, ["clear-status", remote_key(name), "--workspace", ref]):
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
        if not cmux_cli(cfg, ["clear-status", remote_key(name), "--workspace", ref]):
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
    for ws_id, ws in state.get("workspaces", {}).items():
        if not has_attention_panes(ws.get("panes", {})):
            continue
        label = ws.get("label") or ""
        ref = cfg.get("workspaces", {}).get(ws_id) or cfg.get("labels", {}).get(label)
        if not ref:
            ref = env_ref
        if not ref:
            ref = (state.get("session_ref") or {}).get("ref")
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
        if p["status"] in ATTENTION_STATUSES and old_statuses.get(pid) != p["status"]:
            what = p["title"] or labels.get(p["workspace_id"]) or pid
            body = f"{label} · {what}"
            if p["status"] == "blocked":
                remote_notify(
                    cfg, rstate, name, entry, f"{p['agent']}: waiting for input", body
                )
            else:
                remote_notify(cfg, rstate, name, entry, f"{p['agent']}: finished", body)

    was_attention = has_attention_panes(old)
    entry["panes"] = new
    changed = True
    if was_attention and not has_attention_panes(new):
        # The prompt was answered or dismissed on the remote side.
        remote_mark_read(cfg, rstate, name, entry)
    update_remote_pill(cfg, rstate, name, entry)
    return changed


def resolve_remote_socket(ssh_target, session):
    proc = run(
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            ssh_target,
            "herdr",
            "session",
            "list",
            "--json",
        ],
        timeout=20,
    )
    if proc.returncode != 0:
        raise SocketError(
            f"ssh {ssh_target} herdr session list failed: {proc.stderr.strip()}"
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
        remote_path = resolve_remote_socket(rcfg["ssh_target"], rcfg["session"])
        entry["socket_path"] = remote_path
    if DAEMON_SHUTDOWN.is_set():
        raise SocketError("daemon is shutting down")
    local_sock.unlink(missing_ok=True)
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
    for ref in cmux_workspaces_by_title(cfg).values():
        for key in cmux_herdr_status_keys(cfg, ref):
            if key.startswith(REMOTE_KEY_PREFIX) and key not in active:
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
        if not r.get("name") and (":" in r["ssh_target"] or ":" in r["session"]):
            log(f"remote {r!r}: ':' in ssh_target/session needs an explicit name")
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


def daemon_self_info():
    script = os.path.abspath(__file__)
    try:
        mtime = os.path.getmtime(script)
    except OSError:
        mtime = 0
    return {"pid": os.getpid(), "script": script, "mtime": mtime}


def read_pidfile(pid_path):
    try:
        data = json.loads(pid_path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    if isinstance(data, int):  # pre-JSON format
        return {"pid": data}
    return data if isinstance(data, dict) else {}


def pid_is_remote_daemon(pid):
    """Guard against signaling an unrelated process after PID reuse."""
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
    return any(p.endswith("cmux_herdr.py") for p in parts) and "remote" in parts


def live_daemon_pid(pid_path):
    """PID of the running remote daemon, or None (stale/absent pidfile)."""
    if not pid_path.exists():
        return None
    try:
        pid = int(read_pidfile(pid_path).get("pid", 0))
    except (TypeError, ValueError):
        return None
    if not pid:
        return None
    try:
        os.kill(pid, 0)
    except OSError:
        return None
    if not pid_is_remote_daemon(pid):
        log(f"ignoring stale pidfile: pid {pid} is not a cmux-herdr daemon")
        return None
    return pid


def take_over_daemon(pid_path):
    """Ensure this is the only (and latest-code) remote daemon running."""
    old_pid = live_daemon_pid(pid_path)
    if old_pid is None:
        return
    info = read_pidfile(pid_path)
    self = daemon_self_info()
    if info.get("script") == self["script"] and info.get("mtime") == self["mtime"]:
        log(f"remote daemon already running (pid {old_pid})")
        raise SystemExit(0)
    # The plugin was reinstalled or edited since the old daemon started.
    log(f"replacing outdated remote daemon (pid {old_pid})")
    os.kill(old_pid, signal.SIGTERM)
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            os.kill(old_pid, 0)
        except OSError:
            return
        time.sleep(0.1)
    # The old daemon refuses to die; starting another one would fight over
    # forwarded sockets and state. Retry on the next startup/refresh.
    log(f"outdated remote daemon (pid {old_pid}) did not exit; aborting")
    raise SystemExit(1)


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


def tombstone_worker(cfg, rstate, names, done):
    """Retry retired remotes' pill clears off the scheduling thread.

    Reads shared state but never mutates it; the main thread applies the
    results only for remotes that are still retired.
    """
    for name in names:
        entry = rstate["remotes"].get(name)
        if not entry:
            continue
        pending = entry.get("pending_clear")
        if isinstance(pending, str):  # scalar format from an earlier version
            pending = [pending]
        remaining = []
        for ref in pending or []:
            if not cmux_cli(
                cfg, ["clear-status", remote_key(name), "--workspace", ref]
            ):
                remaining.append(ref)
        done[name] = remaining


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
    pid_path = state_dir / "remote.pid"
    if not remotes:
        # No remotes configured: retire anything a previous daemon left
        # behind (pills, unread notifications, stale state), then exit.
        # A new daemon is spawned by each startup hook and refresh action.
        with open(state_dir / "remote.lock", "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if live_daemon_pid(pid_path) is not None:
                # The running daemon hot-reloads the empty config and
                # retires its remotes itself.
                log("remote daemon already running; leaving cleanup to it")
                return 0
            rstate = load_remote_state()
            for name in list(rstate.get("remotes", {})):
                retire_remote(cfg, rstate, name, {}, {})
            sweep_remote_orphan_pills(cfg, [])
            save_remote_state(rstate)
        log("no [[remotes]] configured; remote daemon exiting")
        return 0
    # Serialize concurrent startup/refresh invocations: only one daemon
    # may take over and write the pidfile.
    with open(state_dir / "remote.lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        take_over_daemon(pid_path)
        pid_path.write_text(json.dumps(daemon_self_info()))
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    rstate = load_remote_state()
    procs = {}
    next_due = {}
    inflight = {}  # name -> worker thread (fetch only; main thread applies)
    results = {}
    tombstone = {"thread": None, "done": {}}
    tombstone_due = {}
    # Seed watched from the startup config so a config change between
    # startup reconciliation and the first loop iteration is still caught.
    watched = {
        remote_name(r): (r["ssh_target"], r["session"], r.get("cmux_title") or "")
        for r in remotes
    }
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
            new_cfg = load_config_strict()  # hot-reload, keep last valid
            if new_cfg is not None:
                if remote_configs(new_cfg) is None:
                    log("ignoring invalid [[remotes]]; keeping previous config")
                else:
                    cfg = new_cfg
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
            finished = tombstone["thread"]
            if finished is not None and not finished.is_alive():
                for name, remaining in tombstone["done"].items():
                    if name in active:
                        continue  # re-added meanwhile; manages its own pills
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
                    tombstone["thread"] = threading.Thread(
                        target=tombstone_worker,
                        args=(cfg, rstate, pending_names, tombstone["done"]),
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


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in {
        "event",
        "reconcile",
        "detect",
        "remote",
    }:
        print("usage: cmux_herdr.py <event|reconcile|detect|remote>", file=sys.stderr)
        return 2
    mode = sys.argv[1]
    cfg = load_config()
    state = load_state()
    try:
        if mode == "remote":
            return remote_daemon(cfg)
        if mode == "event":
            raw = os.environ.get("HERDR_PLUGIN_EVENT_JSON")
            if not raw:
                return 0
            handle_event(cfg, state, json.loads(raw))
        elif mode == "reconcile":
            reconcile(cfg, state)
        else:
            state.pop("session_ref", None)
            ref = detect_session_workspace(cfg, state)
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
