#!/usr/bin/env python3
import json
import os
import re
import subprocess
import sys
import time
import tomllib
from pathlib import Path

ATTENTION_STATUSES = {"blocked", "done"}
TRACKED_STATUSES = ATTENTION_STATUSES | {"working"}

DEFAULT_CMUX_BIN = "/Applications/cmux.app/Contents/Resources/bin/cmux"
STATUS_COLOR_WAITING = "#ff9500"
STATUS_COLOR_WORKING = "#0a84ff"
DETECT_TTL_SECONDS = 300

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


def herdr_session_pids():
    session = os.environ.get("HERDR_SESSION", "")
    try:
        proc = subprocess.run(["ps", "-eo", "pid=,args="], capture_output=True,
                              text=True, timeout=10)
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
            if session and not re.search(rf"--session(=|\s+){re.escape(session)}(\s|$)", args):
                continue
            pids.append(int(pid))
        elif re.search(r"(^|\s)session\s+attach(\s|$)", args):
            if session and not re.search(rf"session\s+attach\s+{re.escape(session)}(\s|$)", args):
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
        except (FileNotFoundError, subprocess.TimeoutExpired, json.JSONDecodeError) as e:
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
        log(f"no cmux workspace mapping for herdr workspace {ws_id} ({label!r}); skipping")
    return ref, label


def status_key(ws_id):
    return f"herdr.{ws_id}"


def update_pill(cfg, state, ws_id):
    ref, _ = resolve_cmux_workspace(cfg, state, ws_id)
    if not ref:
        return
    panes = state["workspaces"].get(ws_id, {}).get("panes", {})
    waiting = sum(1 for p in panes.values() if p["status"] in ATTENTION_STATUSES)
    working = sum(1 for p in panes.values() if p["status"] == "working")
    key = status_key(ws_id)
    if waiting:
        parts = [f"{waiting} waiting"]
        if working:
            parts.append(f"{working} working")
        cmux_cli(cfg, ["set-status", key, " · ".join(parts),
                       "--workspace", ref, "--color", STATUS_COLOR_WAITING,
                       "--priority", "20"])
    elif working:
        cmux_cli(cfg, ["set-status", key, f"{working} working",
                       "--workspace", ref, "--color", STATUS_COLOR_WORKING,
                       "--priority", "10"])
    else:
        cmux_cli(cfg, ["clear-status", key, "--workspace", ref])


def notify(cfg, state, ws_id, title, body):
    ref, _ = resolve_cmux_workspace(cfg, state, ws_id)
    if not ref:
        return
    cmux_cli(cfg, ["notify", "--title", title, "--body", body, "--workspace", ref])


def get_workspace(state, ws_id):
    return state["workspaces"].setdefault(ws_id, {"label": "", "panes": {}})


def on_agent_status_changed(cfg, state, data):
    ws_id = data["workspace_id"]
    pane_id = data["pane_id"]
    status = data["agent_status"]
    ws = get_workspace(state, ws_id)
    panes = ws["panes"]

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
            notify(cfg, state, ws_id, f"{agent}: waiting for input",
                   f"{label} · {pane['title'] or pane_id}")
        else:
            notify(cfg, state, ws_id, f"{agent}: finished",
                   f"{label} · {pane['title'] or pane_id}")

    update_pill(cfg, state, ws_id)


def on_pane_closed(cfg, state, data):
    ws_id = data.get("workspace_id")
    pane_id = data.get("pane_id")
    if not ws_id or not pane_id:
        return
    ws = state["workspaces"].get(ws_id)
    if ws and ws["panes"].pop(pane_id, None) is not None:
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
    ref, _ = resolve_cmux_workspace(cfg, state, ws_id)
    if ref:
        cmux_cli(cfg, ["mark-notification-read", "--workspace", ref])


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


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in {"event", "reconcile", "detect"}:
        print("usage: cmux_herdr.py <event|reconcile|detect>", file=sys.stderr)
        return 2
    mode = sys.argv[1]
    cfg = load_config()
    state = load_state()
    try:
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
            print(json.dumps({
                "cmux_workspace": ref,
                "herdr_session": os.environ.get("HERDR_SESSION"),
                "candidate_pids": herdr_session_pids(),
            }))
        save_state(state)
    except Exception as e:
        log(f"unhandled error in {mode}: {e!r}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
