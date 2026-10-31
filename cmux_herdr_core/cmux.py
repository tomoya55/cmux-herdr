"""cmux and herdr command wrappers and the legacy sidebar pills."""

import json
import os
import re
import subprocess
import time

from cmux_herdr_core import core


def run(cmd, timeout=10):
    env = {
        k: v for k, v in os.environ.items() if k not in core.CMUX_CALLER_IDENTITY_VARS
    }
    context = core.LEGACY_REPLAY_CONTEXT.get()
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
    binary = cfg.get("cmux_bin") or core.DEFAULT_CMUX_BIN
    try:
        proc = run([binary, *args])
    except FileNotFoundError:
        core.log(f"cmux binary not found: {binary}")
        return False
    except subprocess.TimeoutExpired:
        core.log(f"cmux {' '.join(args)} timed out")
        return False
    if proc.returncode != 0:
        core.log(f"cmux {' '.join(args)} failed: {proc.stderr.strip()}")
        return False
    return True


def cmux_workspaces_by_title(cfg):
    binary = cfg.get("cmux_bin") or core.DEFAULT_CMUX_BIN
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
    binary = cfg.get("cmux_bin") or core.DEFAULT_CMUX_BIN
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
    binary = cfg.get("cmux_bin") or core.DEFAULT_CMUX_BIN
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


def cmux_notifications_checked(cfg):
    """Return None on failure so an unknown notification list is not empty."""
    binary = cfg.get("cmux_bin") or core.DEFAULT_CMUX_BIN
    try:
        proc = run([binary, "list-notifications", "--json"])
        if proc.returncode:
            return None
        result = json.loads(proc.stdout)
        return result if isinstance(result, list) else None
    except (OSError, subprocess.TimeoutExpired, ValueError):
        return None


def herdr_session_pids():
    session = core.current_session()
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
    context = core.LEGACY_REPLAY_CONTEXT.get()
    env_ref = (
        context.get("workspace")
        if context is not None
        else os.environ.get("CMUX_WORKSPACE_ID")
    )
    if env_ref:
        return env_ref

    cached = bucket.get("session_ref") or {}
    if (
        cached.get("ref")
        and time.time() - cached.get("at", 0) < core.DETECT_TTL_SECONDS
    ):
        return cached["ref"]

    ref = None
    pids = herdr_session_pids()
    if pids:
        binary = cfg.get("cmux_bin") or core.DEFAULT_CMUX_BIN
        try:
            proc = run([binary, "top", "--all", "--processes", "--json"], timeout=20)
            if proc.returncode == 0:
                ref = find_pid_workspace(json.loads(proc.stdout), set(pids))
        except (
            FileNotFoundError,
            subprocess.TimeoutExpired,
            json.JSONDecodeError,
        ) as e:
            core.log(f"session workspace detection failed: {e}")
    if ref:
        bucket["session_ref"] = {"ref": ref, "at": time.time()}
        core.log(f"detected cmux workspace {ref} for this herdr session")
    elif cached.get("ref"):
        return cached["ref"]
    return ref


def workspace_label(ws_id):
    try:
        for ws in herdr_cli(["workspace", "list"]).get("workspaces", []):
            if ws.get("workspace_id") == ws_id or ws.get("id") == ws_id:
                return ws.get("label") or ""
    except Exception as e:
        core.log(f"workspace list failed: {e}")
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
        core.log(
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


def push_pill(cfg, ref, key, panes, *, is_current=lambda: True):
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
    if not is_current():
        return False
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
            color, icon, priority = core.STATUS_COLOR_WAITING, core.ICON_WAITING, "20"
        elif done:
            color, icon, priority = core.STATUS_COLOR_DONE, core.ICON_DONE, "18"
        else:
            color, icon, priority = core.STATUS_COLOR_WORKING, core.ICON_WORKING, "10"
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
        if not is_current():
            return False
        status = pane["status"]
        cmux_cli(
            cfg,
            [
                "set-status",
                pane_key(key, pane_id),
                f"{pane['agent']}: {core.PANE_STATUS_WORDS.get(status, status)}",
                "--workspace",
                ref,
                "--color",
                core.PANE_STATUS_COLORS.get(status, core.STATUS_COLOR_WORKING),
                "--priority",
                core.PANE_STATUS_PRIORITIES.get(status, "5"),
                "--icon",
                core.PANE_STATUS_ICONS.get(status, core.ICON_WORKING),
            ],
        )
    return True


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


def legacy_workspace_aliases(cfg, ref):
    """Resolve a workspace reference to both its UUID and short reference."""
    binary = cfg.get("cmux_bin") or core.DEFAULT_CMUX_BIN
    try:
        proc = run(
            [binary, "--id-format", "both", "top", "--all", "--processes", "--json"],
            timeout=20,
        )
        if proc.returncode:
            return None
        tree = json.loads(proc.stdout)
        workspaces = [
            workspace
            for window in tree["windows"]
            for workspace in window["workspaces"]
        ]
        matches = [
            workspace
            for workspace in workspaces
            if any(
                isinstance(workspace.get(key), str)
                and workspace[key].casefold() == ref.casefold()
                for key in ("id", "ref")
            )
        ]
        if len(matches) != 1:
            return None
        aliases = {matches[0].get("id"), matches[0].get("ref")}
        if any(not isinstance(value, str) or not value for value in aliases):
            return None
        return {value.casefold() for value in aliases}
    except (
        OSError,
        ValueError,
        TypeError,
        KeyError,
        AttributeError,
        subprocess.TimeoutExpired,
    ):
        return None


def legacy_notification_preflight(cfg, ref):
    """Defer legacy creation while an unowned unread notification may be replaced."""
    visible = cmux_notifications_checked(cfg)
    if visible is None or any(
        not isinstance(item, dict) or type(item.get("is_read")) is not bool
        for item in visible
    ):
        return False
    unread = [item for item in visible if not item["is_read"]]
    if not unread:
        return True
    aliases = legacy_workspace_aliases(cfg, ref)
    return aliases is not None and all(
        isinstance(item.get("workspace_id"), str)
        and bool(item["workspace_id"])
        and item["workspace_id"].casefold() not in aliases
        for item in unread
    )


def notify(cfg, bucket, ws_id, title, body):
    ref, _ = resolve_cmux_workspace(cfg, bucket, ws_id)
    if not ref:
        return False
    if not legacy_notification_preflight(cfg, ref):
        return False
    return cmux_cli(
        cfg,
        [
            "notify",
            "--title",
            title,
            "--subtitle",
            core.NOTIFICATION_SUBTITLE,
            "--body",
            body,
            "--workspace",
            ref,
        ],
    )


def mark_read(cfg, bucket, ws_id):
    """Legacy notifications lack IDs; workspace-wide reads would affect others."""
    return


def has_attention_panes(panes):
    return any(p["status"] in core.ATTENTION_STATUSES for p in panes.values())
