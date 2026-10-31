"""Legacy hook event handling and reconciliation."""

import json
import os
import subprocess

from cmux_herdr_core import cmux, core, remote_bridge, watcher


def sweep_orphan_pills(cfg, state):
    """Clear herdr.* status pills that no active herdr workspace owns.

    Orphans appear when the plugin state was lost (e.g. the herdr server was
    reinstalled) while cmux kept the sidebar entries. Active keys are the
    union over all session buckets, so one session's reconcile never sweeps
    another session's pills.
    """
    active_keys = set()
    for _, ws_id, ws in core.all_workspaces(state):
        if not ws.get("panes"):
            continue
        active_keys.add(cmux.status_key(ws_id))
        for pane_id in ws["panes"]:
            active_keys.add(cmux.pane_key(cmux.status_key(ws_id), pane_id))
    for ref in cmux.cmux_workspaces_by_title(cfg).values():
        for key in cmux.cmux_herdr_status_keys(cfg, ref):
            if key.startswith(core.REMOTE_KEY_PREFIX):
                continue  # owned by the remote daemon
            if key not in active_keys:
                core.log(f"clearing orphaned status {key} on {ref}")
                cmux.cmux_cli(cfg, ["clear-status", key, "--workspace", ref])


def is_our_notification(item):
    if not isinstance(item, dict):
        return False
    if item.get("subtitle") == core.NOTIFICATION_SUBTITLE:
        return True
    # Notifications sent before the subtitle tag existed
    return bool(core.NOTIFICATION_TITLE_RE.search(str(item.get("title") or "")))


def sweep_notifications(cfg, state):
    """Do not infer ownership of legacy notices from their title or subtitle."""
    return


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
    if removed["status"] in core.ATTENTION_STATUSES and not cmux.has_attention_panes(
        panes
    ):
        # Leaving blocked/done means the prompt was answered or the finished
        # pane was viewed; the notifications for this workspace are actioned.
        cmux.mark_read(cfg, bucket, ws_id)
    cmux.clear_pane_pill(cfg, bucket, ws_id, pane_id)
    cmux.update_pill(cfg, bucket, ws_id)
    return True


def on_agent_status_changed(cfg, bucket, data):
    ws_id = data["workspace_id"]
    pane_id = data["pane_id"]
    status = data["agent_status"]
    ws = get_workspace(bucket, ws_id)
    panes = ws["panes"]
    was_attention = panes.get(pane_id, {}).get("status") in core.ATTENTION_STATUSES

    if status == "unknown" and panes.get(pane_id, {}).get("status") == "blocked":
        return

    if status not in core.TRACKED_STATUSES:
        # forget_pane republishes the pill itself when it removes something.
        if not forget_pane(cfg, bucket, ws_id, pane_id):
            cmux.update_pill(cfg, bucket, ws_id)
        return

    panes[pane_id] = {
        "status": status,
        "agent": data.get("display_agent") or data.get("agent") or "agent",
        "title": data.get("title") or "",
    }

    if status in core.ATTENTION_STATUSES:
        pane = panes[pane_id]
        agent = pane["agent"]
        label = ws.get("label") or ws_id
        if status == "blocked":
            sent = cmux.notify(
                cfg,
                bucket,
                ws_id,
                f"{agent}: waiting for input",
                f"{label} · {pane['title'] or pane_id}",
            )
        else:
            sent = cmux.notify(
                cfg,
                bucket,
                ws_id,
                f"{agent}: finished",
                f"{label} · {pane['title'] or pane_id}",
            )
        if not sent:
            pane["notification_pending"] = True
    elif was_attention and not cmux.has_attention_panes(panes):
        # Answering the prompt retires the notification just as viewing the
        # finished pane does; forget_pane covers the leaving-tracked case.
        cmux.mark_read(cfg, bucket, ws_id)

    cmux.update_pill(cfg, bucket, ws_id)


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
    cmux.clear_pane_pill(cfg, bucket, ws_id, pane_id)
    if pane["status"] in core.ATTENTION_STATUSES and not cmux.has_attention_panes(
        ws["panes"]
    ):
        cmux.mark_read(cfg, bucket, ws_id)
    cmux.update_pill(cfg, bucket, ws_id)


def on_workspace_closed(cfg, bucket, data):
    ws_id = data.get("workspace_id")
    if not ws_id:
        return
    ws = bucket["workspaces"].pop(ws_id, None)
    if ws:
        ref, _ = cmux.resolve_cmux_workspace(cfg, bucket, ws_id)
        if ref:
            cmux.cmux_cli(
                cfg, ["clear-status", cmux.status_key(ws_id), "--workspace", ref]
            )
            for pane_id in ws["panes"]:
                cmux.cmux_cli(
                    cfg,
                    [
                        "clear-status",
                        cmux.pane_key(cmux.status_key(ws_id), pane_id),
                        "--workspace",
                        ref,
                    ],
                )


def on_workspace_focused(cfg, bucket, data):
    """Workspace focus does not identify the agent pane receiving input."""
    return


def retry_legacy_notifications(cfg, *, session=None):
    """Retry only attention episodes that the legacy publisher deferred."""
    sock_path = watcher.session_socket_path(session)
    if not sock_path:
        return
    with core.state_lock():
        if (core.state_dir / "v1-enabled").exists():
            return
        try:
            agents = remote_bridge.socket_rpc(sock_path, "agent.list", {}).get("agents")
            if not isinstance(agents, list) or any(
                not isinstance(a, dict) for a in agents
            ):
                return
            observed = {a.get("pane_id"): a.get("agent_status") for a in agents}
            if len(observed) != len(agents):
                return
        except (remote_bridge.SocketError, OSError, ValueError, TypeError):
            return
        state = core.load_state()
        bucket = core.session_bucket(state, session)
        changed = False
        for ws_id, workspace in bucket.get("workspaces", {}).items():
            for pane_id, pane in workspace.get("panes", {}).items():
                status = pane.get("status")
                if (
                    not pane.get("notification_pending")
                    or status not in core.ATTENTION_STATUSES
                    or observed.get(pane_id) != status
                ):
                    continue
                label = workspace.get("label") or ws_id
                title = (
                    f"{pane['agent']}: waiting for input"
                    if status == "blocked"
                    else f"{pane['agent']}: finished"
                )
                if cmux.notify(
                    cfg,
                    bucket,
                    ws_id,
                    title,
                    f"{label} · {pane.get('title') or pane_id}",
                ):
                    pane.pop("notification_pending", None)
                    changed = True
        if changed:
            core.save_state(state)


def on_workspace_renamed(cfg, bucket, data):
    ws_id = data.get("workspace_id")
    if ws_id and ws_id in bucket["workspaces"]:
        bucket["workspaces"][ws_id]["label"] = data.get("label") or ""


def handle_event(cfg, state, event, *, session=None):
    bucket = core.session_bucket(state, session)
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
        cmux.clear_pane_pill(cfg, bucket, ws_id, pane_id)
    bucket["workspaces"][ws_id] = {"label": entry.get("label", ""), "panes": {}}
    cmux.update_pill(cfg, bucket, ws_id)
    if cmux.has_attention_panes(panes):
        cmux.mark_read(cfg, bucket, ws_id)
    bucket["workspaces"].pop(ws_id, None)


def known_sessions(*, running_only=False):
    """Names of known Herdr sessions, or None when discovery fails."""
    binary = os.environ.get("HERDR_BIN_PATH", "herdr")
    try:
        proc = cmux.run([binary, "session", "list", "--json"])
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
        core.log(f"cleared state of ended herdr session {name!r}")


def reconcile(cfg, state):
    try:
        agents = cmux.herdr_cli(["agent", "list"]).get("agents", [])
    except Exception as e:
        core.log(f"reconcile: agent list failed: {e}")
        return
    bucket = core.session_bucket(state)
    previous = dict(bucket["workspaces"])
    bucket["workspaces"].clear()
    ws_ids = set()
    for a in agents:
        status = a.get("agent_status")
        if status == "unknown":
            ws_id, pane_id = a.get("workspace_id"), a.get("pane_id")
            old = previous.get(ws_id, {}).get("panes", {}).get(pane_id, {})
            if old.get("status") == "blocked":
                ws_ids.add(ws_id)
                get_workspace(bucket, ws_id)["panes"][pane_id] = old
            continue
        if status not in core.TRACKED_STATUSES:
            continue
        ws_id = a["workspace_id"]
        ws_ids.add(ws_id)
        ws = get_workspace(bucket, ws_id)
        ws["panes"][a["pane_id"]] = {
            "status": status,
            "agent": a.get("display_agent") or a.get("agent") or "agent",
            "title": a.get("terminal_title_stripped") or "",
        }
        old = previous.get(ws_id, {}).get("panes", {}).get(a["pane_id"], {})
        if old.get("status") == status and old.get("notification_pending"):
            ws["panes"][a["pane_id"]]["notification_pending"] = True
    for ws_id in ws_ids:
        cmux.update_pill(cfg, bucket, ws_id)
    # Clear pills/notifications left behind by workspaces that no longer have
    # tracked agents (e.g. the session ended while the server was down). Only
    # this session's bucket is rebuilt: agent list is session-scoped, so
    # other sessions' workspaces must be left alone.
    for ws_id, entry in previous.items():
        panes = entry.get("panes", {})
        if ws_id in bucket["workspaces"]:
            # Panes that vanished since the last run lose their pills.
            for pane_id in set(panes) - set(bucket["workspaces"][ws_id]["panes"]):
                cmux.clear_pane_pill(cfg, bucket, ws_id, pane_id)
            continue
        retire_workspace(cfg, bucket, ws_id, entry)
    # Sweep sessions that ended without restarting, then cmux-side leftovers
    # that the plugin state no longer knows about.
    prune_dead_sessions(cfg, state)
    sweep_orphan_pills(cfg, state)
    sweep_notifications(cfg, state)
