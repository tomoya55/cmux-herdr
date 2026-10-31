import json
from types import SimpleNamespace

from helpers import remote_agent, status_event

from cmux_herdr_core import cmux, core, legacy_events, remote_bridge, watcher


def test_blocked_notifies_and_sets_waiting_pill(cfg, state, cmux_calls):
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))

    notify = [c for c in cmux_calls if c[0] == "notify"]
    assert len(notify) == 1
    assert "claude: waiting for input" in notify[0]
    assert "workspace:1" in notify[0]

    pills = [c for c in cmux_calls if c[0] == "set-status"]
    assert [c[1] for c in pills] == ["herdr.w1", "herdr.w1.w1_p1"]
    assert pills[0][2] == "1 waiting"
    assert "#ff9500" in pills[0]


def test_done_notifies_finished(cfg, state, cmux_calls):
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "done"))

    notify = [c for c in cmux_calls if c[0] == "notify"]
    assert len(notify) == 1
    assert "claude: finished" in notify[0]


def test_legacy_read_does_not_mark_unrelated_workspace_notifications(
    cfg, state, cmux_calls
):
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "done"))
    cmux_calls.clear()
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "idle"))
    assert not [call for call in cmux_calls if call[0] == "mark-notification-read"]


def test_legacy_notification_defers_when_an_unread_notice_exists(
    monkeypatch, cfg, state, cmux_calls
):
    monkeypatch.setattr(
        cmux, "cmux_notifications_checked", lambda cfg: [{"is_read": False}]
    )
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "done"))
    assert not [call for call in cmux_calls if call[0] == "notify"]


def test_legacy_notification_is_not_blocked_by_other_workspace(
    monkeypatch, cfg, state, cmux_calls
):
    monkeypatch.setattr(
        cmux,
        "cmux_notifications_checked",
        lambda cfg: [{"is_read": False, "workspace_id": "other"}],
    )
    monkeypatch.setattr(
        cmux, "legacy_workspace_aliases", lambda cfg, ref: {"workspace:1", "target"}
    )
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "done"))
    assert len([call for call in cmux_calls if call[0] == "notify"]) == 1


def test_legacy_notification_defers_unread_with_empty_workspace(
    monkeypatch, cfg, state, cmux_calls
):
    monkeypatch.setattr(
        cmux,
        "cmux_notifications_checked",
        lambda cfg: [{"is_read": False, "workspace_id": ""}],
    )
    monkeypatch.setattr(
        cmux, "legacy_workspace_aliases", lambda cfg, ref: {"workspace:1", "target"}
    )
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "done"))
    assert not [call for call in cmux_calls if call[0] == "notify"]


def test_legacy_notification_retries_when_other_notice_is_read(
    monkeypatch, tmp_path, cfg, state, cmux_calls
):
    monkeypatch.setattr(
        watcher, "session_socket_path", lambda session=None: "/source.sock"
    )
    monkeypatch.setattr(
        remote_bridge,
        "socket_rpc",
        lambda *args: {"agents": [remote_agent("w1:p1", "done")]},
    )
    visible = [{"is_read": False}]
    monkeypatch.setattr(cmux, "cmux_notifications_checked", lambda cfg: visible)
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "done"))
    core.save_state(state)
    assert not [call for call in cmux_calls if call[0] == "notify"]
    visible.clear()
    legacy_events.retry_legacy_notifications(cfg)
    assert len([call for call in cmux_calls if call[0] == "notify"]) == 1
    legacy_events.retry_legacy_notifications(cfg)
    assert len([call for call in cmux_calls if call[0] == "notify"]) == 1


def test_reconcile_preserves_pending_notification_for_same_attention(
    monkeypatch, cfg, state, cmux_calls
):
    monkeypatch.setattr(
        cmux, "cmux_notifications_checked", lambda cfg: [{"is_read": False}]
    )
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    monkeypatch.setattr(
        cmux, "herdr_cli", lambda args: {"agents": [remote_agent("w1:p1", "blocked")]}
    )
    monkeypatch.setattr(legacy_events, "prune_dead_sessions", lambda *args: None)
    monkeypatch.setattr(legacy_events, "sweep_orphan_pills", lambda *args: None)
    legacy_events.reconcile(cfg, state)
    assert state["sessions"][""]["workspaces"]["w1"]["panes"]["w1:p1"][
        "notification_pending"
    ]


def test_reconcile_preserves_blocked_when_status_unknown(
    monkeypatch, cfg, state, cmux_calls
):
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    monkeypatch.setattr(
        cmux, "herdr_cli", lambda args: {"agents": [remote_agent("w1:p1", "unknown")]}
    )
    monkeypatch.setattr(legacy_events, "prune_dead_sessions", lambda *args: None)
    monkeypatch.setattr(legacy_events, "sweep_orphan_pills", lambda *args: None)
    legacy_events.reconcile(cfg, state)
    assert (
        state["sessions"][""]["workspaces"]["w1"]["panes"]["w1:p1"]["status"]
        == "blocked"
    )


def test_legacy_discards_deferred_notification_after_wait_ends(
    monkeypatch, tmp_path, cfg, state, cmux_calls
):
    monkeypatch.setattr(
        watcher, "session_socket_path", lambda session=None: "/source.sock"
    )
    monkeypatch.setattr(
        remote_bridge,
        "socket_rpc",
        lambda *args: {"agents": [remote_agent("w1:p1", "working")]},
    )
    visible = [{"is_read": False}]
    monkeypatch.setattr(cmux, "cmux_notifications_checked", lambda cfg: visible)
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    core.save_state(state)
    visible.clear()
    legacy_events.retry_legacy_notifications(cfg)
    assert not [call for call in cmux_calls if call[0] == "notify"]


def test_legacy_retry_requires_current_matching_status(
    monkeypatch, tmp_path, cfg, state, cmux_calls
):
    visible = [{"is_read": False}]
    monkeypatch.setattr(cmux, "cmux_notifications_checked", lambda cfg: visible)
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "done"))
    core.save_state(state)
    visible.clear()
    monkeypatch.setattr(
        watcher, "session_socket_path", lambda session=None: "/source.sock"
    )
    monkeypatch.setattr(
        remote_bridge,
        "socket_rpc",
        lambda *args: {"agents": [remote_agent("w1:p1", "idle")]},
    )
    legacy_events.retry_legacy_notifications(cfg)
    assert not [call for call in cmux_calls if call[0] == "notify"]


def test_legacy_retry_observes_agent_after_acquiring_state_lock(
    monkeypatch, tmp_path, cfg
):
    monkeypatch.setattr(
        watcher, "session_socket_path", lambda session=None: "/source.sock"
    )
    order = []

    class Lock:
        def __enter__(self):
            order.append("lock")

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(core, "state_lock", Lock)
    monkeypatch.setattr(
        remote_bridge,
        "socket_rpc",
        lambda *args: order.append("observe") or {"agents": []},
    )
    legacy_events.retry_legacy_notifications(cfg)
    assert order == ["lock", "observe"]


def test_legacy_unknown_status_preserves_waiting_attention(cfg, state, cmux_calls):
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "unknown"))
    pane = state["sessions"][""]["workspaces"]["w1"]["panes"]["w1:p1"]
    assert pane["status"] == "blocked"


def test_watch_unknown_status_preserves_waiting_attention(cfg, state, cmux_calls):
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    watcher.apply_watch_event(
        cfg,
        state,
        "/source.sock",
        status_event("w1:p1", "w1", "unknown")["data"],
        tracked=False,
    )
    pane = state["sessions"][""]["workspaces"]["w1"]["panes"]["w1:p1"]
    assert pane["status"] == "blocked"


def test_done_sets_finished_pill(cfg, state, cmux_calls):
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "done"))

    pills = [c for c in cmux_calls if c[0] == "set-status"]
    assert pills[0][1] == "herdr.w1"
    assert pills[0][2] == "1 finished"
    assert "#30d158" in pills[0]
    assert pills[0][-2:] == ["--icon", "checkmark.circle"]
    pane_pill = next(c for c in pills if c[1] == "herdr.w1.w1_p1")
    assert pane_pill[2] == "claude: finished"
    assert "#30d158" in pane_pill


def test_viewing_finished_pane_clears_its_entries(cfg, state, cmux_calls):
    # herdr flips done to idle when the user views the pane; the finished
    # signal is read and must disappear from the sidebar.
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "done"))
    cmux_calls.clear()
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "idle"))

    clears = [c for c in cmux_calls if c[0] == "clear-status"]
    assert ["clear-status", "herdr.w1.w1_p1", "--workspace", "workspace:1"] in clears
    assert ["clear-status", "herdr.w1", "--workspace", "workspace:1"] in clears
    read = [c for c in cmux_calls if c[0] == "mark-notification-read"]
    assert read == []


def test_working_sets_pill_without_notification(cfg, state, cmux_calls):
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))

    assert [c for c in cmux_calls if c[0] == "notify"] == []
    pills = [c for c in cmux_calls if c[0] == "set-status"]
    assert pills[0][2] == "1 working"
    assert "#0a84ff" in pills[0]


def test_aggregates_across_panes(cfg, state, cmux_calls):
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    legacy_events.handle_event(
        cfg, state, status_event("w1:p2", "w1", "done", agent="codex")
    )
    cmux_calls.clear()
    legacy_events.handle_event(
        cfg, state, status_event("w1:p3", "w1", "working", agent="pi")
    )

    pills = [c for c in cmux_calls if c[0] == "set-status"]
    assert pills[0][2] == "1 waiting · 1 finished · 1 working"


def test_idle_removes_pane_and_clears_pill(cfg, state, workspaces, cmux_calls):
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    cmux_calls.clear()
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "idle"))

    assert workspaces["w1"]["panes"] == {}
    assert [c for c in cmux_calls if c[0] == "notify"] == []
    clears = [c for c in cmux_calls if c[0] == "clear-status"]
    assert [c[1] for c in clears] == ["herdr.w1.w1_p1", "herdr.w1"]


def test_pane_closed_updates_pill(cfg, state, cmux_calls):
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    legacy_events.handle_event(cfg, state, status_event("w1:p2", "w1", "working"))
    cmux_calls.clear()
    legacy_events.handle_event(
        cfg,
        state,
        {
            "event": "pane_closed",
            "data": {"type": "pane_closed", "pane_id": "w1:p1", "workspace_id": "w1"},
        },
    )

    pill = [c for c in cmux_calls if c[0] == "set-status"]
    assert pill[0][2] == "1 working"


def test_leaving_attention_does_not_mark_unowned_notifications_read(
    cfg, state, cmux_calls
):
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    cmux_calls.clear()
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))

    read = [c for c in cmux_calls if c[0] == "mark-notification-read"]
    assert read == []


def test_local_mark_read_respects_remote_attention(
    monkeypatch, cfg, state, cmux_calls, tmp_path
):
    core.save_remote_state(
        {
            "remotes": {
                "maguro-tom": {
                    "panes": {"w9:p1": {"status": "blocked"}},
                    "ref": "workspace:1",
                }
            }
        }
    )
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    cmux_calls.clear()
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))

    # the local pane leaving attention must not clear the remote's
    # still-unresolved notification on the same cmux workspace
    assert [c for c in cmux_calls if c[0] == "mark-notification-read"] == []


def test_other_attention_pane_keeps_notifications_unread(cfg, state, cmux_calls):
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    legacy_events.handle_event(cfg, state, status_event("w1:p2", "w1", "blocked"))
    cmux_calls.clear()
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))

    assert [c for c in cmux_calls if c[0] == "mark-notification-read"] == []


def test_pane_closed_does_not_mark_unowned_notifications_read(cfg, state, cmux_calls):
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    cmux_calls.clear()
    legacy_events.handle_event(
        cfg,
        state,
        {
            "event": "pane_closed",
            "data": {"type": "pane_closed", "pane_id": "w1:p1", "workspace_id": "w1"},
        },
    )

    read = [c for c in cmux_calls if c[0] == "mark-notification-read"]
    assert read == []


def test_workspace_closed_clears_status(cfg, state, workspaces, cmux_calls):
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    cmux_calls.clear()
    legacy_events.handle_event(
        cfg,
        state,
        {
            "event": "workspace_closed",
            "data": {
                "type": "workspace_closed",
                "workspace_id": "w1",
                "workspace": None,
            },
        },
    )

    assert "w1" not in workspaces
    clears = [c for c in cmux_calls if c[0] == "clear-status"]
    assert [c[1] for c in clears] == ["herdr.w1", "herdr.w1.w1_p1"]


def test_pill_includes_icon(cfg, state, cmux_calls):
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    pill = [c for c in cmux_calls if c[0] == "set-status"]
    assert pill[0][-2:] == ["--icon", "hourglass"]

    cmux_calls.clear()
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    pill = [c for c in cmux_calls if c[0] == "set-status"]
    assert pill[0][-2:] == ["--icon", "bolt"]


def test_per_pane_pill_set(cfg, state, cmux_calls):
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    pills = [c for c in cmux_calls if c[0] == "set-status"]
    assert len(pills) == 2
    pane_pill = next(c for c in pills if c[1] == "herdr.w1.w1_p1")
    assert pane_pill[2] == "claude: waiting"
    assert "#ff9500" in pane_pill
    assert pane_pill[-2:] == ["--icon", "hourglass"]


def test_per_pane_pill_cleared_on_idle(cfg, state, cmux_calls):
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    cmux_calls.clear()
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "idle"))

    clears = [c for c in cmux_calls if c[0] == "clear-status"]
    assert ["clear-status", "herdr.w1.w1_p1", "--workspace", "workspace:1"] in clears
    assert ["clear-status", "herdr.w1", "--workspace", "workspace:1"] in clears


def test_per_pane_pill_cleared_on_pane_closed(cfg, state, cmux_calls):
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    cmux_calls.clear()
    legacy_events.handle_event(
        cfg,
        state,
        {
            "event": "pane_closed",
            "data": {"type": "pane_closed", "pane_id": "w1:p1", "workspace_id": "w1"},
        },
    )
    clears = [c for c in cmux_calls if c[0] == "clear-status"]
    assert ["clear-status", "herdr.w1.w1_p1", "--workspace", "workspace:1"] in clears


def test_per_pane_pills_cleared_on_workspace_closed(cfg, state, cmux_calls):
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    legacy_events.handle_event(
        cfg, state, status_event("w1:p2", "w1", "working", agent="codex")
    )
    cmux_calls.clear()
    legacy_events.handle_event(
        cfg,
        state,
        {
            "event": "workspace_closed",
            "data": {
                "type": "workspace_closed",
                "workspace_id": "w1",
                "workspace": None,
            },
        },
    )
    clears = [c for c in cmux_calls if c[0] == "clear-status"]
    assert ["clear-status", "herdr.w1", "--workspace", "workspace:1"] in clears
    assert ["clear-status", "herdr.w1.w1_p1", "--workspace", "workspace:1"] in clears
    assert ["clear-status", "herdr.w1.w1_p2", "--workspace", "workspace:1"] in clears


def test_reconcile_clears_removed_pane_pills(
    monkeypatch, cfg, state, workspaces, cmux_calls
):
    workspaces["w1"] = {
        "label": "",
        "panes": {
            "w1:p1": {"status": "blocked", "agent": "claude", "title": "fix"},
            "w1:p2": {"status": "working", "agent": "codex", "title": "build"},
        },
    }
    monkeypatch.setattr(
        cmux,
        "herdr_cli",
        lambda args: {
            "agents": [
                {
                    "pane_id": "w1:p1",
                    "workspace_id": "w1",
                    "agent": "claude",
                    "agent_status": "blocked",
                    "terminal_title_stripped": "fix",
                },
            ],
        },
    )

    legacy_events.reconcile(cfg, state)

    clears = [c for c in cmux_calls if c[0] == "clear-status"]
    # only the vanished pane's pill is cleared
    assert clears == [["clear-status", "herdr.w1.w1_p2", "--workspace", "workspace:1"]]


def test_sweep_keeps_active_per_pane_keys(monkeypatch, cfg, state):
    agent = {
        "pane_id": "w1:p1",
        "workspace_id": "w1",
        "agent": "claude",
        "agent_status": "working",
        "terminal_title_stripped": "task",
    }
    responses = {
        "list-workspaces": "workspace:1  hd:tom\n",
        ("list-status", "workspace:1"): (
            "herdr.w1=1 working color=#0a84ff priority=10\n"
            "herdr.w1.w1_p1=claude: working color=#0a84ff priority=5\n"
        ),
    }

    calls = reconcile_run(monkeypatch, responses, agents=[agent])
    legacy_events.reconcile(cfg, state)
    assert [c for c in calls if c[0] == "clear-status"] == []


def test_reconcile_preserves_workspace_sidebar_logs(monkeypatch, cfg, state):
    core.session_bucket(state)["workspaces"]["w2"] = {"label": "other", "panes": {}}
    responses = {
        "list-workspaces": "workspace:1  hd:tom\nworkspace:2  other\nworkspace:3  x\n",
        ("list-status", "workspace:1"): "herdr.w1=1 waiting color=#ff9500\n",
        ("list-status", "workspace:2"): "",
        ("list-status", "workspace:3"): "",
    }
    calls = reconcile_run(monkeypatch, responses, agents=[])

    legacy_events.reconcile(cfg, state)
    clears = [c for c in calls if c[0] == "clear-log"]
    assert clears == []

    calls.clear()
    legacy_events.reconcile(cfg, state)
    assert [c for c in calls if c[0] == "clear-log"] == []


def test_reconcile_defers_log_migration_when_cmux_unreachable(monkeypatch, cfg, state):
    calls = reconcile_run(monkeypatch, {}, agents=[])

    legacy_events.reconcile(cfg, state)

    assert [c for c in calls if c[0] == "clear-log"] == []
    assert "legacy_log_cleared" not in state


def test_workspace_focus_does_not_acknowledge_agent_attention(cfg, state, cmux_calls):
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    cmux_calls.clear()
    legacy_events.handle_event(
        cfg,
        state,
        {
            "event": "workspace_focused",
            "data": {"type": "workspace_focused", "workspace_id": "w1"},
        },
    )

    assert not any(call[0] == "mark-notification-read" for call in cmux_calls)


def test_workspace_renamed_updates_label(cfg, state, workspaces, cmux_calls):
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    legacy_events.handle_event(
        cfg,
        state,
        {
            "event": "workspace_renamed",
            "data": {
                "type": "workspace_renamed",
                "workspace_id": "w1",
                "label": "renamed",
            },
        },
    )
    assert workspaces["w1"]["label"] == "renamed"


def test_unmapped_workspace_skips_cmux(monkeypatch, state, workspaces, cmux_calls):
    monkeypatch.setattr(cmux, "herdr_session_pids", lambda: [])
    legacy_events.handle_event({}, state, status_event("w9:p1", "w9", "blocked"))
    assert cmux_calls == []
    assert workspaces["w9"]["panes"]["w9:p1"]["status"] == "blocked"


def test_reconcile_rebuilds_state(monkeypatch, cfg, state, workspaces, cmux_calls):
    workspaces["stale"] = {
        "label": "",
        "panes": {"stale:p1": {"status": "working"}},
    }
    monkeypatch.setattr(
        cmux,
        "herdr_cli",
        lambda args: {
            "agents": [
                {
                    "pane_id": "w1:p1",
                    "workspace_id": "w1",
                    "agent": "claude",
                    "agent_status": "blocked",
                    "terminal_title_stripped": "fix",
                },
                {
                    "pane_id": "w2:p1",
                    "workspace_id": "w2",
                    "agent": "codex",
                    "agent_status": "working",
                    "terminal_title_stripped": "build",
                },
                {
                    "pane_id": "w2:p2",
                    "workspace_id": "w2",
                    "agent": "pi",
                    "agent_status": "idle",
                    "terminal_title_stripped": "",
                },
            ],
        },
    )

    legacy_events.reconcile(cfg, state)

    assert "stale" not in workspaces
    assert workspaces["w1"]["panes"]["w1:p1"]["status"] == "blocked"
    assert list(workspaces["w2"]["panes"]) == ["w2:p1"]
    pills = [c for c in cmux_calls if c[0] == "set-status"]
    assert {c[1] for c in pills} == {
        "herdr.w1",
        "herdr.w1.w1_p1",
        "herdr.w2",
        "herdr.w2.w2_p1",
    }


def test_reconcile_clears_stale_workspace(
    monkeypatch, cfg, state, workspaces, cmux_calls
):
    workspaces["w1"] = {
        "label": "",
        "panes": {
            "w1:p1": {"status": "blocked", "agent": "claude", "title": "fix"},
        },
    }
    monkeypatch.setattr(cmux, "herdr_cli", lambda args: {"agents": []})

    legacy_events.reconcile(cfg, state)

    assert "w1" not in workspaces
    clear = [c for c in cmux_calls if c[0] == "clear-status"]
    assert clear == [
        ["clear-status", "herdr.w1.w1_p1", "--workspace", "workspace:1"],
        ["clear-status", "herdr.w1", "--workspace", "workspace:1"],
    ]
    read = [c for c in cmux_calls if c[0] == "mark-notification-read"]
    assert read == []


def reconcile_run(monkeypatch, responses, agents):
    """Run reconcile with a fake cmux CLI driven by per-command responses."""
    calls = []

    def fake_run(cmd, timeout=10):
        calls.append(cmd[1:])
        out = responses.get(cmd[1], "") if len(cmd) > 1 else ""
        if cmd[1] == "list-status":
            out = responses.get(("list-status", cmd[-1]), "")
        return SimpleNamespace(returncode=0, stdout=out, stderr="")

    monkeypatch.setattr(cmux, "run", fake_run)
    monkeypatch.setattr(cmux, "herdr_cli", lambda args: {"agents": agents})
    return calls


def test_reconcile_clears_orphan_pills(monkeypatch, cfg, state):
    calls = reconcile_run(
        monkeypatch,
        {
            "list-workspaces": "workspace:1  hd:tom\nworkspace:2  other\n",
            ("list-status", "workspace:1"): (
                "herdr.w9=1 waiting color=#ff9500 priority=20\n"
            ),
        },
        agents=[],
    )

    legacy_events.reconcile(cfg, state)

    assert ["clear-status", "herdr.w9", "--workspace", "workspace:1"] in calls


def test_reconcile_keeps_active_pill_keys(monkeypatch, cfg, state):
    calls = reconcile_run(
        monkeypatch,
        {
            "list-workspaces": "workspace:1  hd:tom\n",
            ("list-status", "workspace:1"): (
                "herdr.w1=1 working color=#0a84ff priority=10\n"
            ),
        },
        agents=[
            {
                "pane_id": "w1:p1",
                "workspace_id": "w1",
                "agent": "claude",
                "agent_status": "working",
                "terminal_title_stripped": "task",
            },
        ],
    )

    legacy_events.reconcile(cfg, state)

    assert [c for c in calls if c[0] == "clear-status"] == []


def test_reconcile_preserves_unidentified_notifications(monkeypatch, cfg, state):
    notifications = json.dumps(
        [
            {"id": "n1", "is_read": False, "title": "claude: waiting for input"},
            {"id": "n2", "is_read": True, "title": "codex: finished"},
            {"id": "n3", "is_read": False, "title": "Build finished"},
        ]
    )
    calls = reconcile_run(monkeypatch, {"list-notifications": notifications}, agents=[])

    legacy_events.reconcile(cfg, state)

    read = [c for c in calls if c[0] == "mark-notification-read"]
    assert read == []


def test_reconcile_keeps_notifications_while_attention(monkeypatch, cfg, state):
    notifications = json.dumps(
        [{"id": "n1", "is_read": False, "title": "claude: waiting for input"}]
    )
    calls = reconcile_run(
        monkeypatch,
        {"list-notifications": notifications},
        agents=[
            {
                "pane_id": "w1:p1",
                "workspace_id": "w1",
                "agent": "claude",
                "agent_status": "blocked",
                "terminal_title_stripped": "fix",
            },
        ],
    )

    legacy_events.reconcile(cfg, state)

    assert [c for c in calls if c[0] == "mark-notification-read"] == []


def test_same_workspace_id_in_two_sessions_stays_isolated(
    monkeypatch, cfg, state, cmux_calls
):
    # Workspace ids are only unique within a herdr session; two sessions may
    # both have a "w1" and must not share panes.
    monkeypatch.setenv("HERDR_SESSION", "coten")
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    monkeypatch.setenv("HERDR_SESSION", "gutenberg")
    legacy_events.handle_event(cfg, state, status_event("w1:p9", "w1", "blocked"))

    sessions = state["sessions"]
    assert list(sessions["coten"]["workspaces"]["w1"]["panes"]) == ["w1:p1"]
    assert list(sessions["gutenberg"]["workspaces"]["w1"]["panes"]) == ["w1:p9"]


def test_reconcile_preserves_other_sessions(monkeypatch, cfg, state):
    # Regression: refresh in one herdr session rebuilt the whole flat state
    # from its own agent list and cleared every other session's pills.
    monkeypatch.setenv("HERDR_SESSION", "coten")
    state["sessions"]["gutenberg"] = {
        "workspaces": {
            "w6": {
                "label": "gutenberg",
                "panes": {
                    "w6:p1": {"status": "working", "agent": "claude", "title": "t"},
                },
            }
        }
    }
    calls = reconcile_run(
        monkeypatch,
        {
            "session": json.dumps(
                {
                    "sessions": [
                        {"name": "coten", "running": True},
                        {"name": "gutenberg", "running": True},
                    ]
                }
            ),
            "list-workspaces": "workspace:1  hd:coten\nworkspace:2  hd:gutenberg\n",
            ("list-status", "workspace:2"): (
                "herdr.w6=1 working color=#0a84ff priority=10\n"
            ),
        },
        agents=[],
    )

    legacy_events.reconcile(cfg, state)

    assert state["sessions"]["gutenberg"]["workspaces"]["w6"]["panes"]
    assert [c for c in calls if c[0] == "clear-status"] == []


def test_reconcile_prunes_ended_sessions(monkeypatch, cfg, state):
    # A session that ended and never restarted would otherwise keep its
    # pills forever, since no hook ever runs for it again.
    monkeypatch.setenv("HERDR_SESSION", "coten")
    cfg["workspaces"]["w5"] = "workspace:5"
    state["sessions"]["ghost"] = {
        "workspaces": {
            "w5": {
                "label": "ghost",
                "panes": {
                    "w5:p1": {"status": "blocked", "agent": "claude", "title": ""},
                },
            }
        }
    }
    calls = reconcile_run(
        monkeypatch,
        {"session": json.dumps({"sessions": [{"name": "coten", "running": True}]})},
        agents=[],
    )

    legacy_events.reconcile(cfg, state)

    assert "ghost" not in state["sessions"]
    assert ["clear-status", "herdr.w5", "--workspace", "workspace:5"] in calls
    assert not any(c[0] == "mark-notification-read" for c in calls)


def test_reconcile_keeps_sessions_when_liveness_unknown(monkeypatch, cfg, state):
    monkeypatch.setenv("HERDR_SESSION", "coten")
    state["sessions"]["ghost"] = {
        "workspaces": {
            "w5": {
                "label": "ghost",
                "panes": {
                    "w5:p1": {"status": "working", "agent": "claude", "title": ""},
                },
            }
        }
    }
    # no "session" response: session list fails and pruning must be skipped
    reconcile_run(monkeypatch, {}, agents=[])

    legacy_events.reconcile(cfg, state)

    assert "ghost" in state["sessions"]


def test_event_tolerates_workspace_entry_without_panes(
    cfg, state, workspaces, cmux_calls
):
    # workspace entries written by older plugin versions may lack "panes"
    workspaces["w1"] = {"label": "old-format"}
    legacy_events.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    assert workspaces["w1"]["panes"]["w1:p1"]["status"] == "working"


def test_sweep_orphan_pills_keeps_remote_keys(monkeypatch, cfg, state):
    calls = reconcile_run(
        monkeypatch,
        {
            "list-workspaces": "workspace:1  hd:tom\n",
            ("list-status", "workspace:1"): (
                "herdr.remote.tom=1 waiting color=#ff9500 priority=20\n"
                "herdr.w9=1 waiting color=#ff9500 priority=20\n"
            ),
        },
        agents=[],
    )

    legacy_events.reconcile(cfg, state)

    clears = [c for c in calls if c[0] == "clear-status"]
    assert clears == [["clear-status", "herdr.w9", "--workspace", "workspace:1"]]


def test_sweep_notifications_skips_with_remote_attention(
    monkeypatch, cfg, state, tmp_path
):
    core.save_remote_state(
        {"remotes": {"tom": {"panes": {"w1:p1": {"status": "blocked"}}}}}
    )
    notifications = json.dumps(
        [{"id": "n1", "is_read": False, "title": "claude: waiting for input"}]
    )
    calls = reconcile_run(monkeypatch, {"list-notifications": notifications}, agents=[])

    legacy_events.reconcile(cfg, state)

    assert [c for c in calls if c[0] == "mark-notification-read"] == []


def test_reconcile_keeps_state_when_agent_snapshot_fails(
    monkeypatch, state, cmux_calls
):
    state["legacy_log_cleared"] = True
    bucket = core.session_bucket(state)
    bucket["workspaces"]["w1"] = {
        "label": "project",
        "panes": {"p": {"status": "done", "agent": "claude", "title": "task"}},
    }
    before = json.loads(json.dumps(state))

    def fail(_args):
        raise RuntimeError("temporary outage")

    monkeypatch.setattr(cmux, "herdr_cli", fail)
    legacy_events.reconcile({}, state)
    assert state == before
    assert not cmux_calls
