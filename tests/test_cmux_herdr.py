import json
import os
import socket
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

import cmux_herdr as ch


@pytest.fixture
def state():
    return {"sessions": {}}


@pytest.fixture
def bucket(state):
    return ch.session_bucket(state)


@pytest.fixture
def workspaces(bucket):
    return bucket["workspaces"]


@pytest.fixture(autouse=True)
def no_session_env(monkeypatch):
    # Tests must not inherit the session/workspace of a surrounding herdr or
    # cmux pane; state is namespaced per herdr session.
    monkeypatch.delenv("HERDR_SESSION", raising=False)
    monkeypatch.delenv("CMUX_WORKSPACE_ID", raising=False)


@pytest.fixture
def cfg():
    return {"workspaces": {"w1": "workspace:1", "w2": "workspace:2"}}


@pytest.fixture
def cmux_calls(monkeypatch):
    calls = []

    def fake_run(cmd, timeout=10):
        calls.append(cmd[1:])
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(ch, "run", fake_run)
    return calls


@pytest.fixture(autouse=True)
def no_label_lookup(monkeypatch):
    monkeypatch.setattr(ch, "workspace_label", lambda ws_id: "")


def status_event(pane_id, ws_id, status, agent="claude", title="task"):
    return {
        "event": "pane_agent_status_changed",
        "data": {
            "type": "pane_agent_status_changed",
            "pane_id": pane_id,
            "workspace_id": ws_id,
            "agent": agent,
            "title": title,
            "agent_status": status,
            "state_labels": {},
        },
    }


def test_blocked_notifies_and_sets_waiting_pill(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))

    notify = [c for c in cmux_calls if c[0] == "notify"]
    assert len(notify) == 1
    assert "claude: waiting for input" in notify[0]
    assert "workspace:1" in notify[0]

    pill = [c for c in cmux_calls if c[0] == "set-status"]
    assert len(pill) == 1
    assert pill[0][1] == "herdr.w1"
    assert pill[0][2] == "1 waiting"
    assert "#ff9500" in pill[0]


def test_done_notifies_finished(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "done"))

    notify = [c for c in cmux_calls if c[0] == "notify"]
    assert len(notify) == 1
    assert "claude: finished" in notify[0]


def test_working_sets_pill_without_notification(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))

    assert [c for c in cmux_calls if c[0] == "notify"] == []
    pill = [c for c in cmux_calls if c[0] == "set-status"]
    assert len(pill) == 1
    assert pill[0][2] == "1 working"
    assert "#0a84ff" in pill[0]


def test_aggregates_across_panes(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    cmux_calls.clear()
    ch.handle_event(cfg, state, status_event("w1:p2", "w1", "working"))

    pill = [c for c in cmux_calls if c[0] == "set-status"]
    assert pill[0][2] == "1 waiting · 1 working"


def test_idle_removes_pane_and_clears_pill(cfg, state, workspaces, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    cmux_calls.clear()
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "idle"))

    assert workspaces["w1"]["panes"] == {}
    assert [c for c in cmux_calls if c[0] == "notify"] == []
    clear = [c for c in cmux_calls if c[0] == "clear-status"]
    assert len(clear) == 1
    assert clear[0][1] == "herdr.w1"


def test_pane_closed_updates_pill(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    ch.handle_event(cfg, state, status_event("w1:p2", "w1", "working"))
    cmux_calls.clear()
    ch.handle_event(
        cfg,
        state,
        {
            "event": "pane_closed",
            "data": {"type": "pane_closed", "pane_id": "w1:p1", "workspace_id": "w1"},
        },
    )

    pill = [c for c in cmux_calls if c[0] == "set-status"]
    assert pill[0][2] == "1 working"


def test_leaving_attention_marks_notifications_read(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    cmux_calls.clear()
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))

    read = [c for c in cmux_calls if c[0] == "mark-notification-read"]
    assert read == [["mark-notification-read", "--workspace", "workspace:1"]]


def test_local_mark_read_respects_remote_attention(
    monkeypatch, cfg, state, cmux_calls, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    ch.save_remote_state(
        {
            "remotes": {
                "maguro-tom": {
                    "panes": {"w9:p1": {"status": "blocked"}},
                    "ref": "workspace:1",
                }
            }
        }
    )
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    cmux_calls.clear()
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))

    # the local pane leaving attention must not clear the remote's
    # still-unresolved notification on the same cmux workspace
    assert [c for c in cmux_calls if c[0] == "mark-notification-read"] == []


def test_other_attention_pane_keeps_notifications_unread(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    ch.handle_event(cfg, state, status_event("w1:p2", "w1", "blocked"))
    cmux_calls.clear()
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))

    assert [c for c in cmux_calls if c[0] == "mark-notification-read"] == []


def test_pane_closed_attention_marks_notifications_read(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    cmux_calls.clear()
    ch.handle_event(
        cfg,
        state,
        {
            "event": "pane_closed",
            "data": {"type": "pane_closed", "pane_id": "w1:p1", "workspace_id": "w1"},
        },
    )

    read = [c for c in cmux_calls if c[0] == "mark-notification-read"]
    assert read == [["mark-notification-read", "--workspace", "workspace:1"]]


def test_workspace_closed_clears_status(cfg, state, workspaces, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    cmux_calls.clear()
    ch.handle_event(
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
    clear = [c for c in cmux_calls if c[0] == "clear-status"]
    assert len(clear) == 1


def test_pill_includes_icon(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    pill = [c for c in cmux_calls if c[0] == "set-status"]
    assert pill[0][-2:] == ["--icon", "hourglass"]

    cmux_calls.clear()
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    pill = [c for c in cmux_calls if c[0] == "set-status"]
    assert pill[0][-2:] == ["--icon", "bolt"]


def test_blocked_logs_to_sidebar_log(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    logs = [c for c in cmux_calls if c[0] == "log"]
    assert logs == [
        [
            "log",
            "--level",
            "warning",
            "--source",
            "claude",
            "--workspace",
            "workspace:1",
            "--",
            "waiting for input · task",
        ]
    ]


def test_done_logs_success(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "done"))
    logs = [c for c in cmux_calls if c[0] == "log"]
    assert logs[0][2] == "success"
    assert logs[0][-1] == "finished · task"


def test_working_not_logged_by_default(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    assert [c for c in cmux_calls if c[0] == "log"] == []


def test_working_logged_when_enabled(cfg, state, cmux_calls):
    cfg["sidebar_log_working"] = True
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    logs = [c for c in cmux_calls if c[0] == "log"]
    assert logs[0][2] == "progress"
    assert logs[0][-1] == "working · task"


def test_sidebar_log_can_be_disabled(cfg, state, cmux_calls):
    cfg["sidebar_log"] = False
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    assert [c for c in cmux_calls if c[0] == "log"] == []
    # notifications are unaffected
    assert len([c for c in cmux_calls if c[0] == "notify"]) == 1


def test_per_pane_pill_set(cfg, state, cmux_calls):
    cfg["per_pane_status"] = True
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    pills = [c for c in cmux_calls if c[0] == "set-status"]
    assert len(pills) == 2
    pane_pill = next(c for c in pills if c[1] == "herdr.w1.w1_p1")
    assert pane_pill[2] == "claude: waiting"
    assert "#ff9500" in pane_pill
    assert pane_pill[-2:] == ["--icon", "hourglass"]


def test_per_pane_pill_cleared_on_idle(cfg, state, cmux_calls):
    cfg["per_pane_status"] = True
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    cmux_calls.clear()
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "idle"))

    clears = [c for c in cmux_calls if c[0] == "clear-status"]
    assert ["clear-status", "herdr.w1.w1_p1", "--workspace", "workspace:1"] in clears
    assert ["clear-status", "herdr.w1", "--workspace", "workspace:1"] in clears


def test_per_pane_pill_cleared_on_pane_closed(cfg, state, cmux_calls):
    cfg["per_pane_status"] = True
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    cmux_calls.clear()
    ch.handle_event(
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
    cfg["per_pane_status"] = True
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    ch.handle_event(cfg, state, status_event("w1:p2", "w1", "working", agent="codex"))
    cmux_calls.clear()
    ch.handle_event(
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


def test_per_pane_pills_off_by_default(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    pills = [c for c in cmux_calls if c[0] == "set-status"]
    assert [c[1] for c in pills] == ["herdr.w1"]


def test_reconcile_clears_removed_pane_pills(
    monkeypatch, cfg, state, workspaces, cmux_calls
):
    cfg["per_pane_status"] = True
    workspaces["w1"] = {
        "label": "",
        "panes": {
            "w1:p1": {"status": "blocked", "agent": "claude", "title": "fix"},
            "w1:p2": {"status": "working", "agent": "codex", "title": "build"},
        },
    }
    monkeypatch.setattr(
        ch,
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

    ch.reconcile(cfg, state)

    clears = [c for c in cmux_calls if c[0] == "clear-status"]
    # only the vanished pane's pill is cleared
    assert clears == [["clear-status", "herdr.w1.w1_p2", "--workspace", "workspace:1"]]


def test_sweep_keeps_per_pane_keys_only_when_enabled(monkeypatch, cfg, state):
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

    cfg["per_pane_status"] = True
    calls = reconcile_run(monkeypatch, responses, agents=[agent])
    ch.reconcile(cfg, state)
    assert [c for c in calls if c[0] == "clear-status"] == []

    calls = reconcile_run(monkeypatch, responses, agents=[agent])
    ch.reconcile({"workspaces": cfg["workspaces"]}, state)
    assert ["clear-status", "herdr.w1.w1_p1", "--workspace", "workspace:1"] in calls


def test_workspace_focused_marks_notifications_read(cfg, state, cmux_calls):
    ch.handle_event(
        cfg,
        state,
        {
            "event": "workspace_focused",
            "data": {"type": "workspace_focused", "workspace_id": "w1"},
        },
    )

    assert cmux_calls == [["mark-notification-read", "--workspace", "workspace:1"]]


def test_workspace_renamed_updates_label(cfg, state, workspaces, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    ch.handle_event(
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


def test_unmapped_workspace_skips_cmux(state, workspaces, cmux_calls):
    ch.handle_event({}, state, status_event("w9:p1", "w9", "blocked"))
    assert cmux_calls == []
    assert workspaces["w9"]["panes"]["w9:p1"]["status"] == "blocked"


def test_config_mapping_wins_over_env(monkeypatch, state, bucket, cmux_calls):
    monkeypatch.setenv("CMUX_WORKSPACE_ID", "workspace:99")
    cfg = {"workspaces": {"w1": "workspace:1"}}
    ref, _ = ch.resolve_cmux_workspace(cfg, bucket, "w1")
    assert ref == "workspace:1"


def test_env_fallback(monkeypatch, state, bucket, cmux_calls):
    monkeypatch.setenv("CMUX_WORKSPACE_ID", "workspace:99")
    ref, _ = ch.resolve_cmux_workspace({}, bucket, "w1")
    assert ref == "workspace:99"


def test_cached_session_ref_used_without_detection(
    monkeypatch, state, bucket, cmux_calls
):
    monkeypatch.delenv("CMUX_WORKSPACE_ID", raising=False)
    bucket["session_ref"] = {"ref": "workspace:7", "at": ch.time.time()}

    def fail_ps(*args, **kwargs):
        raise AssertionError("ps should not run for a fresh cache")

    monkeypatch.setattr(ch.subprocess, "run", fail_ps)
    ref, _ = ch.resolve_cmux_workspace({}, bucket, "w1")
    assert ref == "workspace:7"


def test_label_match_fallback(monkeypatch, state, bucket, cmux_calls):
    monkeypatch.delenv("CMUX_WORKSPACE_ID", raising=False)
    monkeypatch.setattr(ch, "herdr_session_pids", lambda: [])
    monkeypatch.setattr(ch, "workspace_label", lambda ws_id: "My Project")
    monkeypatch.setattr(
        ch, "cmux_workspaces_by_title", lambda cfg: {"my project": "workspace:5"}
    )
    ref, label = ch.resolve_cmux_workspace({}, bucket, "w1")
    assert (ref, label) == ("workspace:5", "My Project")


def test_detect_via_cmux_top(monkeypatch, state, bucket):
    monkeypatch.delenv("CMUX_WORKSPACE_ID", raising=False)
    monkeypatch.setattr(ch, "herdr_session_pids", lambda: [123])
    top = {
        "windows": [
            {
                "workspaces": [
                    {
                        "type": "workspace",
                        "ref": "workspace:3",
                        "surfaces": [{"pid": 100, "children": [{"pid": 123}]}],
                    }
                ],
            }
        ],
    }

    def fake_run(cmd, timeout=10):
        return SimpleNamespace(returncode=0, stdout=json.dumps(top), stderr="")

    monkeypatch.setattr(ch, "run", fake_run)
    ref = ch.detect_session_workspace({}, bucket)
    assert ref == "workspace:3"
    assert bucket["session_ref"]["ref"] == "workspace:3"


def test_stale_cache_survives_failed_detection(monkeypatch, state, bucket):
    monkeypatch.delenv("CMUX_WORKSPACE_ID", raising=False)
    bucket["session_ref"] = {"ref": "workspace:7", "at": 0}
    monkeypatch.setattr(ch, "herdr_session_pids", lambda: [])
    assert ch.detect_session_workspace({}, bucket) == "workspace:7"


def test_herdr_session_pids_filters(monkeypatch):
    monkeypatch.setenv("HERDR_SESSION", "tom")
    ps_output = "\n".join(
        [
            "  100 /usr/local/bin/herdr server",
            "  200 /usr/local/bin/herdr --session tom",
            "  300 /usr/local/bin/herdr --session gutenberg",
            "  400 /usr/local/bin/herdr session attach tom",
            "  500 /usr/local/bin/herdr --session tom remote-client-bridge",
            "  600 /usr/local/bin/other",
            "  700 /usr/local/bin/herdr",
        ]
    )

    def fake_ps(cmd, **kwargs):
        return SimpleNamespace(returncode=0, stdout=ps_output, stderr="")

    monkeypatch.setattr(ch.subprocess, "run", fake_ps)
    assert sorted(ch.herdr_session_pids()) == [200, 400, 700]


def test_find_pid_workspace_nested():
    tree = {"rows": [{"workspace_id": "workspace:9", "processes": [{"pid": 42}]}]}
    assert ch.find_pid_workspace(tree, {42}) == "workspace:9"
    assert ch.find_pid_workspace(tree, {1}) is None


def test_cmux_workspaces_by_title_parsing(monkeypatch):
    def fake_run(cmd, timeout=10):
        return SimpleNamespace(
            returncode=0,
            stdout="workspace:1  My Project\nworkspace:2  other\n",
            stderr="",
        )

    monkeypatch.setattr(ch, "run", fake_run)
    assert ch.cmux_workspaces_by_title({}) == {
        "my project": "workspace:1",
        "other": "workspace:2",
    }


def test_cmux_workspaces_by_title_parses_selected_row(monkeypatch):
    def fake_run(cmd, timeout=10):
        return SimpleNamespace(
            returncode=0,
            stdout=(
                "  workspace:2  hd:tom\n"
                "* workspace:7  maguro:hd:gutenberg  [selected]\n"
            ),
            stderr="",
        )

    monkeypatch.setattr(ch, "run", fake_run)
    assert ch.cmux_workspaces_by_title({}) == {
        "hd:tom": "workspace:2",
        "maguro:hd:gutenberg": "workspace:7",
    }


def test_run_scrubs_cmux_caller_identity_env(monkeypatch):
    captured = {}

    def fake_subprocess_run(cmd, **kwargs):
        captured["env"] = kwargs.get("env")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(ch.subprocess, "run", fake_subprocess_run)
    monkeypatch.setenv("CMUX_PANEL_ID", "panel-uuid")
    monkeypatch.setenv("CMUX_SURFACE_ID", "surface-uuid")
    monkeypatch.setenv("CMUX_TAB_ID", "tab-uuid")
    monkeypatch.setenv("CMUX_WORKSPACE_ID", "ws-uuid")
    monkeypatch.setenv("CMUX_TERMINAL_LIFECYCLE_ID", "lc-uuid")
    monkeypatch.setenv("CMUX_SOCKET_CAPABILITY", "cap-token")

    ch.run(["cmux", "notify"])

    env = captured["env"]
    assert env is not None
    for var in ch.CMUX_CALLER_IDENTITY_VARS:
        assert var not in env
    assert env["CMUX_SOCKET_CAPABILITY"] == "cap-token"


def test_reconcile_rebuilds_state(monkeypatch, cfg, state, workspaces, cmux_calls):
    workspaces["stale"] = {
        "label": "",
        "panes": {"stale:p1": {"status": "working"}},
    }
    monkeypatch.setattr(
        ch,
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

    ch.reconcile(cfg, state)

    assert "stale" not in workspaces
    assert workspaces["w1"]["panes"]["w1:p1"]["status"] == "blocked"
    assert list(workspaces["w2"]["panes"]) == ["w2:p1"]
    pills = [c for c in cmux_calls if c[0] == "set-status"]
    assert len(pills) == 2


def test_reconcile_clears_stale_workspace(
    monkeypatch, cfg, state, workspaces, cmux_calls
):
    workspaces["w1"] = {
        "label": "",
        "panes": {
            "w1:p1": {"status": "blocked", "agent": "claude", "title": "fix"},
        },
    }
    monkeypatch.setattr(ch, "herdr_cli", lambda args: {"agents": []})

    ch.reconcile(cfg, state)

    assert "w1" not in workspaces
    clear = [c for c in cmux_calls if c[0] == "clear-status"]
    assert clear == [["clear-status", "herdr.w1", "--workspace", "workspace:1"]]
    read = [c for c in cmux_calls if c[0] == "mark-notification-read"]
    assert read == [["mark-notification-read", "--workspace", "workspace:1"]]


def reconcile_run(monkeypatch, responses, agents):
    """Run reconcile with a fake cmux CLI driven by per-command responses."""
    calls = []

    def fake_run(cmd, timeout=10):
        calls.append(cmd[1:])
        out = responses.get(cmd[1], "") if len(cmd) > 1 else ""
        if cmd[1] == "list-status":
            out = responses.get(("list-status", cmd[-1]), "")
        return SimpleNamespace(returncode=0, stdout=out, stderr="")

    monkeypatch.setattr(ch, "run", fake_run)
    monkeypatch.setattr(ch, "herdr_cli", lambda args: {"agents": agents})
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

    ch.reconcile(cfg, state)

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

    ch.reconcile(cfg, state)

    assert [c for c in calls if c[0] == "clear-status"] == []


def test_reconcile_marks_stale_notifications_read(monkeypatch, cfg, state):
    notifications = json.dumps(
        [
            {"id": "n1", "is_read": False, "title": "claude: waiting for input"},
            {"id": "n2", "is_read": True, "title": "codex: finished"},
            {"id": "n3", "is_read": False, "title": "Build finished"},
        ]
    )
    calls = reconcile_run(monkeypatch, {"list-notifications": notifications}, agents=[])

    ch.reconcile(cfg, state)

    read = [c for c in calls if c[0] == "mark-notification-read"]
    assert read == [["mark-notification-read", "--id", "n1"]]


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

    ch.reconcile(cfg, state)

    assert [c for c in calls if c[0] == "mark-notification-read"] == []


def test_same_workspace_id_in_two_sessions_stays_isolated(
    monkeypatch, cfg, state, cmux_calls
):
    # Workspace ids are only unique within a herdr session; two sessions may
    # both have a "w1" and must not share panes.
    monkeypatch.setenv("HERDR_SESSION", "coten")
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    monkeypatch.setenv("HERDR_SESSION", "gutenberg")
    ch.handle_event(cfg, state, status_event("w1:p9", "w1", "blocked"))

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

    ch.reconcile(cfg, state)

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

    ch.reconcile(cfg, state)

    assert "ghost" not in state["sessions"]
    assert ["clear-status", "herdr.w5", "--workspace", "workspace:5"] in calls
    assert ["mark-notification-read", "--workspace", "workspace:5"] in calls


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

    ch.reconcile(cfg, state)

    assert "ghost" in state["sessions"]


def test_state_roundtrip(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    assert ch.load_state() == {"sessions": {}}
    bucket = {"workspaces": {"w1": {"label": "x", "panes": {}}}}
    ch.save_state({"sessions": {"tom": bucket}})
    assert ch.load_state()["sessions"]["tom"]["workspaces"]["w1"]["label"] == "x"


def test_load_state_drops_legacy_flat_format(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    (tmp_path / "state.json").write_text(
        json.dumps(
            {
                "workspaces": {"w1": {"label": "x", "panes": {}}},
                "session_ref": {"ref": "workspace:1", "at": 0},
            }
        )
    )
    assert ch.load_state() == {"sessions": {}}


def test_load_config(monkeypatch, tmp_path):
    (tmp_path / "config.toml").write_text('[workspaces]\nw1 = "workspace:1"\n')
    monkeypatch.setattr(ch, "config_dir", tmp_path)
    assert ch.load_config()["workspaces"]["w1"] == "workspace:1"

    (tmp_path / "config.toml").write_text("not = [valid")
    assert ch.load_config() == {}


def test_main_event_mode(monkeypatch, tmp_path, cfg):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(ch, "config_dir", tmp_path)
    monkeypatch.setattr(ch, "load_config", lambda: cfg)
    monkeypatch.setattr(sys, "argv", ["cmux_herdr.py", "event"])
    monkeypatch.setenv(
        "HERDR_PLUGIN_EVENT_JSON", json.dumps(status_event("w1:p1", "w1", "blocked"))
    )
    calls = []
    monkeypatch.setattr(
        ch,
        "run",
        lambda cmd, timeout=10: (
            calls.append(cmd[1:]),
            SimpleNamespace(returncode=0, stdout="", stderr=""),
        )[1],
    )

    assert ch.main() == 0
    assert any(c[0] == "notify" for c in calls)
    saved = json.loads((tmp_path / "state.json").read_text())
    panes = saved["sessions"][""]["workspaces"]["w1"]["panes"]
    assert panes["w1:p1"]["status"] == "blocked"


def test_main_rejects_unknown_mode(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["cmux_herdr.py", "bogus"])
    assert ch.main() == 2
    assert "usage" in capsys.readouterr().err


def test_cmux_cli_tolerates_missing_binary(monkeypatch):
    def raise_not_found(cmd, timeout=10):
        raise FileNotFoundError

    monkeypatch.setattr(ch, "run", raise_not_found)
    assert ch.cmux_cli({}, ["ping"]) is False


def test_cmux_cli_tolerates_failure(monkeypatch):
    monkeypatch.setattr(
        ch,
        "run",
        lambda cmd, timeout=10: SimpleNamespace(returncode=1, stdout="", stderr="boom"),
    )
    assert ch.cmux_cli({}, ["ping"]) is False


def test_cmux_cli_tolerates_timeout(monkeypatch):
    def slow(cmd, timeout=10):
        raise subprocess.TimeoutExpired(cmd, timeout)

    monkeypatch.setattr(ch, "run", slow)
    assert ch.cmux_cli({}, ["ping"]) is False


# --- Remote daemon -----------------------------------------------------------


REMOTE_CFG = {"ssh_target": "maguro", "session": "tom", "cmux_title": "maguro:hd:tom"}


@pytest.fixture
def remote_title_map(monkeypatch):
    monkeypatch.setattr(
        ch, "cmux_workspaces_by_title", lambda cfg: {"maguro:hd:tom": "workspace:9"}
    )


def remote_agent(pane_id, status, agent="claude", title="task", ws_id="w1"):
    return {
        "pane_id": pane_id,
        "workspace_id": ws_id,
        "agent": agent,
        "agent_status": status,
        "terminal_title_stripped": title,
    }


def test_remote_blocked_notifies_and_sets_pill(cmux_calls, remote_title_map):
    rstate = {"remotes": {}}
    changed = ch.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "blocked")], {}
    )

    assert changed is True
    notify = [c for c in cmux_calls if c[0] == "notify"]
    assert len(notify) == 1
    assert "claude: waiting for input" in notify[0]
    assert "workspace:9" in notify[0]
    assert "maguro:hd:tom · task" in notify[0]

    pill = [c for c in cmux_calls if c[0] == "set-status"]
    assert len(pill) == 1
    assert pill[0][1] == "herdr.remote.tom"
    assert pill[0][2] == "1 waiting"
    assert "#ff9500" in pill[0]


def test_remote_done_notifies_finished(cmux_calls, remote_title_map):
    rstate = {"remotes": {}}
    ch.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "done")], {}
    )
    notify = [c for c in cmux_calls if c[0] == "notify"]
    assert len(notify) == 1
    assert "claude: finished" in notify[0]


def test_remote_working_pill_without_notification(cmux_calls, remote_title_map):
    rstate = {"remotes": {}}
    ch.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "working")], {}
    )
    assert [c for c in cmux_calls if c[0] == "notify"] == []
    pill = [c for c in cmux_calls if c[0] == "set-status"]
    assert pill[0][2] == "1 working"
    assert "#0a84ff" in pill[0]


def test_remote_blocked_logs_to_sidebar(cmux_calls, remote_title_map):
    rstate = {"remotes": {}}
    ch.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "blocked")], {}
    )
    logs = [c for c in cmux_calls if c[0] == "log"]
    assert logs == [
        [
            "log",
            "--level",
            "warning",
            "--source",
            "claude",
            "--workspace",
            "workspace:9",
            "--",
            "waiting for input · task",
        ]
    ]


def test_remote_per_pane_pills(cmux_calls, remote_title_map):
    cfg = {"per_pane_status": True}
    rstate = {"remotes": {}}
    ch.apply_remote_snapshot(
        cfg, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "blocked")], {}
    )
    pills = [c for c in cmux_calls if c[0] == "set-status"]
    pane_pill = next(c for c in pills if c[1] == "herdr.remote.tom.w1_p1")
    assert pane_pill[2] == "claude: waiting"

    cmux_calls.clear()
    ch.apply_remote_snapshot(cfg, rstate, "tom", REMOTE_CFG, [], {})
    clears = [c for c in cmux_calls if c[0] == "clear-status"]
    assert [
        "clear-status",
        "herdr.remote.tom.w1_p1",
        "--workspace",
        "workspace:9",
    ] in clears
    assert ["clear-status", "herdr.remote.tom", "--workspace", "workspace:9"] in clears


def test_retire_remote_clears_per_pane_pills(
    monkeypatch, cmux_calls, remote_title_map, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    rstate = {
        "remotes": {
            "tom": {
                "panes": {"w1:p1": {"status": "blocked"}},
                "ref": "workspace:9",
                "cmux_title": "maguro:hd:tom",
            }
        }
    }
    ch.retire_remote({"per_pane_status": True}, rstate, "tom", {}, {})

    clears = [c for c in cmux_calls if c[0] == "clear-status"]
    assert [
        "clear-status",
        "herdr.remote.tom.w1_p1",
        "--workspace",
        "workspace:9",
    ] in clears
    assert ["clear-status", "herdr.remote.tom", "--workspace", "workspace:9"] in clears


def test_remote_unchanged_snapshot_is_noop(cmux_calls, remote_title_map):
    rstate = {"remotes": {}}
    agents = [remote_agent("w1:p1", "blocked")]
    ch.apply_remote_snapshot({}, rstate, "tom", REMOTE_CFG, agents, {})
    cmux_calls.clear()

    changed = ch.apply_remote_snapshot({}, rstate, "tom", REMOTE_CFG, agents, {})
    assert changed is False
    assert cmux_calls == []


def test_remote_leaving_attention_marks_read(cmux_calls, remote_title_map):
    rstate = {"remotes": {}}
    ch.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "blocked")], {}
    )
    cmux_calls.clear()
    ch.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "working")], {}
    )

    read = [c for c in cmux_calls if c[0] == "mark-notification-read"]
    assert read == [["mark-notification-read", "--workspace", "workspace:9"]]
    pill = [c for c in cmux_calls if c[0] == "set-status"]
    assert pill[0][2] == "1 working"


def test_remote_all_idle_clears_pill(cmux_calls, remote_title_map):
    rstate = {"remotes": {}}
    ch.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "working")], {}
    )
    cmux_calls.clear()
    ch.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "idle")], {}
    )

    clear = [c for c in cmux_calls if c[0] == "clear-status"]
    assert clear == [["clear-status", "herdr.remote.tom", "--workspace", "workspace:9"]]


def test_remote_aggregates_across_workspaces(cmux_calls, remote_title_map):
    rstate = {"remotes": {}}
    agents = [
        remote_agent("w1:p1", "blocked", ws_id="w1"),
        remote_agent("w2:p1", "working", agent="codex", ws_id="w2"),
    ]
    ch.apply_remote_snapshot({}, rstate, "tom", REMOTE_CFG, agents, {})

    pill = [c for c in cmux_calls if c[0] == "set-status"]
    assert pill[0][2] == "1 waiting · 1 working"


def test_remote_without_title_skips_cmux(cmux_calls):
    rstate = {"remotes": {}}
    rcfg = {"ssh_target": "maguro", "session": "tom"}
    changed = ch.apply_remote_snapshot(
        {}, rstate, "tom", rcfg, [remote_agent("w1:p1", "blocked")], {}
    )
    assert changed is True
    assert cmux_calls == []
    assert rstate["remotes"]["tom"]["panes"]["w1:p1"]["status"] == "blocked"


def test_remote_title_change_reroutes_pill(cmux_calls, remote_title_map):
    rstate = {"remotes": {}}
    ch.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "working")], {}
    )
    cmux_calls.clear()
    rcfg = dict(REMOTE_CFG, cmux_title="maguro:hd:other")
    changed = ch.apply_remote_snapshot(
        {}, rstate, "tom", rcfg, [remote_agent("w1:p1", "working")], {}
    )
    assert changed is True
    # the pill and unread notifications on the previous workspace are cleared
    clears = [c for c in cmux_calls if c[0] == "clear-status"]
    assert clears == [
        ["clear-status", "herdr.remote.tom", "--workspace", "workspace:9"]
    ]
    read = [c for c in cmux_calls if c[0] == "mark-notification-read"]
    assert read == [["mark-notification-read", "--workspace", "workspace:9"]]
    # new title is not in the map -> no stale ref reuse, no pill yet
    assert [c for c in cmux_calls if c[0] == "set-status"] == []


def test_remote_title_change_publishes_to_new_workspace(monkeypatch, cmux_calls):
    monkeypatch.setattr(
        ch,
        "cmux_workspaces_by_title",
        lambda cfg: {"maguro:hd:tom": "workspace:9", "maguro:hd:other": "workspace:10"},
    )
    rstate = {"remotes": {}}
    agents = [remote_agent("w1:p1", "working")]
    ch.apply_remote_snapshot({}, rstate, "tom", REMOTE_CFG, agents, {})
    cmux_calls.clear()

    rcfg = dict(REMOTE_CFG, cmux_title="maguro:hd:other")
    ch.apply_remote_snapshot({}, rstate, "tom", rcfg, agents, {})

    pill = [c for c in cmux_calls if c[0] == "set-status"]
    assert len(pill) == 1
    assert pill[0][1] == "herdr.remote.tom"
    assert "workspace:10" in pill[0]


def test_remote_daemon_without_remotes_cleans_up(monkeypatch, cmux_calls, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "config_dir", tmp_path)
    ch.save_remote_state(
        {
            "remotes": {
                "tom": {
                    "panes": {"w1:p1": {"status": "blocked"}},
                    "ref": "workspace:9",
                    "cmux_title": "maguro:hd:tom",
                }
            }
        }
    )
    monkeypatch.setattr(ch, "cmux_workspaces_by_title", lambda cfg: {})

    assert ch.remote_daemon({}) == 0

    read = [c for c in cmux_calls if c[0] == "mark-notification-read"]
    assert read == [["mark-notification-read", "--workspace", "workspace:9"]]
    saved = json.loads((tmp_path / "remote-state.json").read_text())
    assert saved["remotes"] == {}


def test_remote_daemon_malformed_config_does_not_clean_up(
    monkeypatch, cmux_calls, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "config_dir", tmp_path)
    (tmp_path / "config.toml").write_text("not = [valid")
    ch.save_remote_state(
        {"remotes": {"tom": {"panes": {"w1:p1": {"status": "blocked"}}}}}
    )

    assert ch.remote_daemon({}) == 1
    assert cmux_calls == []
    saved = json.loads((tmp_path / "remote-state.json").read_text())
    assert "tom" in saved["remotes"]


def test_read_pidfile_formats(tmp_path):
    p = tmp_path / "remote.pid"
    p.write_text("1234")  # pre-JSON format
    assert ch.read_pidfile(p) == {"pid": 1234}
    p.write_text('{"pid": 5, "script": "x", "mtime": 1}')
    assert ch.read_pidfile(p)["script"] == "x"
    p.write_text("garbage")
    assert ch.read_pidfile(p) == {}


def test_remote_identity_change_drops_socket_path(cmux_calls, remote_title_map):
    rstate = {"remotes": {}}
    agents = [remote_agent("w1:p1", "working")]
    ch.apply_remote_snapshot({}, rstate, "tom", REMOTE_CFG, agents, {})
    rstate["remotes"]["tom"]["socket_path"] = "/old/path.sock"
    cmux_calls.clear()

    rcfg = dict(REMOTE_CFG, ssh_target="other-host")
    changed = ch.apply_remote_snapshot({}, rstate, "tom", rcfg, agents, {})

    assert changed is True
    entry = rstate["remotes"]["tom"]
    assert "socket_path" not in entry
    assert entry["identity"] == ["other-host", "tom"]


def test_remote_mark_read_respects_other_remote_on_same_workspace(cmux_calls):
    rstate = {
        "remotes": {
            "a": {"panes": {}, "ref": "workspace:9", "cmux_title": "t"},
            "b": {"panes": {"w1:p1": {"status": "blocked"}}, "ref": "workspace:9"},
        }
    }
    ch.remote_mark_read_ref({}, rstate, "a", "workspace:9")
    assert cmux_calls == []

    ch.remote_mark_read_ref({}, rstate, "b", "workspace:9")
    assert cmux_calls == [["mark-notification-read", "--workspace", "workspace:9"]]


def test_remote_ref_clears_pill_when_title_moves(monkeypatch, cmux_calls, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(
        ch, "cmux_workspaces_by_title", lambda cfg: {"maguro:hd:tom": "workspace:10"}
    )
    entry = {"cmux_title": "maguro:hd:tom", "ref": "workspace:9", "ref_at": 0}

    ref = ch.remote_ref({}, {"remotes": {}}, "tom", entry)

    assert ref == "workspace:10"
    assert ["clear-status", "herdr.remote.tom", "--workspace", "workspace:9"] in (
        cmux_calls
    )
    assert ["mark-notification-read", "--workspace", "workspace:9"] in cmux_calls


def test_poll_interval_validation():
    assert ch.poll_interval({}) == ch.REMOTE_DEFAULT_POLL_SECONDS
    assert ch.poll_interval({"poll_seconds": 5}) == 5.0
    assert ch.poll_interval({"poll_seconds": "abc"}) == ch.REMOTE_DEFAULT_POLL_SECONDS
    assert ch.poll_interval({"poll_seconds": -1}) == ch.REMOTE_DEFAULT_POLL_SECONDS
    assert ch.poll_interval({"poll_seconds": 0.1}) == 0.5


def test_remote_republishes_pill_once_workspace_appears(monkeypatch, cmux_calls):
    titles = {}
    monkeypatch.setattr(ch, "cmux_workspaces_by_title", lambda cfg: dict(titles))
    rstate = {"remotes": {}}
    agents = [remote_agent("w1:p1", "working")]

    ch.apply_remote_snapshot({}, rstate, "tom", REMOTE_CFG, agents, {})
    assert cmux_calls == []  # title not resolvable yet

    titles["maguro:hd:tom"] = "workspace:9"
    changed = ch.apply_remote_snapshot({}, rstate, "tom", REMOTE_CFG, agents, {})
    assert changed is False  # snapshot unchanged, but the pill is retried
    pill = [c for c in cmux_calls if c[0] == "set-status"]
    assert len(pill) == 1
    assert pill[0][1] == "herdr.remote.tom"
    assert "workspace:9" in pill[0]


def test_remote_name_defaults_to_target_and_session():
    # The full target with an unambiguous separator, so distinct
    # users/hosts/sessions cannot collide.
    assert (
        ch.remote_name({"ssh_target": "tom@maguro.example.ts.net", "session": "tom"})
        == "tom@maguro.example.ts.net:tom"
    )
    assert ch.remote_name({"ssh_target": "maguro", "session": "hd"}) == "maguro:hd"
    assert ch.remote_name({"ssh_target": "maguro", "session": "hd", "name": "x"}) == "x"
    # colon-containing targets (IPv6, ssh:// URIs) fall back to a hash
    name = ch.remote_name({"ssh_target": "ssh://tom@host:2222", "session": "hd"})
    assert name.startswith("remote-") and len(name) == len("remote-") + 8


def test_remote_configs_rejects_malformed_entries(capsys):
    # any invalid entry rejects the whole config so the last valid one is kept
    bad_entries = [
        "bad",
        {"ssh_target": 42, "session": "s"},
        {"ssh_target": "a", "session": "s", "cmux_title": 1},
        {"ssh_target": "a", "session": "s", "name": ["x"], "cmux_title": "t"},
        {"ssh_target": "a", "session": "s"},  # cmux_title is required
    ]
    for bad in bad_entries:
        assert ch.remote_configs({"remotes": [bad]}) is None, bad
    assert ch.remote_configs({"remotes": "bad"}) is None
    assert ch.remote_configs({"remotes": ""}) is None
    assert ch.remote_configs({}) == []
    assert ch.remote_configs({"remotes": []}) == []
    valid = {"ssh_target": "a", "session": "s", "cmux_title": "t"}
    assert ch.remote_configs({"remotes": [valid]}) == [valid]
    assert "expected a table" in capsys.readouterr().err


def test_remote_configs_rejects_duplicate_names(capsys):
    cfg = {
        "remotes": [
            {"ssh_target": "a", "session": "s", "name": "dup", "cmux_title": "t"},
            {"ssh_target": "b", "session": "s", "name": "dup", "cmux_title": "t"},
        ]
    }
    assert ch.remote_configs(cfg) is None
    assert "duplicate remote name" in capsys.readouterr().err


def test_retire_remote_clears_pill_and_state(monkeypatch, cmux_calls, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    rstate = {
        "remotes": {
            "tom": {
                "panes": {"w1:p1": {"status": "blocked"}},
                "ref": "workspace:9",
                "cmux_title": "maguro:hd:tom",
            }
        }
    }
    ch.retire_remote({}, rstate, "tom", {}, {})

    assert rstate["remotes"] == {}
    assert ["clear-status", "herdr.remote.tom", "--workspace", "workspace:9"] in (
        cmux_calls
    )
    assert ["mark-notification-read", "--workspace", "workspace:9"] in cmux_calls
    saved = json.loads((tmp_path / "remote-state.json").read_text())
    assert saved["remotes"] == {}


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

    ch.reconcile(cfg, state)

    clears = [c for c in calls if c[0] == "clear-status"]
    assert clears == [["clear-status", "herdr.w9", "--workspace", "workspace:1"]]


def test_sweep_notifications_skips_with_remote_attention(
    monkeypatch, cfg, state, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    ch.save_remote_state(
        {"remotes": {"tom": {"panes": {"w1:p1": {"status": "blocked"}}}}}
    )
    notifications = json.dumps(
        [{"id": "n1", "is_read": False, "title": "claude: waiting for input"}]
    )
    calls = reconcile_run(monkeypatch, {"list-notifications": notifications}, agents=[])

    ch.reconcile(cfg, state)

    assert [c for c in calls if c[0] == "mark-notification-read"] == []


def short_sock_path(name):
    # AF_UNIX paths are limited to ~104 chars on macOS; tmp_path is too long.
    path = Path(tempfile.gettempdir()) / f"ch-{os.getpid()}-{name}.sock"
    path.unlink(missing_ok=True)
    return path


def test_socket_rpc_roundtrip():
    import threading

    sock_path = short_sock_path("rpc")
    received = []
    ready = threading.Event()

    def serve():
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(str(sock_path))
        srv.listen(1)
        ready.set()
        conn, _ = srv.accept()
        data = b""
        while b"\n" not in data:
            data += conn.recv(1 << 16)
        received.append(json.loads(data.strip()))
        conn.sendall(b'{"id":"1","result":{"type":"pong"}}\n')
        conn.close()
        srv.close()

    t = threading.Thread(target=serve, daemon=True)
    t.start()
    ready.wait(5)
    result = ch.socket_rpc(sock_path, "ping", {})
    t.join(5)

    assert result == {"type": "pong"}
    assert received == [{"id": "1", "method": "ping", "params": {}}]


def test_socket_rpc_error_raises():
    import threading

    sock_path = short_sock_path("err")
    ready = threading.Event()

    def serve():
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(str(sock_path))
        srv.listen(1)
        ready.set()
        conn, _ = srv.accept()
        conn.recv(1 << 16)
        conn.sendall(b'{"id":"1","error":{"code":"bad","message":"nope"}}\n')
        conn.close()
        srv.close()

    t = threading.Thread(target=serve, daemon=True)
    t.start()
    ready.wait(5)
    with pytest.raises(ch.SocketError, match="nope"):
        ch.socket_rpc(sock_path, "ping", {})
    t.join(5)
