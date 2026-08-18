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
    return {"workspaces": {}}


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


def test_idle_removes_pane_and_clears_pill(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    cmux_calls.clear()
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "idle"))

    assert state["workspaces"]["w1"]["panes"] == {}
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


def test_workspace_closed_clears_status(cfg, state, cmux_calls):
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

    assert "w1" not in state["workspaces"]
    clear = [c for c in cmux_calls if c[0] == "clear-status"]
    assert len(clear) == 1


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


def test_workspace_renamed_updates_label(cfg, state, cmux_calls):
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
    assert state["workspaces"]["w1"]["label"] == "renamed"


def test_unmapped_workspace_skips_cmux(state, cmux_calls):
    ch.handle_event({}, state, status_event("w9:p1", "w9", "blocked"))
    assert cmux_calls == []
    assert state["workspaces"]["w9"]["panes"]["w9:p1"]["status"] == "blocked"


def test_config_mapping_wins_over_env(monkeypatch, state, cmux_calls):
    monkeypatch.setenv("CMUX_WORKSPACE_ID", "workspace:99")
    cfg = {"workspaces": {"w1": "workspace:1"}}
    ref, _ = ch.resolve_cmux_workspace(cfg, state, "w1")
    assert ref == "workspace:1"


def test_env_fallback(monkeypatch, state, cmux_calls):
    monkeypatch.setenv("CMUX_WORKSPACE_ID", "workspace:99")
    ref, _ = ch.resolve_cmux_workspace({}, state, "w1")
    assert ref == "workspace:99"


def test_cached_session_ref_used_without_detection(monkeypatch, state, cmux_calls):
    monkeypatch.delenv("CMUX_WORKSPACE_ID", raising=False)
    state["session_ref"] = {"ref": "workspace:7", "at": ch.time.time()}

    def fail_ps(*args, **kwargs):
        raise AssertionError("ps should not run for a fresh cache")

    monkeypatch.setattr(ch.subprocess, "run", fail_ps)
    ref, _ = ch.resolve_cmux_workspace({}, state, "w1")
    assert ref == "workspace:7"


def test_label_match_fallback(monkeypatch, state, cmux_calls):
    monkeypatch.delenv("CMUX_WORKSPACE_ID", raising=False)
    monkeypatch.setattr(ch, "herdr_session_pids", lambda: [])
    monkeypatch.setattr(ch, "workspace_label", lambda ws_id: "My Project")
    monkeypatch.setattr(
        ch, "cmux_workspaces_by_title", lambda cfg: {"my project": "workspace:5"}
    )
    ref, label = ch.resolve_cmux_workspace({}, state, "w1")
    assert (ref, label) == ("workspace:5", "My Project")


def test_detect_via_cmux_top(monkeypatch, state):
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
    ref = ch.detect_session_workspace({}, state)
    assert ref == "workspace:3"
    assert state["session_ref"]["ref"] == "workspace:3"


def test_stale_cache_survives_failed_detection(monkeypatch, state):
    monkeypatch.delenv("CMUX_WORKSPACE_ID", raising=False)
    state["session_ref"] = {"ref": "workspace:7", "at": 0}
    monkeypatch.setattr(ch, "herdr_session_pids", lambda: [])
    assert ch.detect_session_workspace({}, state) == "workspace:7"


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


def test_reconcile_rebuilds_state(monkeypatch, cfg, state, cmux_calls):
    state["workspaces"]["stale"] = {
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

    assert "stale" not in state["workspaces"]
    assert state["workspaces"]["w1"]["panes"]["w1:p1"]["status"] == "blocked"
    assert list(state["workspaces"]["w2"]["panes"]) == ["w2:p1"]
    pills = [c for c in cmux_calls if c[0] == "set-status"]
    assert len(pills) == 2


def test_reconcile_clears_stale_workspace(monkeypatch, cfg, state, cmux_calls):
    state["workspaces"]["w1"] = {
        "label": "",
        "panes": {
            "w1:p1": {"status": "blocked", "agent": "claude", "title": "fix"},
        },
    }
    monkeypatch.setattr(ch, "herdr_cli", lambda args: {"agents": []})

    ch.reconcile(cfg, state)

    assert "w1" not in state["workspaces"]
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


def test_state_roundtrip(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    assert ch.load_state() == {"workspaces": {}}
    ch.save_state({"workspaces": {"w1": {"label": "x", "panes": {}}}})
    assert ch.load_state()["workspaces"]["w1"]["label"] == "x"


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
    assert saved["workspaces"]["w1"]["panes"]["w1:p1"]["status"] == "blocked"


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
    # title no longer resolvable -> no stale ref reuse after title change
    assert cmux_calls == []


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
