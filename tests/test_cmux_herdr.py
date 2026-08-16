import json
import subprocess
import sys
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
