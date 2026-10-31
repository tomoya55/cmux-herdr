import json

import pytest

import cmux_herdr as ch


def test_v1_missing_agent_after_generation_change_retires_old_blocked():
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "blocked",
        "state_change_seq": 7,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "old", [agent], ["p"])
    absent = {**agent, "agent_status": "unknown", "state_change_seq": 1}
    ch.apply_v1_snapshot(
        state, "source", "new", [], ["p"], pane_records=[absent], reconnect=True
    )
    source = state["v1"]["sources"]["source"]
    assert not ch.has_pending_attention(source["attention"], "p")
    assert ch.sidebar_panes(source["panes"], source["attention"]) == {}
    snapshot = {"agents": [], "panes": [absent]}
    ch.v1_reconcile_absent_agents(
        state, "source", "new", snapshot, [], [absent], snapshot
    )
    assert ch.sidebar_panes(source["panes"], source["attention"]) == {}


def test_v1_unknown_snapshot_after_generation_change_retires_old_blocked():
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "blocked",
        "state_change_seq": 7,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "old", [agent], ["p"])
    unknown = {**agent, "agent_status": "unknown", "state_change_seq": 1}
    ch.apply_v1_snapshot(state, "source", "new", [unknown], ["p"], reconnect=True)
    source = state["v1"]["sources"]["source"]
    assert not ch.has_pending_attention(source["attention"], "p")
    assert ch.sidebar_panes(source["panes"], source["attention"]) == {}


def test_v1_legacy_unknown_absence_without_identity_stays_blocked(monkeypatch):
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "blocked",
        "state_change_seq": 7,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    assert ch.apply_v1_status_event(
        state, "source", {"pane_id": "p", "agent_status": "unknown"}
    )
    pane = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "unknown",
    }

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": {"agents": [], "panes": [pane]}}
        return {"agents": []} if method == "agent.list" else {"panes": [pane]}

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    assert ch.refresh_v1_source(state, "source", "g", "/socket", reconnect=True)
    source = state["v1"]["sources"]["source"]
    assert source["attention"]["p"]["status"] == "blocked"


def test_v1_unknown_snapshot_of_present_agent_preserves_blocked():
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "blocked",
        "state_change_seq": 7,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"], boot_id=boot)
    source = state["v1"]["sources"]["source"]
    unknown = {**agent, "agent_status": "unknown", "state_change_seq": 8}
    ch.apply_v1_snapshot(
        state, "source", "g", [unknown], ["p"], boot_id=boot, reconnect=True
    )
    assert source["attention"]["p"]["status"] == "blocked"
    assert source["attention"]["p"]["sequence"] == 8


def test_v1_unknown_keeps_blocked_until_confirmed_absence():
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "blocked",
        "state_change_seq": 7,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"], boot_id=boot)
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    unknown = {
        "pane_id": "p",
        "terminal_id": "t",
        "agent_status": "unknown",
        "state_change_seq": 8,
    }
    assert ch.apply_v1_status_event(state, "source", unknown, event_boot=boot)
    assert source["attention"]["p"]["status"] == "blocked"
    assert source["attention"]["p"]["sequence"] == 8
    absent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "unknown",
        "state_change_seq": 8,
    }
    snapshot = {"server_boot_id": boot, "agents": [], "panes": [absent]}
    ch.v1_reconcile_absent_agents(
        state, "source", "g", snapshot, [], [absent], snapshot
    )
    assert not ch.has_pending_attention(source["attention"], "p")


def test_v1_reconnect_retires_missing_agent_and_rejects_queued_old_events(monkeypatch):
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "blocked",
        "state_change_seq": 7,
        "agent": "codex",
    }
    present = True

    def rpc(path, method, params):
        pane = {
            "pane_id": "p",
            "terminal_id": "t",
            "workspace_id": "w",
            "agent_status": "blocked" if present else "unknown",
            "state_change_seq": 7 if present else 8,
        }
        agents = [dict(agent)] if present else []
        if method == "session.snapshot":
            return {
                "snapshot": {"server_boot_id": boot, "agents": agents, "panes": [pane]}
            }
        return {"agents": agents} if method == "agent.list" else {"panes": [pane]}

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    state = {}
    assert ch.refresh_v1_source(state, "source", "g", "/socket")
    present = False
    assert ch.refresh_v1_source(state, "source", "g", "/socket", reconnect=True)
    source = state["v1"]["sources"]["source"]
    assert not ch.has_pending_attention(source["attention"], "p")
    assert source["panes"]["p"]["status"] == "unknown"
    assert ch.apply_v1_status_event(state, "source", agent, event_boot=boot)
    assert not ch.has_pending_attention(source["attention"], "p")
    assert ch.apply_v1_status_event(
        state, "source", {**agent, "state_change_seq": 9}, event_boot=boot
    )
    assert ch.has_pending_attention(source["attention"], "p")


def test_v1_absent_agent_with_replaced_terminal_rejects_old_events(monkeypatch):
    boot = "a" * 32
    pane = {
        "pane_id": "p",
        "terminal_id": "new",
        "workspace_id": "w",
        "agent_status": "unknown",
        "state_change_seq": 9,
    }
    old = {
        "pane_id": "p",
        "terminal_id": "old",
        "workspace_id": "w",
        "agent_status": "blocked",
        "state_change_seq": 7,
        "agent": "codex",
    }
    state = {}
    generation = json.dumps(["g", boot], separators=(",", ":"))
    ch.apply_v1_snapshot(state, "source", generation, [old], ["p"])
    state["v1"]["sources"]["source"]["server_boot_id"] = boot

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": boot, "agents": [], "panes": [pane]}}
        return {"agents": []} if method == "agent.list" else {"panes": [pane]}

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    assert ch.refresh_v1_source(state, "source", "g", "/socket", reconnect=True)
    source = state["v1"]["sources"]["source"]
    assert not ch.has_pending_attention(source["attention"], "p")
    assert source["panes"]["p"]["terminal_id"] == "new"
    assert not ch.apply_v1_status_event(state, "source", old, event_boot=boot)
    assert ch.apply_v1_status_event(
        state,
        "source",
        {**old, "terminal_id": "new", "state_change_seq": 10},
        event_boot=boot,
    )
    assert ch.has_pending_attention(source["attention"], "p")


def test_v1_absent_agent_preserves_known_unread_completion_and_label(monkeypatch):
    boot = "a" * 32
    generation = json.dumps(["g", boot], separators=(",", ":"))
    old = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 7,
        "agent": "codex",
    }
    pane = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "unknown",
        "state_change_seq": 8,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", generation, [old], ["p"])
    state["v1"]["sources"]["source"]["server_boot_id"] = boot

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": boot, "agents": [], "panes": [pane]}}
        return {"agents": []} if method == "agent.list" else {"panes": [pane]}

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    assert ch.refresh_v1_source(state, "source", "g", "/socket", reconnect=True)
    source = state["v1"]["sources"]["source"]
    assert source["attention"]["p"]["status"] == "done"
    assert source["panes"]["p"]["agent"] == "codex"


@pytest.mark.parametrize("difference", ["boot", "terminal", "sequence", "agent"])
def test_v1_missing_agent_keeps_attention_when_snapshots_disagree(
    monkeypatch, difference
):
    boot = "a" * 32
    pane = {
        "pane_id": "p",
        "terminal_id": "t",
        "agent_status": "unknown",
        "state_change_seq": 8,
    }
    old = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "blocked",
        "state_change_seq": 7,
        "agent": "codex",
    }
    state = {}
    ch.apply_v1_snapshot(
        state, "source", json.dumps(["g", boot], separators=(",", ":")), [old], ["p"]
    )
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    source["socket_generation"] = "g"
    calls = 0

    def rpc(path, method, params):
        nonlocal calls
        if method == "session.snapshot":
            calls += 1
            changed = dict(pane)
            if calls == 2 and difference == "terminal":
                changed["terminal_id"] = "other"
            if calls == 2 and difference == "sequence":
                changed["state_change_seq"] = 9
            agents = [old] if calls == 2 and difference == "agent" else []
            return {
                "snapshot": {
                    "server_boot_id": "b" * 32
                    if calls == 2 and difference == "boot"
                    else boot,
                    "agents": agents,
                    "panes": [changed],
                }
            }
        return {"agents": []} if method == "agent.list" else {"panes": [pane]}

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    ch.refresh_v1_source(state, "source", "g", "/socket", reconnect=True)
    assert ch.has_pending_attention(state["v1"]["sources"]["source"]["attention"], "p")
