import json

import pytest

from cmux_herdr_core import attention_ledger, focus, remote_bridge, source_sync


@pytest.mark.parametrize("release_status", ["done", "idle", "unknown"])
def test_v1_release_event_preserves_the_exact_completed_episode(release_status):
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 7,
        "completion_seq": 7,
    }
    state = {}
    attention_ledger.apply_v1_snapshot(
        state, "source", "g", [agent], ["p"], boot_id=boot
    )
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    source["attention"]["p"]["server_boot_id"] = boot
    event = {
        "pane_id": "p",
        "terminal_id": "t",
        "agent_status": release_status,
        "state_change_seq": 8,
        "completion_seq": None,
        "released_completion_seq": 7,
    }
    assert attention_ledger.apply_v1_status_event(
        state, "source", event, event_boot=boot
    )
    entry = source["attention"]["p"]
    assert entry["status"] == "done"
    assert entry["episode"] == "g:t:7"
    assert entry["sequence"] == 8
    observation = {
        "boot_id": boot,
        "pane_id": "p",
        "outer_focused": True,
        "agent_state_change_seq": None,
        "pane_state": {
            "terminal_id": "t",
            "state_change_seq": 8,
            "agent_present": False,
            "released_completion_seq": 7,
        },
    }
    snapshot = {
        "server_boot_id": boot,
        "agents": [],
        "panes": [{"pane_id": "p", "terminal_id": "t", "state_change_seq": 8}],
    }
    assert (
        focus.v1_verified_focus_episode(state, "source", observation, snapshot)
        == "g:t:7"
    )


def test_v1_release_event_keeps_acknowledgment_and_rejects_stale_provenance():
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 7,
        "completion_seq": 7,
    }
    state = {}
    attention_ledger.apply_v1_snapshot(
        state, "source", "g", [agent], ["p"], boot_id=boot
    )
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    assert attention_ledger.force_acknowledge_attention(
        source["attention"], "p", "g:t:7"
    )
    event = {
        "pane_id": "p",
        "terminal_id": "t",
        "agent_status": "done",
        "state_change_seq": 8,
        "released_completion_seq": 7,
    }
    assert attention_ledger.apply_v1_status_event(
        state, "source", event, event_boot=boot
    )
    assert source["attention"]["p"]["acknowledged"] == "g:t:7"
    assert source["attention"]["p"]["sequence"] == 8
    assert attention_ledger.apply_v1_status_event(
        state, "source", event, event_boot=boot
    )
    assert not attention_ledger.has_pending_attention(source["attention"], "p")
    assert not attention_ledger.apply_v1_status_event(
        state,
        "source",
        {**event, "state_change_seq": 9, "released_completion_seq": 6},
        event_boot=boot,
    )
    assert not attention_ledger.has_pending_attention(source["attention"], "p")


def test_v1_old_release_does_not_restore_completion_after_new_work():
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 7,
        "completion_seq": 7,
    }
    state = {}
    attention_ledger.apply_v1_snapshot(
        state, "source", "g", [agent], ["p"], boot_id=boot
    )
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    working = {
        "pane_id": "p",
        "terminal_id": "t",
        "agent_status": "working",
        "state_change_seq": 9,
    }
    assert attention_ledger.apply_v1_status_event(
        state, "source", working, event_boot=boot
    )
    release = {
        **working,
        "agent_status": "done",
        "state_change_seq": 8,
        "released_completion_seq": 7,
    }
    assert not attention_ledger.apply_v1_status_event(
        state, "source", release, event_boot=boot
    )
    assert not attention_ledger.has_pending_attention(source["attention"], "p")


@pytest.mark.parametrize("present", [False, True])
@pytest.mark.parametrize("acknowledged", [False, True])
def test_v1_release_snapshot_preserves_exact_episode(
    monkeypatch, present, acknowledged
):
    boot = "a" * 32
    generation = json.dumps(["g", boot], separators=(",", ":"))
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 7,
        "completion_seq": 7,
    }
    state = {}
    attention_ledger.apply_v1_snapshot(
        state, "source", generation, [agent], ["p"], boot_id=boot
    )
    source = state["v1"]["sources"]["source"]
    source.update(server_boot_id=boot, socket_generation="g")
    source["attention"]["p"]["server_boot_id"] = boot
    episode = f"{generation}:t:7"
    if acknowledged:
        assert attention_ledger.force_acknowledge_attention(
            source["attention"], "p", episode
        )
    pane = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "idle" if present else "unknown",
        "state_change_seq": 8,
        "released_completion_seq": 7,
    }
    agents = (
        [
            {
                **agent,
                "agent_status": "idle",
                "state_change_seq": 8,
                "completion_seq": None,
            }
        ]
        if present
        else []
    )
    snapshot = {"server_boot_id": boot, "agents": agents, "panes": [pane]}

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": snapshot}
        return {"agents": agents} if method == "agent.list" else {"panes": [pane]}

    monkeypatch.setattr(remote_bridge, "socket_rpc", rpc)
    assert source_sync.refresh_v1_source(
        state, "source", "g", "/socket", reconnect=True
    )
    entry = state["v1"]["sources"]["source"]["attention"]["p"]
    assert entry.get("acknowledged" if acknowledged else "episode") == episode
    assert entry["sequence"] == 8
    assert attention_ledger.has_pending_attention({"p": entry}, "p") is not acknowledged
    release = {
        "pane_id": "p",
        "terminal_id": "t",
        "agent_status": "done",
        "state_change_seq": 8,
        "released_completion_seq": 7,
    }
    assert attention_ledger.apply_v1_status_event(
        state, "source", release, event_boot=boot
    )
    entry = state["v1"]["sources"]["source"]["attention"]["p"]
    assert entry.get("acknowledged" if acknowledged else "episode") == episode
    assert attention_ledger.has_pending_attention({"p": entry}, "p") is not acknowledged
    if present and not acknowledged:
        observation = {
            "boot_id": boot,
            "pane_id": "p",
            "outer_focused": True,
            "agent_state_change_seq": 8,
            "pane_state": {
                "terminal_id": "t",
                "state_change_seq": 8,
                "agent_present": True,
                "released_completion_seq": 7,
            },
        }
        assert (
            focus.v1_verified_focus_episode(state, "source", observation, snapshot)
            == episode
        )


@pytest.mark.parametrize("fresh", [False, True])
def test_v1_release_snapshot_does_not_invent_completion_from_release_sequence(
    monkeypatch, fresh
):
    boot = "a" * 32
    generation = json.dumps(["g", boot], separators=(",", ":"))
    initial = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 6,
    }
    state = {}
    if not fresh:
        attention_ledger.apply_v1_snapshot(
            state, "source", generation, [initial], ["p"], boot_id=boot
        )
        state["v1"]["sources"]["source"].update(
            server_boot_id=boot, socket_generation="g"
        )
    pane = {
        **initial,
        "agent_status": "done",
        "state_change_seq": 8,
        "released_completion_seq": 7,
    }
    agent = {**pane, "completion_seq": None}
    snapshot = {"server_boot_id": boot, "agents": [agent], "panes": [pane]}

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": snapshot}
        return {"agents": [agent]} if method == "agent.list" else {"panes": [pane]}

    monkeypatch.setattr(remote_bridge, "socket_rpc", rpc)
    assert source_sync.refresh_v1_source(
        state, "source", "g", "/socket", reconnect=True
    )
    assert not attention_ledger.has_pending_attention(
        state["v1"]["sources"]["source"]["attention"], "p"
    )


def test_v1_release_during_snapshot_does_not_replace_pending_completion(monkeypatch):
    boot = "a" * 32
    generation = json.dumps(["g", boot], separators=(",", ":"))
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 7,
        "completion_seq": 7,
    }
    state = {}
    attention_ledger.apply_v1_snapshot(
        state, "source", generation, [agent], ["p"], boot_id=boot
    )
    state["v1"]["sources"]["source"].update(server_boot_id=boot, socket_generation="g")
    before = {"server_boot_id": boot, "panes": [agent]}
    released = {
        **agent,
        "state_change_seq": 8,
        "completion_seq": None,
        "released_completion_seq": 7,
    }
    after = {"server_boot_id": boot, "panes": [released]}
    snapshots = iter([before, after])

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": next(snapshots)}
        return (
            {"agents": [released]} if method == "agent.list" else {"panes": [released]}
        )

    monkeypatch.setattr(remote_bridge, "socket_rpc", rpc)
    assert not source_sync.refresh_v1_source(
        state, "source", "g", "/socket", reconnect=True
    )
    entry = state["v1"]["sources"]["source"]["attention"]["p"]
    assert entry["episode"] == f"{generation}:t:7"


@pytest.mark.parametrize("acknowledged", [False, True])
@pytest.mark.parametrize("present", [False, True])
@pytest.mark.parametrize("fresh", [False, True])
@pytest.mark.parametrize("initial_status", ["working", "blocked"])
def test_v1_delayed_completion_after_release_snapshot_is_restored(
    monkeypatch, acknowledged, present, fresh, initial_status
):
    boot = "a" * 32
    generation = json.dumps(["g", boot], separators=(",", ":"))
    initial = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": initial_status,
        "state_change_seq": 6,
    }
    state = {}
    if not fresh:
        attention_ledger.apply_v1_snapshot(
            state, "source", generation, [initial], ["p"], boot_id=boot
        )
        state["v1"]["sources"]["source"].update(
            server_boot_id=boot, socket_generation="g"
        )
    pane = {
        **initial,
        "agent_status": "unknown" if initial_status == "blocked" else "done",
        "state_change_seq": 8,
        "completion_seq": None,
        "released_completion_seq": 7,
    }
    if acknowledged:
        pane["tokens"] = {"cmux_herdr_ack": f"{generation}:t:7"}
    agents = [pane] if present else []
    snapshot = {"server_boot_id": boot, "agents": agents, "panes": [pane]}

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": snapshot}
        return {"agents": agents} if method == "agent.list" else {"panes": [pane]}

    monkeypatch.setattr(remote_bridge, "socket_rpc", rpc)
    assert source_sync.refresh_v1_source(
        state, "source", "g", "/socket", reconnect=True
    )
    event = {
        **initial,
        "agent_status": "done",
        "state_change_seq": 7,
        "completion_seq": 7,
    }
    assert attention_ledger.apply_v1_status_event(
        state, "source", event, event_boot=boot
    )
    entry = state["v1"]["sources"]["source"]["attention"]["p"]
    assert (
        entry.get("acknowledged" if acknowledged else "episode") == f"{generation}:t:7"
    )
    assert entry["sequence"] == 8
    assert attention_ledger.has_pending_attention({"p": entry}, "p") is not acknowledged
    if not acknowledged:
        invalidation = {**pane, "released_completion_seq": None}
        assert attention_ledger.apply_v1_status_event(
            state, "source", invalidation, event_boot=boot
        )
        assert not attention_ledger.has_pending_attention(
            state["v1"]["sources"]["source"]["attention"], "p"
        )


@pytest.mark.parametrize("new_sequence", [8, 9])
def test_v1_new_state_invalidates_old_release_provenance(new_sequence):
    boot = "a" * 32
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 6,
    }
    attention_ledger.apply_v1_snapshot(
        state, "source", "g", [agent], ["p"], boot_id=boot
    )
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    source["attention"]["p"].update(sequence=8, released_completion_seq=7)
    assert attention_ledger.apply_v1_status_event(
        state,
        "source",
        {**agent, "agent_status": "unknown", "state_change_seq": new_sequence},
        event_boot=boot,
    )
    assert attention_ledger.apply_v1_status_event(
        state,
        "source",
        {**agent, "agent_status": "idle", "state_change_seq": 7, "completion_seq": 7},
        event_boot=boot,
    )
    assert not attention_ledger.has_pending_attention(source["attention"], "p")


@pytest.mark.parametrize("sequence,status", [(8, "idle"), (9, "unknown")])
def test_v1_release_invalidation_retires_unfocusable_completion(sequence, status):
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 7,
        "completion_seq": 7,
    }
    state = {}
    attention_ledger.apply_v1_snapshot(
        state, "source", "g", [agent], ["p"], boot_id=boot
    )
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    release = {
        **agent,
        "agent_status": "idle",
        "state_change_seq": 8,
        "completion_seq": None,
        "released_completion_seq": 7,
    }
    assert attention_ledger.apply_v1_status_event(
        state, "source", release, event_boot=boot
    )
    assert attention_ledger.has_pending_attention(source["attention"], "p")
    assert attention_ledger.apply_v1_status_event(
        state,
        "source",
        {
            **release,
            "released_completion_seq": None,
            "state_change_seq": sequence,
            "agent_status": status,
        },
        event_boot=boot,
    )
    assert not attention_ledger.has_pending_attention(source["attention"], "p")


def test_v1_repeated_unknown_without_release_preserves_completion(monkeypatch):
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 7,
        "completion_seq": 7,
    }
    state = {}
    attention_ledger.apply_v1_snapshot(
        state, "source", "g", [agent], ["p"], boot_id=boot
    )
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    event = {
        **agent,
        "agent_status": "unknown",
        "state_change_seq": 8,
        "completion_seq": None,
    }
    assert attention_ledger.apply_v1_status_event(
        state, "source", event, event_boot=boot
    )
    assert attention_ledger.apply_v1_status_event(
        state, "source", event, event_boot=boot
    )
    assert attention_ledger.has_pending_attention(source["attention"], "p")
    pane = {**event, "released_completion_seq": None}
    snapshot = {"server_boot_id": boot, "agents": [pane], "panes": [pane]}
    monkeypatch.setattr(
        remote_bridge,
        "socket_rpc",
        lambda path, method, params: (
            {"snapshot": snapshot}
            if method == "session.snapshot"
            else {"agents": [pane]}
            if method == "agent.list"
            else {"panes": [pane]}
        ),
    )
    # Keep the same boot-derived generation used by a source refresh.
    generation = json.dumps(["g", boot], separators=(",", ":"))
    source["cursor_generation"] = generation
    source["attention"]["p"]["generation"] = generation
    source["attention"]["p"]["episode"] = f"{generation}:t:7"
    source["socket_generation"] = "g"
    assert source_sync.refresh_v1_source(state, "source", "g", "/socket")
    assert attention_ledger.has_pending_attention(
        state["v1"]["sources"]["source"]["attention"], "p"
    )


def test_v1_acknowledged_release_invalidation_advances_watermark(monkeypatch):
    boot = "a" * 32
    generation = json.dumps(["g", boot], separators=(",", ":"))
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 7,
        "completion_seq": 7,
    }
    state = {}
    attention_ledger.apply_v1_snapshot(
        state, "source", generation, [agent], ["p"], boot_id=boot
    )
    source = state["v1"]["sources"]["source"]
    source.update(server_boot_id=boot, socket_generation="g")
    assert attention_ledger.apply_v1_status_event(
        state,
        "source",
        {
            **agent,
            "agent_status": "idle",
            "state_change_seq": 8,
            "completion_seq": None,
            "released_completion_seq": 7,
        },
        event_boot=boot,
    )
    assert attention_ledger.force_acknowledge_attention(
        source["attention"], "p", f"{generation}:t:7"
    )
    pane = {
        **agent,
        "agent_status": "unknown",
        "state_change_seq": 12,
        "completion_seq": None,
    }
    snapshot = {"server_boot_id": boot, "agents": [], "panes": [pane]}
    monkeypatch.setattr(
        remote_bridge,
        "socket_rpc",
        lambda path, method, params: (
            {"snapshot": snapshot}
            if method == "session.snapshot"
            else {"agents": []}
            if method == "agent.list"
            else {"panes": [pane]}
        ),
    )
    assert source_sync.refresh_v1_source(state, "source", "g", "/socket")
    assert attention_ledger.apply_v1_status_event(
        state,
        "source",
        {**agent, "state_change_seq": 10, "completion_seq": 10},
        event_boot=boot,
    )
    assert not attention_ledger.has_pending_attention(
        state["v1"]["sources"]["source"]["attention"], "p"
    )
