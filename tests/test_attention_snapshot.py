import json

import pytest

import cmux_herdr as ch


def test_attention_ignores_stale_forced_acknowledgement():
    attention = {}
    ch.apply_attention_status(attention, "pane", "done", "episode-a")
    ch.apply_attention_status(attention, "pane", "working", "working-b")
    ch.apply_attention_status(attention, "pane", "done", "episode-b")
    assert not ch.force_acknowledge_attention(attention, "pane", "episode-a")
    assert attention["pane"] == {"status": "done", "episode": "episode-b"}
    assert ch.force_acknowledge_attention(attention, "pane", "episode-b")
    assert not ch.has_pending_attention(attention, "pane")


def test_attention_preserves_done_on_idle_and_never_forces_blocked():
    attention = {}
    ch.apply_attention_status(attention, "pane", "done", "episode-a")
    ch.apply_attention_status(attention, "pane", "idle", "idle-a")
    assert attention["pane"]["episode"] == "episode-a"
    ch.apply_attention_status(attention, "pane", "blocked", "episode-b")
    assert not ch.force_acknowledge_attention(attention, "pane", "episode-b")
    assert attention["pane"]["status"] == "blocked"
    ch.apply_attention_status(attention, "pane", "working", "working-b")
    assert "pane" not in attention


def test_acknowledged_episode_is_not_resurrected_by_duplicate_event():
    attention = {}
    ch.apply_attention_status(attention, "pane", "done", "episode-a")
    assert ch.force_acknowledge_attention(attention, "pane", "episode-a")
    ch.apply_attention_status(attention, "pane", "done", "episode-a")
    assert not ch.has_pending_attention(attention, "pane")
    ch.apply_attention_status(attention, "pane", "done", "episode-b")
    assert ch.has_pending_attention(attention, "pane")


def test_attention_rejects_out_of_order_status_after_new_completion():
    attention = {}
    ch.apply_attention_status(attention, "pane", "done", "episode-a", sequence=1)
    ch.apply_attention_status(attention, "pane", "working", "working", sequence=2)
    ch.apply_attention_status(attention, "pane", "done", "episode-b", sequence=3)
    ch.apply_attention_status(attention, "pane", "working", "old", sequence=2)
    assert attention["pane"]["episode"] == "episode-b"


def test_unsequenced_duplicate_preserves_ordering_watermark():
    attention = {}
    ch.apply_attention_status(attention, "pane", "done", "episode-b", sequence=30)
    ch.apply_attention_status(attention, "pane", "done", "episode-b")
    ch.apply_attention_status(attention, "pane", "working", "old", sequence=20)
    assert attention["pane"]["episode"] == "episode-b"
    assert attention["pane"]["sequence"] == 30


def test_snapshot_reconciles_attention_with_seen_and_new_work():
    attention = {}

    def agent(status, sequence):
        return {
            "pane_id": "p",
            "terminal_id": "t",
            "workspace_id": "w",
            "agent_status": status,
            "state_change_seq": sequence,
            "agent": "claude",
        }

    ch.reconcile_agent_snapshot(attention, [agent("done", 1)], "generation")
    original = attention["p"]["episode"]
    ch.reconcile_agent_snapshot(attention, [agent("idle", 1)], "generation")
    assert attention["p"]["episode"] == original
    ch.reconcile_agent_snapshot(attention, [agent("working", 2)], "generation")
    assert not ch.has_pending_attention(attention, "p")
    ch.reconcile_agent_snapshot(attention, [agent("done", 3)], "generation")
    assert attention["p"]["episode"] != original


def test_snapshot_new_server_generation_accepts_reset_sequence():
    attention = {}
    first = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 100,
    }
    second = {**first, "state_change_seq": 1}
    ch.reconcile_agent_snapshot(attention, [first], "old")
    ch.reconcile_agent_snapshot(attention, [second], "new")
    assert attention["p"]["episode"] != "old:t:100"
    assert attention["p"]["sequence"] == 1


def test_snapshot_replaced_terminal_does_not_reuse_old_episode():
    attention = {}
    first = {
        "pane_id": "p",
        "terminal_id": "old",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 9,
    }
    second = {
        **first,
        "terminal_id": "new",
        "agent_status": "working",
        "state_change_seq": 0,
    }
    ch.reconcile_agent_snapshot(attention, [first], "generation")
    ch.reconcile_agent_snapshot(attention, [second], "generation")
    assert not ch.has_pending_attention(attention, "p")


def test_v1_snapshot_persists_ledger_and_agent_metadata():
    state = {}
    agents = [
        {
            "pane_id": "p",
            "terminal_id": "t",
            "workspace_id": "w",
            "agent_status": "done",
            "state_change_seq": 1,
            "agent": "codex",
        }
    ]
    ch.apply_v1_snapshot(state, "local:tom", "boot", agents, ["p"])
    source = state["v1"]["sources"]["local:tom"]
    assert source["attention"]["p"]["status"] == "done"
    assert source["panes"]["p"]["agent"] == "codex"
    before = json.loads(json.dumps(state))
    ch.apply_v1_snapshot(state, "local:tom", "boot", None, None)
    assert state == before


def test_v1_snapshot_only_removes_panes_from_authoritative_list():
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 1,
    }
    ch.apply_v1_snapshot(state, "remote:one", "boot", [agent], ["p"])
    ch.apply_v1_snapshot(state, "remote:one", "boot", [], None)
    assert ch.has_pending_attention(
        state["v1"]["sources"]["remote:one"]["attention"], "p"
    )
    ch.apply_v1_snapshot(state, "remote:one", "boot", [], [])
    assert "p" not in state["v1"]["sources"]["remote:one"]["attention"]


def test_unsequenced_event_keeps_generation_for_restart():
    attention = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "agent_status": "done",
        "state_change_seq": 30,
    }
    ch.reconcile_agent_snapshot(attention, [agent], "old")
    ch.apply_attention_status(attention, "p", "working", "work")
    ch.reconcile_agent_snapshot(attention, [{**agent, "state_change_seq": 1}], "new")
    assert attention["p"]["status"] == "done"
    assert attention["p"]["generation"] == "new"


def test_stale_snapshot_does_not_replace_sidebar_pane_status():
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 30,
    }
    ch.apply_v1_snapshot(state, "source", "boot", [agent], ["p"])
    ch.apply_v1_snapshot(
        state,
        "source",
        "boot",
        [{**agent, "agent_status": "working", "state_change_seq": 20}],
        ["p"],
    )
    source = state["v1"]["sources"]["source"]
    assert (
        ch.sidebar_panes(source["panes"], source["attention"])["p"]["status"] == "done"
    )


def test_v1_newer_noncompletion_idle_event_retires_previous_done():
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
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    event = {
        "pane_id": "p",
        "terminal_id": "t",
        "state_change_seq": 8,
        "completion_seq": None,
        "agent_status": "idle",
    }
    assert ch.apply_v1_status_event(state, "source", event, event_boot=boot)
    assert not ch.has_pending_attention(source["attention"], "p")
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [
            {
                **agent,
                "agent_status": "idle",
                "state_change_seq": 8,
                "completion_seq": None,
            }
        ],
        ["p"],
        reconnect=True,
    )
    assert not ch.has_pending_attention(source["attention"], "p")


def test_v1_sequenced_events_do_not_resurrect_a_newer_acknowledged_completion():
    boot = "a" * 32
    generation = "generation"
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 6,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", generation, [agent], ["p"])
    state["v1"]["sources"]["source"]["server_boot_id"] = boot

    def event(status, sequence):
        return {
            "pane_id": "p",
            "terminal_id": "t",
            "state_change_seq": sequence,
            "agent_status": status,
        }

    assert ch.apply_v1_status_event(state, "source", event("done", 7), event_boot=boot)
    ch.apply_v1_snapshot(
        state,
        "source",
        generation,
        [{**agent, "agent_status": "done", "state_change_seq": 9}],
        ["p"],
    )
    attention = state["v1"]["sources"]["source"]["attention"]
    episode = attention["p"]["episode"]
    assert ch.force_acknowledge_attention(attention, "p", episode)
    assert ch.apply_v1_status_event(
        state, "source", event("working", 8), event_boot=boot
    )
    assert ch.apply_v1_status_event(state, "source", event("done", 9), event_boot=boot)
    assert attention["p"]["acknowledged"] == episode
    assert not ch.has_pending_attention(attention, "p")


def test_v1_sequenced_done_event_has_exact_episode_before_snapshot():
    boot = "a" * 32
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 6,
    }
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    assert ch.apply_v1_status_event(
        state,
        "source",
        {
            "pane_id": "p",
            "terminal_id": "t",
            "state_change_seq": 7,
            "agent_status": "done",
        },
        event_boot=boot,
    )
    assert source["attention"]["p"]["episode"] == "g:t:7"
    assert not source["attention"]["p"].get("provisional")


def test_v1_seen_idle_event_still_records_a_completed_turn():
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 6,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    event = {
        "pane_id": "p",
        "terminal_id": "t",
        "state_change_seq": 7,
        "completion_seq": 7,
        "agent_status": "idle",
    }
    assert ch.apply_v1_status_event(state, "source", event, event_boot=boot)
    assert source["attention"]["p"]["status"] == "done"
    assert source["attention"]["p"]["episode"] == "g:t:7"


def test_v1_queued_seen_completion_survives_ahead_idle_snapshot():
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 6,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "idle", "state_change_seq": 7, "completion_seq": 7}],
        ["p"],
    )
    event = {
        "pane_id": "p",
        "terminal_id": "t",
        "state_change_seq": 7,
        "completion_seq": 7,
        "agent_status": "idle",
    }
    assert ch.apply_v1_status_event(state, "source", event, event_boot=boot)
    assert source["attention"]["p"]["episode"] == "g:t:7"


def test_v1_queued_done_survives_ahead_seen_idle_snapshot():
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 6,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "idle", "state_change_seq": 7, "completion_seq": 7}],
        ["p"],
    )
    event = {
        "pane_id": "p",
        "terminal_id": "t",
        "state_change_seq": 7,
        "completion_seq": 7,
        "agent_status": "done",
    }
    assert ch.apply_v1_status_event(state, "source", event, event_boot=boot)
    assert source["attention"]["p"]["episode"] == "g:t:7"


def test_v1_idle_snapshot_new_completion_survives_old_events_after_ack():
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 6,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot

    def event(status, sequence):
        return {
            "pane_id": "p",
            "terminal_id": "t",
            "state_change_seq": sequence,
            "agent_status": status,
        }

    assert ch.apply_v1_status_event(state, "source", event("done", 7), event_boot=boot)
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "idle", "state_change_seq": 9, "completion_seq": 9}],
        ["p"],
    )
    attention = source["attention"]
    assert attention["p"]["episode"] == "g:t:9"
    assert ch.force_acknowledge_attention(attention, "p", "g:t:9")
    assert ch.apply_v1_status_event(
        state, "source", event("working", 8), event_boot=boot
    )
    assert ch.apply_v1_status_event(state, "source", event("done", 9), event_boot=boot)
    assert attention["p"]["acknowledged"] == "g:t:9"


def test_v1_duplicate_done_after_seen_does_not_resurrect_acknowledged(monkeypatch):
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 1,
    }
    ch.apply_v1_snapshot(state, "source", "boot", [agent], ["p"])
    monkeypatch.setattr(ch, "new_v1_episode_id", lambda: "episode-a")
    ch.apply_v1_status_event(state, "source", {"pane_id": "p", "agent_status": "done"})
    attention = state["v1"]["sources"]["source"]["attention"]
    ch.apply_v1_snapshot(
        state,
        "source",
        "boot",
        [{**agent, "agent_status": "done", "state_change_seq": 2}],
        ["p"],
    )
    assert ch.force_acknowledge_attention(attention, "p", attention["p"]["episode"])
    ch.apply_v1_status_event(state, "source", {"pane_id": "p", "agent_status": "idle"})
    ch.apply_v1_status_event(state, "source", {"pane_id": "p", "agent_status": "done"})
    assert not ch.has_pending_attention(attention, "p")


def test_v1_status_event_is_persisted_for_existing_source(monkeypatch, tmp_path):
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 1,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "boot", [agent], ["p"])
    ch.save_state(state)
    assert ch.persist_v1_status_event(
        "source", {"pane_id": "p", "agent_status": "blocked"}
    )
    saved = json.loads(ch.STATE_PATH.read_text())
    assert saved["v1"]["sources"]["source"]["attention"]["p"]["status"] == "blocked"


@pytest.mark.parametrize("token,expected_pending", [("g:t:1", False), ("old", True)])
def test_v1_snapshot_recovers_only_matching_force_read(token, expected_pending):
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 1,
    }
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [agent],
        ["p"],
        [{"pane_id": "p", "tokens": {"cmux_herdr_ack": token}}],
        reconnect=True,
    )
    assert (
        ch.has_pending_attention(state["v1"]["sources"]["source"]["attention"], "p")
        == expected_pending
    )


def test_v1_snapshot_does_not_resurrect_event_acknowledged_completion(monkeypatch):
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 1,
    }
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    monkeypatch.setattr(ch, "new_v1_episode_id", lambda: "event-a")
    ch.apply_v1_status_event(state, "source", {"pane_id": "p", "agent_status": "done"})
    source = state["v1"]["sources"]["source"]
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "done", "state_change_seq": 2}],
        ["p"],
    )
    assert ch.force_acknowledge_attention(
        source["attention"], "p", source["attention"]["p"]["episode"]
    )
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "done", "state_change_seq": 2}],
        ["p"],
    )
    assert not ch.has_pending_attention(source["attention"], "p")


def test_v1_reconnect_does_not_acknowledge_unseen_new_completion(monkeypatch):
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 1,
    }
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    monkeypatch.setattr(ch, "new_v1_episode_id", lambda: "event-a")
    ch.apply_v1_status_event(state, "source", {"pane_id": "p", "agent_status": "done"})
    source = state["v1"]["sources"]["source"]
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "done", "state_change_seq": 2}],
        ["p"],
    )
    assert ch.force_acknowledge_attention(
        source["attention"], "p", source["attention"]["p"]["episode"]
    )
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "done", "state_change_seq": 4}],
        ["p"],
    )
    assert ch.has_pending_attention(source["attention"], "p")


def test_v1_duplicate_event_preserves_provisional_acknowledgment(monkeypatch):
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 1,
    }
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    monkeypatch.setattr(ch, "new_v1_episode_id", lambda: "event-a")
    event = {"pane_id": "p", "agent_status": "done"}
    ch.apply_v1_status_event(state, "source", event)
    ch.apply_v1_status_event(state, "source", event)
    source = state["v1"]["sources"]["source"]
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "done", "state_change_seq": 2}],
        ["p"],
    )
    assert ch.force_acknowledge_attention(
        source["attention"], "p", source["attention"]["p"]["episode"]
    )
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "done", "state_change_seq": 2}],
        ["p"],
    )
    assert not ch.has_pending_attention(source["attention"], "p")


def test_v1_observed_transitions_correlate_confirmed_completion(monkeypatch):
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 1,
    }
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    ids = iter(["blocked-a", "done-b"])
    monkeypatch.setattr(ch, "new_v1_episode_id", lambda: next(ids))
    for status in ("blocked", "working", "done"):
        ch.apply_v1_status_event(
            state, "source", {"pane_id": "p", "agent_status": status}
        )
    source = state["v1"]["sources"]["source"]
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "done", "state_change_seq": 4}],
        ["p"],
    )
    assert ch.force_acknowledge_attention(
        source["attention"], "p", source["attention"]["p"]["episode"]
    )
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "done", "state_change_seq": 4}],
        ["p"],
    )
    assert not ch.has_pending_attention(source["attention"], "p")


def test_v1_observed_events_across_panes_correlate_global_sequence(monkeypatch):
    state = {}
    a = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 1,
    }
    b = {
        "pane_id": "q",
        "terminal_id": "u",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 2,
    }
    ch.apply_v1_snapshot(state, "source", "g", [a, b], ["p", "q"])
    ids = iter(["done-a", "blocked-b"])
    monkeypatch.setattr(ch, "new_v1_episode_id", lambda: next(ids))
    ch.apply_v1_status_event(state, "source", {"pane_id": "p", "agent_status": "done"})
    ch.apply_v1_status_event(
        state, "source", {"pane_id": "q", "agent_status": "blocked"}
    )
    source = state["v1"]["sources"]["source"]
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [
            {**a, "agent_status": "done", "state_change_seq": 3},
            {**b, "agent_status": "blocked", "state_change_seq": 4},
        ],
        ["p", "q"],
    )
    assert ch.force_acknowledge_attention(
        source["attention"], "p", source["attention"]["p"]["episode"]
    )
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [
            {**a, "agent_status": "done", "state_change_seq": 3},
            {**b, "agent_status": "blocked", "state_change_seq": 4},
        ],
        ["p", "q"],
    )
    assert not ch.has_pending_attention(source["attention"], "p")


def test_v1_reconnect_discards_untrusted_provisional_sequence(monkeypatch, tmp_path):
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 3,
    }
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    ch.apply_v1_status_event(
        state, "source", {"pane_id": "p", "agent_status": "working"}
    )
    ch.apply_v1_status_event(state, "source", {"pane_id": "p", "agent_status": "done"})
    episode = state["v1"]["sources"]["source"]["attention"]["p"]["episode"]
    assert not ch.force_acknowledge_attention(
        state["v1"]["sources"]["source"]["attention"], "p", episode
    )
    ch.save_state(state)
    latest = {**agent, "state_change_seq": 5}
    monkeypatch.setattr(
        ch,
        "socket_rpc",
        lambda path, method, params: (
            {"agents": [latest]}
            if method == "agent.list"
            else {"panes": [{"pane_id": "p"}]}
        ),
    )
    assert ch.sync_v1_source("source", "g", "/socket", reset_predictions=True)
    saved = json.loads(ch.STATE_PATH.read_text())
    assert ch.has_pending_attention(saved["v1"]["sources"]["source"]["attention"], "p")


def test_v1_force_ack_requires_authoritative_episode(monkeypatch):
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 1,
    }
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    monkeypatch.setattr(ch, "new_v1_episode_id", lambda: "unverified")
    ch.apply_v1_status_event(state, "source", {"pane_id": "p", "agent_status": "done"})
    attention = state["v1"]["sources"]["source"]["attention"]
    assert not ch.force_acknowledge_attention(attention, "p", "unverified")
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "done", "state_change_seq": 2}],
        ["p"],
    )
    confirmed = attention["p"]["episode"]
    assert ch.force_acknowledge_attention(attention, "p", confirmed)


def test_v1_seen_snapshot_confirms_provisional_completion(monkeypatch):
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 1,
    }
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    monkeypatch.setattr(ch, "new_v1_episode_id", lambda: "unverified")
    ch.apply_v1_status_event(state, "source", {"pane_id": "p", "agent_status": "done"})
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "idle", "state_change_seq": 2}],
        ["p"],
    )
    attention = state["v1"]["sources"]["source"]["attention"]
    assert ch.force_acknowledge_attention(attention, "p", attention["p"]["episode"])


def test_v1_seen_event_then_snapshot_confirms_completion(monkeypatch):
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 1,
    }
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    monkeypatch.setattr(ch, "new_v1_episode_id", lambda: "pending")
    ch.apply_v1_status_event(state, "source", {"pane_id": "p", "agent_status": "done"})
    ch.apply_v1_status_event(state, "source", {"pane_id": "p", "agent_status": "idle"})
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "idle", "state_change_seq": 2}],
        ["p"],
    )
    attention = state["v1"]["sources"]["source"]["attention"]
    assert ch.force_acknowledge_attention(attention, "p", attention["p"]["episode"])
