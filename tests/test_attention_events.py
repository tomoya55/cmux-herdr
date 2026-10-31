import json

import pytest

from cmux_herdr_core import attention_ledger, core, focus, remote_bridge, source_sync


def test_v1_status_event_rejects_a_stale_subscription_server():
    state = {
        "v1": {
            "sources": {
                "source": {
                    "server_boot_id": "a" * 32,
                    "panes": {"p": {"status": "working"}},
                    "attention": {},
                }
            }
        }
    }
    before = json.loads(json.dumps(state))
    assert not attention_ledger.apply_v1_status_event(
        state, "source", {"pane_id": "p", "agent_status": "done"}, event_boot="b" * 32
    )
    assert state == before


@pytest.mark.parametrize("next_status", ["working", "idle", "done"])
def test_v1_unknown_keeps_blocked_until_known_transition(next_status):
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "blocked",
        "state_change_seq": 7,
    }
    state = {}
    attention_ledger.apply_v1_snapshot(
        state, "source", "g", [agent], ["p"], boot_id=boot
    )
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    unknown = {
        "pane_id": "p",
        "terminal_id": "t",
        "agent_status": "unknown",
        "state_change_seq": 8,
    }
    assert attention_ledger.apply_v1_status_event(
        state, "source", unknown, event_boot=boot
    )
    assert source["attention"]["p"]["status"] == "blocked"
    known = {**unknown, "agent_status": next_status, "state_change_seq": 9}
    assert attention_ledger.apply_v1_status_event(
        state, "source", known, event_boot=boot
    )
    assert source["attention"]["p"].get("status") != "blocked"


def test_v1_unknown_event_preserves_completion_identity_and_ordering():
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
        "state_change_seq": 8,
        "agent_status": "unknown",
    }
    assert attention_ledger.apply_v1_status_event(
        state, "source", event, event_boot=boot
    )
    observation = {
        "boot_id": boot,
        "pane_id": "p",
        "outer_focused": True,
        "agent_state_change_seq": None,
        "pane_state": {
            "terminal_id": "t",
            "state_change_seq": 9,
            "agent_present": False,
            "released_completion_seq": 7,
        },
    }
    snapshot = {
        "server_boot_id": boot,
        "agents": [],
        "panes": [{"pane_id": "p", "terminal_id": "t", "state_change_seq": 9}],
    }
    assert (
        focus.v1_verified_focus_episode(state, "source", observation, snapshot)
        == "g:t:7"
    )
    assert attention_ledger.apply_v1_status_event(
        state,
        "source",
        {**event, "state_change_seq": 7, "agent_status": "blocked"},
        event_boot=boot,
    )
    assert source["attention"]["p"]["status"] == "done"
    assert source["attention"]["p"]["sequence"] == 8


def test_v1_idle_snapshot_does_not_skip_unprocessed_later_completion():
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 6,
    }
    state = {}
    attention_ledger.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot

    def event(status, sequence):
        return {
            "pane_id": "p",
            "terminal_id": "t",
            "state_change_seq": sequence,
            "agent_status": status,
        }

    assert attention_ledger.apply_v1_status_event(
        state, "source", event("done", 7), event_boot=boot
    )
    attention_ledger.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "idle", "state_change_seq": 9}],
        ["p"],
    )
    assert attention_ledger.apply_v1_status_event(
        state, "source", event("working", 8), event_boot=boot
    )
    assert attention_ledger.apply_v1_status_event(
        state, "source", event("done", 9), event_boot=boot
    )
    assert source["attention"]["p"]["episode"] == "g:t:9"


def test_v1_ack_event_rejects_another_server_subscription(monkeypatch, tmp_path):
    state = {
        "v1": {
            "sources": {
                "source": {
                    "server_boot_id": "a" * 32,
                    "attention": {"p": {"status": "done", "episode": "episode"}},
                }
            }
        }
    }
    core.save_state(state)
    data = {"pane": {"pane_id": "p", "tokens": {"cmux_herdr_ack": "episode"}}}
    assert not source_sync.persist_v1_ack_event("source", data, "b" * 32)
    assert core.load_state()["v1"] == state["v1"]


def test_v1_status_events_keep_each_connected_completion_distinct(monkeypatch):
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 1,
    }
    attention_ledger.apply_v1_snapshot(state, "source", "boot", [agent], ["p"])
    ids = iter(["episode-a", "episode-b"])
    monkeypatch.setattr(attention_ledger, "new_v1_episode_id", lambda: next(ids))
    attention_ledger.apply_v1_status_event(
        state, "source", {"pane_id": "p", "agent_status": "done"}
    )
    a = state["v1"]["sources"]["source"]["attention"]["p"]["episode"]
    attention_ledger.apply_v1_status_event(
        state, "source", {"pane_id": "p", "agent_status": "working"}
    )
    attention_ledger.apply_v1_status_event(
        state, "source", {"pane_id": "p", "agent_status": "done"}
    )
    b = state["v1"]["sources"]["source"]["attention"]["p"]["episode"]
    assert (a, b) == ("episode-a", "episode-b")
    assert not attention_ledger.force_acknowledge_attention(
        state["v1"]["sources"]["source"]["attention"], "p", a
    )


def test_v1_remote_ack_event_only_accepts_matching_done_episode():
    state = {
        "v1": {
            "sources": {
                "remote": {
                    "attention": {"p": {"status": "done", "episode": "b"}},
                    "panes": {"p": {"status": "done"}},
                }
            }
        }
    }
    assert not attention_ledger.apply_v1_ack_event(
        state, "remote", {"pane": {"pane_id": "p", "tokens": {"cmux_herdr_ack": "a"}}}
    )
    assert attention_ledger.apply_v1_ack_event(
        state, "remote", {"pane": {"pane_id": "p", "tokens": {"cmux_herdr_ack": "b"}}}
    )
    assert not attention_ledger.has_pending_attention(
        state["v1"]["sources"]["remote"]["attention"], "p"
    )


def test_v1_ack_event_persists_matching_completion(monkeypatch, tmp_path):
    state = {
        "v1": {
            "sources": {
                "remote": {
                    "attention": {"p": {"status": "done", "episode": "b"}},
                    "panes": {"p": {"status": "done"}},
                }
            }
        }
    }
    core.save_state(state)
    assert source_sync.persist_v1_ack_event(
        "remote", {"pane": {"pane_id": "p", "tokens": {"cmux_herdr_ack": "b"}}}
    )
    saved = json.loads(core.STATE_PATH.read_text())
    assert not attention_ledger.has_pending_attention(
        saved["v1"]["sources"]["remote"]["attention"], "p"
    )


def test_v1_missing_agent_blocks_stale_blocked_but_preserves_queued_done(monkeypatch):
    boot = "a" * 32
    pane = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "unknown",
        "state_change_seq": 8,
    }

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": boot, "agents": [], "panes": [pane]}}
        return {"agents": []} if method == "agent.list" else {"panes": [pane]}

    monkeypatch.setattr(remote_bridge, "socket_rpc", rpc)
    state = {}
    assert source_sync.refresh_v1_source(state, "source", "g", "/socket")
    source = state["v1"]["sources"]["source"]
    blocked = {
        "pane_id": "p",
        "terminal_id": "t",
        "agent_status": "blocked",
        "state_change_seq": 6,
    }
    assert attention_ledger.apply_v1_status_event(
        state, "source", blocked, event_boot=boot
    )
    assert not attention_ledger.has_pending_attention(source["attention"], "p")
    done = {
        **blocked,
        "agent_status": "done",
        "state_change_seq": 7,
        "completion_seq": 7,
    }
    assert attention_ledger.apply_v1_status_event(
        state, "source", done, event_boot=boot
    )
    assert source["attention"]["p"]["status"] == "done"


def test_v1_slow_token_write_does_not_block_updates_or_commit_stale_episode(
    monkeypatch, tmp_path
):
    import threading

    core.save_state(
        {
            "v1": {
                "sources": {
                    "a": {
                        "server_boot_id": "boot",
                        "attention": {"p": {"status": "done", "episode": "old"}},
                        "panes": {"p": {}},
                    },
                    "b": {"value": 1},
                }
            }
        }
    )
    entered, release, updated = threading.Event(), threading.Event(), threading.Event()
    writes = []

    def rpc(path, method, params):
        value = params["tokens"]["cmux_herdr_done"]
        if value == "old":
            entered.set()
            assert release.wait(5)
        writes.append(value)
        return {}

    monkeypatch.setattr(remote_bridge, "socket_rpc", rpc)
    worker = threading.Thread(
        target=lambda: source_sync.sync_v1_episode_tokens(
            "a", "/socket", expected_boot="boot"
        )
    )

    def update():
        with core.state_lock():
            state = core.load_state()
            state["v1"]["sources"]["a"]["attention"]["p"]["episode"] = "new"
            state["v1"]["sources"]["b"]["value"] = 2
            core.save_state(state)
        updated.set()

    writer = threading.Thread(target=update)
    worker.start()
    try:
        assert entered.wait(3)
        writer.start()
        assert updated.wait(1), "remote token write held the shared state lock"
    finally:
        release.set()
        worker.join(5)
        if writer.ident is not None:
            writer.join(5)
    state = core.load_state()["v1"]["sources"]
    assert state["b"]["value"] == 2
    assert state["a"]["published_tokens"]["p"] != "old"
    source_sync.sync_v1_episode_tokens("a", "/socket", expected_boot="boot")
    assert writes == ["old", "new"]
    assert core.load_state()["v1"]["sources"]["a"]["published_tokens"]["p"] == "new"


def test_v1_episode_tokens_publish_only_confirmed_completions(monkeypatch, tmp_path):
    state = {
        "v1": {
            "sources": {
                "source": {
                    "attention": {
                        "p": {"status": "done", "episode": "confirmed"},
                        "q": {
                            "status": "done",
                            "episode": "pending",
                            "provisional": True,
                        },
                    },
                    "panes": {"p": {"status": "done"}, "q": {"status": "done"}},
                }
            }
        }
    }
    core.save_state(state)
    calls = []
    monkeypatch.setattr(
        remote_bridge,
        "socket_rpc",
        lambda path, method, params: calls.append((method, params)) or {"type": "ok"},
    )
    source_sync.sync_v1_episode_tokens("source", "/socket")
    assert len(calls) == 1
    assert calls[0][1]["pane_id"] == "p"
    assert calls[0][1]["tokens"] == {"cmux_herdr_done": "confirmed"}
    source_sync.sync_v1_episode_tokens("source", "/socket")
    assert len(calls) == 1


def test_v1_episode_tokens_retry_clear_after_lost_write_response(monkeypatch, tmp_path):
    core.save_state(
        {
            "v1": {
                "sources": {
                    "source": {
                        "server_boot_id": "boot",
                        "attention": {"p": {"status": "done", "episode": "episode"}},
                        "panes": {"p": {}},
                    }
                }
            }
        }
    )
    remote = {}

    def lost_response(path, method, params):
        remote.update(params["tokens"])
        raise remote_bridge.SocketError("response lost")

    monkeypatch.setattr(remote_bridge, "socket_rpc", lost_response)
    source_sync.sync_v1_episode_tokens("source", "/socket", expected_boot="boot")
    state = core.load_state()
    state["v1"]["sources"]["source"]["attention"] = {}
    core.save_state(state)
    monkeypatch.setattr(
        remote_bridge,
        "socket_rpc",
        lambda path, method, params: remote.update(params["tokens"]) or {},
    )
    source_sync.sync_v1_episode_tokens("source", "/socket", expected_boot="boot")
    assert remote["cmux_herdr_done"] is None


def test_v1_episode_tokens_use_stored_boot_for_legacy_caller(monkeypatch, tmp_path):
    core.save_state(
        {
            "v1": {
                "sources": {
                    "s": {
                        "server_boot_id": "boot",
                        "attention": {"p": {"status": "done", "episode": "episode"}},
                        "panes": {"p": {}},
                    }
                }
            }
        }
    )
    calls = []
    monkeypatch.setattr(
        remote_bridge,
        "socket_rpc",
        lambda path, method, params: calls.append((method, params)) or {},
    )
    source_sync.sync_v1_episode_tokens("s", "/socket")
    assert calls[0][0] == "pane.report_metadata_if_boot"
    assert calls[0][1]["expected_server_boot_id"] == "boot"


def test_v1_episode_tokens_clear_after_acknowledgment_or_pane_close(
    monkeypatch, tmp_path
):
    core.save_state(
        {
            "v1": {
                "sources": {
                    "source": {
                        "attention": {},
                        "panes": {"p": {"status": "idle"}},
                        "published_tokens": {"p": "old", "closed": "old"},
                    }
                }
            }
        }
    )
    calls = []
    monkeypatch.setattr(
        remote_bridge,
        "socket_rpc",
        lambda path, method, params: calls.append(params) or {},
    )
    source_sync.sync_v1_episode_tokens("source", "/socket")
    assert [(call["pane_id"], call["tokens"]) for call in calls] == [
        ("p", {"cmux_herdr_done": None})
    ]
    assert core.load_state()["v1"]["sources"]["source"]["published_tokens"] == {}


def test_v1_episode_tokens_guard_writes_with_server_boot(monkeypatch, tmp_path):
    core.save_state(
        {
            "v1": {
                "sources": {
                    "source": {
                        "server_boot_id": "old",
                        "attention": {"p": {"status": "done", "episode": "episode"}},
                        "panes": {"p": {}},
                    }
                }
            }
        }
    )
    calls = []

    def rpc(path, method, params):
        calls.append((method, params))
        raise remote_bridge.SocketError("server boot identity changed")

    monkeypatch.setattr(remote_bridge, "socket_rpc", rpc)
    source_sync.sync_v1_episode_tokens("source", "/socket", expected_boot="old")
    assert calls[0][0] == "pane.report_metadata_if_boot"
    assert calls[0][1]["expected_server_boot_id"] == "old"
    assert (
        core.load_state()["v1"]["sources"]["source"]["published_tokens"]["p"]
        != "episode"
    )
