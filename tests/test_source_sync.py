import json

import pytest

import cmux_herdr as ch


def test_v1_refresh_reads_agent_and_pane_snapshots(monkeypatch):
    calls = []

    def fake_rpc(path, method, params):
        calls.append(method)
        if method == "agent.list":
            return {
                "agents": [
                    {
                        "pane_id": "p",
                        "terminal_id": "t",
                        "workspace_id": "w",
                        "agent_status": "blocked",
                        "state_change_seq": 2,
                    }
                ]
            }
        return {"panes": [{"pane_id": "p"}]}

    monkeypatch.setattr(ch, "socket_rpc", fake_rpc)
    state = {}
    assert ch.refresh_v1_source(state, "source", "boot", "/socket")
    assert calls == ["session.snapshot", "agent.list", "pane.list", "session.snapshot"]
    assert state["v1"]["sources"]["source"]["attention"]["p"]["status"] == "blocked"


def test_v1_refresh_binds_completion_to_same_server_instance(monkeypatch):
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "agent_status": "done",
        "state_change_seq": 7,
    }

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": boot, "agents": [agent]}}
        if method == "agent.list":
            return {"agents": [agent]}
        return {"panes": [{"pane_id": "p"}]}

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    state = {}
    assert ch.refresh_v1_source(state, "source", "gen", "/socket")
    assert state["v1"]["sources"]["source"]["attention"]["p"]["server_boot_id"] == boot


def test_v1_refresh_binds_quick_done_to_idle_event_for_known_server(monkeypatch):
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 6,
    }

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": boot, "agents": [dict(agent)]}}
        return (
            {"agents": [dict(agent)]}
            if method == "agent.list"
            else {"panes": [{"pane_id": "p"}]}
        )

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    state = {}
    assert ch.refresh_v1_source(state, "source", "gen", "/socket")
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
    agent.update(agent_status="idle", state_change_seq=7)
    assert ch.refresh_v1_source(state, "source", "gen", "/socket")
    entry = state["v1"]["sources"]["source"]["attention"]["p"]
    assert entry["status"] == "done" and entry["server_boot_id"] == boot


@pytest.mark.parametrize("present", [False, True])
def test_v1_new_snapshot_invalidates_old_release_provenance(monkeypatch, present):
    boot = "a" * 32
    pane = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "unknown",
        "state_change_seq": 8,
        "released_completion_seq": 7,
    }
    state = {}

    def rpc(path, method, params):
        agents = [pane] if present else []
        if method == "session.snapshot":
            return {
                "snapshot": {"server_boot_id": boot, "agents": agents, "panes": [pane]}
            }
        return {"agents": agents} if method == "agent.list" else {"panes": [pane]}

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    assert ch.refresh_v1_source(state, "source", "g", "/socket")
    pane.update(state_change_seq=9, released_completion_seq=None)
    assert ch.refresh_v1_source(state, "source", "g", "/socket")
    event = {**pane, "agent_status": "done", "state_change_seq": 7, "completion_seq": 7}
    assert ch.apply_v1_status_event(state, "source", event, event_boot=boot)
    assert not ch.has_pending_attention(
        state["v1"]["sources"]["source"]["attention"], "p"
    )


def test_v1_reconnect_recovers_completion_already_marked_idle(monkeypatch):
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 6,
        "completion_seq": None,
    }

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": boot, "agents": [dict(agent)]}}
        return (
            {"agents": [dict(agent)]}
            if method == "agent.list"
            else {"panes": [{"pane_id": "p"}]}
        )

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    state = {}
    assert ch.refresh_v1_source(state, "source", "gen", "/socket")
    agent.update(agent_status="idle", state_change_seq=7, completion_seq=7)
    assert ch.refresh_v1_source(state, "source", "gen", "/socket", reconnect=True)
    entry = state["v1"]["sources"]["source"]["attention"]["p"]
    assert entry["status"] == "done" and entry["server_boot_id"] == boot
    assert ch.force_acknowledge_attention(
        state["v1"]["sources"]["source"]["attention"], "p", entry["episode"]
    )
    assert ch.refresh_v1_source(state, "source", "gen", "/socket", reconnect=True)
    assert not ch.has_pending_attention(
        state["v1"]["sources"]["source"]["attention"], "p"
    )


def test_v1_focus_accepts_completion_when_snapshot_changes_done_to_idle(monkeypatch):
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 7,
        "completion_seq": 7,
    }
    calls = 0

    def rpc(path, method, params):
        nonlocal calls
        if method == "session.snapshot":
            calls += 1
            status = "done" if calls == 1 else "idle"
            return {
                "snapshot": {
                    "server_boot_id": boot,
                    "agents": [{**agent, "agent_status": status}],
                }
            }
        return (
            {"agents": [dict(agent)]}
            if method == "agent.list"
            else {"panes": [{"pane_id": "p"}]}
        )

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    state = {}
    assert ch.refresh_v1_source(state, "source", "gen", "/socket")
    idle = {**agent, "agent_status": "idle"}
    assert ch.apply_v1_status_event(state, "source", idle, boot)
    observation = {
        "boot_id": boot,
        "pane_id": "p",
        "agent_state_change_seq": 7,
        "outer_focused": True,
    }
    assert ch.v1_verified_focus_episode(
        state, "source", observation, {"server_boot_id": boot, "agents": [idle]}
    )


def test_v1_sequenced_done_survives_failed_refresh_and_idle_reconnect(
    monkeypatch, tmp_path
):
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 6,
    }

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": boot, "agents": [dict(agent)]}}
        return (
            {"agents": [dict(agent)]}
            if method == "agent.list"
            else {"panes": [{"pane_id": "p"}]}
        )

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    assert ch.sync_v1_source("source", "gen", "/socket")
    event = {
        "pane_id": "p",
        "terminal_id": "t",
        "state_change_seq": 7,
        "completion_seq": 7,
        "agent_status": "done",
    }
    assert ch.persist_v1_status_event("source", event, boot)
    monkeypatch.setattr(
        ch, "socket_rpc", lambda *args: (_ for _ in ()).throw(OSError("disconnected"))
    )
    assert not ch.sync_v1_source("source", "gen", "/socket")
    agent.update(agent_status="idle", state_change_seq=7, completion_seq=7)
    monkeypatch.setattr(ch, "socket_rpc", rpc)
    assert ch.sync_v1_source("source", "gen", "/socket", reset_predictions=True)
    entry = ch.load_state()["v1"]["sources"]["source"]["attention"]["p"]
    assert entry["status"] == "done" and not entry.get("provisional")
    assert entry["sequence"] == 7 and entry["server_boot_id"] == boot


def test_v1_reconnect_retires_superseded_completion_when_identity_was_reset(
    monkeypatch, tmp_path
):
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 7,
        "completion_seq": 7,
    }

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": boot, "agents": [dict(agent)]}}
        return (
            {"agents": [dict(agent)]}
            if method == "agent.list"
            else {"panes": [{"pane_id": "p"}]}
        )

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    assert ch.sync_v1_source("source", "gen", "/socket")
    agent.update(agent_status="idle", state_change_seq=9, completion_seq=None)
    assert ch.sync_v1_source("source", "gen", "/socket", reset_predictions=True)
    attention = ch.load_state()["v1"]["sources"]["source"]["attention"]
    assert not ch.has_pending_attention(attention, "p")
    event = {
        "pane_id": "p",
        "terminal_id": "t",
        "state_change_seq": 9,
        "completion_seq": 9,
        "agent_status": "done",
    }
    assert ch.persist_v1_status_event("source", event, boot)
    attention = ch.load_state()["v1"]["sources"]["source"]["attention"]
    assert not ch.has_pending_attention(attention, "p")


def test_v1_refresh_rejects_server_restart_during_snapshot(monkeypatch):
    boots = iter(["a" * 32, "b" * 32])

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": next(boots), "agents": []}}
        return {"agents": []} if method == "agent.list" else {"panes": []}

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    state = {}
    assert not ch.refresh_v1_source(state, "source", "gen", "/socket")
    assert state == {}


def test_v1_supervisor_refresh_rejects_new_boot_without_overwriting_old_state(
    monkeypatch,
):
    state = {
        "v1": {
            "sources": {
                "old": {
                    "server_boot_id": "a" * 32,
                    "attention": {"p": {"status": "done", "episode": "old"}},
                }
            }
        }
    }
    before = json.loads(json.dumps(state))

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": "b" * 32, "agents": []}}
        return {"agents": []} if method == "agent.list" else {"panes": []}

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    assert not ch.refresh_v1_source(
        state, "old", "gen", "/socket", expected_boot="a" * 32
    )
    assert state == before


def test_v1_refresh_distinguishes_server_restart_when_socket_generation_repeats(
    monkeypatch,
):
    boot = ["a" * 32]
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "agent_status": "done",
        "state_change_seq": 7,
    }

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": boot[0], "agents": [agent]}}
        return (
            {"agents": [agent]}
            if method == "agent.list"
            else {"panes": [{"pane_id": "p"}]}
        )

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    state = {}
    assert ch.refresh_v1_source(state, "source", "gen", "/socket")
    old = state["v1"]["sources"]["source"]["attention"]["p"]["episode"]
    boot[0] = "b" * 32
    assert ch.refresh_v1_source(state, "source", "gen", "/socket")
    attention = state["v1"]["sources"]["source"]["attention"]
    assert attention["p"]["episode"] != old
    assert not ch.force_acknowledge_attention(attention, "p", old)


def test_v1_refresh_preserves_known_identity_if_boot_id_temporarily_missing(
    monkeypatch,
):
    boot = ["a" * 32]
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "agent_status": "done",
        "state_change_seq": 7,
    }

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": boot[0], "agents": [agent]}}
        return (
            {"agents": [agent]}
            if method == "agent.list"
            else {"panes": [{"pane_id": "p"}]}
        )

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    state = {}
    assert ch.refresh_v1_source(state, "source", "gen", "/socket")
    before = json.loads(json.dumps(state))
    boot[0] = None
    assert not ch.refresh_v1_source(state, "source", "gen", "/socket")
    assert state == before


def test_v1_refresh_retains_state_on_pane_list_failure(monkeypatch):
    state = {
        "v1": {
            "sources": {
                "source": {
                    "attention": {"p": {"status": "done", "episode": "a"}},
                    "panes": {},
                }
            }
        }
    }
    before = json.loads(json.dumps(state))

    def fake_rpc(path, method, params):
        if method == "pane.list":
            raise OSError("disconnected")
        return {"agents": []}

    monkeypatch.setattr(ch, "socket_rpc", fake_rpc)
    assert not ch.refresh_v1_source(state, "source", "boot", "/socket")
    assert state == before


def test_v1_refresh_rejects_malformed_agent_snapshot_atomically(monkeypatch):
    state = {}

    def fake_rpc(path, method, params):
        return (
            {
                "agents": [
                    {
                        "pane_id": "p",
                        "terminal_id": "t",
                        "state_change_seq": 1,
                        "agent_status": "done",
                    },
                    None,
                ]
            }
            if method == "agent.list"
            else {"panes": [{"pane_id": "p"}]}
        )

    monkeypatch.setattr(ch, "socket_rpc", fake_rpc)
    assert not ch.refresh_v1_source(state, "source", "boot", "/socket")
    assert state == {}


def test_v1_source_refresh_persists_success_and_keeps_failed_state(
    monkeypatch, tmp_path
):
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "blocked",
        "state_change_seq": 1,
    }
    monkeypatch.setattr(
        ch,
        "socket_rpc",
        lambda path, method, params: (
            {"agents": [agent]}
            if method == "agent.list"
            else {"panes": [{"pane_id": "p"}]}
        ),
    )
    assert ch.sync_v1_source("local:tom", "boot", "/socket")
    saved = json.loads(ch.STATE_PATH.read_text())
    assert saved["v1"]["sources"]["local:tom"]["attention"]["p"]["status"] == "blocked"

    def fail(*args):
        raise OSError("disconnected")

    monkeypatch.setattr(ch, "socket_rpc", fail)
    assert not ch.sync_v1_source("local:tom", "boot", "/socket")
    assert json.loads(ch.STATE_PATH.read_text()) == saved


def test_v1_refresh_invalid_status_does_not_partially_update(monkeypatch):
    state = {
        "v1": {
            "sources": {
                "s": {
                    "attention": {
                        "p": {
                            "status": "done",
                            "episode": "old",
                            "sequence": 1,
                            "generation": "g",
                            "terminal_id": "t",
                        }
                    },
                    "panes": {},
                }
            }
        }
    }
    before = json.loads(json.dumps(state))
    agents = [
        {
            "pane_id": "p",
            "terminal_id": "t",
            "agent_status": "working",
            "state_change_seq": 2,
        },
        {"pane_id": "q", "terminal_id": "u", "agent_status": [], "state_change_seq": 1},
    ]
    monkeypatch.setattr(
        ch,
        "socket_rpc",
        lambda path, method, params: (
            {"agents": agents}
            if method == "agent.list"
            else {"panes": [{"pane_id": "p"}, {"pane_id": "q"}]}
        ),
    )
    assert not ch.refresh_v1_source(state, "s", "g", "/socket")
    assert state == before


def test_v1_watch_retries_release_snapshot_without_losing_buffered_events(monkeypatch):
    import io

    events = [
        {"result": {"type": "subscription_started"}},
        {"event": "pane_agent_status_changed", "data": {"agent_status": "working"}},
    ]
    calls = []

    class Connection:
        def makefile(self, mode):
            return io.BytesIO(b"".join((json.dumps(e) + "\n").encode() for e in events))

        def close(self):
            calls.append("close")

    def sync(*args, **kwargs):
        calls.append("snapshot")
        if calls == ["snapshot"]:
            raise ch.V1SnapshotRace("release changed")
        return True

    monkeypatch.setattr(ch, "list_pane_ids", lambda path: ["p"])
    monkeypatch.setattr(ch, "open_v1_subscription", lambda path, subs: Connection())
    monkeypatch.setattr(ch, "sync_v1_source", sync)
    monkeypatch.setattr(ch.time, "sleep", lambda delay: None)
    monkeypatch.setattr(
        ch, "persist_v1_status_event", lambda *args: calls.append("event") or True
    )
    ch.v1_watch_connection("source", "generation", "/socket")
    assert calls == ["snapshot", "snapshot", "event", "close"]


def test_v1_refresh_tracks_empty_panes_before_agent_starts(monkeypatch):
    def rpc(path, method, params):
        return (
            {"agents": []}
            if method == "agent.list"
            else {
                "panes": [
                    {"pane_id": "p", "workspace_id": "w", "agent_status": "unknown"}
                ]
            }
        )

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    state = {}
    assert ch.refresh_v1_source(state, "source", "g", "/socket")
    assert ch.apply_v1_status_event(
        state, "source", {"pane_id": "p", "agent_status": "blocked"}
    )
    assert state["v1"]["sources"]["source"]["attention"]["p"]["status"] == "blocked"


@pytest.mark.parametrize("changed_source", ["a", "b"])
def test_v1_slow_source_refresh_does_not_lock_other_updates(
    monkeypatch, tmp_path, changed_source
):
    import threading

    ch.save_state({"v1": {"sources": {"a": {"value": 1}, "b": {"value": 1}}}})
    entered, release, updated = threading.Event(), threading.Event(), threading.Event()

    def refresh(state, *args, **kwargs):
        entered.set()
        assert release.wait(5)
        state["v1"]["sources"]["a"]["value"] = 2
        return True

    monkeypatch.setattr(ch, "refresh_v1_source", refresh)
    result = []
    worker = threading.Thread(
        target=lambda: result.append(ch.sync_v1_source("a", "g", "/socket"))
    )

    def update():
        with ch.state_lock():
            state = ch.load_state()
            state["v1"]["sources"][changed_source]["value"] = 3
            ch.save_state(state)
        updated.set()

    writer = threading.Thread(target=update)
    worker.start()
    try:
        assert entered.wait(3)
        writer.start()
        assert updated.wait(1), "remote refresh held the shared state lock"
    finally:
        release.set()
        worker.join(5)
        if writer.ident is not None:
            writer.join(5)
    state = ch.load_state()["v1"]["sources"]
    assert state[changed_source]["value"] == 3
    assert result == [changed_source != "a"]
    if changed_source == "b":
        assert state["a"]["value"] == 2


@pytest.mark.parametrize("new_generation", [False, True])
def test_v1_source_refresh_merges_concurrent_token_bookkeeping(
    monkeypatch, tmp_path, new_generation
):
    import threading

    ch.save_state(
        {
            "v1": {
                "sources": {
                    "a": {
                        "value": 1,
                        "cursor_generation": "g",
                        "published_tokens": {"p": {"pending": True, "id": "old"}},
                    }
                }
            }
        }
    )
    entered, release = threading.Event(), threading.Event()

    def refresh(state, *args, **kwargs):
        entered.set()
        assert release.wait(5)
        state["v1"]["sources"]["a"]["value"] = 2
        if new_generation:
            state["v1"]["sources"]["a"]["cursor_generation"] = "next"
            state["v1"]["sources"]["a"].pop("published_tokens")
        return True

    monkeypatch.setattr(ch, "refresh_v1_source", refresh)
    result = []
    worker = threading.Thread(
        target=lambda: result.append(ch.sync_v1_source("a", "g", "/socket"))
    )
    worker.start()
    try:
        assert entered.wait(3)
        with ch.state_lock():
            state = ch.load_state()
            state["v1"]["sources"]["a"]["published_tokens"]["p"]["id"] = "new"
            ch.save_state(state)
    finally:
        release.set()
        worker.join(5)
    assert result == [True]
    source = ch.load_state()["v1"]["sources"]["a"]
    assert source["value"] == 2
    if new_generation:
        assert "published_tokens" not in source
    else:
        assert source["published_tokens"]["p"]["id"] == "new"
