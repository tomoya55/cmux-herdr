import json
import socket
from types import SimpleNamespace

import pytest

import cmux_herdr as ch


def test_v1_running_local_socket_paths_rejects_ambiguous_session_list(monkeypatch):
    monkeypatch.setattr(
        ch,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout=json.dumps(
                {
                    "sessions": [
                        {"running": True, "socket_path": "/tmp/a.sock"},
                        {"running": True, "socket_path": "/tmp/a.sock"},
                    ]
                }
            ),
            stderr="",
        ),
    )
    assert ch.v1_running_local_socket_paths() is None


def test_v1_running_local_socket_paths_preserves_usable_spelling(monkeypatch, tmp_path):
    real = tmp_path / "real"
    real.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(real, target_is_directory=True)
    path = str(alias / "herdr.sock")
    monkeypatch.setattr(
        ch,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout=json.dumps({"sessions": [{"running": True, "socket_path": path}]}),
        ),
    )
    assert ch.v1_running_local_socket_paths() == [path]


def test_local_socket_provider_uses_remaining_scan_budget(monkeypatch):
    monkeypatch.setattr(ch.time, "monotonic", lambda: 100.0)
    received = []
    monkeypatch.setattr(
        ch,
        "v1_running_local_socket_paths",
        lambda *, timeout: received.append(timeout) or [],
    )
    provider = ch.v1_local_socket_provider(100.25)
    assert provider() == []
    assert provider() == []
    assert received == [0.25]


def seed_state(path, panes, label="proj"):
    path.write_text(
        json.dumps(
            {"sessions": {"": {"workspaces": {"w1": {"label": label, "panes": panes}}}}}
        )
    )


def done_pane(agent="claude"):
    return {"status": "done", "agent": agent, "title": "task"}


def socket_event(pane_id, ws_id, status):
    return {"pane_id": pane_id, "workspace_id": ws_id, "agent_status": status}


def test_watch_clears_pane_herdr_no_longer_tracks(
    monkeypatch, cfg, cmux_calls, watch_state
):
    seed_state(watch_state, {"w1:p1": done_pane()})
    monkeypatch.setattr(ch, "pane_is_tracked", lambda sock, pane_id: False)

    ch.handle_watch_event(cfg, "/tmp/x.sock", socket_event("w1:p1", "w1", "idle"))

    assert ["clear-status", "herdr.w1", "--workspace", "workspace:1"] in cmux_calls
    saved = json.loads(watch_state.read_text())
    assert saved["sessions"][""]["workspaces"]["w1"]["panes"] == {}


def test_watch_leaves_unowned_notifications_when_attention_drains(
    monkeypatch, cfg, cmux_calls, watch_state
):
    seed_state(watch_state, {"w1:p1": done_pane()})
    monkeypatch.setattr(ch, "pane_is_tracked", lambda sock, pane_id: False)

    ch.handle_watch_event(cfg, "/tmp/x.sock", socket_event("w1:p1", "w1", "idle"))

    assert not any(c[0] == "mark-notification-read" for c in cmux_calls)


def test_watch_ignores_tracked_status(monkeypatch, cfg, cmux_calls, watch_state):
    seed_state(watch_state, {"w1:p1": done_pane()})
    monkeypatch.setattr(
        ch, "pane_is_tracked", lambda sock, pane_id: pytest.fail("must not confirm")
    )

    for status in sorted(ch.TRACKED_STATUSES):
        ch.handle_watch_event(cfg, "/tmp/x.sock", socket_event("w1:p1", "w1", status))

    assert cmux_calls == []


def test_watch_keeps_pane_a_newer_transition_reclaimed(
    monkeypatch, cfg, cmux_calls, watch_state
):
    # The hook wrote `working` between the event and the lock; the in-lock
    # confirmation sees it and the stale idle event must not remove the pane.
    seed_state(watch_state, {"w1:p1": {**done_pane(), "status": "working"}})
    monkeypatch.setattr(ch, "pane_is_tracked", lambda sock, pane_id: True)

    ch.handle_watch_event(cfg, "/tmp/x.sock", socket_event("w1:p1", "w1", "idle"))

    assert cmux_calls == []
    saved = json.loads(watch_state.read_text())
    assert "w1:p1" in saved["sessions"][""]["workspaces"]["w1"]["panes"]


def test_watch_ignores_pane_the_plugin_does_not_track(
    monkeypatch, cfg, cmux_calls, watch_state
):
    seed_state(watch_state, {"w1:p1": done_pane()})
    monkeypatch.setattr(
        ch, "pane_is_tracked", lambda sock, pane_id: pytest.fail("must not confirm")
    )

    ch.handle_watch_event(cfg, "/tmp/x.sock", socket_event("w1:p9", "w1", "idle"))

    assert cmux_calls == []


def test_watch_keeps_pane_when_confirmation_fails(cfg, cmux_calls, watch_state):
    seed_state(watch_state, {"w1:p1": done_pane()})

    # pane_is_tracked fails safe: an unreachable server must not drop pills.
    ch.handle_watch_event(cfg, "/nonexistent.sock", socket_event("w1:p1", "w1", "idle"))

    assert cmux_calls == []
    saved = json.loads(watch_state.read_text())
    assert "w1:p1" in saved["sessions"][""]["workspaces"]["w1"]["panes"]


def test_watch_clears_per_pane_pill(monkeypatch, cfg, cmux_calls, watch_state):
    seed_state(watch_state, {"w1:p1": done_pane()})
    monkeypatch.setattr(ch, "pane_is_tracked", lambda sock, pane_id: False)

    ch.handle_watch_event(cfg, "/tmp/x.sock", socket_event("w1:p1", "w1", "idle"))

    assert [
        "clear-status",
        "herdr.w1.w1_p1",
        "--workspace",
        "workspace:1",
    ] in cmux_calls


def test_watch_subscriptions_cover_panes_and_lifecycle():
    subs = ch.watch_subscriptions(["w1:p1", "w2:p3"])

    status = [s for s in subs if s["type"] == "pane.agent_status_changed"]
    assert [s["pane_id"] for s in status] == ["w1:p1", "w2:p3"]
    wildcards = {s["type"] for s in subs if "pane_id" not in s}
    assert wildcards == {"pane.created", "pane.closed"}


def test_pane_is_tracked_reads_current_status(monkeypatch):
    agents = [{"pane_id": "w1:p1", "agent_status": "working"}]
    monkeypatch.setattr(ch, "socket_rpc", lambda *a, **k: {"agents": agents})

    assert ch.pane_is_tracked("/tmp/x.sock", "w1:p1") is True
    assert ch.pane_is_tracked("/tmp/x.sock", "w1:p2") is False


def test_watch_pid_path_is_per_session(monkeypatch, tmp_path):

    assert ch.watch_pid_path("tom") != ch.watch_pid_path("coten")
    # Session names reach the filesystem; keep them to safe characters.
    assert "/" not in ch.watch_pid_path("a/b").name


def test_v1_subscriptions_cover_pane_status_focus_and_metadata():
    subscriptions = ch.v1_subscriptions(["p1", "p2"])
    assert {"type": "pane.agent_status_changed", "pane_id": "p1"} in subscriptions
    assert {"type": "pane.agent_status_changed", "pane_id": "p2"} in subscriptions
    assert {"type": "pane.updated"} in subscriptions
    assert {"type": "pane.focused"} in subscriptions
    assert {"type": "pane.created"} in subscriptions
    assert {"type": "pane.closed"} in subscriptions


def test_v1_watch_subscribes_then_processes_ordered_events(monkeypatch):
    import io

    events = [
        {"id": "watch", "result": {"type": "subscription_started"}},
        {
            "event": "pane_agent_status_changed",
            "data": {"pane_id": "p", "agent_status": "blocked"},
        },
        {
            "event": "pane_agent_status_changed",
            "data": {"pane_id": "p", "agent_status": "working"},
        },
        {"event": "pane_created", "data": {}},
    ]

    class Connection:
        def makefile(self, mode):
            return io.BytesIO(b"".join((json.dumps(e) + "\n").encode() for e in events))

        def close(self):
            pass

    order = []
    monkeypatch.setattr(ch, "list_pane_ids", lambda path: ["p"])
    monkeypatch.setattr(ch, "open_v1_subscription", lambda path, subs: Connection())
    monkeypatch.setattr(
        ch,
        "sync_v1_source",
        lambda source, generation, path, **kwargs: order.append("snapshot") or True,
    )
    monkeypatch.setattr(
        ch,
        "persist_v1_status_event",
        lambda source, data, *args: order.append(data["agent_status"]) or True,
    )
    ch.v1_watch_connection("source", "boot", "/socket")
    assert order == ["snapshot", "blocked", "working"]


def test_v1_watch_refreshes_on_agent_release_without_trusting_payload(monkeypatch):
    import io

    events = [
        {"id": "watch", "result": {"type": "subscription_started"}},
        {"event": "pane.agent_detected", "data": {"pane_id": "p", "released": True}},
    ]

    class Connection:
        def makefile(self, mode):
            return io.BytesIO(
                b"".join((json.dumps(event) + "\n").encode() for event in events)
            )

        def close(self):
            pass

    subscriptions = []
    refreshes = []
    monkeypatch.setattr(ch, "list_pane_ids", lambda path: ["p"])
    monkeypatch.setattr(
        ch,
        "open_v1_subscription",
        lambda path, subs: subscriptions.extend(subs) or Connection(),
    )
    monkeypatch.setattr(
        ch, "sync_v1_source", lambda *args, **kwargs: refreshes.append(True) or True
    )
    ch.v1_watch_connection("source", "generation", "/socket")
    assert {"type": "pane.agent_detected"} in subscriptions
    assert len(refreshes) == 2


def test_v1_watch_reconnects_if_subscription_and_snapshot_servers_differ(monkeypatch):
    import io

    class Connection:
        def makefile(self, mode):
            return io.BytesIO(
                (
                    json.dumps(
                        {
                            "result": {
                                "type": "subscription_started",
                                "server_boot_id": "a" * 32,
                            }
                        }
                    )
                    + "\n"
                ).encode()
            )

        def close(self):
            pass

    monkeypatch.setattr(ch, "list_pane_ids", lambda path: [])
    monkeypatch.setattr(ch, "open_v1_subscription", lambda *args: Connection())
    monkeypatch.setattr(ch, "sync_v1_source", lambda *args, **kwargs: True)
    monkeypatch.setattr(ch, "v1_subscription_matches_source", lambda *args: False)
    with pytest.raises(ch.SocketError):
        ch.v1_watch_connection("source", "generation", "/socket")


def test_v1_supervisor_watch_rejects_changed_boot_before_ready(monkeypatch):
    import io

    class Connection:
        def makefile(self, mode):
            return io.BytesIO(
                (
                    json.dumps(
                        {
                            "result": {
                                "type": "subscription_started",
                                "server_boot_id": "b" * 32,
                            }
                        }
                    )
                    + "\n"
                ).encode()
            )

        def close(self):
            pass

    monkeypatch.setattr(ch, "list_pane_ids", lambda path: [])
    monkeypatch.setattr(ch, "open_v1_subscription", lambda *args: Connection())
    monkeypatch.setattr(
        ch,
        "sync_v1_source",
        lambda *args, **kwargs: pytest.fail("stale source was synchronized"),
    )
    with pytest.raises(ch.SocketError):
        ch.v1_watch_connection(
            "source", "generation", "/socket", expected_boot="a" * 32
        )


def test_v1_supervisor_watch_does_not_report_ready_for_stale_pane_set(monkeypatch):
    import io

    class Connection:
        def makefile(self, mode):
            return io.BytesIO(
                (
                    json.dumps(
                        {
                            "result": {
                                "type": "subscription_started",
                                "server_boot_id": "a" * 32,
                            }
                        }
                    )
                    + "\n"
                ).encode()
            )

        def close(self):
            pass

    panes = iter([["p"], ["p", "q"]])
    monkeypatch.setattr(ch, "list_pane_ids", lambda path: next(panes))
    monkeypatch.setattr(ch, "open_v1_subscription", lambda *args: Connection())
    monkeypatch.setattr(ch, "sync_v1_source", lambda *args, **kwargs: True)
    monkeypatch.setattr(ch, "v1_subscription_matches_source", lambda *args: True)
    ready = []
    ch.v1_watch_connection(
        "source",
        "generation",
        "/socket",
        expected_boot="a" * 32,
        on_change=lambda: ready.append(True),
    )
    assert not ready


def test_v1_supervisor_source_reports_ready_only_once_after_subscription(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "v1_local_generation", lambda path: "generation")
    calls = []

    def watch(source, generation, socket_path, *, on_change, expected_boot):
        calls.append((source, generation, socket_path, expected_boot))
        on_change()
        on_change()

    monkeypatch.setattr(ch, "v1_watch_connection", watch)
    ready = []
    target = {"remote": None, "socket_path": "/source.sock", "server_boot_id": "a" * 32}
    assert ch.v1_watch_supervised_source("source", target, lambda: ready.append(True))
    assert ready == [True]
    assert calls == [("source", "generation", "/source.sock", "a" * 32)]


@pytest.mark.parametrize("failure", ["connect", "send"])
def test_v1_subscription_bounds_connect_and_closes_on_failure(monkeypatch, failure):
    events = []

    class Connection:
        def settimeout(self, value):
            events.append(("timeout", value))

        def connect(self, path):
            events.append(("connect", path))
            if failure == "connect":
                raise TimeoutError("connect stalled")

        def sendall(self, data):
            events.append(("send",))
            raise TimeoutError("send stalled")

        def close(self):
            events.append(("close",))

    monkeypatch.setattr(ch.socket, "socket", lambda *args: Connection())
    with pytest.raises(TimeoutError):
        ch.open_v1_subscription("/socket", [])
    assert events == [
        ("timeout", ch.V1_SUBSCRIPTION_IDLE_TIMEOUT),
        ("connect", "/socket"),
        *([("send",)] if failure == "send" else []),
        ("close",),
    ]


def test_v1_subscription_reconnects_after_silent_stream(monkeypatch):
    client, server = socket.socketpair()
    monkeypatch.setattr(ch, "V1_SUBSCRIPTION_IDLE_TIMEOUT", 0.1)

    def open_socket(*args, timeout):
        client.settimeout(timeout)
        return client

    monkeypatch.setattr(ch, "open_subscription", open_socket)
    monkeypatch.setattr(ch, "list_pane_ids", lambda path: [])
    monkeypatch.setattr(ch, "sync_v1_source", lambda *args, **kwargs: True)
    monkeypatch.setattr(ch, "v1_subscription_matches_source", lambda *args: True)
    server.sendall(
        (
            json.dumps(
                {"result": {"type": "subscription_started", "server_boot_id": "a" * 32}}
            )
            + "\n"
        ).encode()
    )
    try:
        with pytest.raises(TimeoutError):
            ch.v1_watch_connection(
                "source", "generation", "/socket", expected_boot="a" * 32
            )
    finally:
        server.close()


def test_v1_supervised_source_retries_after_subscription_timeout(monkeypatch):
    stop = ch.threading.Event()
    attempts, ready = [], []

    def watch(source, target, on_ready, *, on_not_ready):
        attempts.append(source)
        if len(attempts) == 1:
            on_ready()
            raise TimeoutError("silent subscription")
        on_ready()
        stop.set()
        return True

    monkeypatch.setattr(ch, "v1_watch_supervised_source", watch)
    assert ch.run_v1_supervised_source(
        "source",
        {},
        lambda: ready.append(True),
        stop,
        on_not_ready=lambda: ready.append(False),
    )
    assert attempts == ["source", "source"]
    assert ready == [True, False, True, False]


@pytest.mark.parametrize("marker", ["pending", "enabled"])
@pytest.mark.parametrize("boot", ["a" * 32, None])
def test_v1_supervised_source_syncs_tokens_without_supervisor_discovery(
    monkeypatch, tmp_path, marker, boot
):
    import threading

    (tmp_path / "v1-enabled").write_text(marker)
    monkeypatch.setattr(ch, "v1_local_generation", lambda path: "generation")
    monkeypatch.setattr(ch, "v1_unix_peer_pid", lambda path: 123)
    monkeypatch.setattr(ch, "v1_native_process_info", lambda pid: {"start_token": 5})
    synced = threading.Event()
    calls = []

    def sync(source, path, *, expected_boot):
        calls.append((source, path, expected_boot))
        synced.set()

    monkeypatch.setattr(ch, "sync_v1_episode_tokens", sync)

    def watch(source, generation, path, *, on_change, expected_boot):
        on_change()
        if marker == "pending":
            assert not synced.wait(0.2)
            (tmp_path / "v1-enabled").write_text("enabled")
        assert synced.wait(3), "token synchronization depended on supervisor discovery"

    monkeypatch.setattr(ch, "v1_watch_connection", watch)
    target = {"remote": None, "socket_path": "/source.sock", "server_boot_id": boot}
    if boot is None:
        target["socket_generation"] = "generation"
    assert ch.v1_watch_supervised_source("source", target, lambda: None)
    assert calls and calls[0] == ("source", "/source.sock", boot)


def test_v1_supervised_upstream_source_rejects_changed_socket(monkeypatch):
    target = {"remote": None, "socket_path": "/source.sock", "socket_generation": "1:2"}
    monkeypatch.setattr(ch, "v1_local_generation", lambda path: "1:3")
    events = []
    monkeypatch.setattr(
        ch, "v1_watch_connection", lambda *args, **kwargs: events.append(1)
    )
    assert not ch.v1_watch_supervised_source("source", target, lambda: None)
    assert events == []


def test_v1_supervised_upstream_source_rejects_replaced_server(monkeypatch):
    target = {
        "remote": None,
        "socket_path": "/source.sock",
        "socket_generation": "1:2",
        "server_pid": 123,
        "server_start_token": 5,
    }
    monkeypatch.setattr(ch, "v1_local_generation", lambda path: "1:2")
    monkeypatch.setattr(ch, "v1_unix_peer_pid", lambda path: 123)
    monkeypatch.setattr(ch, "v1_native_process_info", lambda pid: {"start_token": 6})
    calls = []
    monkeypatch.setattr(
        ch, "v1_watch_connection", lambda *args, **kwargs: calls.append(1)
    )
    assert not ch.v1_watch_supervised_source("source", target, lambda: None)
    assert calls == []


def test_v1_watch_reconnects_when_done_refresh_fails(monkeypatch):
    import io

    events = [
        {"result": {"type": "subscription_started"}},
        {
            "event": "pane_agent_status_changed",
            "data": {"pane_id": "p", "agent_status": "done"},
        },
    ]

    class Connection:
        def makefile(self, mode):
            return io.BytesIO(b"".join((json.dumps(e) + "\n").encode() for e in events))

        def close(self):
            pass

    calls = iter([True, False])
    monkeypatch.setattr(ch, "list_pane_ids", lambda path: ["p"])
    monkeypatch.setattr(ch, "open_v1_subscription", lambda *args: Connection())
    monkeypatch.setattr(ch, "sync_v1_source", lambda *args, **kwargs: next(calls))
    monkeypatch.setattr(ch, "persist_v1_status_event", lambda *args: True)
    with pytest.raises(ch.SocketError):
        ch.v1_watch_connection("source", "generation", "/socket")


def test_v1_watch_confirms_done_from_snapshot_before_acknowledgment(monkeypatch):
    import io

    events = [
        {"id": "watch", "result": {"type": "subscription_started"}},
        {
            "event": "pane_agent_status_changed",
            "data": {"pane_id": "p", "agent_status": "done"},
        },
    ]

    class Connection:
        def makefile(self, mode):
            return io.BytesIO(b"".join((json.dumps(e) + "\n").encode() for e in events))

        def close(self):
            pass

    order = []
    monkeypatch.setattr(ch, "list_pane_ids", lambda path: ["p"])
    monkeypatch.setattr(ch, "open_v1_subscription", lambda path, subs: Connection())
    monkeypatch.setattr(
        ch,
        "sync_v1_source",
        lambda source, generation, path, **kwargs: order.append("snapshot") or True,
    )
    monkeypatch.setattr(
        ch,
        "persist_v1_status_event",
        lambda source, data, *args: order.append(data["agent_status"]) or True,
    )
    ch.v1_watch_connection("source", "boot", "/socket")
    assert order == ["snapshot", "done", "snapshot"]


def test_v1_watch_notifies_observer_after_each_state_change(monkeypatch):
    import io

    events = [
        {"id": "watch", "result": {"type": "subscription_started"}},
        {
            "event": "pane_agent_status_changed",
            "data": {"pane_id": "p", "agent_status": "blocked"},
        },
        {
            "event": "pane_updated",
            "data": {"pane": {"pane_id": "p", "tokens": {"cmux_herdr_ack": "a"}}},
        },
    ]

    class Connection:
        def makefile(self, mode):
            return io.BytesIO(b"".join((json.dumps(e) + "\n").encode() for e in events))

        def close(self):
            pass

    changes = []
    monkeypatch.setattr(ch, "list_pane_ids", lambda path: ["p"])
    monkeypatch.setattr(ch, "open_v1_subscription", lambda path, subs: Connection())
    monkeypatch.setattr(ch, "sync_v1_source", lambda *args, **kwargs: True)
    monkeypatch.setattr(ch, "persist_v1_status_event", lambda *args: True)
    monkeypatch.setattr(ch, "persist_v1_ack_event", lambda *args: True)
    ch.v1_watch_connection(
        "source", "boot", "/socket", on_change=lambda: changes.append(True)
    )
    assert len(changes) == 3


def test_v1_local_socket_resolves_running_named_and_default_session(monkeypatch):
    sessions = {
        "sessions": [
            {
                "name": "default",
                "default": True,
                "running": True,
                "socket_path": "/default.sock",
            },
            {"name": "work", "running": True, "socket_path": "/work.sock"},
            {"name": "off", "running": False, "socket_path": "/off.sock"},
        ]
    }
    monkeypatch.setattr(
        ch,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0, stdout=json.dumps(sessions), stderr=""
        ),
    )
    assert ch.v1_local_socket("") == "/default.sock"
    assert ch.v1_local_socket("work") == "/work.sock"
    assert ch.v1_local_socket("off") is None


def test_v1_client_stops_when_launching_shell_exits(monkeypatch):
    class Stop:
        def wait(self, timeout):
            assert timeout == ch.LOOP_TICK_SECONDS
            return False

    sent = []
    monkeypatch.setattr(ch.os, "getppid", lambda: 1)
    monkeypatch.setattr(ch.os, "getpid", lambda: 123)
    monkeypatch.setattr(ch.os, "kill", lambda pid, sig: sent.append((pid, sig)))
    ch.v1_watch_parent(42, Stop())
    assert sent == [(123, ch.signal.SIGTERM)]


def test_v1_remote_probe_forwards_configured_binary(monkeypatch):
    calls = []
    monkeypatch.setattr(
        ch, "ensure_forward", lambda name, cfg, *args: calls.append(cfg) or "/local"
    )
    monkeypatch.setattr(ch, "drop_forward", lambda *args: None)
    monkeypatch.setattr(ch, "socket_rpc", lambda *args: {"snapshot": {}})
    assert ch.v1_probe_source(
        {"remote": "tom@host", "session": "work", "herdr_bin": "/opt/bin/herdr"}
    )
    assert calls == [
        {"ssh_target": "tom@host", "session": "work", "herdr_bin": "/opt/bin/herdr"}
    ]


def test_v1_source_probe_rejects_non_socket_and_invalid_snapshot(monkeypatch, tmp_path):
    path = tmp_path / "not-socket"
    path.touch()
    assert not ch.v1_probe_source({"remote": None, "socket_path": str(path)})
    monkeypatch.setattr(
        ch, "socket_rpc", lambda *args: {"snapshot": {"focused_pane_id": "p"}}
    )
    assert ch.v1_probe_source({"remote": None, "socket_path": str(path)})
    monkeypatch.setattr(ch, "socket_rpc", lambda *args: {})
    assert not ch.v1_probe_source({"remote": None, "socket_path": str(path)})


def test_v1_source_probe_requires_discovered_server_incarnation(monkeypatch, tmp_path):
    path = tmp_path / "source.sock"
    expected = "a" * 32
    target = {"remote": None, "socket_path": str(path), "server_boot_id": expected}
    monkeypatch.setattr(
        ch, "socket_rpc", lambda *args: {"snapshot": {"server_boot_id": "b" * 32}}
    )
    assert not ch.v1_probe_source(target)
    monkeypatch.setattr(
        ch, "socket_rpc", lambda *args: {"snapshot": {"server_boot_id": expected}}
    )
    assert ch.v1_probe_source(target)


def test_v1_replay_keeps_offline_watcher_event_for_retry(monkeypatch, tmp_path, cfg):
    monkeypatch.setattr(ch, "load_config_strict", lambda: cfg)
    monkeypatch.setattr(ch, "pending_v1_legacy_restores", lambda: [])
    monkeypatch.setattr(
        ch, "socket_rpc", lambda *args: (_ for _ in ()).throw(ch.SocketError("offline"))
    )
    (tmp_path / "v1-enabled").write_text("pending")
    ch.save_state(
        {
            "v1_pending_legacy_events": [
                {
                    "kind": "watch",
                    "session": "work",
                    "socket_path": "/socket",
                    "event": {
                        "workspace_id": "w1",
                        "pane_id": "p",
                        "agent_status": "idle",
                    },
                }
            ]
        }
    )
    assert not ch.v1_recover_legacy_handoff()
    assert (tmp_path / "v1-enabled").exists()
    assert len(ch.load_state()["v1_pending_legacy_events"]) == 1


def test_pane_is_tracked_fails_safe(monkeypatch):
    def boom(*a, **k):
        raise ch.SocketError("gone")

    monkeypatch.setattr(ch, "socket_rpc", boom)

    # Unverified means "leave it alone", never "remove it".
    assert ch.pane_is_tracked("/tmp/x.sock", "w1:p1") is True
