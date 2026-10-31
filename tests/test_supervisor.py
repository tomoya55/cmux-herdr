import json
import socket
import time
import uuid
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from helpers import short_sock_path

import cmux_herdr as ch


def test_v1_owned_supervisor_publishes_over_one_session(tmp_path, monkeypatch):
    import threading
    from contextlib import contextmanager

    (tmp_path / "v1-enabled").write_text("enabled")
    marker = tmp_path / "v1-notification-backend"
    marker.write_text("owned")
    marker.chmod(0o600)
    workspace, surface = str(uuid.uuid4()), str(uuid.uuid4())
    ch.save_state(
        {
            "sessions": {},
            "v1": {
                "clients": {
                    "c": {"workspace": workspace, "surface": surface, "source": "a"}
                },
                "sources": {"a": {"attention": {"p": {"status": "blocked"}}}},
            },
        }
    )
    stop = threading.Event()
    opened = []
    published = []

    class Publisher:
        def publish(self, destinations):
            published.append(destinations)
            if len(published) == 2:
                stop.set()

    @contextmanager
    def factory(*_args):
        opened.append(True)
        yield object(), Publisher()

    monkeypatch.setattr(ch, "v1_owned_socket_transport", factory)
    ch.v1_supervised_owned_publish_loop(stop, SimpleNamespace(scope=lambda: None), {})
    assert opened == [True]
    assert len(published) == 2
    assert published[0][0]["title"] == "herdr: 1 waiting"


def test_v1_owned_supervisor_publishes_before_and_after_transition(
    tmp_path, monkeypatch
):
    import threading

    (tmp_path / "v1-enabled").write_text("enabled")
    stop = threading.Event()
    session, publisher = object(), object()
    calls = []

    @contextmanager
    def factory(*_args):
        yield session, publisher

    def transition(current_session, current_publisher, _verification):
        assert (current_session, current_publisher) == (session, publisher)
        calls.append("transition")
        marker = tmp_path / "v1-notification-backend"
        marker.write_text("owned")
        marker.chmod(0o600)
        return True

    def publish(*_args):
        calls.append("publish")
        if calls.count("publish") == 2:
            stop.set()

    monkeypatch.setattr(ch, "v1_owned_socket_transport", factory)
    monkeypatch.setattr(ch, "v1_transition_owned_backend", transition)
    monkeypatch.setattr(ch, "v1_publish_owned_once", publish)
    ch.v1_supervised_owned_publish_loop(stop, SimpleNamespace(scope=lambda: None), {})
    assert calls == ["publish", "transition", "publish"]


def test_v1_owned_supervisor_transitions_over_socket(monkeypatch, tmp_path):
    import threading

    monkeypatch.setattr(ch, "v1_cmux_socket_password", lambda *_args: None)
    (tmp_path / "v1-enabled").write_text("enabled")
    store, workspace, surface = (str(uuid.uuid4()) for _ in range(3))
    ch.save_state(
        {
            "v1": {
                "clients": {
                    "c": {"workspace": workspace, "surface": surface, "source": "a"}
                },
                "sources": {"a": {"attention": {"p": {"status": "blocked"}}}},
            }
        }
    )
    sock_path = short_sock_path(uuid.uuid4().hex[:8])
    ready, stop = threading.Event(), threading.Event()
    requests = []

    def serve():
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(sock_path))
            server.listen(1)
            server.settimeout(10)
            ready.set()
            conn, _ = server.accept()
            with conn, conn.makefile("rb") as stream:
                for line in stream:
                    request = json.loads(line)
                    requests.append(request)
                    method = request["method"]
                    if method == "notification.state.info":
                        result = {"store_id": store}
                    elif method == "notification.list":
                        result = {"notifications": []}
                    else:
                        result = {"state": None}
                    conn.sendall(
                        json.dumps(
                            {
                                "id": request["id"],
                                "ok": True,
                                "result": result,
                            }
                        ).encode()
                        + b"\n"
                    )

    def monitor():
        marker = tmp_path / "v1-notification-backend"
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                if marker.read_text() == "owned":
                    break
            except FileNotFoundError:
                pass
            time.sleep(0.01)
        stop.set()

    worker = threading.Thread(target=serve, daemon=True)
    monitor_thread = threading.Thread(target=monitor, daemon=True)
    worker.start()
    assert ready.wait(5)
    monitor_thread.start()
    try:
        ch.v1_supervised_owned_publish_loop(
            stop,
            SimpleNamespace(scope=lambda: None),
            {"CMUX_SOCKET_PATH": str(sock_path)},
        )
        worker.join(5)
        monitor_thread.join(5)
        assert not worker.is_alive()
        assert (tmp_path / "v1-notification-backend").read_text() == "owned"
        sets = [item for item in requests if item["params"].get("action") == "set"]
        assert len(sets) == 2
        assert sets[0]["params"]["destinations"][0]["title"] == "herdr: 1 waiting"
    finally:
        stop.set()
        sock_path.unlink(missing_ok=True)


def test_v1_supervisor_starts_and_joins_ordinary_publisher(monkeypatch):
    calls = []

    class Leases:
        def acquire_leader(self):
            return True

        def reconcile(self, *_args, **_kwargs):
            pass

        def close(self):
            calls.append("leases closed")

    class Sources:
        def __init__(self, _start):
            pass

        def close(self):
            calls.append("sources closed")

    class Thread:
        def __init__(self, *, target, **_kwargs):
            self.name = target.__name__

        def start(self):
            calls.append(("start", self.name))

        def join(self):
            calls.append(("join", self.name))

    monkeypatch.setattr(ch, "V1SupervisedLeases", Leases)
    monkeypatch.setattr(ch, "V1SupervisedSources", Sources)
    monkeypatch.setattr(ch.threading, "Thread", Thread)
    stop = SimpleNamespace(is_set=lambda: True)
    assert ch.v1_run_supervisor({}, stop)
    assert calls == [
        ("start", "v1_supervised_publish_loop"),
        ("join", "v1_supervised_publish_loop"),
        "sources closed",
        "leases closed",
    ]


def test_v1_supervisor_restarts_after_configuration_change(monkeypatch):
    calls = []

    class Leases:
        def acquire_leader(self):
            return True

        def reconcile(self, *_args, **_kwargs):
            pass

        def close(self):
            calls.append("leases closed")

    class Sources:
        def __init__(self, _start):
            pass

        def close(self):
            calls.append("sources closed")

    class Thread:
        def __init__(self, **kwargs):
            pass

        def start(self):
            pass

        def join(self):
            calls.append("publisher joined")

    monkeypatch.setattr(ch, "V1SupervisedLeases", Leases)
    monkeypatch.setattr(ch, "V1SupervisedSources", Sources)
    monkeypatch.setattr(ch.threading, "Thread", Thread)
    stop = SimpleNamespace(is_set=lambda: False)
    assert (
        ch.v1_run_supervisor({}, stop, config_changed=lambda: True) == "config-changed"
    )
    assert calls == ["publisher joined", "sources closed", "leases closed"]


def test_v1_owned_supervisor_publishes_partial_scope_during_migration(
    monkeypatch, tmp_path
):
    (tmp_path / "v1-enabled").write_text("enabled")
    stop = ch.threading.Event()
    session, publisher = object(), object()
    calls = []

    @ch.contextmanager
    def transport(*args):
        yield session, publisher

    monkeypatch.setattr(ch, "v1_owned_socket_transport", transport)
    monkeypatch.setattr(ch, "v1_notification_backend", lambda: "ordinary")
    monkeypatch.setattr(
        ch, "v1_publish_owned_once", lambda *args: calls.append("publish")
    )

    def transition(*args):
        calls.append("transition")
        stop.set()
        return False

    monkeypatch.setattr(ch, "v1_transition_owned_backend", transition)
    verified = SimpleNamespace(scope=lambda: {("workspace", "surface")})
    ch.v1_supervised_owned_publish_loop(stop, verified, {})
    assert calls == ["publish", "transition"]


def test_v1_owned_supervisor_recovers_lost_update_over_socket(tmp_path, monkeypatch):
    import threading

    monkeypatch.setattr(ch, "v1_cmux_socket_password", lambda *_: None)
    (tmp_path / "v1-enabled").write_text("enabled")
    marker = tmp_path / "v1-notification-backend"
    marker.write_text("owned")
    marker.chmod(0o600)
    workspace, surface = str(uuid.uuid4()), str(uuid.uuid4())
    ch.save_state(
        {
            "sessions": {},
            "v1": {
                "clients": {
                    "c": {"workspace": workspace, "surface": surface, "source": "a"}
                },
                "sources": {"a": {"attention": {"p": {"status": "blocked"}}}},
            },
        }
    )
    sock_path = short_sock_path(uuid.uuid4().hex[:8])
    ready, stop = threading.Event(), threading.Event()
    store_id, new_epoch = str(uuid.uuid4()), str(uuid.uuid4())
    requests = []
    committed = []

    def serve():
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(sock_path))
            server.listen(2)
            server.settimeout(10)
            ready.set()
            for connection_number in range(2):
                conn, _ = server.accept()
                with conn, conn.makefile("rb") as stream:
                    while line := stream.readline():
                        request = json.loads(line)
                        requests.append(request)
                        params = request["params"]
                        if request["method"] == "notification.state.info":
                            result = {"store_id": store_id}
                        elif params["action"] == "set":
                            if connection_number == 0:
                                committed.extend(params["destinations"])
                                break  # The server committed but its reply was lost.
                            assert params["destinations"] == committed
                            result = {"state": None}
                        elif params["action"] == "resume":
                            result = {
                                "state": {
                                    "epoch": new_epoch,
                                    "revision": "0",
                                    "destinations": committed,
                                }
                            }
                        else:
                            result = {"state": None}
                        conn.sendall(
                            json.dumps(
                                {
                                    "id": request["id"],
                                    "ok": True,
                                    "result": result,
                                }
                            ).encode()
                            + b"\n"
                        )
                        if params.get("action") == "resume":
                            stop.set()
                            break

    worker = threading.Thread(target=serve, daemon=True)
    worker.start()
    assert ready.wait(5)
    watchdog = threading.Timer(15, stop.set)
    watchdog.start()
    try:
        ch.v1_supervised_owned_publish_loop(
            stop,
            SimpleNamespace(scope=lambda: None),
            {"CMUX_SOCKET_PATH": str(sock_path)},
        )
        worker.join(5)
        assert not worker.is_alive()
        journal = ch.v1_publisher_identity(
            tmp_path / "v1-owned-publisher.json", store_id, create=False
        )
        assert journal["epoch"] == new_epoch
        assert journal["pending"] is None
        assert journal["destinations"] == committed
        assert committed[0]["title"] == "herdr: 1 waiting"
        assert [r["params"].get("action", "info") for r in requests] == [
            "info",
            "register",
            "set",
            "info",
            "resume",
        ]
    finally:
        watchdog.cancel()
        stop.set()
        sock_path.unlink(missing_ok=True)


def test_v1_supervisor_handoff_requires_complete_and_supported_backend(
    monkeypatch, tmp_path
):
    calls = []

    monkeypatch.setattr(ch, "cmux_notifications_checked", lambda cfg: [])
    monkeypatch.setattr(
        ch,
        "v1_try_supervisor_handoff",
        lambda *_args, **kwargs: calls.append("handoff") or kwargs["final_check"](),
    )
    verified = SimpleNamespace(scope=lambda: set())
    assert not ch.v1_maybe_supervisor_handoff({}, object(), object(), verified, {})
    assert calls == []
    verified.scope = lambda: None
    assert ch.v1_maybe_supervisor_handoff(
        {}, object(), object(), verified, {"CMUX_SOCKET_PATH": "/tmp/cmux"}
    )
    assert calls == ["handoff"]
    marker = tmp_path / "v1-notification-backend"
    marker.write_text("owned")
    marker.chmod(0o600)
    calls.clear()
    assert not ch.v1_maybe_supervisor_handoff({}, object(), object(), verified, {})
    assert calls == []


def test_v1_supervisor_handoff_uses_existing_cmux_notification_api(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "cmux_notifications_checked", lambda cfg: [])
    calls = []

    def handoff(*args, final_check):
        calls.append("handoff")
        return final_check()

    monkeypatch.setattr(ch, "v1_try_supervisor_handoff", handoff)
    verified = SimpleNamespace(scope=lambda: None)
    assert ch.v1_maybe_supervisor_handoff({}, object(), object(), verified, {})
    assert calls == ["handoff"]


def test_v1_supervisor_keeps_legacy_when_store_provenance_is_missing(
    monkeypatch, tmp_path
):
    ch.STATE_PATH.write_text(
        json.dumps({"v1": {"destinations": {"old": {"notification_id": "old-notice"}}}})
    )
    calls = []

    monkeypatch.setattr(
        ch,
        "cmux_notifications_checked",
        lambda cfg: [
            {"id": "old-notice", "is_read": False, "subtitle": ch.NOTIFICATION_SUBTITLE}
        ],
    )
    monkeypatch.setattr(
        ch, "v1_try_supervisor_handoff", lambda *args, **kwargs: calls.append("retire")
    )
    verified = SimpleNamespace(scope=lambda: None)
    assert not ch.v1_maybe_supervisor_handoff(
        {}, object(), object(), verified, {"CMUX_SOCKET_PATH": "/tmp/cmux"}
    )
    assert calls == []


def test_v1_supervisor_attempts_handoff_after_recovery_and_discovery(
    monkeypatch, tmp_path
):
    calls = []

    class Leases:
        def acquire_leader(self):
            return True

        def reconcile(self, *_args, **_kwargs):
            pass

        def close(self):
            pass

    class Sources:
        def __init__(self, _start):
            pass

        def reconcile(self, desired, **_kwargs):
            return desired

        def close(self):
            pass

    class Verification:
        def __init__(self, _sources):
            pass

        def update(self, *_args, **_kwargs):
            pass

    class Thread:
        def __init__(self, **_kwargs):
            pass

        def start(self):
            pass

        def join(self):
            pass

    class Stop:
        done = False

        def is_set(self):
            return self.done

        def wait(self, _seconds):
            self.done = True

    monkeypatch.setattr(ch, "V1SupervisedLeases", Leases)
    monkeypatch.setattr(ch, "V1SupervisedSources", Sources)
    monkeypatch.setattr(ch, "V1PublicationVerification", Verification)
    monkeypatch.setattr(ch.threading, "Thread", Thread)
    monkeypatch.setattr(
        ch, "v1_recover_legacy_handoff", lambda: calls.append("recover") or True
    )
    monkeypatch.setattr(ch, "v1_discover_clients", lambda _cfg: {"complete": True})
    monkeypatch.setattr(ch, "v1_discovered_targets", lambda _inventory: {"c": {}})
    monkeypatch.setattr(ch, "v1_reconcile_supervisor_inventory", lambda *_args: None)
    monkeypatch.setattr(
        ch,
        "v1_maybe_supervisor_handoff",
        lambda *_args: calls.append("handoff") or False,
    )
    assert ch.v1_run_supervisor({}, Stop())
    assert calls == ["recover", "handoff"]


def test_v1_supervisor_focus_checks_only_prepared_bindings(monkeypatch):
    local = {
        "workspace": "w",
        "surface": "s",
        "target": {
            "remote": None,
            "socket_path": "/tmp/source.sock",
            "server_boot_id": "a" * 32,
        },
        "connection_generation": 1,
    }
    pending = {**local, "surface": "other"}
    calls = []
    monkeypatch.setattr(ch, "v1_reconcile_focus", lambda *args: calls.append(args))
    ch.v1_supervisor_focus_tick({}, {"a": local, "b": pending}, {"a": local})
    assert calls == [
        (
            {},
            "a",
            ch.v1_supervised_source_key(local["target"]),
            "w",
            "s",
            "/tmp/source.sock",
        )
    ]


def test_v1_supervisor_publishes_ordinary_notifications_for_verified_surface(
    monkeypatch, tmp_path
):
    (tmp_path / "v1-enabled").write_text("enabled")
    stop = ch.threading.Event()
    scope = {("workspace", "surface")}
    verified = SimpleNamespace(scope=lambda: scope, workspace_allowed=lambda _: True)
    calls = []

    def publish(cfg, **kwargs):
        calls.append(kwargs)
        stop.set()

    monkeypatch.setattr(ch, "publish_v1_clients", publish)
    ch.v1_supervised_publish_loop({}, stop, verified)
    assert calls[0]["eligible_destinations"] == scope
    assert calls[0].get("notifications", True)


def test_v1_supervisor_loop_publishes_only_after_enabled(monkeypatch, tmp_path):
    import threading

    spec = {
        "workspace": "w",
        "surface": "s",
        "target": {
            "remote": None,
            "socket_path": "/tmp/source.sock",
            "server_boot_id": "a" * 32,
        },
        "connection_generation": 1,
    }
    inventory = {"complete": True, "clients": []}
    calls = []
    published = threading.Event()

    class Leases:
        def acquire_leader(self):
            return True

        def reconcile(self, desired, ready, *, complete):
            calls.append("reclaim")

        def close(self):
            calls.append("leases closed")

    class Sources:
        def __init__(self, start):
            pass

        def reconcile(self, desired, *, complete):
            return {"c": spec}

        def current_ready(self, desired):
            return {"c": spec}

        def sync_tokens(self):
            calls.append("tokens")

        def close(self):
            calls.append("sources closed")

    class Stop:
        done = False

        def is_set(self):
            return self.done

        def wait(self, seconds):
            if (tmp_path / "v1-enabled").exists() and (
                tmp_path / "v1-enabled"
            ).read_text() == "enabled":
                assert published.wait(3)
            self.done = True

    monkeypatch.setattr(ch, "V1SupervisedLeases", Leases)
    monkeypatch.setattr(ch, "V1SupervisedSources", Sources)
    monkeypatch.setattr(ch, "load_state", lambda: {"v1": {"clients": {"c": {}}}})
    monkeypatch.setattr(ch, "v1_bindings_match_clients", lambda *args, **kwargs: True)
    monkeypatch.setattr(ch, "v1_recover_legacy_handoff", lambda: True)
    monkeypatch.setattr(
        ch, "v1_supervisor_bindings_match", lambda *args, **kwargs: True
    )
    monkeypatch.setattr(ch, "v1_discover_clients", lambda cfg: inventory)
    monkeypatch.setattr(ch, "v1_discovered_targets", lambda value: {"c": spec})
    monkeypatch.setattr(
        ch, "v1_reconcile_supervisor_inventory", lambda *args: calls.append("reconcile")
    )
    monkeypatch.setattr(
        ch, "v1_supervisor_focus_tick", lambda *args: calls.append("focus")
    )

    def publish(*args, **kwargs):
        calls.append("publish")
        published.set()

    monkeypatch.setattr(ch, "publish_v1_clients", publish)
    ch.v1_run_supervisor({}, Stop())
    assert calls == ["reclaim", "reconcile", "sources closed", "leases closed"]
    calls.clear()
    (tmp_path / "v1-enabled").write_text("pending")
    ch.v1_run_supervisor({}, Stop())
    assert calls == ["reclaim", "reconcile", "sources closed", "leases closed"]
    calls.clear()
    (tmp_path / "v1-enabled").write_text("enabled")
    ch.v1_run_supervisor({}, Stop())
    assert calls[0] == "reclaim"
    assert calls[-2:] == ["sources closed", "leases closed"]
    assert sorted(calls[1:-2]) == sorted(["reconcile", "focus", "publish"])


def test_v1_supervisor_publishes_ready_surface_after_partial_restart(
    monkeypatch, tmp_path
):
    import threading

    (tmp_path / "v1-enabled").write_text("enabled")
    healthy = {"workspace": "wa", "surface": "sa", "target": {"socket_path": "/a"}}
    offline = {"workspace": "wb", "surface": "sb", "target": {"socket_path": "/b"}}
    desired = {"a": healthy, "b": offline}
    published = threading.Event()
    calls = []

    class Leases:
        def acquire_leader(self):
            return True

        def reconcile(self, *args, **kwargs):
            pass

        def close(self):
            pass

    class Sources:
        def __init__(self, start):
            pass

        def reconcile(self, *args, **kwargs):
            return {"a": healthy}

        def current_ready(self, desired):
            return {"a": healthy}

        def close(self):
            pass

    class Stop:
        done = False

        def is_set(self):
            return self.done

        def wait(self, seconds):
            assert published.wait(3)
            self.done = True

    monkeypatch.setattr(ch, "V1SupervisedLeases", Leases)
    monkeypatch.setattr(ch, "V1SupervisedSources", Sources)
    monkeypatch.setattr(ch, "v1_recover_legacy_handoff", lambda: True)
    monkeypatch.setattr(
        ch, "v1_discover_clients", lambda cfg: {"complete": True, "clients": []}
    )
    monkeypatch.setattr(ch, "v1_discovered_targets", lambda inventory: desired)
    monkeypatch.setattr(ch, "v1_reconcile_supervisor_inventory", lambda *args: None)
    monkeypatch.setattr(ch, "v1_supervisor_focus_tick", lambda *args: None)
    monkeypatch.setattr(
        ch,
        "v1_verified_publication_targets",
        lambda desired, ready: {("wa", "sa")} if "a" in ready else set(),
    )

    def publish(cfg, **kwargs):
        calls.append(kwargs["eligible_destinations"])
        published.set()

    monkeypatch.setattr(ch, "publish_v1_clients", publish)
    ch.v1_run_supervisor({}, Stop())
    assert calls == [{("wa", "sa")}]


@pytest.mark.parametrize(
    "invalid",
    [
        {"cmux_bin": 5},
        {"workspaces": "bad"},
        {"labels": {"x": 4}},
        {"match_by_label": "yes"},
    ],
)
def test_supervisor_rejects_malformed_runtime_configuration(invalid):
    assert not ch.v1_runtime_config_valid(invalid)
    assert ch.v1_runtime_config_valid({})


def test_supervisor_rejects_conflicting_executables_for_same_remote():
    cfg = {
        "remotes": [
            {
                "name": "a",
                "ssh_target": "host",
                "session": "work",
                "cmux_title": "a",
                "herdr_bin": "/one/herdr",
            },
            {
                "name": "b",
                "ssh_target": "host",
                "session": "work",
                "cmux_title": "b",
                "herdr_bin": "/two/herdr",
            },
        ]
    }
    assert not ch.v1_runtime_config_valid(cfg)
