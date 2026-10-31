import json
from types import SimpleNamespace

import pytest

import cmux_herdr as ch


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
