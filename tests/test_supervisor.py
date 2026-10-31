import json
import threading
from types import SimpleNamespace

import pytest

from cmux_herdr_core import (
    cmux,
    core,
    discovery,
    focus,
    legacy_handoff,
    publication,
    supervisor,
)


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

    monkeypatch.setattr(supervisor, "V1SupervisedLeases", Leases)
    monkeypatch.setattr(supervisor, "V1SupervisedSources", Sources)
    monkeypatch.setattr(threading, "Thread", Thread)
    stop = SimpleNamespace(is_set=lambda: True)
    assert supervisor.v1_run_supervisor({}, stop)
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

    monkeypatch.setattr(supervisor, "V1SupervisedLeases", Leases)
    monkeypatch.setattr(supervisor, "V1SupervisedSources", Sources)
    monkeypatch.setattr(threading, "Thread", Thread)
    stop = SimpleNamespace(is_set=lambda: False)
    assert (
        supervisor.v1_run_supervisor({}, stop, config_changed=lambda: True)
        == "config-changed"
    )
    assert calls == ["publisher joined", "sources closed", "leases closed"]


def test_v1_supervisor_handoff_requires_complete_and_supported_backend(
    monkeypatch, tmp_path
):
    calls = []

    monkeypatch.setattr(cmux, "cmux_notifications_checked", lambda cfg: [])
    monkeypatch.setattr(
        legacy_handoff,
        "v1_try_supervisor_handoff",
        lambda *_args, **kwargs: calls.append("handoff") or kwargs["final_check"](),
    )
    verified = SimpleNamespace(scope=lambda: set())
    assert not supervisor.v1_maybe_supervisor_handoff(
        {}, object(), object(), verified, {}
    )
    assert calls == []
    verified.scope = lambda: None
    assert supervisor.v1_maybe_supervisor_handoff(
        {}, object(), object(), verified, {"CMUX_SOCKET_PATH": "/tmp/cmux"}
    )
    assert calls == ["handoff"]
    marker = tmp_path / "v1-notification-backend"
    marker.write_text("owned")
    marker.chmod(0o600)
    calls.clear()
    assert not supervisor.v1_maybe_supervisor_handoff(
        {}, object(), object(), verified, {}
    )
    assert calls == []


def test_v1_supervisor_handoff_uses_existing_cmux_notification_api(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(cmux, "cmux_notifications_checked", lambda cfg: [])
    calls = []

    def handoff(*args, final_check):
        calls.append("handoff")
        return final_check()

    monkeypatch.setattr(legacy_handoff, "v1_try_supervisor_handoff", handoff)
    verified = SimpleNamespace(scope=lambda: None)
    assert supervisor.v1_maybe_supervisor_handoff({}, object(), object(), verified, {})
    assert calls == ["handoff"]


def test_v1_supervisor_keeps_legacy_when_store_provenance_is_missing(
    monkeypatch, tmp_path
):
    core.STATE_PATH.write_text(
        json.dumps({"v1": {"destinations": {"old": {"notification_id": "old-notice"}}}})
    )
    calls = []

    monkeypatch.setattr(
        cmux,
        "cmux_notifications_checked",
        lambda cfg: [
            {
                "id": "old-notice",
                "is_read": False,
                "subtitle": core.NOTIFICATION_SUBTITLE,
            }
        ],
    )
    monkeypatch.setattr(
        legacy_handoff,
        "v1_try_supervisor_handoff",
        lambda *args, **kwargs: calls.append("retire"),
    )
    verified = SimpleNamespace(scope=lambda: None)
    assert not supervisor.v1_maybe_supervisor_handoff(
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

    monkeypatch.setattr(supervisor, "V1SupervisedLeases", Leases)
    monkeypatch.setattr(supervisor, "V1SupervisedSources", Sources)
    monkeypatch.setattr(supervisor, "V1PublicationVerification", Verification)
    monkeypatch.setattr(threading, "Thread", Thread)
    monkeypatch.setattr(
        legacy_handoff,
        "v1_recover_legacy_handoff",
        lambda: calls.append("recover") or True,
    )
    monkeypatch.setattr(
        discovery, "v1_discover_clients", lambda _cfg: {"complete": True}
    )
    monkeypatch.setattr(
        discovery, "v1_discovered_targets", lambda _inventory: {"c": {}}
    )
    monkeypatch.setattr(
        supervisor, "v1_reconcile_supervisor_inventory", lambda *_args: None
    )
    monkeypatch.setattr(
        supervisor,
        "v1_maybe_supervisor_handoff",
        lambda *_args: calls.append("handoff") or False,
    )
    assert supervisor.v1_run_supervisor({}, Stop())
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
    monkeypatch.setattr(focus, "v1_reconcile_focus", lambda *args: calls.append(args))
    supervisor.v1_supervisor_focus_tick({}, {"a": local, "b": pending}, {"a": local})
    assert calls == [
        (
            {},
            "a",
            core.v1_supervised_source_key(local["target"]),
            "w",
            "s",
            "/tmp/source.sock",
        )
    ]


def test_v1_supervisor_publishes_ordinary_notifications_for_verified_surface(
    monkeypatch, tmp_path
):
    (tmp_path / "v1-enabled").write_text("enabled")
    stop = threading.Event()
    scope = {("workspace", "surface")}
    verified = SimpleNamespace(scope=lambda: scope, workspace_allowed=lambda _: True)
    calls = []

    def publish(cfg, **kwargs):
        calls.append(kwargs)
        stop.set()

    monkeypatch.setattr(publication, "publish_v1_clients", publish)
    supervisor.v1_supervised_publish_loop({}, stop, verified)
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

    monkeypatch.setattr(supervisor, "V1SupervisedLeases", Leases)
    monkeypatch.setattr(supervisor, "V1SupervisedSources", Sources)
    monkeypatch.setattr(core, "load_state", lambda: {"v1": {"clients": {"c": {}}}})
    monkeypatch.setattr(
        supervisor, "v1_bindings_match_clients", lambda *args, **kwargs: True
    )
    monkeypatch.setattr(legacy_handoff, "v1_recover_legacy_handoff", lambda: True)
    monkeypatch.setattr(
        supervisor, "v1_supervisor_bindings_match", lambda *args, **kwargs: True
    )
    monkeypatch.setattr(discovery, "v1_discover_clients", lambda cfg: inventory)
    monkeypatch.setattr(discovery, "v1_discovered_targets", lambda value: {"c": spec})
    monkeypatch.setattr(
        supervisor,
        "v1_reconcile_supervisor_inventory",
        lambda *args: calls.append("reconcile"),
    )
    monkeypatch.setattr(
        supervisor, "v1_supervisor_focus_tick", lambda *args: calls.append("focus")
    )

    def publish(*args, **kwargs):
        calls.append("publish")
        published.set()

    monkeypatch.setattr(publication, "publish_v1_clients", publish)
    supervisor.v1_run_supervisor({}, Stop())
    assert calls == ["reclaim", "reconcile", "sources closed", "leases closed"]
    calls.clear()
    (tmp_path / "v1-enabled").write_text("pending")
    supervisor.v1_run_supervisor({}, Stop())
    assert calls == ["reclaim", "reconcile", "sources closed", "leases closed"]
    calls.clear()
    (tmp_path / "v1-enabled").write_text("enabled")
    supervisor.v1_run_supervisor({}, Stop())
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

    monkeypatch.setattr(supervisor, "V1SupervisedLeases", Leases)
    monkeypatch.setattr(supervisor, "V1SupervisedSources", Sources)
    monkeypatch.setattr(legacy_handoff, "v1_recover_legacy_handoff", lambda: True)
    monkeypatch.setattr(
        discovery, "v1_discover_clients", lambda cfg: {"complete": True, "clients": []}
    )
    monkeypatch.setattr(discovery, "v1_discovered_targets", lambda inventory: desired)
    monkeypatch.setattr(
        supervisor, "v1_reconcile_supervisor_inventory", lambda *args: None
    )
    monkeypatch.setattr(supervisor, "v1_supervisor_focus_tick", lambda *args: None)
    monkeypatch.setattr(
        supervisor,
        "v1_verified_publication_targets",
        lambda desired, ready: {("wa", "sa")} if "a" in ready else set(),
    )

    def publish(cfg, **kwargs):
        calls.append(kwargs["eligible_destinations"])
        published.set()

    monkeypatch.setattr(publication, "publish_v1_clients", publish)
    supervisor.v1_run_supervisor({}, Stop())
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
    assert not supervisor.v1_runtime_config_valid(invalid)
    assert supervisor.v1_runtime_config_valid({})


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
    assert not supervisor.v1_runtime_config_valid(cfg)
