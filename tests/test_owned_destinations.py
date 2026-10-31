import json
import uuid
from types import SimpleNamespace

import pytest

import cmux_herdr as ch


def test_v1_owned_destinations_aggregate_sources_by_surface():
    workspace, surface = str(uuid.uuid4()), str(uuid.uuid4())
    state = {
        "v1": {
            "clients": {
                "one": {"workspace": workspace, "surface": surface, "source": "a"},
                "two": {
                    "workspace": workspace.upper(),
                    "surface": surface.upper(),
                    "source": "b",
                },
                "three": {"workspace": workspace, "surface": surface, "source": "a"},
            },
            "sources": {
                "a": {"attention": {"p1": {"status": "blocked"}}},
                "b": {"attention": {"p2": {"status": "done"}}},
            },
        }
    }
    assert ch.v1_owned_destinations(state) == [
        {
            "workspace_id": workspace,
            "surface_id": surface,
            "title": "herdr: 1 waiting · 1 finished",
            "body": "",
        }
    ]
    del state["v1"]["sources"]["b"]
    with pytest.raises(ValueError, match="incomplete"):
        ch.v1_owned_destinations(state)


def test_v1_owned_destinations_rejects_corrupt_attention():
    workspace, surface = str(uuid.uuid4()), str(uuid.uuid4())
    state = {
        "v1": {
            "clients": {
                "one": {"workspace": workspace, "surface": surface, "source": "a"}
            },
            "sources": {"a": {"attention": {"p": {"status": "unexpected"}}}},
        }
    }
    with pytest.raises(ValueError, match="attention"):
        ch.v1_owned_destinations(state)
    state["v1"]["sources"]["a"]["attention"]["p"] = {"acknowledged": "episode"}
    assert ch.v1_owned_destinations(state) == []


def test_v1_owned_publication_requires_complete_verified_snapshot(
    tmp_path, monkeypatch
):
    workspace, surface = str(uuid.uuid4()), str(uuid.uuid4())
    state = {
        "sessions": {},
        "v1": {
            "clients": {
                "client": {"workspace": workspace, "surface": surface, "source": "a"}
            },
            "sources": {"a": {"attention": {"p": {"status": "blocked"}}}},
        },
    }
    ch.save_state(state)
    sent = []
    publisher = SimpleNamespace(publish=lambda targets: sent.append(targets))
    partial = SimpleNamespace(scope=lambda: set())
    assert not ch.v1_publish_owned_once(publisher, partial)
    assert sent == []
    complete = SimpleNamespace(scope=lambda: None)
    assert ch.v1_publish_owned_once(publisher, complete)
    assert sent[0][0]["title"] == "herdr: 1 waiting"
    ch.STATE_PATH.write_text("invalid JSON")
    with pytest.raises(ValueError):
        ch.v1_publish_owned_once(publisher, complete)
    assert len(sent) == 1


def test_v1_owned_publication_accepts_complete_empty_initial_state(
    tmp_path, monkeypatch
):
    sent = []
    publisher = SimpleNamespace(publish=lambda targets: sent.append(targets))
    assert ch.v1_publish_owned_once(publisher, SimpleNamespace(scope=lambda: None))
    assert sent == [[]]


def test_v1_owned_publication_updates_only_certified_targets(tmp_path, monkeypatch):
    workspace, first, second = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
    ch.save_state(
        {
            "sessions": {},
            "v1": {
                "clients": {
                    "a": {"workspace": workspace, "surface": first, "source": "a"},
                    "b": {"workspace": workspace, "surface": second, "source": "b"},
                },
                "sources": {
                    "a": {"attention": {"p": {"status": "blocked"}}},
                    "b": {"attention": {"p": {"status": "done"}}},
                },
            },
        }
    )
    previous = [
        {
            "workspace_id": workspace,
            "surface_id": second,
            "title": "Existing",
            "body": "",
        }
    ]
    sent = []
    publisher = SimpleNamespace(
        current_destinations=lambda: previous,
        publish=lambda value: sent.append(value),
    )
    verification = SimpleNamespace(scope=lambda: {(workspace, first)})
    assert ch.v1_publish_owned_once(publisher, verification)
    by_surface = {item["surface_id"]: item for item in sent[0]}
    assert by_surface[first]["title"] == "herdr: 1 waiting"
    assert by_surface[second] == previous[0]
    sent.clear()
    assert not ch.v1_publish_owned_once(publisher, verification, require_complete=True)
    assert sent == []


def test_v1_owned_publication_revalidates_after_preparation(tmp_path, monkeypatch):
    workspace, surface = str(uuid.uuid4()), str(uuid.uuid4())
    state = {
        "sessions": {},
        "v1": {
            "clients": {
                "c": {"workspace": workspace, "surface": surface, "source": "a"}
            },
            "sources": {"a": {"attention": {"p": {"status": "blocked"}}}},
        },
    }
    ch.save_state(state)
    sent = []

    class Publisher:
        send_guarded = True

        def current_destinations(self):
            return []

        def publish_verified(self, prepare):
            targets, send_guard = prepare()
            state["v1"]["sources"]["a"]["attention"] = {}
            ch.save_state(state)
            with send_guard():
                sent.append(targets)

    with pytest.raises(ValueError, match="stale publication"):
        ch.v1_publish_owned_once(Publisher(), SimpleNamespace(scope=lambda: None))
    assert sent == []


def test_v1_owned_publication_does_not_send_changed_snapshot(tmp_path, monkeypatch):
    import threading

    workspace, surface = str(uuid.uuid4()), str(uuid.uuid4())
    state = {
        "sessions": {},
        "v1": {
            "clients": {
                "client": {"workspace": workspace, "surface": surface, "source": "a"}
            },
            "sources": {"a": {"attention": {"p": {"status": "blocked"}}}},
        },
    }
    ch.save_state(state)
    started, updated = threading.Event(), threading.Event()

    def update():
        started.set()
        with ch.state_lock():
            state["v1"]["sources"]["a"]["attention"]["p"]["status"] = "done"
            ch.save_state(state)
        updated.set()

    worker = threading.Thread(target=update)

    def publish(targets):
        worker.start()
        assert started.wait(5)
        assert not updated.wait(0.05)
        assert targets[0]["title"] == "herdr: 1 waiting"

    assert ch.v1_publish_owned_once(
        SimpleNamespace(publish=publish), SimpleNamespace(scope=lambda: None)
    )
    worker.join(5)
    assert updated.is_set()


def test_v1_supervisor_never_marks_unrelated_notice_with_stale_id(monkeypatch):
    calls = []
    monkeypatch.setattr(ch, "cmux_cli", lambda cfg, args: calls.append(args) or True)
    foreign = [
        {
            "id": "stale",
            "workspace_id": "w",
            "surface_id": "s",
            "title": "Other app",
            "subtitle": "",
            "is_read": False,
        }
    ]
    ch.reconcile_aggregate_notification({}, "w", "s", {}, foreign, "stale")
    assert calls == []


def test_v1_retiring_destination_does_not_read_foreign_stale_id(monkeypatch):
    key = ch.v1_destination_key("w", "s")
    state = {"v1": {"destinations": {key: {"notification_id": "stale"}}}}
    foreign = [
        {
            "id": "stale",
            "workspace_id": "w",
            "surface_id": "s",
            "title": "Other app",
            "subtitle": "",
            "is_read": False,
        }
    ]
    monkeypatch.setattr(ch, "cmux_notifications_checked", lambda cfg: foreign)
    calls = []
    monkeypatch.setattr(ch, "cmux_cli", lambda cfg, args: calls.append(args) or True)
    assert ch.retire_v1_destination({}, state, key)
    assert calls == []


def test_v1_supervisor_keeps_previous_bindings_until_replacements_are_ready():
    old = {
        "source": "old-source",
        "workspace": "w",
        "surface": "s",
        "pid": 7,
        "supervised": True,
    }
    state = {"v1": {"clients": {"old": old}}}
    entry = {
        "workspace": "w",
        "surface": "s2",
        "target": {
            "remote": None,
            "socket_path": "/tmp/new.sock",
            "server_boot_id": "a" * 32,
        },
        "connection_generation": 2,
    }
    desired = {"new": entry}
    assert not ch.reconcile_v1_supervised_bindings(
        state, desired, {}, complete=True, supervisor_pid=9
    )
    assert state["v1"]["clients"] == {"old": old}
    assert ch.reconcile_v1_supervised_bindings(
        state, desired, {"new": entry}, complete=False, supervisor_pid=9
    )
    assert set(state["v1"]["clients"]) == {"old", "new"}
    assert ch.reconcile_v1_supervised_bindings(
        state, desired, {"new": entry}, complete=True, supervisor_pid=9
    )
    assert set(state["v1"]["clients"]) == {"new"}
    assert state["v1"]["clients"]["new"]["source"] == ch.v1_supervised_source_key(
        entry["target"]
    )


def test_v1_certified_binding_retirement_normalizes_surface_ids():
    state = {
        "v1": {
            "clients": {
                "old": {
                    "source": "old",
                    "workspace": "W",
                    "surface": "S",
                    "pid": 7,
                    "supervised": True,
                }
            }
        }
    }
    spec = {
        "workspace": "w",
        "surface": "s",
        "target": {"remote": None, "socket_path": "/a", "server_boot_id": "a" * 32},
        "connection_generation": 1,
    }
    ch.reconcile_v1_supervised_bindings(
        state,
        {"new": spec},
        {},
        complete=False,
        supervisor_pid=9,
        certified_surfaces={("w", "s"): True},
    )
    assert "old" in state["v1"]["clients"]


def test_v1_supervisor_retires_binding_from_certified_empty_surface():
    state = {
        "v1": {
            "clients": {
                "old": {
                    "source": "old",
                    "workspace": "w",
                    "surface": "s",
                    "pid": 7,
                    "supervised": True,
                }
            }
        }
    }
    ch.reconcile_v1_supervised_bindings(
        state,
        {},
        {},
        complete=False,
        supervisor_pid=9,
        certified_surfaces={("w", "s"): True, ("other", "s"): False},
    )
    assert state["v1"]["clients"] == {}


def test_v1_supervisor_retires_departed_binding_on_certified_surface():
    old = {
        "source": "old",
        "workspace": "w",
        "surface": "s",
        "pid": 7,
        "supervised": True,
    }
    state = {"v1": {"clients": {"departed": old}}}
    spec = {
        "workspace": "w",
        "surface": "s",
        "target": {"remote": None, "socket_path": "/a", "server_boot_id": "a" * 32},
        "connection_generation": 1,
    }
    ch.reconcile_v1_supervised_bindings(
        state,
        {"new": spec},
        {"new": spec},
        complete=False,
        supervisor_pid=9,
        certified_surfaces={("w", "s"): True},
    )
    assert set(state["v1"]["clients"]) == {"new"}


def test_v1_supervisor_retires_departed_binding_on_ready_surface_only():
    old = {
        "source": "old",
        "workspace": "w",
        "surface": "s",
        "pid": 7,
        "supervised": True,
    }
    unavailable = {
        "source": "old-b",
        "workspace": "other",
        "surface": "b",
        "pid": 7,
        "supervised": True,
    }
    state = {"v1": {"clients": {"departed": old, "offline-old": unavailable}}}
    a = {
        "workspace": "w",
        "surface": "s",
        "target": {"remote": None, "socket_path": "/a", "server_boot_id": "a" * 32},
        "connection_generation": 1,
    }
    b = {
        "workspace": "other",
        "surface": "b",
        "target": {"remote": None, "socket_path": "/b", "server_boot_id": "b" * 32},
        "connection_generation": 1,
    }
    ch.reconcile_v1_supervised_bindings(
        state, {"a": a, "b": b}, {"a": a}, complete=True, supervisor_pid=9
    )
    assert set(state["v1"]["clients"]) == {"a", "offline-old"}


def test_v1_supervisor_does_not_retire_unmanaged_clients():
    state = {
        "v1": {
            "clients": {
                "manual": {"source": "s", "workspace": "w", "surface": "p", "pid": 7}
            }
        }
    }
    ch.reconcile_v1_supervised_bindings(state, {}, {}, complete=True, supervisor_pid=9)
    assert "manual" in state["v1"]["clients"]


def test_v1_supervisor_rejects_stale_prepared_binding():
    old = {
        "source": "old-source",
        "workspace": "w",
        "surface": "s",
        "pid": 7,
        "supervised": True,
    }
    state = {"v1": {"clients": {"c": old}}}
    entry = {
        "workspace": "w",
        "surface": "s",
        "target": {
            "remote": None,
            "socket_path": "/tmp/new.sock",
            "server_boot_id": "a" * 32,
        },
        "connection_generation": 2,
    }
    assert not ch.reconcile_v1_supervised_bindings(
        state,
        {"c": entry},
        {"c": {**entry, "connection_generation": 1}},
        complete=True,
        supervisor_pid=9,
    )
    assert state["v1"]["clients"]["c"] == old


def test_v1_client_registration_tracks_individual_surface_and_session():
    state = {}
    ch.register_v1_client(
        state, "client-a", "local:one", "workspace-a", "surface-a", 123
    )
    ch.register_v1_client(
        state, "client-b", "local:one", "workspace-b", "surface-b", 456
    )
    clients = state["v1"]["clients"]
    assert clients["client-a"] == {
        "source": "local:one",
        "workspace": "workspace-a",
        "surface": "surface-a",
        "pid": 123,
    }
    ch.unregister_v1_client(state, "client-a")
    assert list(clients) == ["client-b"]


def test_v1_partial_publication_preserves_unverified_destinations(
    monkeypatch, tmp_path
):
    state = {}
    ch.register_v1_client(state, "a", "healthy", "wa", "sa", 1)
    ch.register_v1_client(state, "b", "offline", "wb", "sb", 2)
    stale = ch.v1_destination_key("wb", "sb")
    state["v1"]["destinations"] = {stale: {"notification_id": "offline-notice"}}
    state["v1"]["sidebar_keys"] = {"wb": ["offline-key"]}
    ch.save_state(state)
    monkeypatch.setattr(ch, "prune_v1_clients", lambda state: None)
    monkeypatch.setattr(ch, "retire_v1_legacy_daemons", lambda: True)
    monkeypatch.setattr(ch, "load_remote_state", lambda: {})
    monkeypatch.setattr(ch, "reconcile_v1_legacy_sidebar", lambda *args: None)
    destinations, workspaces, retired = [], [], []
    monkeypatch.setattr(
        ch,
        "reconcile_v1_sidebar",
        lambda cfg, state, items, **kwargs: workspaces.append(items),
    )
    monkeypatch.setattr(
        ch,
        "reconcile_v1_destination",
        lambda cfg, state, w, s, sources, **kwargs: destinations.append(
            (w, s, sources)
        ),
    )
    monkeypatch.setattr(
        ch,
        "retire_v1_destination",
        lambda cfg, state, key, **kwargs: retired.append(key),
    )
    ch.publish_v1_clients({}, eligible_destinations={("wa", "sa")})
    assert destinations == [("wa", "sa", ["healthy"])]
    assert workspaces == [{"wa": {"healthy"}}]
    assert not retired
    assert (
        ch.load_state()["v1"]["destinations"][stale]["notification_id"]
        == "offline-notice"
    )
    assert ch.load_state()["v1"]["sidebar_keys"]["wb"] == ["offline-key"]


def test_v1_publication_aggregates_sources_across_destination_case(
    monkeypatch, tmp_path
):
    state = {}
    ch.register_v1_client(state, "a", "done", "W", "S", 1)
    ch.register_v1_client(state, "b", "waiting", "w", "s", 2)
    ch.save_state(state)
    monkeypatch.setattr(ch, "prune_v1_clients", lambda state: None)
    monkeypatch.setattr(ch, "retire_v1_legacy_daemons", lambda: True)
    monkeypatch.setattr(ch, "load_remote_state", lambda: {})
    monkeypatch.setattr(ch, "reconcile_v1_legacy_sidebar", lambda *args: None)
    monkeypatch.setattr(ch, "reconcile_v1_sidebar", lambda *args: None)
    calls = []
    monkeypatch.setattr(
        ch,
        "reconcile_v1_destination",
        lambda cfg, state, w, s, sources, **kwargs: calls.append(set(sources)),
    )
    ch.publish_v1_clients({})
    assert calls == [{"done", "waiting"}]


def test_v1_sidebar_only_publication_does_not_write_notifications(
    monkeypatch, tmp_path
):
    state = {}
    ch.register_v1_client(state, "a", "source", "w", "s", 1)
    stale = ch.v1_destination_key("old", "surface")
    state["v1"]["destinations"] = {stale: {"notification_id": "old-notice"}}
    ch.save_state(state)
    monkeypatch.setattr(ch, "prune_v1_clients", lambda state: None)
    monkeypatch.setattr(ch, "retire_v1_legacy_daemons", lambda: True)
    monkeypatch.setattr(ch, "load_remote_state", lambda: {})
    monkeypatch.setattr(ch, "reconcile_v1_legacy_sidebar", lambda *args: None)
    sidebar, notices = [], []
    monkeypatch.setattr(
        ch, "reconcile_v1_sidebar", lambda *args, **kwargs: sidebar.append(args[2])
    )
    monkeypatch.setattr(
        ch, "reconcile_v1_destination", lambda *args, **kwargs: notices.append("write")
    )
    monkeypatch.setattr(
        ch, "retire_v1_destination", lambda *args, **kwargs: notices.append("retire")
    )
    ch.publish_v1_clients({}, notifications=False)
    assert sidebar == [{"w": {"source"}}]
    assert notices == []
    assert (
        ch.load_state()["v1"]["destinations"][stale]["notification_id"] == "old-notice"
    )


@pytest.mark.parametrize("backend", ["preparing-owned", "owned", "unknown"])
def test_v1_publication_backend_fences_ordinary_notification_writers(
    monkeypatch, tmp_path, backend
):
    marker = tmp_path / "v1-notification-backend"
    marker.write_text(backend)
    marker.chmod(0o600)
    state = {}
    ch.register_v1_client(state, "a", "source", "w", "s", 1)
    ch.save_state(state)
    monkeypatch.setattr(ch, "prune_v1_clients", lambda state: None)
    monkeypatch.setattr(ch, "retire_v1_legacy_daemons", lambda: True)
    monkeypatch.setattr(ch, "load_remote_state", lambda: {})
    monkeypatch.setattr(ch, "reconcile_v1_legacy_sidebar", lambda *args: None)
    sidebar, notices = [], []
    monkeypatch.setattr(
        ch, "reconcile_v1_sidebar", lambda *args, **kwargs: sidebar.append(args[2])
    )
    monkeypatch.setattr(
        ch, "reconcile_v1_destination", lambda *args, **kwargs: notices.append(args)
    )
    ch.publish_v1_clients({})
    assert sidebar == [{"w": {"source"}}]
    assert notices == []


def test_v1_destination_stops_after_alias_cleanup_invalidates_plan(monkeypatch):
    upper, lower = ch.v1_destination_key("W", "S"), ch.v1_destination_key("w", "s")
    state = {
        "v1": {
            "sources": {"a": {"attention": {}}},
            "destinations": {
                upper: {"notification_id": "first"},
                lower: {"notification_id": "second"},
            },
        }
    }
    notices = [
        {
            "id": name,
            "workspace_id": "w",
            "surface_id": "s",
            "is_read": False,
            "subtitle": ch.NOTIFICATION_SUBTITLE,
        }
        for name in ("first", "second")
    ]
    monkeypatch.setattr(ch, "cmux_notifications_checked", lambda cfg: notices)
    current = [True]
    effects = []

    def mark(cfg, args):
        effects.append(args)
        current[0] = False
        return True

    monkeypatch.setattr(ch, "cmux_cli", mark)
    assert not ch.reconcile_v1_destination(
        {}, state, "w", "s", ["a"], is_current=lambda: current[0]
    )
    assert len(effects) == 1
    assert set(state["v1"]["destinations"]) == {upper, lower}


def test_v1_destination_merges_duplicate_owned_aliases(monkeypatch):
    upper, lower = ch.v1_destination_key("W", "S"), ch.v1_destination_key("w", "s")
    state = {
        "v1": {
            "sources": {"a": {"attention": {}}},
            "destinations": {
                upper: {"notification_id": "first"},
                lower: {"notification_id": "second"},
            },
        }
    }
    notices = [
        {
            "id": name,
            "workspace_id": "w",
            "surface_id": "s",
            "is_read": False,
            "subtitle": ch.NOTIFICATION_SUBTITLE,
        }
        for name in ("first", "second")
    ]
    monkeypatch.setattr(ch, "cmux_notifications_checked", lambda cfg: notices)
    effects = []
    monkeypatch.setattr(ch, "cmux_cli", lambda cfg, args: effects.append(args) or True)
    assert ch.reconcile_v1_destination({}, state, "w", "s", ["a"])
    assert {args[-1] for args in effects} == {"first", "second"}
    assert len(state["v1"]["destinations"]) == 1


def test_v1_destination_owns_notification_across_id_case(monkeypatch):
    key = ch.v1_destination_key("W", "S")
    state = {
        "v1": {
            "sources": {"a": {"attention": {}}},
            "destinations": {key: {"notification_id": "owned"}},
        }
    }
    notices = [
        {
            "id": "owned",
            "workspace_id": "w",
            "surface_id": "s",
            "is_read": False,
            "subtitle": ch.NOTIFICATION_SUBTITLE,
        }
    ]
    monkeypatch.setattr(ch, "cmux_notifications_checked", lambda cfg: notices)
    effects = []
    monkeypatch.setattr(ch, "cmux_cli", lambda cfg, args: effects.append(args) or True)
    ch.reconcile_v1_destination({}, state, "w", "s", ["a"])
    assert ["mark-notification-read", "--id", "owned"] in effects
    assert set(state["v1"]["destinations"]) == {key}


def test_v1_retire_destination_matches_notification_case(monkeypatch):
    key = ch.v1_destination_key("W", "S")
    state = {"v1": {"destinations": {key: {"notification_id": "owned"}}}}
    monkeypatch.setattr(
        ch,
        "cmux_notifications_checked",
        lambda cfg: [
            {
                "id": "owned",
                "workspace_id": "w",
                "surface_id": "s",
                "is_read": False,
                "subtitle": ch.NOTIFICATION_SUBTITLE,
            }
        ],
    )
    effects = []
    monkeypatch.setattr(ch, "cmux_cli", lambda cfg, args: effects.append(args) or True)
    assert ch.retire_v1_destination({}, state, key)
    assert ["mark-notification-read", "--id", "owned"] in effects


def test_v1_partial_publication_preserves_active_destination_across_case_changes(
    monkeypatch, tmp_path
):
    state = {}
    ch.register_v1_client(state, "a", "source", "w", "s", 1)
    key = ch.v1_destination_key("W", "S")
    state["v1"]["destinations"] = {key: {"notification_id": "owned"}}
    ch.save_state(state)
    monkeypatch.setattr(ch, "prune_v1_clients", lambda state: None)
    monkeypatch.setattr(ch, "retire_v1_legacy_daemons", lambda: True)
    monkeypatch.setattr(ch, "reconcile_v1_sidebar", lambda *args, **kwargs: None)
    monkeypatch.setattr(ch, "reconcile_v1_destination", lambda *args, **kwargs: None)
    retired = []
    monkeypatch.setattr(
        ch,
        "retire_v1_destination",
        lambda cfg, state, key, **kwargs: retired.append(key),
    )
    ch.publish_v1_clients({}, eligible_destinations={("w", "s")})
    assert not retired


def test_v1_partial_publication_retires_certified_empty_surface(monkeypatch, tmp_path):
    key = ch.v1_destination_key("w", "s")
    ch.save_state(
        {
            "v1": {
                "destinations": {key: {"notification_id": "owned"}},
                "sidebar_keys": {"w": ["old"]},
            }
        }
    )
    monkeypatch.setattr(ch, "prune_v1_clients", lambda state: None)
    monkeypatch.setattr(ch, "retire_v1_legacy_daemons", lambda: True)
    monkeypatch.setattr(
        ch,
        "cmux_notifications_checked",
        lambda cfg: [
            {
                "id": "owned",
                "workspace_id": "w",
                "surface_id": "s",
                "is_read": False,
                "subtitle": ch.NOTIFICATION_SUBTITLE,
            }
        ],
    )
    effects = []
    monkeypatch.setattr(ch, "cmux_cli", lambda cfg, args: effects.append(args) or True)
    ch.publish_v1_clients({}, eligible_destinations={("w", "s")})
    assert ["mark-notification-read", "--id", "owned"] in effects
    assert ["clear-status", "old", "--workspace", "w"] in effects
    assert key not in ch.load_state()["v1"]["destinations"]


def test_v1_partial_publication_rechecks_readiness_before_notification(
    monkeypatch, tmp_path
):
    state = {}
    ch.register_v1_client(state, "a", "healthy", "w", "s", 1)
    state["v1"]["sources"] = {"healthy": {"attention": {"p": {"status": "done"}}}}
    ch.save_state(state)
    monkeypatch.setattr(ch, "prune_v1_clients", lambda state: None)
    monkeypatch.setattr(ch, "retire_v1_legacy_daemons", lambda: True)
    monkeypatch.setattr(ch, "reconcile_v1_sidebar", lambda *args, **kwargs: None)
    eligible = {("w", "s")}

    def notifications(cfg):
        eligible.clear()
        return []

    monkeypatch.setattr(ch, "cmux_notifications_checked", notifications)
    effects = []
    monkeypatch.setattr(ch, "run", lambda *args, **kwargs: effects.append(args))
    ch.publish_v1_clients(
        {}, eligible_destinations={("w", "s")}, eligibility_check=lambda: eligible
    )
    assert not effects


def test_v1_partial_publication_freezes_mixed_workspace_sidebar(monkeypatch, tmp_path):
    state = {}
    ch.register_v1_client(state, "a", "healthy", "w", "sa", 1)
    ch.register_v1_client(state, "b", "offline", "w", "sb", 2)
    state["v1"]["sidebar_keys"] = {"w": ["preserved"]}
    ch.save_state(state)
    monkeypatch.setattr(ch, "prune_v1_clients", lambda state: None)
    monkeypatch.setattr(ch, "retire_v1_legacy_daemons", lambda: True)
    side_effects, destinations = [], []
    monkeypatch.setattr(ch, "cmux_cli", lambda *args: side_effects.append(args) or True)
    monkeypatch.setattr(ch, "push_pill", lambda *args: side_effects.append(args))
    monkeypatch.setattr(
        ch,
        "reconcile_v1_destination",
        lambda cfg, state, w, s, sources, **kwargs: destinations.append(
            (w, s, sources)
        ),
    )
    ch.publish_v1_clients({}, eligible_destinations={("w", "sa")})
    assert destinations == [("w", "sa", ["healthy"])]
    assert not side_effects
    assert ch.load_state()["v1"]["sidebar_keys"]["w"] == ["preserved"]


def test_v1_publish_uses_registered_destinations_and_combines_sources(
    monkeypatch, tmp_path
):
    state = {}
    ch.register_v1_client(state, "a", "local", "w", "s", 1)
    ch.register_v1_client(state, "b", "remote", "w", "s", 2)
    ch.register_v1_client(state, "c", "local", "w2", "s2", 3)
    ch.save_state(state)
    calls = []
    monkeypatch.setattr(
        ch,
        "reconcile_v1_destination",
        lambda cfg, current, workspace, surface, sources, **kwargs: (
            calls.append((workspace, surface, set(sources))) or True
        ),
    )
    with (
        ch.v1_source_leader("client:a"),
        ch.v1_source_leader("client:b"),
        ch.v1_source_leader("client:c"),
    ):
        ch.publish_v1_clients({})
    assert ("w", "s", {"local", "remote"}) in calls
    assert ("w2", "s2", {"local"}) in calls
    assert len(calls) == 2


def test_v1_prunes_only_clients_without_live_lease(monkeypatch, tmp_path):
    state = {}
    ch.register_v1_client(state, "live", "source", "w", "s", 10)
    ch.register_v1_client(state, "dead", "source", "w2", "s2", 11)
    with ch.v1_source_leader("client:live"):
        ch.prune_v1_clients(state)
        assert set(state["v1"]["clients"]) == {"live"}
    ch.prune_v1_clients(state)
    assert not state["v1"]["clients"]


def test_v1_publish_retires_only_owned_notification_after_last_client_leaves(
    monkeypatch, tmp_path
):
    key = ch.v1_destination_key("w", "s")
    state = {
        "v1": {"clients": {}, "destinations": {key: {"notification_id": "our-id"}}}
    }
    ch.save_state(state)
    monkeypatch.setattr(
        ch,
        "cmux_notifications_checked",
        lambda cfg: [
            {
                "id": "our-id",
                "workspace_id": "w",
                "surface_id": "s",
                "is_read": False,
                "subtitle": ch.NOTIFICATION_SUBTITLE,
            },
            {
                "id": "foreign",
                "workspace_id": "w",
                "surface_id": "other",
                "is_read": False,
            },
        ],
    )
    calls = []
    monkeypatch.setattr(ch, "cmux_cli", lambda cfg, args: calls.append(args) or True)
    ch.publish_v1_clients({})
    assert calls == [["mark-notification-read", "--id", "our-id"]]
    assert key not in json.loads(ch.STATE_PATH.read_text())["v1"]["destinations"]


def test_v1_destination_retirement_retries_when_cmux_fails(monkeypatch):
    key = ch.v1_destination_key("w", "s")
    state = {"v1": {"destinations": {key: {"notification_id": "our-id"}}}}
    monkeypatch.setattr(
        ch,
        "cmux_notifications_checked",
        lambda cfg: [
            {
                "id": "our-id",
                "workspace_id": "w",
                "surface_id": "s",
                "is_read": False,
                "subtitle": ch.NOTIFICATION_SUBTITLE,
            }
        ],
    )
    monkeypatch.setattr(ch, "cmux_cli", lambda cfg, args: False)
    assert not ch.retire_v1_destination({}, state, key)
    assert key in state["v1"]["destinations"]


def test_v1_publisher_does_not_restore_acknowledged_attention_from_stale_snapshot(
    monkeypatch, tmp_path
):
    import threading

    key = ch.v1_destination_key("w", "s")
    ch.save_state(
        {
            "v1": {
                "sources": {
                    "a": {"attention": {"p": {"status": "done", "episode": "old"}}}
                },
                "clients": {"c": {"workspace": "w", "surface": "s", "source": "a"}},
                "destinations": {key: {"notification_id": "owned"}},
            }
        }
    )
    entered, release = threading.Event(), threading.Event()
    monkeypatch.setattr(ch, "retire_v1_legacy_daemons", lambda: True)
    monkeypatch.setattr(ch, "prune_v1_clients", lambda state: None)
    monkeypatch.setattr(ch, "load_remote_state", lambda: {})
    monkeypatch.setattr(ch, "reconcile_v1_legacy_sidebar", lambda *args: None)

    def sidebar(*args):
        entered.set()
        assert release.wait(5)

    monkeypatch.setattr(ch, "reconcile_v1_sidebar", sidebar)
    monkeypatch.setattr(
        ch,
        "cmux_notifications_checked",
        lambda cfg: [
            {"id": "owned", "workspace_id": "w", "surface_id": "s", "is_read": True}
        ],
    )
    creates = []
    monkeypatch.setattr(
        ch,
        "run",
        lambda *args, **kwargs: (
            creates.append(True)
            or SimpleNamespace(returncode=0, stdout='{"id":"new"}', stderr="")
        ),
    )
    worker = threading.Thread(target=lambda: ch.publish_v1_clients({}))
    worker.start()
    try:
        assert entered.wait(3)
        with ch.state_lock():
            state = ch.load_state()
            state["v1"]["sources"]["a"]["attention"].clear()
            ch.save_state(state)
    finally:
        release.set()
        worker.join(5)
    assert not creates


def test_supervised_remote_binding_adopts_matching_legacy_notification(monkeypatch):
    spec = {
        "workspace": "w",
        "surface": "s",
        "connection_generation": 1,
        "target": {"remote": "tom@host", "session": "work", "server_boot_id": "a" * 32},
    }
    state = {}
    ch.reconcile_v1_supervised_bindings(
        state, {"c": spec}, {"c": spec}, complete=True, supervisor_pid=123
    )
    source_id = state["v1"]["clients"]["c"]["source"]
    state["v1"]["sources"] = {
        source_id: {
            "panes": {"p": {"agent": "codex"}},
            "attention": {"p": {"status": "blocked"}},
        }
    }
    entry = {
        "identity": ["tom@host", "work"],
        "panes": {"p": {"agent": "codex", "status": "blocked"}},
    }
    monkeypatch.setattr(ch, "load_remote_state", lambda: {"remotes": {"one": entry}})
    notification = {
        "id": "legacy",
        "workspace_id": "w",
        "surface_id": "s",
        "subtitle": "herdr",
        "title": "codex: waiting for input",
        "is_read": False,
    }
    assert (
        ch.find_v1_legacy_notification(state, [source_id], "w", "s", [notification])
        == "legacy"
    )
