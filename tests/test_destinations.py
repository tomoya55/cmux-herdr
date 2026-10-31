import json
from types import SimpleNamespace

from cmux_herdr_core import cmux, core, legacy_daemons, publication


def test_v1_supervisor_never_marks_unrelated_notice_with_stale_id(monkeypatch):
    calls = []
    monkeypatch.setattr(cmux, "cmux_cli", lambda cfg, args: calls.append(args) or True)
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
    publication.reconcile_aggregate_notification({}, "w", "s", {}, foreign, "stale")
    assert calls == []


def test_v1_retiring_destination_does_not_read_foreign_stale_id(monkeypatch):
    key = publication.v1_destination_key("w", "s")
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
    monkeypatch.setattr(cmux, "cmux_notifications_checked", lambda cfg: foreign)
    calls = []
    monkeypatch.setattr(cmux, "cmux_cli", lambda cfg, args: calls.append(args) or True)
    assert publication.retire_v1_destination({}, state, key)
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
    assert not publication.reconcile_v1_supervised_bindings(
        state, desired, {}, complete=True, supervisor_pid=9
    )
    assert state["v1"]["clients"] == {"old": old}
    assert publication.reconcile_v1_supervised_bindings(
        state, desired, {"new": entry}, complete=False, supervisor_pid=9
    )
    assert set(state["v1"]["clients"]) == {"old", "new"}
    assert publication.reconcile_v1_supervised_bindings(
        state, desired, {"new": entry}, complete=True, supervisor_pid=9
    )
    assert set(state["v1"]["clients"]) == {"new"}
    assert state["v1"]["clients"]["new"]["source"] == core.v1_supervised_source_key(
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
    publication.reconcile_v1_supervised_bindings(
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
    publication.reconcile_v1_supervised_bindings(
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
    publication.reconcile_v1_supervised_bindings(
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
    publication.reconcile_v1_supervised_bindings(
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
    publication.reconcile_v1_supervised_bindings(
        state, {}, {}, complete=True, supervisor_pid=9
    )
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
    assert not publication.reconcile_v1_supervised_bindings(
        state,
        {"c": entry},
        {"c": {**entry, "connection_generation": 1}},
        complete=True,
        supervisor_pid=9,
    )
    assert state["v1"]["clients"]["c"] == old


def test_v1_destination_stops_after_alias_cleanup_invalidates_plan(monkeypatch):
    upper, lower = (
        publication.v1_destination_key("W", "S"),
        publication.v1_destination_key("w", "s"),
    )
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
            "subtitle": core.NOTIFICATION_SUBTITLE,
        }
        for name in ("first", "second")
    ]
    monkeypatch.setattr(cmux, "cmux_notifications_checked", lambda cfg: notices)
    current = [True]
    effects = []

    def mark(cfg, args):
        effects.append(args)
        current[0] = False
        return True

    monkeypatch.setattr(cmux, "cmux_cli", mark)
    assert not publication.reconcile_v1_destination(
        {}, state, "w", "s", ["a"], is_current=lambda: current[0]
    )
    assert len(effects) == 1
    assert set(state["v1"]["destinations"]) == {upper, lower}


def test_v1_destination_merges_duplicate_owned_aliases(monkeypatch):
    upper, lower = (
        publication.v1_destination_key("W", "S"),
        publication.v1_destination_key("w", "s"),
    )
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
            "subtitle": core.NOTIFICATION_SUBTITLE,
        }
        for name in ("first", "second")
    ]
    monkeypatch.setattr(cmux, "cmux_notifications_checked", lambda cfg: notices)
    effects = []
    monkeypatch.setattr(
        cmux, "cmux_cli", lambda cfg, args: effects.append(args) or True
    )
    assert publication.reconcile_v1_destination({}, state, "w", "s", ["a"])
    assert {args[-1] for args in effects} == {"first", "second"}
    assert len(state["v1"]["destinations"]) == 1


def test_v1_destination_owns_notification_across_id_case(monkeypatch):
    key = publication.v1_destination_key("W", "S")
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
            "subtitle": core.NOTIFICATION_SUBTITLE,
        }
    ]
    monkeypatch.setattr(cmux, "cmux_notifications_checked", lambda cfg: notices)
    effects = []
    monkeypatch.setattr(
        cmux, "cmux_cli", lambda cfg, args: effects.append(args) or True
    )
    publication.reconcile_v1_destination({}, state, "w", "s", ["a"])
    assert ["mark-notification-read", "--id", "owned"] in effects
    assert set(state["v1"]["destinations"]) == {key}


def test_v1_retire_destination_matches_notification_case(monkeypatch):
    key = publication.v1_destination_key("W", "S")
    state = {"v1": {"destinations": {key: {"notification_id": "owned"}}}}
    monkeypatch.setattr(
        cmux,
        "cmux_notifications_checked",
        lambda cfg: [
            {
                "id": "owned",
                "workspace_id": "w",
                "surface_id": "s",
                "is_read": False,
                "subtitle": core.NOTIFICATION_SUBTITLE,
            }
        ],
    )
    effects = []
    monkeypatch.setattr(
        cmux, "cmux_cli", lambda cfg, args: effects.append(args) or True
    )
    assert publication.retire_v1_destination({}, state, key)
    assert ["mark-notification-read", "--id", "owned"] in effects


def test_v1_partial_publication_retires_certified_empty_surface(monkeypatch, tmp_path):
    key = publication.v1_destination_key("w", "s")
    core.save_state(
        {
            "v1": {
                "destinations": {key: {"notification_id": "owned"}},
                "sidebar_keys": {"w": ["old"]},
            }
        }
    )
    monkeypatch.setattr(publication, "prune_v1_clients", lambda state: None)
    monkeypatch.setattr(legacy_daemons, "retire_v1_legacy_daemons", lambda: True)
    monkeypatch.setattr(
        cmux,
        "cmux_notifications_checked",
        lambda cfg: [
            {
                "id": "owned",
                "workspace_id": "w",
                "surface_id": "s",
                "is_read": False,
                "subtitle": core.NOTIFICATION_SUBTITLE,
            }
        ],
    )
    effects = []
    monkeypatch.setattr(
        cmux, "cmux_cli", lambda cfg, args: effects.append(args) or True
    )
    publication.publish_v1_clients({}, eligible_destinations={("w", "s")})
    assert ["mark-notification-read", "--id", "owned"] in effects
    assert ["clear-status", "old", "--workspace", "w"] in effects
    assert key not in core.load_state()["v1"]["destinations"]


def test_v1_publish_retires_only_owned_notification_after_last_client_leaves(
    monkeypatch, tmp_path
):
    key = publication.v1_destination_key("w", "s")
    state = {
        "v1": {"clients": {}, "destinations": {key: {"notification_id": "our-id"}}}
    }
    core.save_state(state)
    monkeypatch.setattr(
        cmux,
        "cmux_notifications_checked",
        lambda cfg: [
            {
                "id": "our-id",
                "workspace_id": "w",
                "surface_id": "s",
                "is_read": False,
                "subtitle": core.NOTIFICATION_SUBTITLE,
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
    monkeypatch.setattr(cmux, "cmux_cli", lambda cfg, args: calls.append(args) or True)
    publication.publish_v1_clients({})
    assert calls == [["mark-notification-read", "--id", "our-id"]]
    assert key not in json.loads(core.STATE_PATH.read_text())["v1"]["destinations"]


def test_v1_destination_retirement_retries_when_cmux_fails(monkeypatch):
    key = publication.v1_destination_key("w", "s")
    state = {"v1": {"destinations": {key: {"notification_id": "our-id"}}}}
    monkeypatch.setattr(
        cmux,
        "cmux_notifications_checked",
        lambda cfg: [
            {
                "id": "our-id",
                "workspace_id": "w",
                "surface_id": "s",
                "is_read": False,
                "subtitle": core.NOTIFICATION_SUBTITLE,
            }
        ],
    )
    monkeypatch.setattr(cmux, "cmux_cli", lambda cfg, args: False)
    assert not publication.retire_v1_destination({}, state, key)
    assert key in state["v1"]["destinations"]


def test_v1_publisher_does_not_restore_acknowledged_attention_from_stale_snapshot(
    monkeypatch, tmp_path
):
    import threading

    key = publication.v1_destination_key("w", "s")
    core.save_state(
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
    monkeypatch.setattr(legacy_daemons, "retire_v1_legacy_daemons", lambda: True)
    monkeypatch.setattr(publication, "prune_v1_clients", lambda state: None)
    monkeypatch.setattr(core, "load_remote_state", lambda: {})
    monkeypatch.setattr(publication, "reconcile_v1_legacy_sidebar", lambda *args: None)

    def sidebar(*args):
        entered.set()
        assert release.wait(5)

    monkeypatch.setattr(publication, "reconcile_v1_sidebar", sidebar)
    monkeypatch.setattr(
        cmux,
        "cmux_notifications_checked",
        lambda cfg: [
            {"id": "owned", "workspace_id": "w", "surface_id": "s", "is_read": True}
        ],
    )
    creates = []
    monkeypatch.setattr(
        cmux,
        "run",
        lambda *args, **kwargs: (
            creates.append(True)
            or SimpleNamespace(returncode=0, stdout='{"id":"new"}', stderr="")
        ),
    )
    worker = threading.Thread(target=lambda: publication.publish_v1_clients({}))
    worker.start()
    try:
        assert entered.wait(3)
        with core.state_lock():
            state = core.load_state()
            state["v1"]["sources"]["a"]["attention"].clear()
            core.save_state(state)
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
    publication.reconcile_v1_supervised_bindings(
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
    monkeypatch.setattr(core, "load_remote_state", lambda: {"remotes": {"one": entry}})
    notification = {
        "id": "legacy",
        "workspace_id": "w",
        "surface_id": "s",
        "subtitle": "herdr",
        "title": "codex: waiting for input",
        "is_read": False,
    }
    assert (
        publication.find_v1_legacy_notification(
            state, [source_id], "w", "s", [notification]
        )
        == "legacy"
    )
