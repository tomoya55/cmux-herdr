import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from cmux_herdr_core import cmux, core, legacy_daemons, publication


def test_v1_destination_combines_sources_and_saves_notification_id(monkeypatch):
    state = {
        "v1": {
            "sources": {
                "local": {"attention": {"p": {"status": "done", "episode": "a"}}},
                "remote": {"attention": {"p": {"status": "blocked", "episode": "b"}}},
            }
        }
    }
    calls = []
    monkeypatch.setattr(cmux, "cmux_notifications_checked", lambda cfg: [])

    def reconcile(
        cfg, workspace, surface, attention, notifications, notification_id=None
    ):
        calls.append(sorted(item["status"] for item in attention.values()))
        return "created-id"

    monkeypatch.setattr(publication, "reconcile_aggregate_notification", reconcile)
    assert publication.reconcile_v1_destination(
        {}, state, "w", "s", ["local", "remote"]
    )
    assert calls == [["blocked", "done"]]
    assert (
        state["v1"]["destinations"][publication.v1_destination_key("w", "s")][
            "notification_id"
        ]
        == "created-id"
    )


def test_v1_destination_does_not_publish_if_notification_list_fails(monkeypatch):
    state = {
        "v1": {
            "sources": {
                "local": {"attention": {"p": {"status": "blocked", "episode": "a"}}}
            }
        }
    }
    before = json.loads(json.dumps(state))
    monkeypatch.setattr(cmux, "cmux_notifications_checked", lambda cfg: None)
    monkeypatch.setattr(
        publication,
        "reconcile_aggregate_notification",
        lambda *args: pytest.fail("must not publish"),
    )
    assert not publication.reconcile_v1_destination({}, state, "w", "s", ["local"])
    assert state == before


def test_v1_publication_backend_logs_unknown_state(monkeypatch, tmp_path):
    marker = tmp_path / "v1-notification-backend"
    marker.write_text("unknown")
    marker.chmod(0o600)
    monkeypatch.setattr(legacy_daemons, "retire_v1_legacy_daemons", lambda: True)
    logs = []
    monkeypatch.setattr(core, "log", logs.append)
    monkeypatch.setattr(
        publication, "publish_v1_clients_once", lambda *args, **kwargs: False
    )
    publication.publish_v1_clients({})
    assert any("invalid notification backend" in item for item in logs)


def test_v1_publication_backend_read_failure_keeps_sidebar(monkeypatch, tmp_path):
    monkeypatch.setattr(legacy_daemons, "retire_v1_legacy_daemons", lambda: True)
    open_file = os.open

    def read(path, *args, **kwargs):
        if Path(path).name == "v1-notification-backend":
            raise PermissionError("unavailable")
        return open_file(path, *args, **kwargs)

    monkeypatch.setattr(os, "open", read)
    calls = []
    monkeypatch.setattr(
        publication,
        "publish_v1_clients_once",
        lambda *args, **kwargs: calls.append(kwargs["notifications"]) or False,
    )
    publication.publish_v1_clients({})
    assert calls == [False]


@pytest.mark.parametrize("kind", ["symlink", "fifo"])
def test_v1_publication_backend_rejects_unsafe_marker(monkeypatch, tmp_path, kind):
    monkeypatch.setattr(legacy_daemons, "retire_v1_legacy_daemons", lambda: True)
    marker = tmp_path / "v1-notification-backend"
    if kind == "symlink":
        marker.symlink_to(tmp_path / "missing")
    else:
        os.mkfifo(marker, 0o600)
    calls = []
    monkeypatch.setattr(
        publication,
        "publish_v1_clients_once",
        lambda *args, **kwargs: calls.append(kwargs["notifications"]) or False,
    )
    publication.publish_v1_clients({})
    assert calls == [False]


def test_v1_sidebar_combines_active_and_tracked_case_aliases(monkeypatch):
    state = {
        "v1": {
            "sources": {
                "a": {
                    "panes": {
                        "p": {
                            "workspace_id": "g",
                            "status": "working",
                            "agent": "codex",
                        }
                    }
                }
            },
            "sidebar_keys": {},
        }
    }
    effects = []
    monkeypatch.setattr(
        cmux, "cmux_cli", lambda cfg, args: effects.append(args) or True
    )
    publication.reconcile_v1_sidebar({}, state, {"W": {"a"}})
    effects.clear()
    publication.reconcile_v1_sidebar({}, state, {"w": {"a"}})
    assert not any(args[0] == "clear-status" for args in effects)


def test_v1_partial_publication_clears_certified_sidebar_case_insensitively(
    monkeypatch, tmp_path
):
    core.save_state({"v1": {"sidebar_keys": {"W": ["old"]}}})
    monkeypatch.setattr(publication, "prune_v1_clients", lambda state: None)
    monkeypatch.setattr(legacy_daemons, "retire_v1_legacy_daemons", lambda: True)
    effects = []
    monkeypatch.setattr(
        cmux, "cmux_cli", lambda cfg, args: effects.append(args) or True
    )
    publication.publish_v1_clients({}, eligible_destinations={("w", "s")})
    assert ["clear-status", "old", "--workspace", "W"] in effects


def test_v1_publication_preserves_orphan_sidebar_when_full_scope_is_revoked(
    monkeypatch, tmp_path
):
    core.save_state({"v1": {"sidebar_keys": {"w": ["old-key"]}}})
    monkeypatch.setattr(publication, "prune_v1_clients", lambda state: None)
    monkeypatch.setattr(legacy_daemons, "retire_v1_legacy_daemons", lambda: True)
    monkeypatch.setattr(core, "load_remote_state", lambda: {})
    effects = []
    monkeypatch.setattr(cmux, "cmux_cli", lambda *args: effects.append(args) or True)
    publication.publish_v1_clients({}, eligibility_check=lambda: set())
    assert not effects
    assert core.load_state()["v1"]["sidebar_keys"] == {"w": ["old-key"]}


def test_v1_sidebar_preserves_remaining_keys_when_readiness_is_lost(monkeypatch):
    state = {"v1": {"sidebar_keys": {"w": ["a", "b"]}}}
    ready = [True]
    calls = []

    def clear(cfg, args):
        calls.append(args)
        ready[0] = False
        return True

    monkeypatch.setattr(cmux, "cmux_cli", clear)
    publication.reconcile_v1_sidebar(
        {}, state, {}, is_current=lambda workspace: ready[0]
    )
    assert len(calls) == 1
    assert len(state["v1"]["sidebar_keys"]["w"]) == 1


def test_v1_destination_validation_detects_client_with_different_id_case(
    monkeypatch, tmp_path
):
    snapshot = {"v1": {"clients": {}}}
    core.save_state(snapshot)
    assert publication.v1_destination_inputs_current(snapshot, "W", "S")
    live = core.load_state()
    live["v1"]["clients"]["new"] = {"workspace": "w", "surface": "s", "source": "a"}
    core.save_state(live)
    assert not publication.v1_destination_inputs_current(snapshot, "W", "S")


def test_v1_workspace_validation_detects_replaced_binding(monkeypatch, tmp_path):
    snapshot = {
        "v1": {
            "clients": {"c": {"workspace": "w", "surface": "s", "source": "old"}},
            "sources": {"old": {}, "new": {}},
        }
    }
    core.save_state(snapshot)
    assert publication.v1_workspace_inputs_current(snapshot, "w")
    live = core.load_state()
    live["v1"]["clients"]["c"]["source"] = "new"
    core.save_state(live)
    assert not publication.v1_workspace_inputs_current(snapshot, "w")


def test_v1_slow_publisher_does_not_hold_shared_state_lock(monkeypatch, tmp_path):
    import threading

    core.save_state({"v1": {"sources": {"a": {"value": 1}}, "clients": {}}})
    entered, release, updated = threading.Event(), threading.Event(), threading.Event()
    monkeypatch.setattr(legacy_daemons, "retire_v1_legacy_daemons", lambda: True)
    monkeypatch.setattr(publication, "prune_v1_clients", lambda state: None)
    monkeypatch.setattr(core, "load_remote_state", lambda: {})
    monkeypatch.setattr(publication, "reconcile_v1_legacy_sidebar", lambda *args: None)

    def sidebar(cfg, state, workspaces):
        entered.set()
        assert release.wait(5)
        state["v1"]["sidebar_keys"] = {"w": ["herdr.v1.test"]}

    monkeypatch.setattr(publication, "reconcile_v1_sidebar", sidebar)
    worker = threading.Thread(target=lambda: publication.publish_v1_clients({}))

    def update():
        with core.state_lock():
            state = core.load_state()
            state["v1"]["sources"]["a"]["value"] = 2
            core.save_state(state)
        updated.set()

    writer = threading.Thread(target=update)
    worker.start()
    try:
        assert entered.wait(3)
        writer.start()
        assert updated.wait(1), "cmux publication held the shared state lock"
    finally:
        release.set()
        worker.join(5)
        if writer.ident is not None:
            writer.join(5)
    state = core.load_state()
    assert state["v1"]["sources"]["a"]["value"] == 2
    assert state["v1"]["sidebar_keys"] == {"w": ["herdr.v1.test"]}


def test_v1_publisher_repairs_attention_acknowledged_during_notification_creation(
    monkeypatch, tmp_path
):
    core.save_state(
        {
            "v1": {
                "sources": {"a": {"attention": {"p": {"status": "done"}}}},
                "clients": {"c": {"workspace": "w", "surface": "s", "source": "a"}},
            }
        }
    )
    monkeypatch.setattr(legacy_daemons, "retire_v1_legacy_daemons", lambda: True)
    monkeypatch.setattr(publication, "prune_v1_clients", lambda state: None)
    monkeypatch.setattr(core, "load_remote_state", lambda: {})
    monkeypatch.setattr(publication, "reconcile_v1_legacy_sidebar", lambda *args: None)
    monkeypatch.setattr(publication, "reconcile_v1_sidebar", lambda *args: None)
    notices = []
    monkeypatch.setattr(cmux, "cmux_notifications_checked", lambda cfg: list(notices))

    def legacy(*args):
        with core.state_lock():
            live = core.load_state()
            live["v1"]["sources"]["a"]["attention"].clear()
            core.save_state(live)
        return None

    monkeypatch.setattr(publication, "find_v1_legacy_notification", legacy)

    def run(cmd, **kwargs):
        notices.append(
            {
                "id": "new",
                "workspace_id": "w",
                "surface_id": "s",
                "is_read": False,
                "subtitle": core.NOTIFICATION_SUBTITLE,
            }
        )
        return SimpleNamespace(returncode=0, stdout='{"id":"new"}', stderr="")

    monkeypatch.setattr(cmux, "run", run)
    monkeypatch.setattr(
        cmux, "cmux_cli", lambda cfg, cmd: notices[0].update(is_read=True) or True
    )
    publication.publish_v1_clients({})
    assert not notices or notices[0]["is_read"]


def test_v1_final_publisher_waits_for_inflight_publication(monkeypatch, tmp_path):
    import threading

    core.save_state(
        {
            "v1": {
                "sources": {"a": {}},
                "clients": {"c": {"workspace": "w", "surface": "s", "source": "a"}},
            }
        }
    )
    entered, release = threading.Event(), threading.Event()
    seen = []
    monkeypatch.setattr(legacy_daemons, "retire_v1_legacy_daemons", lambda: True)
    monkeypatch.setattr(publication, "prune_v1_clients", lambda state: None)
    monkeypatch.setattr(core, "load_remote_state", lambda: {})
    monkeypatch.setattr(publication, "reconcile_v1_legacy_sidebar", lambda *args: None)

    def sidebar(cfg, state, workspaces):
        seen.append(set(workspaces))
        if len(seen) == 1:
            entered.set()
            assert release.wait(5)

    monkeypatch.setattr(publication, "reconcile_v1_sidebar", sidebar)
    monkeypatch.setattr(
        publication, "reconcile_v1_destination", lambda *args, **kwargs: True
    )
    first = threading.Thread(target=lambda: publication.publish_v1_clients({}))
    final = threading.Thread(
        target=lambda: publication.publish_v1_clients({}, wait=True)
    )
    first.start()
    try:
        assert entered.wait(3)
        with core.state_lock():
            state = core.load_state()
            state["v1"]["clients"].clear()
            core.save_state(state)
        final.start()
        release.set()
        final.join(5)
    finally:
        release.set()
        first.join(5)
        if final.ident is not None:
            final.join(5)
    assert seen == [{"w"}, set(), set()]


def test_v1_sidebar_reconciles_per_workspace_and_retries_stale_clears(monkeypatch):
    state = {
        "v1": {
            "sources": {
                "source": {
                    "panes": {
                        "p:1": {
                            "workspace_id": "herdr-w",
                            "status": "working",
                            "agent": "one",
                        },
                        "p_1": {
                            "workspace_id": "herdr-w",
                            "status": "blocked",
                            "agent": "two",
                        },
                    },
                    "attention": {"p_1": {"status": "blocked"}},
                }
            }
        }
    }
    calls = []
    monkeypatch.setattr(cmux, "cmux_cli", lambda cfg, args: calls.append(args) or True)
    publication.reconcile_v1_sidebar({}, state, {"cmux-w": {"source"}})
    keys = [call[1] for call in calls if call[0] == "set-status"]
    assert len(keys) == len(set(keys)) == 3
    assert all(call[call.index("--workspace") + 1] == "cmux-w" for call in calls)
    calls.clear()
    state["v1"]["sources"]["source"]["panes"].pop("p:1")
    monkeypatch.setattr(
        cmux,
        "cmux_cli",
        lambda cfg, args: calls.append(args) or args[0] != "clear-status",
    )
    publication.reconcile_v1_sidebar({}, state, {"cmux-w": {"source"}})
    stale = [call[1] for call in calls if call[0] == "clear-status"]
    assert len(stale) == 1
    assert stale[0] in state["v1"]["sidebar_keys"]["cmux-w"]
    calls.clear()
    monkeypatch.setattr(cmux, "cmux_cli", lambda cfg, args: calls.append(args) or True)
    publication.reconcile_v1_sidebar({}, state, {})
    assert {call[1] for call in calls if call[0] == "clear-status"} == set(keys)
    assert not state["v1"]["sidebar_keys"]


def test_v1_legacy_sidebar_cleanup_uses_owned_keys_and_retries_failures(monkeypatch):
    state = {
        "sessions": {
            "work": {
                "session_ref": {"ref": "workspace:1"},
                "workspaces": {"w1": {"panes": {"w1:p1": {}}}},
            }
        }
    }
    remote = {
        "remotes": {
            "remote-a": {
                "ref": "workspace:2",
                "panes": {"w2:p2": {}},
                "pending_clear": ["workspace:3"],
            }
        }
    }
    visible = {
        "workspace:1": {"herdr.w1", "herdr.w1.w1_p1", "foreign"},
        "workspace:2": {"herdr.remote.remote-a", "herdr.remote.remote-a.w2_p2"},
        "workspace:3": {"herdr.remote.remote-a", "herdr.remote.remote-a.w2_p2"},
    }
    calls = []
    monkeypatch.setattr(
        cmux, "cmux_herdr_status_keys_checked", lambda cfg, ref: list(visible[ref])
    )

    def clear(cfg, args):
        calls.append(args)
        if args[1] == "herdr.w1.w1_p1" and len(calls) <= 6:
            return False
        visible[args[-1]].discard(args[1])
        return True

    monkeypatch.setattr(cmux, "cmux_cli", clear)
    publication.reconcile_v1_legacy_sidebar({}, state, remote)
    assert len(calls) == 6
    assert "foreign" in visible["workspace:1"]
    publication.reconcile_v1_legacy_sidebar({}, state, remote)
    assert calls[-1] == ["clear-status", "herdr.w1.w1_p1", "--workspace", "workspace:1"]
    visible["workspace:1"].add("herdr.w1")
    publication.reconcile_v1_legacy_sidebar({}, state, remote)
    assert calls[-1] == ["clear-status", "herdr.w1", "--workspace", "workspace:1"]


def test_v1_publish_does_not_compete_with_unstopped_legacy_writer(monkeypatch):
    monkeypatch.setattr(legacy_daemons, "retire_v1_legacy_daemons", lambda: False)
    monkeypatch.setattr(
        core, "load_state", lambda: pytest.fail("publication must wait")
    )
    publication.publish_v1_clients({})


def test_v1_legacy_sidebar_finds_old_keys_in_active_and_label_workspaces(monkeypatch):
    state = {
        "sessions": {
            "work": {"workspaces": {"w1": {"label": "Old title", "panes": {"p1": {}}}}}
        },
        "v1": {"clients": {"client": {"workspace": "workspace:active"}}},
    }
    monkeypatch.setattr(cmux, "cmux_workspace_refs", lambda cfg: {"workspace:old"})
    monkeypatch.setattr(
        cmux,
        "cmux_herdr_status_keys_checked",
        lambda cfg, ref: ["herdr.w1", "herdr.w1.p1"],
    )
    calls = []
    monkeypatch.setattr(cmux, "cmux_cli", lambda cfg, args: calls.append(args) or True)
    publication.reconcile_v1_legacy_sidebar({}, state, {})
    assert {(call[-1], call[1]) for call in calls} == {
        ("workspace:active", "herdr.w1"),
        ("workspace:active", "herdr.w1.p1"),
        ("workspace:old", "herdr.w1"),
        ("workspace:old", "herdr.w1.p1"),
    }


def test_v1_adopts_matching_legacy_notification_only_with_live_attention(monkeypatch):
    state = {
        "sessions": {
            "old": {
                "workspaces": {
                    "w": {"panes": {"p": {"status": "blocked", "agent": "codex"}}}
                }
            }
        },
        "v1": {
            "clients": {
                "c": {
                    "source": "source",
                    "workspace": "w",
                    "surface": "s",
                    "legacy_target": {"session": "old", "remote": None},
                }
            },
            "sources": {
                "source": {
                    "panes": {"p": {"agent": "codex", "status": "blocked"}},
                    "attention": {"p": {"status": "blocked", "episode": "e"}},
                }
            },
        },
    }
    notifications = [
        {
            "id": "foreign",
            "workspace_id": "w",
            "surface_id": "s",
            "subtitle": "other",
            "title": "codex: waiting for input",
            "is_read": False,
        },
        {
            "id": "legacy",
            "workspace_id": "w",
            "surface_id": "s",
            "subtitle": "herdr",
            "title": "codex: waiting for input",
            "is_read": False,
        },
    ]
    assert (
        publication.find_v1_legacy_notification(
            state, ["source"], "w", "s", notifications
        )
        == "legacy"
    )
    assert (
        publication.find_v1_legacy_notification(
            state, ["source"], "W", "S", notifications
        )
        == "legacy"
    )
    state["v1"]["sources"]["source"]["attention"] = {}
    assert (
        publication.find_v1_legacy_notification(
            state, ["source"], "w", "s", notifications
        )
        is None
    )
    state["v1"]["sources"]["source"]["attention"] = {
        "p": {"status": "done", "episode": "e"}
    }
    assert (
        publication.find_v1_legacy_notification(
            state, ["source"], "w", "s", notifications
        )
        is None
    )


def test_v1_does_not_adopt_legacy_notification_from_another_session():
    state = {
        "sessions": {
            "other": {
                "workspaces": {
                    "w": {"panes": {"p": {"status": "blocked", "agent": "codex"}}}
                }
            }
        },
        "v1": {
            "clients": {
                "c": {
                    "source": "source",
                    "workspace": "w",
                    "surface": "s",
                    "legacy_target": {"remote": None, "session": "work"},
                }
            },
            "sources": {
                "source": {
                    "panes": {"p": {"agent": "codex"}},
                    "attention": {"p": {"status": "blocked"}},
                }
            },
        },
    }
    notifications = [
        {
            "id": "foreign",
            "workspace_id": "w",
            "surface_id": "s",
            "subtitle": "herdr",
            "title": "codex: waiting for input",
            "is_read": False,
        }
    ]
    assert (
        publication.find_v1_legacy_notification(
            state, ["source"], "w", "s", notifications
        )
        is None
    )


def test_v1_legacy_sidebar_finds_known_keys_in_inactive_workspaces(monkeypatch):
    state = {"sessions": {"old": {"workspaces": {"w1": {"panes": {"p1": {}}}}}}}
    monkeypatch.setattr(cmux, "cmux_workspace_refs", lambda cfg: {"workspace:old"})
    monkeypatch.setattr(
        cmux,
        "cmux_herdr_status_keys_checked",
        lambda cfg, ref: ["herdr.w1", "herdr.w1.p1", "herdr.unrelated"],
    )
    calls = []
    monkeypatch.setattr(cmux, "cmux_cli", lambda cfg, args: calls.append(args) or True)
    publication.reconcile_v1_legacy_sidebar({}, state, {})
    assert {call[1] for call in calls} == {"herdr.w1", "herdr.w1.p1"}


def test_v1_legacy_notification_adoption_requires_matching_remote_and_pane(monkeypatch):
    state = {
        "v1": {
            "clients": {
                "c": {
                    "source": "remote-source",
                    "workspace": "w",
                    "surface": "s",
                    "legacy_target": {"remote": "tom@host", "session": "work"},
                }
            },
            "sources": {
                "remote-source": {
                    "panes": {"p": {"agent": "codex"}},
                    "attention": {"p": {"status": "blocked"}},
                }
            },
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
            state, ["remote-source"], "w", "s", [notification]
        )
        == "legacy"
    )
    entry["identity"] = ["tom@other", "work"]
    assert (
        publication.find_v1_legacy_notification(
            state, ["remote-source"], "w", "s", [notification]
        )
        is None
    )
    entry["identity"] = ["tom@host", "work"]
    entry["panes"] = {"other": {"agent": "codex", "status": "blocked"}}
    assert (
        publication.find_v1_legacy_notification(
            state, ["remote-source"], "w", "s", [notification]
        )
        is None
    )


def test_v1_does_not_adopt_ambiguous_same_title_from_another_source(monkeypatch):
    pane = {"status": "blocked", "agent": "codex"}
    state = {
        "sessions": {
            "work": {"workspaces": {"w": {"panes": {"p": pane}}}},
            "other": {"workspaces": {"w": {"panes": {"p": pane}}}},
        },
        "v1": {
            "clients": {
                "c": {
                    "source": "source",
                    "workspace": "w",
                    "surface": "s",
                    "legacy_target": {"remote": None, "session": "work"},
                }
            },
            "sources": {
                "source": {
                    "panes": {"p": {"agent": "codex"}},
                    "attention": {"p": {"status": "blocked"}},
                }
            },
        },
    }
    monkeypatch.setattr(core, "load_remote_state", lambda: {"remotes": {}})
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
            state, ["source"], "w", "s", [notification]
        )
        is None
    )


def test_aggregate_notification_waits_for_foreign_unread(cfg, cmux_calls):
    notifications = [
        {
            "id": "other",
            "workspace_id": "w",
            "surface_id": "s",
            "is_read": False,
            "subtitle": "build",
        }
    ]
    publication.reconcile_aggregate_notification(
        cfg, "w", "s", {"p": {"status": "blocked", "episode": "1"}}, notifications
    )
    assert cmux_calls == []


def test_aggregate_notification_restores_unread_silently(cfg, cmux_calls):
    publication.reconcile_aggregate_notification(
        cfg, "w", "s", {"p": {"status": "blocked", "episode": "1"}}, []
    )
    command = cmux_calls[0]
    assert command[:2] == ["rpc", "notification.create_for_target"]
    payload = json.loads(command[2])
    assert payload["workspace_id"] == "w"
    assert payload["surface_id"] == "s"
    assert payload["effects"]["markUnread"] is True
    assert payload["effects"]["sound"] is False
    assert payload["effects"]["desktop"] is False


def test_aggregate_notification_returns_created_id(cfg, monkeypatch):
    def fake_run(cmd, timeout=10):
        return SimpleNamespace(
            returncode=0, stdout=json.dumps({"id": "new-id"}), stderr=""
        )

    monkeypatch.setattr(cmux, "run", fake_run)
    result = publication.reconcile_aggregate_notification(
        cfg, "w", "s", {"p": {"status": "done", "episode": "1"}}, []
    )
    assert result == "new-id"


def test_aggregate_notification_updates_when_attention_changes(cfg, cmux_calls):
    previous = [
        {
            "id": "our-id",
            "workspace_id": "w",
            "surface_id": "s",
            "is_read": False,
            "title": "herdr: 1 waiting",
            "subtitle": core.NOTIFICATION_SUBTITLE,
        }
    ]
    attention = {
        "p1": {"status": "blocked", "episode": "1"},
        "p2": {"status": "done", "episode": "2"},
    }
    publication.reconcile_aggregate_notification(
        cfg, "w", "s", attention, previous, "our-id"
    )
    assert cmux_calls[0][:2] == ["rpc", "notification.create_for_target"]
    assert json.loads(cmux_calls[0][2])["title"] == "herdr: 1 waiting · 1 finished"


def test_aggregate_notification_marks_only_its_own_id_read(cfg, cmux_calls):
    notifications = [
        {
            "id": "our-id",
            "workspace_id": "w",
            "surface_id": "s",
            "is_read": False,
            "subtitle": core.NOTIFICATION_SUBTITLE,
        },
        {"id": "foreign", "workspace_id": "w", "surface_id": "other", "is_read": False},
    ]
    publication.reconcile_aggregate_notification(
        cfg, "w", "s", {}, notifications, "our-id"
    )
    assert cmux_calls == [["mark-notification-read", "--id", "our-id"]]


def test_attention_sidebar_uses_pending_completion_after_idle():
    panes = {"p": {"status": "idle", "agent": "claude", "title": "task"}}
    attention = {"p": {"status": "done", "episode": "a"}}
    visible = publication.sidebar_panes(panes, attention)
    assert visible["p"]["status"] == "done"
    assert panes["p"]["status"] == "idle"


def test_attention_sidebar_preserves_pending_blocked_when_agent_state_unknown():
    panes = {"p": {"status": "unknown", "agent": "letta", "title": "task"}}
    attention = {"p": {"status": "blocked", "episode": "a"}}
    assert publication.sidebar_panes(panes, attention)["p"]["status"] == "blocked"


def test_attention_sidebar_prefers_current_work_over_old_completion():
    panes = {"p": {"status": "working", "agent": "claude", "title": "task"}}
    attention = {"p": {"status": "done", "episode": "a"}}
    assert publication.sidebar_panes(panes, attention)["p"]["status"] == "working"


def test_attention_sidebar_hides_acknowledged_done():
    panes = {"p": {"status": "done", "agent": "claude", "title": "task"}}
    attention = {"p": {"acknowledged": "a"}}
    assert publication.sidebar_panes(panes, attention) == {}
