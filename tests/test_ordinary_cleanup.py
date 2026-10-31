import json
import os
import socket
import uuid
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from helpers import short_sock_path

import cmux_herdr as ch


@pytest.mark.parametrize("visible", [[], [{"id": "old-notice", "is_read": True}]])
def test_v1_cleanup_can_skip_resolved_unproven_ordinary_notice(visible):
    state = {"v1": {"destinations": {"old": {"notification_id": "old-notice"}}}}
    resolved = ch.v1_resolved_unproven_destinations(state, visible)
    inventory = ch.v1_ordinary_cleanup_inventory(
        state, str(uuid.uuid4()), resolved_unproven=resolved
    )
    assert inventory["items"] == []


def test_v1_cleanup_stage_merges_unfenced_inventory_after_interruption(
    monkeypatch, tmp_path
):
    store, workspace, surface = (str(uuid.uuid4()) for _ in range(3))
    (tmp_path / "v1-enabled").write_text("enabled")
    old = {"id": "old", "workspace_id": workspace, "surface_id": surface}
    ch.v1_prepare_ordinary_cleanup(
        tmp_path / "v1-ordinary-cleanup.json", {"store_id": store, "items": [old]}
    )
    other_surface = str(uuid.uuid4())
    ch.save_state(
        {
            "v1": {
                "destinations": {
                    ch.v1_destination_key(workspace, other_surface): {
                        "notification_id": "new",
                        "store_id": store,
                    }
                }
            }
        }
    )
    session = SimpleNamespace(store_identity=lambda: store, list_notifications=list)
    inventory = ch.v1_prepare_ordinary_cleanup_stage(session)
    assert {item["id"] for item in inventory["items"]} == {"old", "new"}


def test_v1_cleanup_stage_rejects_conflicting_unfenced_inventory(monkeypatch, tmp_path):
    store, workspace, surface = (str(uuid.uuid4()) for _ in range(3))
    (tmp_path / "v1-enabled").write_text("enabled")
    previous = {
        "store_id": store,
        "items": [{"id": "owned", "workspace_id": workspace, "surface_id": surface}],
    }
    journal = tmp_path / "v1-ordinary-cleanup.json"
    ch.v1_prepare_ordinary_cleanup(journal, previous)
    ch.save_state(
        {
            "v1": {
                "destinations": {
                    ch.v1_destination_key(workspace, str(uuid.uuid4())): {
                        "notification_id": "owned",
                        "store_id": store,
                    }
                }
            }
        }
    )
    session = SimpleNamespace(store_identity=lambda: store, list_notifications=list)
    with pytest.raises(ValueError, match="conflicting cleanup notification"):
        ch.v1_prepare_ordinary_cleanup_stage(session)
    assert json.loads(journal.read_text()) == previous
    assert not (tmp_path / "v1-notification-backend").exists()


def test_v1_cleanup_stage_rejects_saved_inventory_from_another_store(
    monkeypatch, tmp_path
):
    (tmp_path / "v1-enabled").write_text("enabled")
    marker = tmp_path / "v1-notification-backend"
    marker.write_text("preparing-owned")
    marker.chmod(0o600)
    ch.v1_prepare_ordinary_cleanup(
        tmp_path / "v1-ordinary-cleanup.json",
        {"store_id": str(uuid.uuid4()), "items": []},
    )
    session = SimpleNamespace(
        store_identity=lambda: str(uuid.uuid4()), list_notifications=list
    )
    with pytest.raises(ValueError, match="store identity"):
        ch.v1_prepare_ordinary_cleanup_stage(session)


def test_v1_cleanup_stage_recognizes_owned_row_after_interrupted_publish(
    monkeypatch, tmp_path
):
    (tmp_path / "v1-enabled").write_text("enabled")
    store, producer = str(uuid.uuid4()), str(uuid.uuid4())
    marker = tmp_path / "v1-notification-backend"
    marker.write_text("preparing-owned")
    marker.chmod(0o600)
    ch.v1_prepare_ordinary_cleanup(
        tmp_path / "v1-ordinary-cleanup.json", {"store_id": store, "items": []}
    )
    session = SimpleNamespace(
        store_identity=lambda: store,
        list_notifications=lambda: [
            {
                "id": str(uuid.uuid4()),
                "subtitle": ch.NOTIFICATION_SUBTITLE,
                "is_read": False,
                "owned_producer_id": producer.upper(),
            }
        ],
    )
    assert ch.v1_prepare_ordinary_cleanup_stage(session, producer=producer) == {
        "store_id": store,
        "items": [],
    }


def test_v1_commit_owned_backend_rejects_directory_replacement(monkeypatch, tmp_path):
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    monkeypatch.setattr(ch, "state_dir", state_dir)
    store = str(uuid.uuid4())
    (state_dir / "v1-enabled").write_text("enabled")
    marker = state_dir / "v1-notification-backend"
    marker.write_text("preparing-owned")
    marker.chmod(0o600)
    ch.v1_prepare_ordinary_cleanup(
        state_dir / "v1-ordinary-cleanup.json", {"store_id": store, "items": []}
    )
    session = SimpleNamespace(store_identity=lambda: store, list_notifications=list)

    @contextmanager
    def verified(_receipt):
        yield True

    publisher = SimpleNamespace(transport_session=session, validated_receipt=verified)

    def replace_directory(*_args):
        state_dir.rename(tmp_path / "old-state")
        state_dir.mkdir()
        replacement = state_dir / "v1-notification-backend"
        replacement.write_text("preparing-owned")
        replacement.chmod(0o600)
        return True

    monkeypatch.setattr(ch, "v1_drain_ordinary_cleanup", replace_directory)
    with pytest.raises(ValueError, match="directory changed"):
        ch.v1_commit_owned_backend(session, publisher, {"store_id": store})
    assert (state_dir / "v1-notification-backend").read_text() == "preparing-owned"


def test_v1_commit_owned_backend_rechecks_marker_before_replace(monkeypatch, tmp_path):
    store = str(uuid.uuid4())
    (tmp_path / "v1-enabled").write_text("enabled")
    marker = tmp_path / "v1-notification-backend"
    marker.write_text("preparing-owned")
    marker.chmod(0o600)
    ch.v1_prepare_ordinary_cleanup(
        tmp_path / "v1-ordinary-cleanup.json", {"store_id": store, "items": []}
    )
    session = SimpleNamespace(store_identity=lambda: store, list_notifications=list)

    @contextmanager
    def verified(_receipt):
        yield True

    publisher = SimpleNamespace(transport_session=session, validated_receipt=verified)
    fsync = os.fsync
    changed = []

    def swap_during_sync(fd):
        fsync(fd)
        if changed:
            return
        for candidate in tmp_path.glob(".backend-*"):
            if candidate.stat().st_ino == os.fstat(fd).st_ino:
                replacement = tmp_path / "replacement"
                replacement.write_text("ordinary")
                replacement.chmod(0o600)
                os.replace(replacement, marker)
                changed.append(True)
                break

    monkeypatch.setattr(ch.os, "fsync", swap_during_sync)
    with pytest.raises(ValueError, match="backend file changed"):
        ch.v1_commit_owned_backend(session, publisher, {"store_id": store})
    assert marker.read_text() == "ordinary"


def test_v1_cleanup_inventory_is_durable_and_cannot_be_replaced(tmp_path):
    store = str(uuid.uuid4())
    path = tmp_path / "cleanup.json"
    inventory = {"store_id": store, "items": []}
    assert ch.v1_prepare_ordinary_cleanup(path, inventory) == inventory
    assert json.loads(path.read_text()) == inventory
    assert path.stat().st_mode & 0o077 == 0
    assert ch.v1_prepare_ordinary_cleanup(path, inventory) == inventory
    with pytest.raises(ValueError, match="cleanup inventory changed"):
        ch.v1_prepare_ordinary_cleanup(
            path, {"store_id": str(uuid.uuid4()), "items": []}
        )
    assert json.loads(path.read_text()) == inventory


def test_v1_cleanup_inventory_rejects_symlink_without_touching_target(tmp_path):
    target = tmp_path / "unrelated.json"
    target.write_text("unrelated")
    path = tmp_path / "cleanup.json"
    path.symlink_to(target)
    with pytest.raises(OSError):
        ch.v1_prepare_ordinary_cleanup(
            path, {"store_id": str(uuid.uuid4()), "items": []}
        )
    assert target.read_text() == "unrelated"


def test_v1_cleanup_inventory_rejects_insecure_existing_file(tmp_path):
    path = tmp_path / "cleanup.json"
    inventory = {"store_id": str(uuid.uuid4()), "items": []}
    path.write_text(json.dumps(inventory))
    path.chmod(0o644)
    with pytest.raises(ValueError, match="invalid cleanup inventory file"):
        ch.v1_prepare_ordinary_cleanup(path, inventory)


def test_v1_cleanup_inventory_retries_after_file_sync_failure(tmp_path, monkeypatch):
    path = tmp_path / "cleanup.json"
    inventory = {"store_id": str(uuid.uuid4()), "items": []}
    fsync = ch.os.fsync
    calls = []

    def fail_once(fd):
        calls.append(fd)
        if len(calls) == 1:
            raise OSError("sync failed")
        return fsync(fd)

    monkeypatch.setattr(ch.os, "fsync", fail_once)
    with pytest.raises(OSError, match="sync failed"):
        ch.v1_prepare_ordinary_cleanup(path, inventory)
    assert not path.exists()
    assert ch.v1_prepare_ordinary_cleanup(path, inventory) == inventory


def test_v1_cleanup_inventory_retries_after_directory_sync_failure(
    tmp_path, monkeypatch
):
    path = tmp_path / "cleanup.json"
    inventory = {"store_id": str(uuid.uuid4()), "items": []}
    fsync = ch.os.fsync
    calls = []

    def fail_directory_once(fd):
        calls.append(fd)
        if len(calls) == 2:
            raise OSError("directory sync failed")
        return fsync(fd)

    monkeypatch.setattr(ch.os, "fsync", fail_directory_once)
    with pytest.raises(OSError, match="directory sync failed"):
        ch.v1_prepare_ordinary_cleanup(path, inventory)
    assert json.loads(path.read_text()) == inventory
    assert ch.v1_prepare_ordinary_cleanup(path, inventory) == inventory
    assert len(calls) >= 4


def test_v1_cleanup_inventory_syncs_existing_file(tmp_path, monkeypatch):
    path = tmp_path / "cleanup.json"
    inventory = {"store_id": str(uuid.uuid4()), "items": []}
    ch.v1_prepare_ordinary_cleanup(path, inventory)
    inode = path.stat().st_ino
    fsync = ch.os.fsync
    synced = []

    def record_sync(fd):
        synced.append(os.fstat(fd).st_ino)
        return fsync(fd)

    monkeypatch.setattr(ch.os, "fsync", record_sync)
    assert ch.v1_prepare_ordinary_cleanup(path, inventory) == inventory
    assert inode in synced


def test_v1_cleanup_inventory_rejects_removed_file_during_read(tmp_path, monkeypatch):
    path = tmp_path / "cleanup.json"
    inventory = {"store_id": str(uuid.uuid4()), "items": []}
    ch.v1_prepare_ordinary_cleanup(path, inventory)
    load = ch.json.load

    def remove_after_read(file):
        saved = load(file)
        path.unlink()
        return saved

    monkeypatch.setattr(ch.json, "load", remove_after_read)
    with pytest.raises((OSError, ValueError)):
        ch.v1_prepare_ordinary_cleanup(path, inventory)


def test_v1_ordinary_cleanup_inventory_rejects_object_destination_key():
    workspace, surface = str(uuid.uuid4()), str(uuid.uuid4())
    key = json.dumps({workspace: "ignored", surface: "ignored"})
    store = str(uuid.uuid4())
    state = {
        "v1": {"destinations": {key: {"notification_id": "owned", "store_id": store}}}
    }
    with pytest.raises(ValueError, match="invalid notification target"):
        ch.v1_ordinary_cleanup_inventory(state, store)


def test_v1_ordinary_cleanup_reads_only_recorded_ids_and_verifies():
    store, workspace, surface = (str(uuid.uuid4()) for _ in range(3))
    inventory = {
        "store_id": store,
        "items": [{"id": "owned", "workspace_id": workspace, "surface_id": surface}],
    }
    notice = {
        "id": "owned",
        "workspace_id": workspace,
        "surface_id": surface,
        "is_read": False,
    }
    foreign = {
        "id": "foreign",
        "workspace_id": workspace,
        "surface_id": surface,
        "is_read": False,
    }
    calls = []

    def mark(notice_id):
        calls.append(notice_id)
        notice["is_read"] = True
        return True

    session = SimpleNamespace(
        store_identity=lambda: store,
        list_notifications=lambda: [notice, foreign],
        mark_notification_read=mark,
    )
    assert ch.v1_drain_ordinary_cleanup(session, inventory)
    assert calls == ["owned"]
    assert not foreign["is_read"]


def test_v1_ordinary_cleanup_preserves_obligation_on_failure():
    store, workspace, surface = (str(uuid.uuid4()) for _ in range(3))
    inventory = {
        "store_id": store,
        "items": [{"id": "owned", "workspace_id": workspace, "surface_id": surface}],
    }
    notice = {
        "id": "owned",
        "workspace_id": workspace,
        "surface_id": surface,
        "is_read": False,
    }
    calls = []
    session = SimpleNamespace(
        store_identity=lambda: store,
        list_notifications=lambda: [notice],
        mark_notification_read=lambda item: calls.append(item) or True,
    )
    assert not ch.v1_drain_ordinary_cleanup(session, inventory)
    assert len(calls) == 1
    calls.clear()
    with pytest.raises(ValueError, match="store identity"):
        session.store_identity = lambda: str(uuid.uuid4())
        ch.v1_drain_ordinary_cleanup(session, inventory)
    assert not calls


@pytest.mark.parametrize("variant", ["wrong-target", "invalid-read", "duplicate"])
def test_v1_ordinary_cleanup_rejects_ambiguous_listing(variant):
    store, workspace, surface = (str(uuid.uuid4()) for _ in range(3))
    inventory = {
        "store_id": store,
        "items": [{"id": "owned", "workspace_id": workspace, "surface_id": surface}],
    }
    notice = {
        "id": "owned",
        "workspace_id": workspace,
        "surface_id": surface,
        "is_read": False,
    }
    if variant == "wrong-target":
        notice["surface_id"] = str(uuid.uuid4())
    elif variant == "invalid-read":
        notice["is_read"] = None
    listing = [notice, notice] if variant == "duplicate" else [notice]
    calls = []
    session = SimpleNamespace(
        store_identity=lambda: store,
        list_notifications=lambda: listing,
        mark_notification_read=lambda item: calls.append(item) or True,
    )
    assert not ch.v1_drain_ordinary_cleanup(session, inventory)
    assert not calls


def test_v1_ordinary_cleanup_retries_partial_progress():
    store, workspace, surface = (str(uuid.uuid4()) for _ in range(3))
    notices = [
        {"id": item, "workspace_id": workspace, "surface_id": surface, "is_read": False}
        for item in ("first", "second")
    ]
    inventory = {
        "store_id": store,
        "items": [
            {"id": item["id"], "workspace_id": workspace, "surface_id": surface}
            for item in notices
        ],
    }
    calls = []
    fail = [True]

    def mark(notice_id):
        calls.append(notice_id)
        if notice_id == "second" and fail[0]:
            return False
        next(item for item in notices if item["id"] == notice_id)["is_read"] = True
        return True

    session = SimpleNamespace(
        store_identity=lambda: store,
        list_notifications=lambda: notices,
        mark_notification_read=mark,
    )
    assert not ch.v1_drain_ordinary_cleanup(session, inventory)
    assert [item["is_read"] for item in notices] == [True, False]
    fail[0] = False
    assert ch.v1_drain_ordinary_cleanup(session, inventory)
    assert calls == ["first", "second", "second"]


@pytest.mark.parametrize("inventory", [None, {}, {"items": []}, {"items": [None]}])
def test_v1_ordinary_cleanup_rejects_invalid_inventory(inventory):
    store = str(uuid.uuid4())
    if isinstance(inventory, dict) and "items" in inventory:
        inventory["store_id"] = store
    session = SimpleNamespace(store_identity=lambda: store)
    if inventory == {"items": [], "store_id": store}:
        session.list_notifications = lambda: []
        assert ch.v1_drain_ordinary_cleanup(session, inventory)
    else:
        with pytest.raises(ValueError, match="invalid cleanup inventory"):
            ch.v1_drain_ordinary_cleanup(session, inventory)


def test_v1_prepare_ordinary_cleanup_stage_rejects_unproven_store(
    monkeypatch, tmp_path
):
    store, workspace, surface = (str(uuid.uuid4()) for _ in range(3))
    (tmp_path / "v1-enabled").write_text("enabled")
    ch.save_state(
        {
            "v1": {
                "destinations": {
                    ch.v1_destination_key(workspace, surface): {
                        "notification_id": "owned"
                    }
                }
            }
        }
    )
    session = SimpleNamespace(
        store_identity=lambda: store,
        list_notifications=lambda: [{"id": "owned", "is_read": False}],
    )
    with pytest.raises(ValueError, match="unproven notification store"):
        ch.v1_prepare_ordinary_cleanup_stage(session)
    assert not (tmp_path / "v1-ordinary-cleanup.json").exists()
    assert not (tmp_path / "v1-notification-backend").exists()


def test_v1_ordinary_cleanup_inventory_pins_ids_and_rejects_conflicts():
    store, workspace, surface = (str(uuid.uuid4()) for _ in range(3))
    first = ch.v1_destination_key(workspace, surface)
    alias = ch.v1_destination_key(workspace.upper(), surface.upper())
    state = {
        "v1": {
            "destinations": {
                first: {"notification_id": "notice-a", "store_id": store},
                alias: {"notification_id": "notice-b", "store_id": store},
            }
        }
    }
    inventory = ch.v1_ordinary_cleanup_inventory(state, store)
    assert inventory == {
        "store_id": store,
        "items": [
            {"workspace_id": workspace, "surface_id": surface, "id": "notice-a"},
            {"workspace_id": workspace, "surface_id": surface, "id": "notice-b"},
        ],
    }
    other = ch.v1_destination_key(str(uuid.uuid4()), surface)
    state["v1"]["destinations"][other] = {
        "notification_id": "notice-a",
        "store_id": store,
    }
    with pytest.raises(ValueError, match="conflicting notification"):
        ch.v1_ordinary_cleanup_inventory(state, store)


@pytest.mark.parametrize("entry", [{"notification_id": ""}, None])
def test_v1_ordinary_cleanup_inventory_rejects_unidentified_entry(entry):
    workspace, surface = str(uuid.uuid4()), str(uuid.uuid4())
    state = {"v1": {"destinations": {ch.v1_destination_key(workspace, surface): entry}}}
    with pytest.raises(ValueError, match="unidentified notification"):
        ch.v1_ordinary_cleanup_inventory(state, str(uuid.uuid4()))


def test_v1_ordinary_cleanup_inventory_skips_empty_destination():
    workspace, surface, store = (str(uuid.uuid4()) for _ in range(3))
    state = {"v1": {"destinations": {ch.v1_destination_key(workspace, surface): {}}}}
    assert ch.v1_ordinary_cleanup_inventory(state, store) == {
        "store_id": store,
        "items": [],
    }


@pytest.mark.parametrize("recorded", [None, "other-store"])
def test_v1_ordinary_cleanup_rejects_unproven_store(recorded):
    store, workspace, surface = (str(uuid.uuid4()) for _ in range(3))
    state = {
        "v1": {
            "destinations": {
                ch.v1_destination_key(workspace, surface): {
                    "notification_id": "owned",
                    "store_id": recorded,
                }
            }
        }
    }
    with pytest.raises(ValueError, match="unproven notification store"):
        ch.v1_ordinary_cleanup_inventory(state, store)


def test_v1_owned_transition_runs_over_one_socket(monkeypatch, tmp_path):
    import threading

    (tmp_path / "v1-enabled").write_text("enabled")
    store, workspace, surface = (str(uuid.uuid4()) for _ in range(3))
    ch.save_state(
        {
            "v1": {
                "destinations": {},
                "clients": {
                    "c": {"workspace": workspace, "surface": surface, "source": "a"}
                },
                "sources": {"a": {"attention": {"p": {"status": "blocked"}}}},
            }
        }
    )
    sock_path = short_sock_path(uuid.uuid4().hex[:8])
    ready = threading.Event()
    requests = []

    def serve():
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(sock_path))
            server.listen(1)
            ready.set()
            conn, _ = server.accept()
            with conn, conn.makefile("rb") as stream:
                for line in stream:
                    request = json.loads(line)
                    requests.append(request)
                    if request["method"] == "notification.state.info":
                        result = {"store_id": store}
                    elif request["method"] == "notification.list":
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

    worker = threading.Thread(target=serve, daemon=True)
    worker.start()
    assert ready.wait(5)
    try:
        with ch.v1_owned_socket_transport(
            tmp_path / "publisher.json", {"CMUX_SOCKET_PATH": str(sock_path)}, tmp_path
        ) as (session, publisher):
            assert ch.v1_transition_owned_backend(
                session, publisher, SimpleNamespace(scope=lambda: None)
            )
        worker.join(5)
        actions = [item["params"].get("action") for item in requests]
        assert actions.count("register") == 1
        assert actions.count("set") == 1
        assert (tmp_path / "v1-notification-backend").read_text() == "owned"
    finally:
        sock_path.unlink(missing_ok=True)


def test_v1_ordinary_cleanup_uses_one_socket_for_store_and_mutation():
    import threading

    store, workspace, surface, notice_id = (str(uuid.uuid4()) for _ in range(4))
    inventory = {
        "store_id": store,
        "items": [{"id": notice_id, "workspace_id": workspace, "surface_id": surface}],
    }
    notice = {
        "id": notice_id,
        "workspace_id": workspace,
        "surface_id": surface,
        "is_read": False,
    }
    sock_path = short_sock_path(uuid.uuid4().hex[:8])
    ready, requests = threading.Event(), []

    def serve():
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(sock_path))
            server.listen(1)
            ready.set()
            conn, _ = server.accept()
            with conn, conn.makefile("rb") as stream:
                for _ in range(4):
                    request = json.loads(stream.readline())
                    requests.append(request)
                    method = request["method"]
                    if method == "notification.state.info":
                        result = {"store_id": store}
                    elif method == "notification.mark_read":
                        notice["is_read"] = True
                        result = {"marked_read": 1}
                    else:
                        result = {"notifications": [notice]}
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

    worker = threading.Thread(target=serve, daemon=True)
    worker.start()
    assert ready.wait(5)
    try:
        with ch.V1OwnedSocketSession(sock_path) as session:
            assert ch.v1_drain_ordinary_cleanup(session, inventory)
        worker.join(5)
        assert [request["method"] for request in requests] == [
            "notification.state.info",
            "notification.list",
            "notification.mark_read",
            "notification.list",
        ]
        assert requests[2]["params"] == {"id": notice_id}
    finally:
        sock_path.unlink(missing_ok=True)
