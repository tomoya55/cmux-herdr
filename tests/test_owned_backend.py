import json
import uuid
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

import cmux_herdr as ch


def test_v1_prepare_owned_backend_is_durable_and_idempotent(monkeypatch, tmp_path):
    assert not ch.v1_prepare_owned_backend()
    (tmp_path / "v1-enabled").write_text("enabled")
    assert ch.v1_prepare_owned_backend()
    marker = tmp_path / "v1-notification-backend"
    assert marker.read_text() == "preparing-owned"
    assert marker.stat().st_mode & 0o077 == 0
    assert ch.v1_prepare_owned_backend()
    marker.write_text("owned")
    assert not ch.v1_prepare_owned_backend()
    assert marker.read_text() == "owned"


def test_v1_prepare_owned_backend_accepts_explicit_ordinary(monkeypatch, tmp_path):
    (tmp_path / "v1-enabled").write_text("enabled")
    marker = tmp_path / "v1-notification-backend"
    marker.write_text("ordinary")
    marker.chmod(0o600)
    assert ch.v1_prepare_owned_backend()
    assert marker.read_text() == "preparing-owned"


def test_v1_prepare_owned_backend_retries_after_directory_sync_failure(
    monkeypatch, tmp_path
):
    (tmp_path / "v1-enabled").write_text("enabled")
    sync = ch.os.fsync
    calls = []

    def fail_once(fd):
        calls.append(fd)
        if len(calls) == 3:
            raise OSError("directory sync failed")
        return sync(fd)

    monkeypatch.setattr(ch.os, "fsync", fail_once)
    with pytest.raises(OSError, match="directory sync failed"):
        ch.v1_prepare_owned_backend()
    assert (tmp_path / "v1-notification-backend").read_text() == "preparing-owned"
    assert ch.v1_prepare_owned_backend()


def test_v1_prepare_owned_backend_waits_for_handoff_lock(monkeypatch, tmp_path):
    import fcntl
    import threading

    (tmp_path / "v1-enabled").write_text("enabled")
    transitioned = threading.Event()
    with (tmp_path / "v1-activation.lock").open("w") as activation:
        fcntl.flock(activation, fcntl.LOCK_EX)
        worker = threading.Thread(
            target=lambda: (ch.v1_prepare_owned_backend(), transitioned.set())
        )
        worker.start()
        try:
            assert not transitioned.wait(0.05)
            assert not (tmp_path / "v1-notification-backend").exists()
        finally:
            fcntl.flock(activation, fcntl.LOCK_UN)
            worker.join(2)
    assert transitioned.is_set()


def test_v1_prepare_ordinary_cleanup_stage_fences_and_persists(monkeypatch, tmp_path):
    store, workspace, surface = (str(uuid.uuid4()) for _ in range(3))
    (tmp_path / "v1-enabled").write_text("enabled")
    state = {
        "v1": {
            "destinations": {
                ch.v1_destination_key(workspace, surface): {
                    "notification_id": "owned",
                    "store_id": store,
                }
            }
        }
    }
    ch.save_state(state)
    session = SimpleNamespace(store_identity=lambda: store, list_notifications=list)
    inventory = ch.v1_prepare_ordinary_cleanup_stage(session)
    assert inventory["items"][0]["id"] == "owned"
    assert (tmp_path / "v1-notification-backend").read_text() == "preparing-owned"
    assert json.loads((tmp_path / "v1-ordinary-cleanup.json").read_text()) == inventory
    assert ch.v1_prepare_ordinary_cleanup_stage(session) == inventory


def test_v1_old_unread_notice_keeps_publication_until_read_then_fences(
    monkeypatch, tmp_path
):
    store, workspace, surface = (str(uuid.uuid4()) for _ in range(3))
    (tmp_path / "v1-enabled").write_text("enabled")
    ch.save_state(
        {
            "v1": {
                "destinations": {
                    ch.v1_destination_key(workspace, surface): {
                        "notification_id": "old"
                    }
                }
            }
        }
    )
    notice = {"id": "old", "is_read": False}
    session = SimpleNamespace(
        store_identity=lambda: store, list_notifications=lambda: [notice.copy()]
    )
    with pytest.raises(ValueError, match="unproven notification store"):
        ch.v1_prepare_ordinary_cleanup_stage(session)
    assert ch.v1_notification_backend() == "ordinary"
    notice["is_read"] = True
    inventory = ch.v1_prepare_ordinary_cleanup_stage(session)
    assert inventory["items"] == []
    assert ch.v1_notification_backend() == "preparing-owned"


def test_v1_cleanup_stage_does_not_prepare_when_already_owned(monkeypatch, tmp_path):
    (tmp_path / "v1-enabled").write_text("enabled")
    marker = tmp_path / "v1-notification-backend"
    marker.write_text("owned")
    marker.chmod(0o600)
    session = SimpleNamespace(store_identity=lambda: str(uuid.uuid4()))
    with pytest.raises(ValueError, match="preparation is unavailable"):
        ch.v1_prepare_ordinary_cleanup_stage(session)
    assert not (tmp_path / "v1-ordinary-cleanup.json").exists()


def test_v1_prepare_ordinary_cleanup_stage_retries_after_persistence_failure(
    monkeypatch, tmp_path
):
    store = str(uuid.uuid4())
    (tmp_path / "v1-enabled").write_text("enabled")
    ch.save_state({"v1": {"destinations": {}}})
    session = SimpleNamespace(store_identity=lambda: store, list_notifications=list)
    persist = ch.v1_prepare_ordinary_cleanup
    calls = []

    def fail_once(*args, **kwargs):
        calls.append(True)
        if len(calls) == 1:
            raise OSError("simulated crash")
        return persist(*args, **kwargs)

    monkeypatch.setattr(ch, "v1_prepare_ordinary_cleanup", fail_once)
    with pytest.raises(OSError, match="simulated crash"):
        ch.v1_prepare_ordinary_cleanup_stage(session)
    assert not (tmp_path / "v1-notification-backend").exists()
    assert ch.v1_prepare_ordinary_cleanup_stage(session) == {
        "store_id": store,
        "items": [],
    }


def test_v1_cleanup_stage_recovers_saved_inventory_after_state_loss(
    monkeypatch, tmp_path
):
    store, workspace, surface = (str(uuid.uuid4()) for _ in range(3))
    (tmp_path / "v1-enabled").write_text("enabled")
    ch.save_state(
        {
            "v1": {
                "destinations": {
                    ch.v1_destination_key(workspace, surface): {
                        "notification_id": "owned",
                        "store_id": store,
                    }
                }
            }
        }
    )
    session = SimpleNamespace(store_identity=lambda: store, list_notifications=list)
    inventory = ch.v1_prepare_ordinary_cleanup_stage(session)
    ch.STATE_PATH.unlink()
    assert ch.v1_prepare_ordinary_cleanup_stage(session) == inventory


@pytest.mark.parametrize("bad_inventory", [None, {}, {"items": [None]}])
def test_v1_cleanup_stage_rejects_invalid_saved_inventory(
    monkeypatch, tmp_path, bad_inventory
):
    store = str(uuid.uuid4())
    (tmp_path / "v1-enabled").write_text("enabled")
    marker = tmp_path / "v1-notification-backend"
    marker.write_text("preparing-owned")
    marker.chmod(0o600)
    journal = tmp_path / "v1-ordinary-cleanup.json"
    if isinstance(bad_inventory, dict) and "items" in bad_inventory:
        bad_inventory["store_id"] = store
    journal.write_text(json.dumps(bad_inventory))
    journal.chmod(0o600)
    session = SimpleNamespace(store_identity=lambda: store, list_notifications=list)
    with pytest.raises(ValueError):
        ch.v1_prepare_ordinary_cleanup_stage(session)
    assert marker.read_text() == "preparing-owned"


def test_v1_cleanup_stage_rejects_unidentified_legacy_notification(
    monkeypatch, tmp_path
):
    (tmp_path / "v1-enabled").write_text("enabled")
    ch.save_state({"v1": {"destinations": {}}})
    session = SimpleNamespace(
        store_identity=lambda: str(uuid.uuid4()),
        list_notifications=lambda: [
            {
                "id": str(uuid.uuid4()),
                "subtitle": ch.NOTIFICATION_SUBTITLE,
                "is_read": False,
            }
        ],
    )
    with pytest.raises(ValueError, match="unidentified legacy notification"):
        ch.v1_prepare_ordinary_cleanup_stage(session)
    assert not (tmp_path / "v1-notification-backend").exists()


def test_v1_cleanup_stage_does_not_adopt_another_producers_notification(
    monkeypatch, tmp_path
):
    (tmp_path / "v1-enabled").write_text("enabled")
    store, producer = str(uuid.uuid4()), str(uuid.uuid4())
    ch.save_state({"v1": {"destinations": {}}})
    session = SimpleNamespace(
        store_identity=lambda: store,
        list_notifications=lambda: [
            {
                "id": str(uuid.uuid4()),
                "subtitle": ch.NOTIFICATION_SUBTITLE,
                "is_read": False,
                "owned_producer_id": str(uuid.uuid4()),
            }
        ],
    )
    with pytest.raises(ValueError, match="unidentified legacy notification"):
        ch.v1_prepare_ordinary_cleanup_stage(session, producer=producer)
    assert not (tmp_path / "v1-notification-backend").exists()


def test_v1_cleanup_stage_drains_and_retries_from_durable_inventory(
    monkeypatch, tmp_path
):
    (tmp_path / "v1-enabled").write_text("enabled")
    store, workspace, surface = (str(uuid.uuid4()) for _ in range(3))
    ch.save_state(
        {
            "v1": {
                "destinations": {
                    ch.v1_destination_key(workspace, surface): {
                        "notification_id": "owned",
                        "store_id": store,
                    }
                }
            }
        }
    )
    notice = {
        "id": "owned",
        "workspace_id": workspace,
        "surface_id": surface,
        "is_read": False,
    }
    attempts = []

    def mark(_notice_id):
        attempts.append(True)
        if len(attempts) == 1:
            return False
        notice["is_read"] = True
        return True

    session = SimpleNamespace(
        store_identity=lambda: store,
        list_notifications=lambda: [notice],
        mark_notification_read=mark,
    )
    assert not ch.v1_drain_ordinary_cleanup_stage(session)
    ch.STATE_PATH.unlink()
    assert ch.v1_drain_ordinary_cleanup_stage(session)
    assert notice["is_read"]
    assert (tmp_path / "v1-notification-backend").read_text() == "preparing-owned"


def test_v1_commit_owned_backend_requires_current_receipt(monkeypatch, tmp_path):
    store = str(uuid.uuid4())
    (tmp_path / "v1-enabled").write_text("enabled")
    marker = tmp_path / "v1-notification-backend"
    marker.write_text("preparing-owned")
    marker.chmod(0o600)
    inventory = {"store_id": store, "items": []}
    ch.v1_prepare_ordinary_cleanup(tmp_path / "v1-ordinary-cleanup.json", inventory)
    session = SimpleNamespace(store_identity=lambda: store, list_notifications=list)
    valid = [False]

    @contextmanager
    def verified(_receipt):
        yield valid[0]

    publisher = SimpleNamespace(transport_session=session, validated_receipt=verified)
    with pytest.raises(ValueError, match="publication receipt"):
        ch.v1_commit_owned_backend(session, publisher, {"store_id": store})
    assert marker.read_text() == "preparing-owned"
    valid[0] = True
    assert ch.v1_commit_owned_backend(session, publisher, {"store_id": store})
    assert marker.read_text() == "owned"
    assert ch.v1_commit_owned_backend(session, publisher, {"store_id": store})


def test_v1_owned_transition_orders_prepare_publish_drain_and_commit(
    monkeypatch, tmp_path
):
    producer = str(uuid.uuid4())
    store = str(uuid.uuid4())
    session = object()
    publisher = SimpleNamespace(
        transport_session=session, path=tmp_path / "publisher.json", store_id=store
    )
    verification = SimpleNamespace(scope=lambda: None)
    calls = []
    receipt = {"store_id": store, "producer": producer}
    monkeypatch.setattr(
        ch, "v1_publisher_identity", lambda *args, **kwargs: {"producer": producer}
    )
    monkeypatch.setattr(
        ch,
        "v1_prepare_ordinary_cleanup_stage",
        lambda current, **kwargs: calls.append(("prepare", kwargs["producer"])),
    )
    monkeypatch.setattr(
        ch,
        "v1_drain_ordinary_cleanup_stage",
        lambda current, **kwargs: calls.append(("drain", kwargs["producer"])) or True,
    )
    monkeypatch.setattr(
        ch,
        "v1_publish_owned_once",
        lambda *args, **kwargs: calls.append(("publish", kwargs)) or receipt,
    )
    monkeypatch.setattr(
        ch,
        "v1_commit_owned_backend",
        lambda *args: calls.append(("commit", args[2])) or True,
    )
    assert ch.v1_transition_owned_backend(session, publisher, verification)
    assert calls == [
        ("prepare", producer),
        ("publish", {"require_complete": True, "force": True, "receipt": True}),
        ("drain", producer),
        ("commit", receipt),
    ]
    calls.clear()
    verification.scope = lambda: set()
    assert not ch.v1_transition_owned_backend(session, publisher, verification)
    assert not calls
    verification.scope = lambda: None
    monkeypatch.setattr(ch, "v1_publish_owned_once", lambda *args, **kwargs: None)
    assert not ch.v1_transition_owned_backend(session, publisher, verification)
    assert calls == [("prepare", producer)]


def test_v1_cleanup_stage_rejects_missing_state_before_fencing(monkeypatch, tmp_path):
    (tmp_path / "v1-enabled").write_text("enabled")
    session = SimpleNamespace(
        store_identity=lambda: str(uuid.uuid4()), list_notifications=list
    )
    with pytest.raises(ValueError, match="missing cleanup source state"):
        ch.v1_prepare_ordinary_cleanup_stage(session)
    assert not (tmp_path / "v1-notification-backend").exists()


def test_v1_cleanup_stage_accepts_initialized_state_without_destinations(
    monkeypatch, tmp_path
):
    (tmp_path / "v1-enabled").write_text("enabled")
    ch.save_state({"v1": {"clients": {}, "sources": {}}})
    store = str(uuid.uuid4())
    session = SimpleNamespace(store_identity=lambda: store, list_notifications=list)
    assert ch.v1_prepare_ordinary_cleanup_stage(session) == {
        "store_id": store,
        "items": [],
    }
