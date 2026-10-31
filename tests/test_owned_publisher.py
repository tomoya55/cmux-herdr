import json
import os
import shutil
import socket
import uuid
from contextlib import contextmanager, nullcontext
from types import SimpleNamespace

import pytest
from helpers import short_sock_path

import cmux_herdr as ch


def test_v1_owned_publisher_holds_lease_and_skips_unchanged_state(
    tmp_path, monkeypatch
):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)
    sent = []

    def send(data):
        sent.append(data)
        return {"state": None}

    destination = {
        "workspace_id": str(uuid.uuid4()),
        "surface_id": str(uuid.uuid4()),
        "title": "Waiting",
        "body": "one",
    }
    with ch.v1_owned_publisher(path, store_id, send) as publisher:
        assert publisher is not None
        other_state = tmp_path / "other-state"
        other_state.mkdir()
        monkeypatch.setattr(ch, "state_dir", other_state)
        with ch.v1_owned_publisher(path, store_id, send) as competing:
            assert competing is None
        assert publisher.publish([destination])
        assert not publisher.publish([destination])
    assert [json.loads(data)["action"] for data in sent] == ["register", "set"]
    with pytest.raises(ValueError, match="lease is closed"):
        publisher.publish([destination])


def test_v1_owned_publisher_lease_survives_directory_replacement(tmp_path):
    parent = tmp_path / "publisher"
    parent.mkdir()
    path = parent / "identity.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)
    with ch.v1_owned_publisher(path, store_id, lambda _: {"state": None}) as first:
        assert first is not None
        old = tmp_path / "old-publisher"
        parent.rename(old)
        shutil.copytree(old, parent)
        with ch.v1_owned_publisher(path, store_id, lambda _: {"state": None}) as second:
            assert second is None


def test_v1_owned_publisher_lease_survives_ancestor_symlink_change(tmp_path):
    first_dir = tmp_path / "first"
    first_dir.mkdir()
    (first_dir / "journal").mkdir()
    link = tmp_path / "current"
    link.symlink_to(first_dir, target_is_directory=True)
    path = link / "journal" / "identity.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)
    with ch.v1_owned_publisher(path, store_id, lambda _: {"state": None}) as first:
        assert first is not None
        second_dir = tmp_path / "second"
        shutil.copytree(first_dir, second_dir)
        link.unlink()
        link.symlink_to(second_dir, target_is_directory=True)
        with ch.v1_owned_publisher(path, store_id, lambda _: {"state": None}) as second:
            assert second is None


def test_v1_owned_publisher_excludes_hard_linked_identity(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)
    alias = tmp_path / "alias.json"
    os.link(path, alias)
    with ch.v1_owned_publisher(path, store_id, lambda _: {"state": None}) as first:
        assert first is not None
        with ch.v1_owned_publisher(
            alias, store_id, lambda _: {"state": None}
        ) as second:
            assert second is None


def test_v1_owned_publisher_excludes_copied_identity_at_another_path(tmp_path):
    first_dir = tmp_path / "first"
    first_dir.mkdir()
    first_path = first_dir / "publisher.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(first_path, store_id, create=True)
    with ch.v1_owned_publisher(
        first_path, store_id, lambda _: {"state": None}
    ) as first:
        assert first is not None
        second_dir = tmp_path / "second"
        shutil.copytree(first_dir, second_dir)
        with ch.v1_owned_publisher(
            second_dir / "publisher.json", store_id, lambda _: {"state": None}
        ) as second:
            assert second is None


def test_v1_owned_publisher_recovers_rejected_guard_without_stale_send(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)
    destination = {
        "workspace_id": str(uuid.uuid4()),
        "surface_id": str(uuid.uuid4()),
        "title": "Waiting",
        "body": "",
    }
    actions = []
    new_epoch = str(uuid.uuid4())

    def send(data):
        action = json.loads(data)["action"]
        actions.append(action)
        if action == "resume":
            return {"state": {"epoch": new_epoch, "revision": "0", "destinations": []}}
        return {"state": None}

    def guarded_send(data, guard):
        with guard():
            return send(data)

    @contextmanager
    def stale():
        raise ValueError("stale publication")
        yield

    with (
        ch.v1_owned_publisher(path, store_id, send, send_guarded=guarded_send) as p,
        pytest.raises(ValueError, match="stale publication"),
    ):
        p.publish([destination], send_guard=stale)
    with ch.v1_owned_publisher(path, store_id, send) as p:
        assert not p.publish([])
    assert actions == ["register", "resume"]


def test_v1_source_has_single_leader_and_recovers_after_exit(monkeypatch, tmp_path):
    with ch.v1_source_leader("remote:server:session") as first:
        assert first is True
        with ch.v1_source_leader("remote:server:session") as second:
            assert second is False
        with ch.v1_source_leader("remote:other:session") as other:
            assert other is True
    with ch.v1_source_leader("remote:server:session") as replacement:
        assert replacement is True


def test_v1_cleanup_stage_lists_after_publication_is_fenced(monkeypatch, tmp_path):
    (tmp_path / "v1-enabled").write_text("enabled")
    ch.save_state({"v1": {"destinations": {}}})
    store = str(uuid.uuid4())

    def list_notifications():
        with ch.v1_source_leader("publication") as leader:
            assert not leader
        return []

    session = SimpleNamespace(
        store_identity=lambda: store, list_notifications=list_notifications
    )
    assert ch.v1_prepare_ordinary_cleanup_stage(session) == {
        "store_id": store,
        "items": [],
    }


def test_v1_owned_publisher_rejects_directory_change_during_recovery(tmp_path):
    parent = tmp_path / "publisher"
    parent.mkdir()
    path = parent / "identity.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)

    def send(_request):
        parent.rename(tmp_path / "moved")
        parent.mkdir()
        return {"state": None}

    with (
        pytest.raises(ValueError, match="lease directory changed"),
        ch.v1_owned_publisher(path, store_id, send),
    ):
        pass


def test_v1_owned_publisher_uses_private_lease_directory(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)
    with ch.v1_owned_publisher(path, store_id, lambda _: {"state": None}):
        directory = tmp_path / ".cmux-herdr-publisher-locks"
        assert directory.stat().st_mode & 0o777 == 0o700
        assert list(directory.glob("*.lock"))


def test_v1_owned_publisher_resolves_symlink_before_parent_components(tmp_path):
    target = tmp_path / "target"
    child = target / "child"
    child.mkdir(parents=True)
    (tmp_path / "link").symlink_to(child, target_is_directory=True)
    path = tmp_path / "link" / ".." / "publisher.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)
    with ch.v1_owned_publisher(path, store_id, lambda _: {"state": None}) as publisher:
        assert publisher is not None
        assert publisher.path == target / "publisher.json"


def test_v1_owned_publisher_rejects_identity_replaced_before_recovery(
    tmp_path, monkeypatch
):
    path = tmp_path / "publisher.json"
    other = tmp_path / "other.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)
    ch.v1_publisher_identity(other, store_id, create=True)
    recover = ch.v1_publisher_recover
    sent = []

    def swap_then_recover(*args):
        os.replace(other, path)
        return recover(*args)

    monkeypatch.setattr(ch, "v1_publisher_recover", swap_then_recover)
    with (
        pytest.raises(ValueError, match="publisher identity changed"),
        ch.v1_owned_publisher(path, store_id, lambda data: sent.append(data)),
    ):
        pass
    assert not sent


def test_v1_owned_publisher_rejects_public_lease_directory(tmp_path):
    directory = tmp_path / ".cmux-herdr-publisher-locks"
    directory.mkdir(mode=0o755)
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)
    with (
        pytest.raises(ValueError, match="invalid publisher lease directory"),
        ch.v1_owned_publisher(path, store_id, lambda _: {"state": None}),
    ):
        pass


def test_v1_owned_publisher_rejects_reentrant_publish(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)
    holder = []
    rejections = []

    def send(request):
        if json.loads(request)["action"] == "set":
            with pytest.raises(ValueError, match="already publishing"):
                holder[0].publish([])
            rejections.append(True)
        return {"state": None}

    destination = {
        "workspace_id": str(uuid.uuid4()),
        "surface_id": str(uuid.uuid4()),
        "title": "Waiting",
        "body": "",
    }
    with ch.v1_owned_publisher(path, store_id, send) as publisher:
        holder.append(publisher)
        publisher.publish([destination])
    assert rejections == [True]


def test_v1_owned_publisher_uses_guarded_send_for_updates(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)
    destination = {
        "workspace_id": str(uuid.uuid4()),
        "surface_id": str(uuid.uuid4()),
        "title": "Waiting",
        "body": "",
    }
    calls = []

    def send(data):
        calls.append(("plain", json.loads(data)["action"]))
        return {"state": None}

    def send_guarded(data, guard):
        with guard():
            calls.append(("guarded", json.loads(data)["action"]))
        return {"state": None}

    @contextmanager
    def guard():
        calls.append(("guard", "enter"))
        try:
            yield
        finally:
            calls.append(("guard", "exit"))

    with ch.v1_owned_publisher(path, store_id, send, send_guarded=send_guarded) as p:
        assert p.publish([destination], send_guard=guard)
    assert calls == [
        ("plain", "register"),
        ("guard", "enter"),
        ("guarded", "set"),
        ("guard", "exit"),
    ]


def test_v1_owned_publisher_serializes_baseline_and_publication(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)

    def send(_data):
        return {"state": None}

    with ch.v1_owned_publisher(path, store_id, send, send_guarded=send) as publisher:

        def prepare():
            assert publisher.current_destinations() == []
            with pytest.raises(ValueError, match="already publishing"):
                publisher.publish([])
            return [], nullcontext

        assert publisher.publish_verified(prepare)


def test_v1_owned_publisher_can_confirm_unchanged_initial_state(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)
    actions = []

    def send(data):
        actions.append(json.loads(data)["action"])
        return {"state": None}

    with ch.v1_owned_publisher(path, store_id, send) as publisher:
        assert not publisher.publish([])
        assert publisher.publish([], force=True)
    assert actions == ["register", "set"]
    assert ch.v1_publisher_identity(path, store_id, create=False)["revision"] == "1"


def test_v1_owned_publisher_returns_durable_initial_publication_receipt(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    identity = ch.v1_publisher_identity(path, store_id, create=True)

    def send(_data):
        return {"state": None}

    def guarded(data, guard):
        with guard():
            return send(data)

    with ch.v1_owned_publisher(path, store_id, send, send_guarded=guarded) as p:
        receipt = p.publish_verified(
            lambda: ([], nullcontext), force=True, receipt=True
        )
        assert receipt == {
            "store_id": store_id,
            "producer": identity["producer"],
            "epoch": p.epoch,
            "revision": "1",
            "destinations": [],
        }
        assert p.receipt_is_current(receipt)
        assert p.publish([], force=True)
        assert not p.receipt_is_current(receipt)
    assert "capability" not in receipt


def test_v1_owned_verified_publication_recovers_pending_in_same_lease(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)
    destination = {
        "workspace_id": str(uuid.uuid4()),
        "surface_id": str(uuid.uuid4()),
        "title": "Waiting",
        "body": "",
    }
    actions = []
    new_epoch = str(uuid.uuid4())

    def send(data):
        action = json.loads(data)["action"]
        actions.append(action)
        if action == "resume":
            return {"state": {"epoch": new_epoch, "revision": "0", "destinations": []}}
        return {"state": None}

    def guarded(data, guard):
        with guard():
            return send(data)

    @contextmanager
    def stale():
        raise ValueError("stale publication")
        yield

    with ch.v1_owned_publisher(path, store_id, send, send_guarded=guarded) as p:
        with pytest.raises(ValueError, match="stale publication"):
            p.publish([destination], send_guard=stale)
        assert p.publish_verified(lambda: (p.current_destinations(), nullcontext))
    assert actions == ["register", "resume"]


def test_v1_owned_publisher_fences_uncertain_set_before_new_state(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)
    destination = {
        "workspace_id": str(uuid.uuid4()),
        "surface_id": str(uuid.uuid4()),
        "title": "Waiting",
        "body": "",
    }
    new_epoch = str(uuid.uuid4())
    actions = []

    def send(data):
        request = json.loads(data)
        actions.append(request["action"])
        if request["action"] == "set" and actions.count("set") == 1:
            raise OSError("response lost")
        if request["action"] == "resume":
            return {"state": {"epoch": new_epoch, "revision": "0", "destinations": []}}
        return {"state": None}

    with ch.v1_owned_publisher(path, store_id, send) as publisher:
        with pytest.raises(OSError, match="response lost"):
            publisher.publish([destination])
        assert not publisher.publish([])
    assert actions == ["register", "set", "resume"]


def test_v1_owned_publisher_reconciles_uncertain_committed_set(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)
    destination = {
        "workspace_id": str(uuid.uuid4()),
        "surface_id": str(uuid.uuid4()),
        "title": "Waiting",
        "body": "",
    }
    new_epoch = str(uuid.uuid4())
    requests = []

    def send(data):
        request = json.loads(data)
        requests.append(request)
        if (
            request["action"] == "set"
            and sum(item["action"] == "set" for item in requests) == 1
        ):
            raise OSError("response lost after commit")
        if request["action"] == "resume":
            return {
                "state": {
                    "epoch": new_epoch,
                    "revision": "0",
                    "destinations": [destination],
                }
            }
        return {"state": None}

    with ch.v1_owned_publisher(path, store_id, send) as publisher:
        with pytest.raises(OSError, match="response lost"):
            publisher.publish([destination])
        assert publisher.publish([])
    assert [request["action"] for request in requests] == [
        "register",
        "set",
        "resume",
        "set",
    ]
    assert requests[-1]["epoch"] == new_epoch
    assert requests[-1]["destinations"] == []


def test_v1_owned_publisher_fences_denied_update_and_can_publish_again(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)
    destination = {
        "workspace_id": str(uuid.uuid4()),
        "surface_id": str(uuid.uuid4()),
        "title": "Waiting",
        "body": "",
    }
    new_epoch = str(uuid.uuid4())
    actions = []

    def send(data):
        action = json.loads(data)["action"]
        actions.append(action)
        if action == "set" and actions.count("set") == 1:
            raise ch.OwnedSocketError("target_denied")
        if action == "resume":
            return {"state": {"epoch": new_epoch, "revision": "0", "destinations": []}}
        return {"state": None}

    with ch.v1_owned_publisher(path, store_id, send) as publisher:
        with pytest.raises(ch.OwnedSocketError, match="target_denied"):
            publisher.publish([destination])
        assert publisher.epoch == new_epoch
        assert publisher.publish([destination])
    assert actions == ["register", "set", "resume", "set"]


def test_v1_owned_publisher_retries_interrupted_fence_without_new_request(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)
    destination = {
        "workspace_id": str(uuid.uuid4()),
        "surface_id": str(uuid.uuid4()),
        "title": "Waiting",
        "body": "",
    }
    epoch = str(uuid.uuid4())
    requests = []

    def send(data):
        requests.append(data)
        action = json.loads(data)["action"]
        if action == "set" and len(requests) == 2:
            raise ch.OwnedSocketError("target_denied")
        if action == "resume":
            if len(requests) == 3:
                raise TimeoutError("response lost")
            return {"state": {"epoch": epoch, "revision": "0", "destinations": []}}
        return {"state": None}

    with ch.v1_owned_publisher(path, store_id, send) as publisher:
        with pytest.raises(TimeoutError):
            publisher.publish([destination])
        assert publisher.publish([destination])
    assert requests[2] == requests[3]
    assert [json.loads(data)["action"] for data in requests] == [
        "register",
        "set",
        "resume",
        "resume",
        "set",
    ]


def test_v1_owned_publisher_recovers_after_resume_journal_sync_failure(
    tmp_path, monkeypatch
):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)
    destination = {
        "workspace_id": str(uuid.uuid4()),
        "surface_id": str(uuid.uuid4()),
        "title": "Waiting",
        "body": "",
    }
    epoch = str(uuid.uuid4())
    calls = []
    write = ch._v1_write_publisher_journal

    def fail_after_resume_write(*args):
        write(*args)
        journal = args[3]
        if journal["epoch"] == epoch and not calls:
            calls.append(True)
            raise OSError("simulated sync failure")

    def send(data):
        action = json.loads(data)["action"]
        if action == "set" and not calls:
            raise ch.OwnedSocketError("target_denied")
        if action == "resume":
            return {
                "state": {"epoch": epoch.upper(), "revision": "0", "destinations": []}
            }
        return {"state": None}

    with ch.v1_owned_publisher(path, store_id, send) as publisher:
        monkeypatch.setattr(ch, "_v1_write_publisher_journal", fail_after_resume_write)
        with pytest.raises(OSError, match="simulated sync failure"):
            publisher.publish([destination])
        assert publisher.publish([destination])


def test_v1_owned_publisher_retries_resume_ack_before_journal_write(
    tmp_path, monkeypatch
):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)
    destination = {
        "workspace_id": str(uuid.uuid4()),
        "surface_id": str(uuid.uuid4()),
        "title": "Waiting",
        "body": "",
    }
    epoch = str(uuid.uuid4())
    failed = []
    write = ch._v1_write_publisher_journal

    def fail_before_resume_write(*args):
        if args[3]["epoch"] == epoch and not failed:
            failed.append(True)
            raise OSError("simulated pre-write failure")
        write(*args)

    def send(data):
        action = json.loads(data)["action"]
        if action == "set" and not failed:
            raise ch.OwnedSocketError("target_denied")
        if action == "resume":
            return {"state": {"epoch": epoch, "revision": "0", "destinations": []}}
        return {"state": None}

    with ch.v1_owned_publisher(path, store_id, send) as publisher:
        monkeypatch.setattr(ch, "_v1_write_publisher_journal", fail_before_resume_write)
        with pytest.raises(OSError, match="simulated pre-write failure"):
            publisher.publish([destination])
        assert publisher.publish([destination])


def test_v1_owned_publisher_retries_invalid_resume_response(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)
    target = {
        "workspace_id": str(uuid.uuid4()),
        "surface_id": str(uuid.uuid4()),
        "title": "Waiting",
        "body": "",
    }
    attempts = []
    epoch = str(uuid.uuid4())

    def send(data):
        action = json.loads(data)["action"]
        if action == "set" and not attempts:
            raise ch.OwnedSocketError("target_denied")
        if action == "resume":
            attempts.append(data)
            if len(attempts) == 1:
                return {"state": None}
            return {"state": {"epoch": epoch, "revision": "0", "destinations": []}}
        return {"state": None}

    with ch.v1_owned_publisher(path, store_id, send) as publisher:
        with pytest.raises(ValueError, match="invalid resumed state"):
            publisher.publish([target])
        assert publisher.publish([target])
    assert attempts[0] == attempts[1]


def test_v1_owned_publication_allows_state_update_during_journal_write(
    tmp_path, monkeypatch
):
    import threading

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
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)
    sent, updated = [], threading.Event()
    write_journal = ch._v1_write_publisher_journal

    def send(data):
        sent.append(json.loads(data)["action"])
        return {"state": None}

    def guarded_send(data, guard):
        with guard():
            return send(data)

    def update():
        with ch.state_lock():
            state["v1"]["sources"]["a"]["attention"] = {}
            ch.save_state(state)
        updated.set()

    def slow_write(*args):
        journal = args[3]
        if journal["pending"] and json.loads(journal["pending"])["action"] == "set":
            worker = threading.Thread(target=update)
            worker.start()
            assert updated.wait(2)
            worker.join(2)
        return write_journal(*args)

    with ch.v1_owned_publisher(path, store_id, send, send_guarded=guarded_send) as p:
        monkeypatch.setattr(ch, "_v1_write_publisher_journal", slow_write)
        with pytest.raises(ValueError, match="stale publication"):
            ch.v1_publish_owned_once(p, SimpleNamespace(scope=lambda: None))
    assert sent == ["register"]


def test_v1_owned_publication_sends_aggregate_over_local_socket(tmp_path, monkeypatch):
    import threading

    workspace, surface = str(uuid.uuid4()), str(uuid.uuid4())
    ch.save_state(
        {
            "sessions": {},
            "v1": {
                "clients": {
                    "client": {
                        "workspace": workspace,
                        "surface": surface,
                        "source": "a",
                    }
                },
                "sources": {"a": {"attention": {"p": {"status": "blocked"}}}},
            },
        }
    )
    store_id = str(uuid.uuid4())
    identity_path = tmp_path / "publisher.json"
    sock_path = short_sock_path(uuid.uuid4().hex[:8])
    ready = threading.Event()
    received = []

    def serve():
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(sock_path))
            server.listen(1)
            ready.set()
            conn, _ = server.accept()
            with conn, conn.makefile("rb") as stream:
                for _ in range(3):
                    request = json.loads(stream.readline())
                    received.append(request)
                    result = (
                        {"store_id": store_id}
                        if request["method"] == "notification.state.info"
                        else {"state": None}
                    )
                    conn.sendall(
                        json.dumps(
                            {"id": request["id"], "ok": True, "result": result}
                        ).encode()
                        + b"\n"
                    )

    worker = threading.Thread(target=serve, daemon=True)
    worker.start()
    assert ready.wait(5)
    try:
        with ch.V1OwnedSocketSession(sock_path) as session:
            assert session.store_identity() == store_id
            ch.v1_publisher_identity(identity_path, store_id, create=True)
            with ch.v1_owned_publisher(
                identity_path,
                store_id,
                session.request,
                send_guarded=session.request_with_send_guard,
            ) as publisher:
                assert ch.v1_publish_owned_once(
                    publisher, SimpleNamespace(scope=lambda: None)
                )
        worker.join(5)
        assert [item["method"] for item in received] == [
            "notification.state.info",
            "notification.state.command",
            "notification.state.command",
        ]
        assert [item["params"]["action"] for item in received[1:]] == [
            "register",
            "set",
        ]
        assert received[2]["params"]["destinations"][0]["title"] == "herdr: 1 waiting"
        identity = ch.v1_publisher_identity(identity_path, store_id, create=False)
        assert identity["pending"] is None
    finally:
        sock_path.unlink(missing_ok=True)


def test_v1_owned_publication_releases_state_lock_before_response(
    tmp_path, monkeypatch
):
    import threading

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
    store_id = str(uuid.uuid4())
    path = tmp_path / "publisher.json"
    sock_path = short_sock_path(uuid.uuid4().hex[:8])
    ready = threading.Event()
    updated = threading.Event()

    def serve():
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(sock_path))
            server.listen(1)
            ready.set()
            conn, _ = server.accept()
            with conn, conn.makefile("rb") as stream:
                for index in range(2):
                    request = json.loads(stream.readline())
                    if index == 1:
                        with ch.state_lock():
                            state["v1"]["sources"]["a"]["attention"] = {}
                            ch.save_state(state)
                        updated.set()
                    conn.sendall(
                        json.dumps(
                            {"id": request["id"], "ok": True, "result": {"state": None}}
                        ).encode()
                        + b"\n"
                    )

    worker = threading.Thread(target=serve, daemon=True)
    worker.start()
    assert ready.wait(5)
    try:
        ch.v1_publisher_identity(path, store_id, create=True)
        with (
            ch.V1OwnedSocketSession(sock_path, timeout=2) as session,
            ch.v1_owned_publisher(
                path,
                store_id,
                session.request,
                send_guarded=session.request_with_send_guard,
            ) as publisher,
        ):
            assert ch.v1_publish_owned_once(
                publisher, SimpleNamespace(scope=lambda: None)
            )
        worker.join(5)
        assert updated.is_set()
    finally:
        sock_path.unlink(missing_ok=True)
