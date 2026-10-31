import json
import os
import socket
import subprocess
import sys
import uuid

import pytest
from helpers import short_sock_path

import cmux_herdr as ch


def _publisher_prepare_set(path, store_id, destinations):
    epoch = ch.v1_publisher_identity(path, store_id, create=False)["epoch"]
    return ch.v1_publisher_prepare_set(
        path, store_id, destinations, expected_epoch=epoch
    )


def test_v1_publisher_identity_is_pinned_and_requires_explicit_creation(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    with pytest.raises(FileNotFoundError):
        ch.v1_publisher_identity(path, store_id, create=False)
    identity = ch.v1_publisher_identity(path, store_id, create=True)
    assert identity["phase"] == "bootstrap"
    assert identity["revision"] == "0"
    assert identity["destinations"] == []
    assert identity["pending"] is None
    assert ch.v1_publisher_identity(path, store_id, create=False) == identity
    assert path.stat().st_mode & 0o077 == 0
    with pytest.raises(ValueError, match="store"):
        ch.v1_publisher_identity(path, str(uuid.uuid4()), create=False)
    assert ch.v1_publisher_identity(path, store_id, create=False) == identity


def test_v1_publisher_identity_rejects_insecure_and_symlinked_files(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)
    path.chmod(0o644)
    with pytest.raises(ValueError, match="identity file"):
        ch.v1_publisher_identity(path, store_id, create=False)
    path.chmod(0o600)
    link = tmp_path / "link.json"
    link.symlink_to(path)
    with pytest.raises(OSError):
        ch.v1_publisher_identity(link, store_id, create=False)


def test_v1_publisher_identity_rejects_invalid_field_types(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    identity = ch.v1_publisher_identity(path, store_id, create=True)
    identity["producer"] = 123
    path.write_text(json.dumps(identity))
    with pytest.raises(ValueError, match="identity"):
        ch.v1_publisher_identity(path, store_id, create=False)


@pytest.mark.parametrize(
    "field,value", [("destinations", [42]), ("pending", "not JSON")]
)
def test_v1_publisher_identity_rejects_corrupt_journal(tmp_path, field, value):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    identity = ch.v1_publisher_identity(path, store_id, create=True)
    identity[field] = value
    path.write_text(json.dumps(identity))
    with pytest.raises(ValueError):
        ch.v1_publisher_identity(path, store_id, create=False)


def test_v1_publisher_identity_rejects_oversized_pending_bytes(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    identity = ch.v1_publisher_identity(path, store_id, create=True)
    registration = ch.v1_publisher_prepare_register(path, store_id)
    ch.v1_publisher_ack_register(path, store_id, registration)
    pending = _publisher_prepare_set(path, store_id, [])
    identity = ch.v1_publisher_identity(path, store_id, create=False)
    identity["pending"] = pending.decode() + " " * (3 * 1024 * 1024)
    path.write_text(json.dumps(identity))
    with pytest.raises(ValueError, match="journal"):
        ch.v1_publisher_identity(path, store_id, create=False)


def test_v1_publisher_identity_synchronizes_directory_on_read(tmp_path, monkeypatch):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)
    original = os.fsync
    synced_directory = []

    def record_sync(fd):
        if os.fstat(fd).st_ino == tmp_path.stat().st_ino:
            synced_directory.append(True)
        return original(fd)

    monkeypatch.setattr(ch.os, "fsync", record_sync)
    ch.v1_publisher_identity(path, store_id, create=False)
    assert synced_directory


def test_v1_publisher_identity_rejects_replaced_parent(tmp_path, monkeypatch):
    parent = tmp_path / "publisher"
    parent.mkdir()
    path = parent / "identity.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)
    original_read = os.read
    moved = tmp_path / "moved"

    def swap_parent(fd, size):
        data = original_read(fd, size)
        parent.rename(moved)
        parent.mkdir()
        return data

    monkeypatch.setattr(ch.os, "read", swap_parent)
    with pytest.raises((ValueError, OSError)):
        ch.v1_publisher_identity(path, store_id, create=False)


def test_v1_publisher_identity_survives_a_new_process(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    original = ch.v1_publisher_identity(path, store_id, create=True)
    script = (
        "import cmux_herdr as c, json, sys; "
        "print(json.dumps(c.v1_publisher_identity(sys.argv[1], sys.argv[2], "
        "create=False)))"
    )
    result = subprocess.run(
        [sys.executable, "-c", script, str(path), store_id],
        capture_output=True,
        text=True,
        check=True,
        timeout=5,
    )
    assert json.loads(result.stdout) == original


def test_v1_publisher_identity_interrupted_before_publication(tmp_path, monkeypatch):
    path = tmp_path / "publisher.json"

    def fail_link(*args, **kwargs):
        raise OSError("injected publication failure")

    monkeypatch.setattr(ch.os, "link", fail_link)
    with pytest.raises(OSError, match="injected"):
        ch.v1_publisher_identity(path, str(uuid.uuid4()), create=True)
    assert not path.exists()
    assert not list(tmp_path.glob(".publisher-*"))


def test_v1_publisher_identity_never_overwrites_existing_file(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    original = ch.v1_publisher_identity(path, store_id, create=True)
    with pytest.raises(FileExistsError):
        ch.v1_publisher_identity(path, store_id, create=True)
    assert ch.v1_publisher_identity(path, store_id, create=False) == original


def test_v1_publisher_identity_concurrent_creation_has_one_winner(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    script = (
        "import cmux_herdr as c, json, sys; "
        "print(json.dumps(c.v1_publisher_identity(sys.argv[1], sys.argv[2], "
        "create=True)))"
    )
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", script, str(path), store_id],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(2)
    ]
    try:
        results = [process.communicate(timeout=5) for process in processes]
        assert sorted(process.returncode == 0 for process in processes) == [False, True]
        winner = next(
            out
            for process, (out, _) in zip(processes, results, strict=True)
            if process.returncode == 0
        )
        assert json.loads(winner) == ch.v1_publisher_identity(
            path, store_id, create=False
        )
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait()


def test_v1_owned_request_uses_pinned_identity_and_decimal_revision(tmp_path):
    store_id = str(uuid.uuid4())
    identity = ch.v1_publisher_identity(
        tmp_path / "publisher.json", store_id, create=True
    )
    destination = {
        "workspace_id": str(uuid.uuid4()),
        "surface_id": str(uuid.uuid4()),
        "title": "Waiting",
        "body": "One agent needs input",
    }
    request = ch.v1_owned_set_request(identity, 2**64 - 1, [destination])
    assert request == {
        **{
            key: identity[key]
            for key in ("store_id", "producer", "epoch", "capability")
        },
        "action": "set",
        "revision": "18446744073709551615",
        "destinations": [destination],
    }
    with pytest.raises(ValueError):
        ch.v1_owned_set_request(identity, 2**64, [destination])


def test_v1_owned_request_rejects_duplicate_and_oversized_destinations(tmp_path):
    identity = ch.v1_publisher_identity(
        tmp_path / "publisher.json", str(uuid.uuid4()), create=True
    )
    destination = {
        "workspace_id": str(uuid.uuid4()),
        "surface_id": str(uuid.uuid4()),
        "title": "Waiting",
        "body": "",
    }
    duplicate = {**destination, "workspace_id": destination["workspace_id"].upper()}
    with pytest.raises(ValueError):
        ch.v1_owned_set_request(identity, 1, [destination, duplicate])
    with pytest.raises(ValueError):
        ch.v1_owned_set_request(identity, 1, [{**destination, "body": "x" * 8193}])
    request = ch.v1_owned_set_request(identity, 1, [destination])
    destination["body"] = "changed"
    assert request["destinations"][0]["body"] == ""


def test_v1_owned_request_normalizes_uuid_and_limits_encoded_size(tmp_path):
    identity = ch.v1_publisher_identity(
        tmp_path / "publisher.json", str(uuid.uuid4()), create=True
    )
    workspace = uuid.uuid4()
    surface = uuid.uuid4()
    destination = {
        "workspace_id": workspace.hex,
        "surface_id": surface.hex,
        "title": "Waiting",
        "body": "",
    }
    request = ch.v1_owned_set_request(identity, 1, [destination])
    assert request["destinations"][0]["workspace_id"] == str(workspace)
    assert request["destinations"][0]["surface_id"] == str(surface)
    large = [
        {**destination, "surface_id": str(uuid.uuid4()), "body": "\0" * 8192}
        for _ in range(100)
    ]
    with pytest.raises(ValueError, match="size"):
        ch.v1_owned_set_request(identity, 1, large)


def test_v1_publisher_replays_persisted_request_before_new_state(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    identity = ch.v1_publisher_identity(path, store_id, create=True)
    identity["phase"] = "active"
    path.write_text(json.dumps(identity))
    destination = {
        "workspace_id": str(uuid.uuid4()),
        "surface_id": str(uuid.uuid4()),
        "title": "Waiting",
        "body": "one",
    }
    pending = _publisher_prepare_set(path, store_id, [destination])
    destination["body"] = "two"
    retry = _publisher_prepare_set(path, store_id, [destination])
    assert retry == pending
    assert json.loads(retry)["destinations"][0]["body"] == "one"
    journal = ch.v1_publisher_identity(path, store_id, create=False)
    assert journal["revision"] == "0"
    assert journal["pending"].encode("utf-8") == pending


def test_v1_publisher_replays_pending_request_after_process_restart(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)
    registration = ch.v1_publisher_prepare_register(path, store_id)
    ch.v1_publisher_ack_register(path, store_id, registration)
    destination = {
        "workspace_id": str(uuid.uuid4()),
        "surface_id": str(uuid.uuid4()),
        "title": "Waiting",
        "body": "before restart",
    }
    pending = _publisher_prepare_set(path, store_id, [destination])
    script = (
        "import cmux_herdr as c, sys; "
        "sys.stdout.buffer.write(c.v1_publisher_prepare_set(sys.argv[1], "
        "sys.argv[2], [], expected_epoch=sys.argv[3]))"
    )
    epoch = ch.v1_publisher_identity(path, store_id, create=False)["epoch"]
    result = subprocess.run(
        [sys.executable, "-c", script, str(path), store_id, epoch],
        capture_output=True,
        check=True,
        timeout=5,
    )
    assert result.stdout == pending


def test_v1_publisher_acknowledges_only_the_exact_pending_request(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    identity = ch.v1_publisher_identity(path, store_id, create=True)
    identity["phase"] = "active"
    path.write_text(json.dumps(identity))
    destination = {
        "workspace_id": str(uuid.uuid4()),
        "surface_id": str(uuid.uuid4()),
        "title": "Waiting",
        "body": "one",
    }
    pending = _publisher_prepare_set(path, store_id, [destination])
    with pytest.raises(ValueError, match="pending"):
        ch.v1_publisher_ack_set(path, store_id, b"different")
    assert ch.v1_publisher_identity(path, store_id, create=False)["pending"] is not None
    ch.v1_publisher_ack_set(path, store_id, pending)
    journal = ch.v1_publisher_identity(path, store_id, create=False)
    assert journal["revision"] == "1"
    assert journal["destinations"] == [destination]
    assert journal["pending"] is None
    next_request = _publisher_prepare_set(path, store_id, [])
    assert json.loads(next_request)["revision"] == "2"


def test_v1_publisher_bootstrap_retries_registration_before_activation(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)
    pending = ch.v1_publisher_prepare_register(path, store_id)
    assert json.loads(pending)["action"] == "register"
    assert ch.v1_publisher_prepare_register(path, store_id) == pending
    with pytest.raises(ValueError, match="pending"):
        ch.v1_publisher_ack_register(path, store_id, b"different")
    ch.v1_publisher_ack_register(path, store_id, pending)
    journal = ch.v1_publisher_identity(path, store_id, create=False)
    assert journal["phase"] == "active"
    assert journal["pending"] is None
    first_update = _publisher_prepare_set(path, store_id, [])
    assert json.loads(first_update)["revision"] == "1"


def test_v1_publisher_resume_retries_same_request_and_persists_new_epoch(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    identity = ch.v1_publisher_identity(path, store_id, create=True)
    registration = ch.v1_publisher_prepare_register(path, store_id)
    ch.v1_publisher_ack_register(path, store_id, registration)
    pending = ch.v1_publisher_prepare_resume(
        path, store_id, expected_epoch=identity["epoch"]
    )
    assert (
        ch.v1_publisher_prepare_resume(path, store_id, expected_epoch=identity["epoch"])
        == pending
    )
    assert json.loads(pending)["expected_epoch"] == identity["epoch"]
    new_epoch = str(uuid.uuid4())
    with pytest.raises(ValueError, match="resumed state"):
        ch.v1_publisher_ack_resume(
            path,
            store_id,
            pending,
            {"epoch": new_epoch, "revision": "1", "destinations": []},
        )
    assert ch.v1_publisher_identity(path, store_id, create=False)["pending"] is not None
    ch.v1_publisher_ack_resume(
        path,
        store_id,
        pending,
        {"epoch": new_epoch, "revision": "0", "destinations": []},
    )
    journal = ch.v1_publisher_identity(path, store_id, create=False)
    assert journal["epoch"] == new_epoch
    assert journal["revision"] == "0"
    assert journal["pending"] is None
    with pytest.raises(ValueError, match="writer epoch"):
        ch.v1_publisher_prepare_set(
            path, store_id, [], expected_epoch=identity["epoch"]
        )
    with pytest.raises(ValueError, match="writer epoch"):
        ch.v1_publisher_prepare_resume(path, store_id, expected_epoch=identity["epoch"])


def test_v1_publisher_fences_pending_set_before_writer_takeover(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    identity = ch.v1_publisher_identity(path, store_id, create=True)
    registration = ch.v1_publisher_prepare_register(path, store_id)
    ch.v1_publisher_ack_register(path, store_id, registration)
    saved_set = ch.v1_publisher_prepare_set(
        path, store_id, [], expected_epoch=identity["epoch"]
    )
    sent = []
    new_epoch = str(uuid.uuid4())

    def send(request):
        sent.append(request)
        if json.loads(request)["action"] == "set":
            return {"state": None}
        return {"state": {"epoch": new_epoch, "revision": "0", "destinations": []}}

    assert ch.v1_publisher_recover(path, store_id, send) == new_epoch
    assert saved_set not in sent
    assert [json.loads(item)["action"] for item in sent] == ["resume"]
    journal = ch.v1_publisher_identity(path, store_id, create=False)
    assert journal["epoch"] == new_epoch
    assert journal["pending"] is None


def test_v1_publisher_fences_rejected_pending_set_before_recovery(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    identity = ch.v1_publisher_identity(path, store_id, create=True)
    registration = ch.v1_publisher_prepare_register(path, store_id)
    ch.v1_publisher_ack_register(path, store_id, registration)
    ch.v1_publisher_prepare_set(path, store_id, [], expected_epoch=identity["epoch"])
    new_epoch = str(uuid.uuid4())
    sent = []

    def send(request):
        sent.append(request)
        if json.loads(request)["action"] == "set":
            raise ch.OwnedSocketError("target_denied")
        return {"state": {"epoch": new_epoch, "revision": "0", "destinations": []}}

    assert ch.v1_publisher_recover(path, store_id, send) == new_epoch
    assert [json.loads(item)["action"] for item in sent] == ["resume"]
    journal = ch.v1_publisher_identity(path, store_id, create=False)
    assert journal["epoch"] == new_epoch
    assert journal["pending"] is None


def test_v1_publisher_retries_fence_after_lost_response(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    identity = ch.v1_publisher_identity(path, store_id, create=True)
    ch.v1_publisher_ack_register(
        path, store_id, ch.v1_publisher_prepare_register(path, store_id)
    )
    ch.v1_publisher_prepare_set(path, store_id, [], expected_epoch=identity["epoch"])
    sent = []

    def interrupted(request):
        sent.append(request)
        if json.loads(request)["action"] == "set":
            raise ch.OwnedSocketError("target_denied")
        raise TimeoutError("response lost")

    with pytest.raises(TimeoutError):
        ch.v1_publisher_recover(path, store_id, interrupted)
    journal = ch.v1_publisher_identity(path, store_id, create=False)
    assert journal["pending"].encode() == sent[0]
    new_epoch = str(uuid.uuid4())

    def retry(request):
        assert request == sent[0]
        return {"state": {"epoch": new_epoch, "revision": "0", "destinations": []}}

    assert ch.v1_publisher_recover(path, store_id, retry) == new_epoch


def test_v1_publisher_recovery_keeps_request_after_lost_response(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    ch.v1_publisher_identity(path, store_id, create=True)

    def lost_response(_request):
        raise TimeoutError("response lost")

    with pytest.raises(TimeoutError):
        ch.v1_publisher_recover(path, store_id, lost_response)
    journal = ch.v1_publisher_identity(path, store_id, create=False)
    assert json.loads(journal["pending"])["action"] == "register"
    sent = []

    def retry(request):
        sent.append(request)
        return {"state": None}

    assert ch.v1_publisher_recover(path, store_id, retry) == journal["epoch"]
    assert sent == [journal["pending"].encode()]


def test_v1_publisher_prepares_large_replacement_after_large_ack(tmp_path):
    path = tmp_path / "publisher.json"
    store_id = str(uuid.uuid4())
    identity = ch.v1_publisher_identity(path, store_id, create=True)
    identity["phase"] = "active"
    path.write_text(json.dumps(identity))
    destinations = [
        {
            "workspace_id": str(uuid.uuid4()),
            "surface_id": str(uuid.uuid4()),
            "title": "Waiting",
            "body": "x" * 8192,
        }
        for _ in range(256)
    ]
    first = _publisher_prepare_set(path, store_id, destinations)
    ch.v1_publisher_ack_set(path, store_id, first)
    second = _publisher_prepare_set(path, store_id, destinations)
    assert json.loads(second)["revision"] == "2"


def test_v1_publisher_rejects_parent_replacement_after_commit(tmp_path, monkeypatch):
    parent = tmp_path / "publisher"
    parent.mkdir()
    path = parent / "identity.json"
    store_id = str(uuid.uuid4())
    identity = ch.v1_publisher_identity(path, store_id, create=True)
    identity["phase"] = "active"
    path.write_text(json.dumps(identity))
    replace = os.replace

    def replace_and_move(*args, **kwargs):
        replace(*args, **kwargs)
        parent.rename(tmp_path / "moved")
        parent.mkdir()

    monkeypatch.setattr(ch.os, "replace", replace_and_move)
    with pytest.raises(ValueError, match="directory changed"):
        _publisher_prepare_set(path, store_id, [])


def test_v1_owned_socket_publisher_rejects_replaced_store(tmp_path):
    import threading

    sock_path = short_sock_path(uuid.uuid4().hex[:8])
    ready = threading.Event()
    requests = []
    path = tmp_path / "publisher.json"
    original = str(uuid.uuid4())
    ch.v1_publisher_identity(path, original, create=True)

    def serve():
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(sock_path))
            server.listen(1)
            ready.set()
            conn, _ = server.accept()
            with conn, conn.makefile("rb") as stream:
                request = json.loads(stream.readline())
                requests.append(request)
                conn.sendall(
                    json.dumps(
                        {
                            "id": request["id"],
                            "ok": True,
                            "result": {"store_id": str(uuid.uuid4())},
                        }
                    ).encode()
                    + b"\n"
                )
                assert stream.readline() == b""

    worker = threading.Thread(target=serve, daemon=True)
    worker.start()
    assert ready.wait(5)
    try:
        with (
            pytest.raises(ValueError, match="store identity changed"),
            ch.v1_owned_socket_publisher(
                path, {"CMUX_SOCKET_PATH": str(sock_path)}, tmp_path
            ),
        ):
            pass
        worker.join(5)
        assert not worker.is_alive()
        assert [request["method"] for request in requests] == [
            "notification.state.info"
        ]
        journal = ch.v1_publisher_identity(path, original, create=False)
        assert journal["phase"] == "bootstrap"
    finally:
        sock_path.unlink(missing_ok=True)


def test_v1_check_private_file_rejects_shared_special_and_oversized_files(tmp_path):
    path = tmp_path / "file"
    path.write_bytes(b"12345")
    path.chmod(0o600)
    ch._v1_check_private_file(path.stat(), "bad", max_size=5)
    with pytest.raises(ValueError, match="bad"):
        ch._v1_check_private_file(path.stat(), "bad", max_size=4)
    path.chmod(0o640)
    with pytest.raises(ValueError, match="bad"):
        ch._v1_check_private_file(path.stat(), "bad")
    with pytest.raises(ValueError, match="bad"):
        ch._v1_check_private_file(tmp_path.stat(), "bad")


def test_v1_private_temporary_writes_private_file_and_cleans_up(tmp_path):
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with ch._v1_private_temporary(directory_fd, "test", b"data") as (name, info):
            temporary = tmp_path / name
            assert temporary.read_bytes() == b"data"
            assert temporary.stat().st_mode & 0o777 == 0o600
            assert (info.st_dev, info.st_ino) == (
                temporary.stat().st_dev,
                temporary.stat().st_ino,
            )
            os.link(name, "installed", src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
        assert not temporary.exists()
        assert (tmp_path / "installed").read_bytes() == b"data"

        with ch._v1_private_temporary(directory_fd, "test", b"x") as (name, _):
            os.replace(
                name, "installed", src_dir_fd=directory_fd, dst_dir_fd=directory_fd
            )
        assert sorted(p.name for p in tmp_path.iterdir()) == ["installed"]

        with (
            pytest.raises(RuntimeError),
            ch._v1_private_temporary(directory_fd, "test", b"x") as (name, _),
        ):
            raise RuntimeError
        assert sorted(p.name for p in tmp_path.iterdir()) == ["installed"]
    finally:
        os.close(directory_fd)
