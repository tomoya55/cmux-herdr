import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from contextlib import contextmanager, nullcontext, suppress
from pathlib import Path
from types import SimpleNamespace

import pytest

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


def test_v1_publisher_identity_rejects_fifo_without_waiting(tmp_path):
    path = tmp_path / "fifo"
    os.mkfifo(path, 0o600)
    script = (
        "import cmux_herdr as c, sys; "
        "c.v1_publisher_identity(sys.argv[1], sys.argv[2], create=False)"
    )
    result = subprocess.run(
        [sys.executable, "-c", script, str(path), str(uuid.uuid4())],
        capture_output=True,
        timeout=2,
    )
    assert result.returncode != 0
    assert b"invalid publisher identity file" in result.stderr


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


def test_v1_owned_publisher_holds_lease_and_skips_unchanged_state(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
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
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    sent = []
    publisher = SimpleNamespace(publish=lambda targets: sent.append(targets))
    assert ch.v1_publish_owned_once(publisher, SimpleNamespace(scope=lambda: None))
    assert sent == [[]]


def test_v1_owned_publication_updates_only_certified_targets(tmp_path, monkeypatch):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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


def test_v1_owned_publication_allows_state_update_during_journal_write(
    tmp_path, monkeypatch
):
    import threading

    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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


def test_v1_owned_publication_does_not_send_changed_snapshot(tmp_path, monkeypatch):
    import threading

    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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


def test_v1_owned_publication_sends_aggregate_over_local_socket(tmp_path, monkeypatch):
    import threading

    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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

    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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


def test_v1_owned_supervisor_publishes_over_one_session(tmp_path, monkeypatch):
    import threading
    from contextlib import contextmanager

    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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


def test_v1_owned_supervisor_reconnects_after_socket_loss(tmp_path, monkeypatch):
    import threading
    from contextlib import contextmanager

    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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
            if len(published) == 1:
                raise ch.SocketError("response lost")
            stop.set()

    @contextmanager
    def factory(*_args):
        opened.append(True)
        yield object(), Publisher()

    monkeypatch.setattr(ch, "v1_owned_socket_transport", factory)
    ch.v1_supervised_owned_publish_loop(stop, SimpleNamespace(scope=lambda: None), {})
    assert opened == [True, True]
    assert published[0] == published[1]


def test_v1_owned_supervisor_publishes_before_and_after_transition(
    tmp_path, monkeypatch
):
    import threading

    monkeypatch.setattr(ch, "state_dir", tmp_path)
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

    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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
    monkeypatch.setattr(ch, "state_dir", tmp_path)
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

    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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


@pytest.fixture
def state():
    return {"sessions": {}}


@pytest.fixture
def bucket(state):
    return ch.session_bucket(state)


@pytest.fixture
def workspaces(bucket):
    return bucket["workspaces"]


@pytest.fixture(autouse=True)
def no_session_env(monkeypatch):
    # Tests must not inherit the session/workspace of a surrounding herdr or
    # cmux pane; state is namespaced per herdr session.
    monkeypatch.delenv("HERDR_SESSION", raising=False)
    monkeypatch.delenv("HERDR_SOCKET_PATH", raising=False)
    monkeypatch.delenv("CMUX_WORKSPACE_ID", raising=False)


@pytest.fixture(autouse=True)
def isolated_publisher_lease_home(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "_v1_publisher_lease_home", lambda: tmp_path)


@pytest.fixture
def cfg():
    return {"workspaces": {"w1": "workspace:1", "w2": "workspace:2"}}


@pytest.fixture
def cmux_calls(monkeypatch):
    calls = []
    monkeypatch.setattr(ch, "cmux_notifications_checked", lambda cfg: [])

    def fake_run(cmd, timeout=10):
        calls.append(cmd[1:])
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(ch, "run", fake_run)
    return calls


@pytest.fixture(autouse=True)
def no_label_lookup(monkeypatch):
    monkeypatch.setattr(ch, "workspace_label", lambda ws_id: "")


def status_event(pane_id, ws_id, status, agent="claude", title="task"):
    return {
        "event": "pane_agent_status_changed",
        "data": {
            "type": "pane_agent_status_changed",
            "pane_id": pane_id,
            "workspace_id": ws_id,
            "agent": agent,
            "title": title,
            "agent_status": status,
            "state_labels": {},
        },
    }


def test_blocked_notifies_and_sets_waiting_pill(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))

    notify = [c for c in cmux_calls if c[0] == "notify"]
    assert len(notify) == 1
    assert "claude: waiting for input" in notify[0]
    assert "workspace:1" in notify[0]

    pills = [c for c in cmux_calls if c[0] == "set-status"]
    assert [c[1] for c in pills] == ["herdr.w1", "herdr.w1.w1_p1"]
    assert pills[0][2] == "1 waiting"
    assert "#ff9500" in pills[0]


def test_done_notifies_finished(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "done"))

    notify = [c for c in cmux_calls if c[0] == "notify"]
    assert len(notify) == 1
    assert "claude: finished" in notify[0]


def test_legacy_read_does_not_mark_unrelated_workspace_notifications(
    cfg, state, cmux_calls
):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "done"))
    cmux_calls.clear()
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "idle"))
    assert not [call for call in cmux_calls if call[0] == "mark-notification-read"]


def test_legacy_notification_defers_when_an_unread_notice_exists(
    monkeypatch, cfg, state, cmux_calls
):
    monkeypatch.setattr(
        ch, "cmux_notifications_checked", lambda cfg: [{"is_read": False}]
    )
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "done"))
    assert not [call for call in cmux_calls if call[0] == "notify"]


def test_legacy_notification_is_not_blocked_by_other_workspace(
    monkeypatch, cfg, state, cmux_calls
):
    monkeypatch.setattr(
        ch,
        "cmux_notifications_checked",
        lambda cfg: [{"is_read": False, "workspace_id": "other"}],
    )
    monkeypatch.setattr(
        ch, "legacy_workspace_aliases", lambda cfg, ref: {"workspace:1", "target"}
    )
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "done"))
    assert len([call for call in cmux_calls if call[0] == "notify"]) == 1


def test_legacy_notification_defers_unread_with_empty_workspace(
    monkeypatch, cfg, state, cmux_calls
):
    monkeypatch.setattr(
        ch,
        "cmux_notifications_checked",
        lambda cfg: [{"is_read": False, "workspace_id": ""}],
    )
    monkeypatch.setattr(
        ch, "legacy_workspace_aliases", lambda cfg, ref: {"workspace:1", "target"}
    )
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "done"))
    assert not [call for call in cmux_calls if call[0] == "notify"]


def test_legacy_workspace_aliases_require_unique_identity(monkeypatch):
    tree = {"windows": [{"workspaces": [{"id": "TARGET", "ref": "workspace:1"}]}]}
    monkeypatch.setattr(
        ch,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=json.dumps(tree)),
    )
    assert ch.legacy_workspace_aliases({}, "workspace:1") == {"target", "workspace:1"}
    tree["windows"][0]["workspaces"].append({"id": "OTHER", "ref": "workspace:1"})
    assert ch.legacy_workspace_aliases({}, "workspace:1") is None


def test_legacy_notification_retries_when_other_notice_is_read(
    monkeypatch, tmp_path, cfg, state, cmux_calls
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(ch, "session_socket_path", lambda session=None: "/source.sock")
    monkeypatch.setattr(
        ch, "socket_rpc", lambda *args: {"agents": [remote_agent("w1:p1", "done")]}
    )
    visible = [{"is_read": False}]
    monkeypatch.setattr(ch, "cmux_notifications_checked", lambda cfg: visible)
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "done"))
    ch.save_state(state)
    assert not [call for call in cmux_calls if call[0] == "notify"]
    visible.clear()
    ch.retry_legacy_notifications(cfg)
    assert len([call for call in cmux_calls if call[0] == "notify"]) == 1
    ch.retry_legacy_notifications(cfg)
    assert len([call for call in cmux_calls if call[0] == "notify"]) == 1


def test_reconcile_preserves_pending_notification_for_same_attention(
    monkeypatch, cfg, state, cmux_calls
):
    monkeypatch.setattr(
        ch, "cmux_notifications_checked", lambda cfg: [{"is_read": False}]
    )
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    monkeypatch.setattr(
        ch, "herdr_cli", lambda args: {"agents": [remote_agent("w1:p1", "blocked")]}
    )
    monkeypatch.setattr(ch, "prune_dead_sessions", lambda *args: None)
    monkeypatch.setattr(ch, "sweep_orphan_pills", lambda *args: None)
    ch.reconcile(cfg, state)
    assert state["sessions"][""]["workspaces"]["w1"]["panes"]["w1:p1"][
        "notification_pending"
    ]


def test_reconcile_preserves_blocked_when_status_unknown(
    monkeypatch, cfg, state, cmux_calls
):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    monkeypatch.setattr(
        ch, "herdr_cli", lambda args: {"agents": [remote_agent("w1:p1", "unknown")]}
    )
    monkeypatch.setattr(ch, "prune_dead_sessions", lambda *args: None)
    monkeypatch.setattr(ch, "sweep_orphan_pills", lambda *args: None)
    ch.reconcile(cfg, state)
    assert (
        state["sessions"][""]["workspaces"]["w1"]["panes"]["w1:p1"]["status"]
        == "blocked"
    )


def test_legacy_retry_loop_runs_until_stopped(monkeypatch):
    stop = ch.threading.Event()
    calls = []

    def retry(cfg, *, session=None):
        calls.append(cfg)
        stop.set()

    monkeypatch.setattr(ch, "retry_legacy_notifications", retry)
    ch.legacy_retry_loop({"key": "value"}, stop)
    assert calls == [{"key": "value"}]


def test_legacy_discards_deferred_notification_after_wait_ends(
    monkeypatch, tmp_path, cfg, state, cmux_calls
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(ch, "session_socket_path", lambda session=None: "/source.sock")
    monkeypatch.setattr(
        ch, "socket_rpc", lambda *args: {"agents": [remote_agent("w1:p1", "working")]}
    )
    visible = [{"is_read": False}]
    monkeypatch.setattr(ch, "cmux_notifications_checked", lambda cfg: visible)
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    ch.save_state(state)
    visible.clear()
    ch.retry_legacy_notifications(cfg)
    assert not [call for call in cmux_calls if call[0] == "notify"]


def test_legacy_retry_requires_current_matching_status(
    monkeypatch, tmp_path, cfg, state, cmux_calls
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    visible = [{"is_read": False}]
    monkeypatch.setattr(ch, "cmux_notifications_checked", lambda cfg: visible)
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "done"))
    ch.save_state(state)
    visible.clear()
    monkeypatch.setattr(ch, "session_socket_path", lambda session=None: "/source.sock")
    monkeypatch.setattr(
        ch, "socket_rpc", lambda *args: {"agents": [remote_agent("w1:p1", "idle")]}
    )
    ch.retry_legacy_notifications(cfg)
    assert not [call for call in cmux_calls if call[0] == "notify"]


def test_legacy_retry_observes_agent_after_acquiring_state_lock(
    monkeypatch, tmp_path, cfg
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(ch, "session_socket_path", lambda session=None: "/source.sock")
    order = []

    class Lock:
        def __enter__(self):
            order.append("lock")

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(ch, "state_lock", Lock)
    monkeypatch.setattr(
        ch, "socket_rpc", lambda *args: order.append("observe") or {"agents": []}
    )
    ch.retry_legacy_notifications(cfg)
    assert order == ["lock", "observe"]


def test_legacy_unknown_status_preserves_waiting_attention(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "unknown"))
    pane = state["sessions"][""]["workspaces"]["w1"]["panes"]["w1:p1"]
    assert pane["status"] == "blocked"


def test_watch_unknown_status_preserves_waiting_attention(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    ch.apply_watch_event(
        cfg,
        state,
        "/source.sock",
        status_event("w1:p1", "w1", "unknown")["data"],
        tracked=False,
    )
    pane = state["sessions"][""]["workspaces"]["w1"]["panes"]["w1:p1"]
    assert pane["status"] == "blocked"


def test_done_sets_finished_pill(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "done"))

    pills = [c for c in cmux_calls if c[0] == "set-status"]
    assert pills[0][1] == "herdr.w1"
    assert pills[0][2] == "1 finished"
    assert "#30d158" in pills[0]
    assert pills[0][-2:] == ["--icon", "checkmark.circle"]
    pane_pill = next(c for c in pills if c[1] == "herdr.w1.w1_p1")
    assert pane_pill[2] == "claude: finished"
    assert "#30d158" in pane_pill


def test_viewing_finished_pane_clears_its_entries(cfg, state, cmux_calls):
    # herdr flips done to idle when the user views the pane; the finished
    # signal is read and must disappear from the sidebar.
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "done"))
    cmux_calls.clear()
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "idle"))

    clears = [c for c in cmux_calls if c[0] == "clear-status"]
    assert ["clear-status", "herdr.w1.w1_p1", "--workspace", "workspace:1"] in clears
    assert ["clear-status", "herdr.w1", "--workspace", "workspace:1"] in clears
    read = [c for c in cmux_calls if c[0] == "mark-notification-read"]
    assert read == []


def test_working_sets_pill_without_notification(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))

    assert [c for c in cmux_calls if c[0] == "notify"] == []
    pills = [c for c in cmux_calls if c[0] == "set-status"]
    assert pills[0][2] == "1 working"
    assert "#0a84ff" in pills[0]


def test_aggregates_across_panes(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    ch.handle_event(cfg, state, status_event("w1:p2", "w1", "done", agent="codex"))
    cmux_calls.clear()
    ch.handle_event(cfg, state, status_event("w1:p3", "w1", "working", agent="pi"))

    pills = [c for c in cmux_calls if c[0] == "set-status"]
    assert pills[0][2] == "1 waiting · 1 finished · 1 working"


def test_idle_removes_pane_and_clears_pill(cfg, state, workspaces, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    cmux_calls.clear()
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "idle"))

    assert workspaces["w1"]["panes"] == {}
    assert [c for c in cmux_calls if c[0] == "notify"] == []
    clears = [c for c in cmux_calls if c[0] == "clear-status"]
    assert [c[1] for c in clears] == ["herdr.w1.w1_p1", "herdr.w1"]


def test_pane_closed_updates_pill(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    ch.handle_event(cfg, state, status_event("w1:p2", "w1", "working"))
    cmux_calls.clear()
    ch.handle_event(
        cfg,
        state,
        {
            "event": "pane_closed",
            "data": {"type": "pane_closed", "pane_id": "w1:p1", "workspace_id": "w1"},
        },
    )

    pill = [c for c in cmux_calls if c[0] == "set-status"]
    assert pill[0][2] == "1 working"


def test_leaving_attention_does_not_mark_unowned_notifications_read(
    cfg, state, cmux_calls
):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    cmux_calls.clear()
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))

    read = [c for c in cmux_calls if c[0] == "mark-notification-read"]
    assert read == []


def test_local_mark_read_respects_remote_attention(
    monkeypatch, cfg, state, cmux_calls, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    ch.save_remote_state(
        {
            "remotes": {
                "maguro-tom": {
                    "panes": {"w9:p1": {"status": "blocked"}},
                    "ref": "workspace:1",
                }
            }
        }
    )
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    cmux_calls.clear()
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))

    # the local pane leaving attention must not clear the remote's
    # still-unresolved notification on the same cmux workspace
    assert [c for c in cmux_calls if c[0] == "mark-notification-read"] == []


def test_other_attention_pane_keeps_notifications_unread(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    ch.handle_event(cfg, state, status_event("w1:p2", "w1", "blocked"))
    cmux_calls.clear()
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))

    assert [c for c in cmux_calls if c[0] == "mark-notification-read"] == []


def test_pane_closed_does_not_mark_unowned_notifications_read(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    cmux_calls.clear()
    ch.handle_event(
        cfg,
        state,
        {
            "event": "pane_closed",
            "data": {"type": "pane_closed", "pane_id": "w1:p1", "workspace_id": "w1"},
        },
    )

    read = [c for c in cmux_calls if c[0] == "mark-notification-read"]
    assert read == []


def test_workspace_closed_clears_status(cfg, state, workspaces, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    cmux_calls.clear()
    ch.handle_event(
        cfg,
        state,
        {
            "event": "workspace_closed",
            "data": {
                "type": "workspace_closed",
                "workspace_id": "w1",
                "workspace": None,
            },
        },
    )

    assert "w1" not in workspaces
    clears = [c for c in cmux_calls if c[0] == "clear-status"]
    assert [c[1] for c in clears] == ["herdr.w1", "herdr.w1.w1_p1"]


def test_pill_includes_icon(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    pill = [c for c in cmux_calls if c[0] == "set-status"]
    assert pill[0][-2:] == ["--icon", "hourglass"]

    cmux_calls.clear()
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    pill = [c for c in cmux_calls if c[0] == "set-status"]
    assert pill[0][-2:] == ["--icon", "bolt"]


def test_per_pane_pill_set(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    pills = [c for c in cmux_calls if c[0] == "set-status"]
    assert len(pills) == 2
    pane_pill = next(c for c in pills if c[1] == "herdr.w1.w1_p1")
    assert pane_pill[2] == "claude: waiting"
    assert "#ff9500" in pane_pill
    assert pane_pill[-2:] == ["--icon", "hourglass"]


def test_per_pane_pill_cleared_on_idle(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    cmux_calls.clear()
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "idle"))

    clears = [c for c in cmux_calls if c[0] == "clear-status"]
    assert ["clear-status", "herdr.w1.w1_p1", "--workspace", "workspace:1"] in clears
    assert ["clear-status", "herdr.w1", "--workspace", "workspace:1"] in clears


def test_per_pane_pill_cleared_on_pane_closed(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    cmux_calls.clear()
    ch.handle_event(
        cfg,
        state,
        {
            "event": "pane_closed",
            "data": {"type": "pane_closed", "pane_id": "w1:p1", "workspace_id": "w1"},
        },
    )
    clears = [c for c in cmux_calls if c[0] == "clear-status"]
    assert ["clear-status", "herdr.w1.w1_p1", "--workspace", "workspace:1"] in clears


def test_per_pane_pills_cleared_on_workspace_closed(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    ch.handle_event(cfg, state, status_event("w1:p2", "w1", "working", agent="codex"))
    cmux_calls.clear()
    ch.handle_event(
        cfg,
        state,
        {
            "event": "workspace_closed",
            "data": {
                "type": "workspace_closed",
                "workspace_id": "w1",
                "workspace": None,
            },
        },
    )
    clears = [c for c in cmux_calls if c[0] == "clear-status"]
    assert ["clear-status", "herdr.w1", "--workspace", "workspace:1"] in clears
    assert ["clear-status", "herdr.w1.w1_p1", "--workspace", "workspace:1"] in clears
    assert ["clear-status", "herdr.w1.w1_p2", "--workspace", "workspace:1"] in clears


def test_reconcile_clears_removed_pane_pills(
    monkeypatch, cfg, state, workspaces, cmux_calls
):
    workspaces["w1"] = {
        "label": "",
        "panes": {
            "w1:p1": {"status": "blocked", "agent": "claude", "title": "fix"},
            "w1:p2": {"status": "working", "agent": "codex", "title": "build"},
        },
    }
    monkeypatch.setattr(
        ch,
        "herdr_cli",
        lambda args: {
            "agents": [
                {
                    "pane_id": "w1:p1",
                    "workspace_id": "w1",
                    "agent": "claude",
                    "agent_status": "blocked",
                    "terminal_title_stripped": "fix",
                },
            ],
        },
    )

    ch.reconcile(cfg, state)

    clears = [c for c in cmux_calls if c[0] == "clear-status"]
    # only the vanished pane's pill is cleared
    assert clears == [["clear-status", "herdr.w1.w1_p2", "--workspace", "workspace:1"]]


def test_sweep_keeps_active_per_pane_keys(monkeypatch, cfg, state):
    agent = {
        "pane_id": "w1:p1",
        "workspace_id": "w1",
        "agent": "claude",
        "agent_status": "working",
        "terminal_title_stripped": "task",
    }
    responses = {
        "list-workspaces": "workspace:1  hd:tom\n",
        ("list-status", "workspace:1"): (
            "herdr.w1=1 working color=#0a84ff priority=10\n"
            "herdr.w1.w1_p1=claude: working color=#0a84ff priority=5\n"
        ),
    }

    calls = reconcile_run(monkeypatch, responses, agents=[agent])
    ch.reconcile(cfg, state)
    assert [c for c in calls if c[0] == "clear-status"] == []


def test_reconcile_preserves_workspace_sidebar_logs(monkeypatch, cfg, state):
    ch.session_bucket(state)["workspaces"]["w2"] = {"label": "other", "panes": {}}
    responses = {
        "list-workspaces": "workspace:1  hd:tom\nworkspace:2  other\nworkspace:3  x\n",
        ("list-status", "workspace:1"): "herdr.w1=1 waiting color=#ff9500\n",
        ("list-status", "workspace:2"): "",
        ("list-status", "workspace:3"): "",
    }
    calls = reconcile_run(monkeypatch, responses, agents=[])

    ch.reconcile(cfg, state)
    clears = [c for c in calls if c[0] == "clear-log"]
    assert clears == []

    calls.clear()
    ch.reconcile(cfg, state)
    assert [c for c in calls if c[0] == "clear-log"] == []


def test_reconcile_defers_log_migration_when_cmux_unreachable(monkeypatch, cfg, state):
    calls = reconcile_run(monkeypatch, {}, agents=[])

    ch.reconcile(cfg, state)

    assert [c for c in calls if c[0] == "clear-log"] == []
    assert "legacy_log_cleared" not in state


def test_workspace_focus_does_not_acknowledge_agent_attention(cfg, state, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "blocked"))
    cmux_calls.clear()
    ch.handle_event(
        cfg,
        state,
        {
            "event": "workspace_focused",
            "data": {"type": "workspace_focused", "workspace_id": "w1"},
        },
    )

    assert not any(call[0] == "mark-notification-read" for call in cmux_calls)


def test_workspace_renamed_updates_label(cfg, state, workspaces, cmux_calls):
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    ch.handle_event(
        cfg,
        state,
        {
            "event": "workspace_renamed",
            "data": {
                "type": "workspace_renamed",
                "workspace_id": "w1",
                "label": "renamed",
            },
        },
    )
    assert workspaces["w1"]["label"] == "renamed"


def test_unmapped_workspace_skips_cmux(monkeypatch, state, workspaces, cmux_calls):
    monkeypatch.setattr(ch, "herdr_session_pids", lambda: [])
    ch.handle_event({}, state, status_event("w9:p1", "w9", "blocked"))
    assert cmux_calls == []
    assert workspaces["w9"]["panes"]["w9:p1"]["status"] == "blocked"


def test_config_mapping_wins_over_env(monkeypatch, state, bucket, cmux_calls):
    monkeypatch.setenv("CMUX_WORKSPACE_ID", "workspace:99")
    cfg = {"workspaces": {"w1": "workspace:1"}}
    ref, _ = ch.resolve_cmux_workspace(cfg, bucket, "w1")
    assert ref == "workspace:1"


def test_env_fallback(monkeypatch, state, bucket, cmux_calls):
    monkeypatch.setenv("CMUX_WORKSPACE_ID", "workspace:99")
    ref, _ = ch.resolve_cmux_workspace({}, bucket, "w1")
    assert ref == "workspace:99"


def test_cached_session_ref_used_without_detection(
    monkeypatch, state, bucket, cmux_calls
):
    monkeypatch.delenv("CMUX_WORKSPACE_ID", raising=False)
    bucket["session_ref"] = {"ref": "workspace:7", "at": ch.time.time()}

    def fail_ps(*args, **kwargs):
        raise AssertionError("ps should not run for a fresh cache")

    monkeypatch.setattr(ch.subprocess, "run", fail_ps)
    ref, _ = ch.resolve_cmux_workspace({}, bucket, "w1")
    assert ref == "workspace:7"


def test_label_match_fallback(monkeypatch, state, bucket, cmux_calls):
    monkeypatch.delenv("CMUX_WORKSPACE_ID", raising=False)
    monkeypatch.setattr(ch, "herdr_session_pids", lambda: [])
    monkeypatch.setattr(ch, "workspace_label", lambda ws_id: "My Project")
    monkeypatch.setattr(
        ch, "cmux_workspaces_by_title", lambda cfg: {"my project": "workspace:5"}
    )
    ref, label = ch.resolve_cmux_workspace({}, bucket, "w1")
    assert (ref, label) == ("workspace:5", "My Project")


def test_detect_via_cmux_top(monkeypatch, state, bucket):
    monkeypatch.delenv("CMUX_WORKSPACE_ID", raising=False)
    monkeypatch.setattr(ch, "herdr_session_pids", lambda: [123])
    top = {
        "windows": [
            {
                "workspaces": [
                    {
                        "type": "workspace",
                        "ref": "workspace:3",
                        "surfaces": [{"pid": 100, "children": [{"pid": 123}]}],
                    }
                ],
            }
        ],
    }

    def fake_run(cmd, timeout=10):
        return SimpleNamespace(returncode=0, stdout=json.dumps(top), stderr="")

    monkeypatch.setattr(ch, "run", fake_run)
    ref = ch.detect_session_workspace({}, bucket)
    assert ref == "workspace:3"
    assert bucket["session_ref"]["ref"] == "workspace:3"


def test_stale_cache_survives_failed_detection(monkeypatch, state, bucket):
    monkeypatch.delenv("CMUX_WORKSPACE_ID", raising=False)
    bucket["session_ref"] = {"ref": "workspace:7", "at": 0}
    monkeypatch.setattr(ch, "herdr_session_pids", lambda: [])
    assert ch.detect_session_workspace({}, bucket) == "workspace:7"


def test_herdr_session_pids_filters(monkeypatch):
    monkeypatch.setenv("HERDR_SESSION", "tom")
    ps_output = "\n".join(
        [
            "  100 /usr/local/bin/herdr server",
            "  200 /usr/local/bin/herdr --session tom",
            "  300 /usr/local/bin/herdr --session gutenberg",
            "  400 /usr/local/bin/herdr session attach tom",
            "  500 /usr/local/bin/herdr --session tom remote-client-bridge",
            "  600 /usr/local/bin/other",
            "  700 /usr/local/bin/herdr",
        ]
    )

    def fake_ps(cmd, **kwargs):
        return SimpleNamespace(returncode=0, stdout=ps_output, stderr="")

    monkeypatch.setattr(ch.subprocess, "run", fake_ps)
    assert sorted(ch.herdr_session_pids()) == [200, 400, 700]


def test_find_pid_workspace_nested():
    tree = {"rows": [{"workspace_id": "workspace:9", "processes": [{"pid": 42}]}]}
    assert ch.find_pid_workspace(tree, {42}) == "workspace:9"
    assert ch.find_pid_workspace(tree, {1}) is None


def test_cmux_workspaces_by_title_parsing(monkeypatch):
    def fake_run(cmd, timeout=10):
        return SimpleNamespace(
            returncode=0,
            stdout="workspace:1  My Project\nworkspace:2  other\n",
            stderr="",
        )

    monkeypatch.setattr(ch, "run", fake_run)
    assert ch.cmux_workspaces_by_title({}) == {
        "my project": "workspace:1",
        "other": "workspace:2",
    }


def test_cmux_workspaces_by_title_parses_selected_row(monkeypatch):
    def fake_run(cmd, timeout=10):
        return SimpleNamespace(
            returncode=0,
            stdout=(
                "  workspace:2  hd:tom\n"
                "* workspace:7  maguro:hd:gutenberg  [selected]\n"
            ),
            stderr="",
        )

    monkeypatch.setattr(ch, "run", fake_run)
    assert ch.cmux_workspaces_by_title({}) == {
        "hd:tom": "workspace:2",
        "maguro:hd:gutenberg": "workspace:7",
    }


def test_run_scrubs_cmux_caller_identity_env(monkeypatch):
    captured = {}

    def fake_subprocess_run(cmd, **kwargs):
        captured["env"] = kwargs.get("env")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(ch.subprocess, "run", fake_subprocess_run)
    monkeypatch.setenv("CMUX_PANEL_ID", "panel-uuid")
    monkeypatch.setenv("CMUX_SURFACE_ID", "surface-uuid")
    monkeypatch.setenv("CMUX_TAB_ID", "tab-uuid")
    monkeypatch.setenv("CMUX_WORKSPACE_ID", "ws-uuid")
    monkeypatch.setenv("CMUX_TERMINAL_LIFECYCLE_ID", "lc-uuid")
    monkeypatch.setenv("CMUX_SOCKET_CAPABILITY", "cap-token")

    ch.run(["cmux", "notify"])

    env = captured["env"]
    assert env is not None
    for var in ch.CMUX_CALLER_IDENTITY_VARS:
        assert var not in env
    assert env["CMUX_SOCKET_CAPABILITY"] == "cap-token"


def test_reconcile_rebuilds_state(monkeypatch, cfg, state, workspaces, cmux_calls):
    workspaces["stale"] = {
        "label": "",
        "panes": {"stale:p1": {"status": "working"}},
    }
    monkeypatch.setattr(
        ch,
        "herdr_cli",
        lambda args: {
            "agents": [
                {
                    "pane_id": "w1:p1",
                    "workspace_id": "w1",
                    "agent": "claude",
                    "agent_status": "blocked",
                    "terminal_title_stripped": "fix",
                },
                {
                    "pane_id": "w2:p1",
                    "workspace_id": "w2",
                    "agent": "codex",
                    "agent_status": "working",
                    "terminal_title_stripped": "build",
                },
                {
                    "pane_id": "w2:p2",
                    "workspace_id": "w2",
                    "agent": "pi",
                    "agent_status": "idle",
                    "terminal_title_stripped": "",
                },
            ],
        },
    )

    ch.reconcile(cfg, state)

    assert "stale" not in workspaces
    assert workspaces["w1"]["panes"]["w1:p1"]["status"] == "blocked"
    assert list(workspaces["w2"]["panes"]) == ["w2:p1"]
    pills = [c for c in cmux_calls if c[0] == "set-status"]
    assert {c[1] for c in pills} == {
        "herdr.w1",
        "herdr.w1.w1_p1",
        "herdr.w2",
        "herdr.w2.w2_p1",
    }


def test_reconcile_clears_stale_workspace(
    monkeypatch, cfg, state, workspaces, cmux_calls
):
    workspaces["w1"] = {
        "label": "",
        "panes": {
            "w1:p1": {"status": "blocked", "agent": "claude", "title": "fix"},
        },
    }
    monkeypatch.setattr(ch, "herdr_cli", lambda args: {"agents": []})

    ch.reconcile(cfg, state)

    assert "w1" not in workspaces
    clear = [c for c in cmux_calls if c[0] == "clear-status"]
    assert clear == [
        ["clear-status", "herdr.w1.w1_p1", "--workspace", "workspace:1"],
        ["clear-status", "herdr.w1", "--workspace", "workspace:1"],
    ]
    read = [c for c in cmux_calls if c[0] == "mark-notification-read"]
    assert read == []


def reconcile_run(monkeypatch, responses, agents):
    """Run reconcile with a fake cmux CLI driven by per-command responses."""
    calls = []

    def fake_run(cmd, timeout=10):
        calls.append(cmd[1:])
        out = responses.get(cmd[1], "") if len(cmd) > 1 else ""
        if cmd[1] == "list-status":
            out = responses.get(("list-status", cmd[-1]), "")
        return SimpleNamespace(returncode=0, stdout=out, stderr="")

    monkeypatch.setattr(ch, "run", fake_run)
    monkeypatch.setattr(ch, "herdr_cli", lambda args: {"agents": agents})
    return calls


def test_reconcile_clears_orphan_pills(monkeypatch, cfg, state):
    calls = reconcile_run(
        monkeypatch,
        {
            "list-workspaces": "workspace:1  hd:tom\nworkspace:2  other\n",
            ("list-status", "workspace:1"): (
                "herdr.w9=1 waiting color=#ff9500 priority=20\n"
            ),
        },
        agents=[],
    )

    ch.reconcile(cfg, state)

    assert ["clear-status", "herdr.w9", "--workspace", "workspace:1"] in calls


def test_reconcile_keeps_active_pill_keys(monkeypatch, cfg, state):
    calls = reconcile_run(
        monkeypatch,
        {
            "list-workspaces": "workspace:1  hd:tom\n",
            ("list-status", "workspace:1"): (
                "herdr.w1=1 working color=#0a84ff priority=10\n"
            ),
        },
        agents=[
            {
                "pane_id": "w1:p1",
                "workspace_id": "w1",
                "agent": "claude",
                "agent_status": "working",
                "terminal_title_stripped": "task",
            },
        ],
    )

    ch.reconcile(cfg, state)

    assert [c for c in calls if c[0] == "clear-status"] == []


def test_reconcile_preserves_unidentified_notifications(monkeypatch, cfg, state):
    notifications = json.dumps(
        [
            {"id": "n1", "is_read": False, "title": "claude: waiting for input"},
            {"id": "n2", "is_read": True, "title": "codex: finished"},
            {"id": "n3", "is_read": False, "title": "Build finished"},
        ]
    )
    calls = reconcile_run(monkeypatch, {"list-notifications": notifications}, agents=[])

    ch.reconcile(cfg, state)

    read = [c for c in calls if c[0] == "mark-notification-read"]
    assert read == []


def test_reconcile_keeps_notifications_while_attention(monkeypatch, cfg, state):
    notifications = json.dumps(
        [{"id": "n1", "is_read": False, "title": "claude: waiting for input"}]
    )
    calls = reconcile_run(
        monkeypatch,
        {"list-notifications": notifications},
        agents=[
            {
                "pane_id": "w1:p1",
                "workspace_id": "w1",
                "agent": "claude",
                "agent_status": "blocked",
                "terminal_title_stripped": "fix",
            },
        ],
    )

    ch.reconcile(cfg, state)

    assert [c for c in calls if c[0] == "mark-notification-read"] == []


def test_same_workspace_id_in_two_sessions_stays_isolated(
    monkeypatch, cfg, state, cmux_calls
):
    # Workspace ids are only unique within a herdr session; two sessions may
    # both have a "w1" and must not share panes.
    monkeypatch.setenv("HERDR_SESSION", "coten")
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    monkeypatch.setenv("HERDR_SESSION", "gutenberg")
    ch.handle_event(cfg, state, status_event("w1:p9", "w1", "blocked"))

    sessions = state["sessions"]
    assert list(sessions["coten"]["workspaces"]["w1"]["panes"]) == ["w1:p1"]
    assert list(sessions["gutenberg"]["workspaces"]["w1"]["panes"]) == ["w1:p9"]


def test_reconcile_preserves_other_sessions(monkeypatch, cfg, state):
    # Regression: refresh in one herdr session rebuilt the whole flat state
    # from its own agent list and cleared every other session's pills.
    monkeypatch.setenv("HERDR_SESSION", "coten")
    state["sessions"]["gutenberg"] = {
        "workspaces": {
            "w6": {
                "label": "gutenberg",
                "panes": {
                    "w6:p1": {"status": "working", "agent": "claude", "title": "t"},
                },
            }
        }
    }
    calls = reconcile_run(
        monkeypatch,
        {
            "session": json.dumps(
                {
                    "sessions": [
                        {"name": "coten", "running": True},
                        {"name": "gutenberg", "running": True},
                    ]
                }
            ),
            "list-workspaces": "workspace:1  hd:coten\nworkspace:2  hd:gutenberg\n",
            ("list-status", "workspace:2"): (
                "herdr.w6=1 working color=#0a84ff priority=10\n"
            ),
        },
        agents=[],
    )

    ch.reconcile(cfg, state)

    assert state["sessions"]["gutenberg"]["workspaces"]["w6"]["panes"]
    assert [c for c in calls if c[0] == "clear-status"] == []


def test_reconcile_prunes_ended_sessions(monkeypatch, cfg, state):
    # A session that ended and never restarted would otherwise keep its
    # pills forever, since no hook ever runs for it again.
    monkeypatch.setenv("HERDR_SESSION", "coten")
    cfg["workspaces"]["w5"] = "workspace:5"
    state["sessions"]["ghost"] = {
        "workspaces": {
            "w5": {
                "label": "ghost",
                "panes": {
                    "w5:p1": {"status": "blocked", "agent": "claude", "title": ""},
                },
            }
        }
    }
    calls = reconcile_run(
        monkeypatch,
        {"session": json.dumps({"sessions": [{"name": "coten", "running": True}]})},
        agents=[],
    )

    ch.reconcile(cfg, state)

    assert "ghost" not in state["sessions"]
    assert ["clear-status", "herdr.w5", "--workspace", "workspace:5"] in calls
    assert not any(c[0] == "mark-notification-read" for c in calls)


def test_reconcile_keeps_sessions_when_liveness_unknown(monkeypatch, cfg, state):
    monkeypatch.setenv("HERDR_SESSION", "coten")
    state["sessions"]["ghost"] = {
        "workspaces": {
            "w5": {
                "label": "ghost",
                "panes": {
                    "w5:p1": {"status": "working", "agent": "claude", "title": ""},
                },
            }
        }
    }
    # no "session" response: session list fails and pruning must be skipped
    reconcile_run(monkeypatch, {}, agents=[])

    ch.reconcile(cfg, state)

    assert "ghost" in state["sessions"]


def test_event_tolerates_workspace_entry_without_panes(
    cfg, state, workspaces, cmux_calls
):
    # workspace entries written by older plugin versions may lack "panes"
    workspaces["w1"] = {"label": "old-format"}
    ch.handle_event(cfg, state, status_event("w1:p1", "w1", "working"))
    assert workspaces["w1"]["panes"]["w1:p1"]["status"] == "working"


def test_concurrent_save_state_does_not_race_tmp(monkeypatch, tmp_path):
    import threading

    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    # simulate separate hook processes sharing one state file
    monkeypatch.setattr(ch.os, "getpid", threading.get_native_id)
    errors = []

    def save(i):
        try:
            for _ in range(30):
                ch.save_state({"sessions": {str(i): {"workspaces": {}}}})
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=save, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert json.loads((tmp_path / "state.json").read_text())["sessions"]


def test_state_roundtrip(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    assert ch.load_state() == {"sessions": {}}
    bucket = {"workspaces": {"w1": {"label": "x", "panes": {}}}}
    ch.save_state({"sessions": {"tom": bucket}})
    assert ch.load_state()["sessions"]["tom"]["workspaces"]["w1"]["label"] == "x"


def test_load_state_drops_legacy_flat_format(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    (tmp_path / "state.json").write_text(
        json.dumps(
            {
                "workspaces": {"w1": {"label": "x", "panes": {}}},
                "session_ref": {"ref": "workspace:1", "at": 0},
            }
        )
    )
    assert ch.load_state() == {"sessions": {}}


def test_load_config(monkeypatch, tmp_path):
    (tmp_path / "config.toml").write_text('[workspaces]\nw1 = "workspace:1"\n')
    monkeypatch.setattr(ch, "config_dir", tmp_path)
    assert ch.load_config()["workspaces"]["w1"] == "workspace:1"

    (tmp_path / "config.toml").write_text("not = [valid")
    assert ch.load_config() == {}


def test_main_event_mode(monkeypatch, tmp_path, cfg):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(ch, "config_dir", tmp_path)
    monkeypatch.setattr(ch, "load_config", lambda: cfg)
    monkeypatch.setattr(ch, "cmux_notifications_checked", lambda cfg: [])
    monkeypatch.setattr(sys, "argv", ["cmux_herdr.py", "event"])
    monkeypatch.setenv(
        "HERDR_PLUGIN_EVENT_JSON", json.dumps(status_event("w1:p1", "w1", "blocked"))
    )
    calls = []
    monkeypatch.setattr(
        ch,
        "run",
        lambda cmd, timeout=10: (
            calls.append(cmd[1:]),
            SimpleNamespace(returncode=0, stdout="", stderr=""),
        )[1],
    )

    assert ch.main() == 0
    assert any(c[0] == "notify" for c in calls)
    saved = json.loads((tmp_path / "state.json").read_text())
    panes = saved["sessions"][""]["workspaces"]["w1"]["panes"]
    assert panes["w1:p1"]["status"] == "blocked"


def test_main_rejects_unknown_mode(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["cmux_herdr.py", "bogus"])
    assert ch.main() == 2
    assert "usage" in capsys.readouterr().err


def test_cmux_cli_tolerates_missing_binary(monkeypatch):
    def raise_not_found(cmd, timeout=10):
        raise FileNotFoundError

    monkeypatch.setattr(ch, "run", raise_not_found)
    assert ch.cmux_cli({}, ["ping"]) is False


def test_cmux_cli_tolerates_failure(monkeypatch):
    monkeypatch.setattr(
        ch,
        "run",
        lambda cmd, timeout=10: SimpleNamespace(returncode=1, stdout="", stderr="boom"),
    )
    assert ch.cmux_cli({}, ["ping"]) is False


def test_cmux_cli_tolerates_timeout(monkeypatch):
    def slow(cmd, timeout=10):
        raise subprocess.TimeoutExpired(cmd, timeout)

    monkeypatch.setattr(ch, "run", slow)
    assert ch.cmux_cli({}, ["ping"]) is False


# --- Remote daemon -----------------------------------------------------------


REMOTE_CFG = {"ssh_target": "maguro", "session": "tom", "cmux_title": "maguro:hd:tom"}


@pytest.fixture
def remote_title_map(monkeypatch):
    monkeypatch.setattr(
        ch, "cmux_workspaces_by_title", lambda cfg: {"maguro:hd:tom": "workspace:9"}
    )


def remote_agent(pane_id, status, agent="claude", title="task", ws_id="w1"):
    return {
        "pane_id": pane_id,
        "workspace_id": ws_id,
        "agent": agent,
        "agent_status": status,
        "terminal_title_stripped": title,
    }


def test_remote_blocked_notifies_and_sets_pill(cmux_calls, remote_title_map):
    rstate = {"remotes": {}}
    changed = ch.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "blocked")], {}
    )

    assert changed is True
    notify = [c for c in cmux_calls if c[0] == "notify"]
    assert len(notify) == 1
    assert "claude: waiting for input" in notify[0]
    assert "workspace:9" in notify[0]
    assert "maguro:hd:tom · task" in notify[0]

    pill = [c for c in cmux_calls if c[0] == "set-status"]
    assert pill[0][1] == "herdr.remote.tom"
    assert pill[0][2] == "1 waiting"
    assert "#ff9500" in pill[0]


def test_remote_done_notifies_finished(cmux_calls, remote_title_map):
    rstate = {"remotes": {}}
    ch.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "done")], {}
    )
    notify = [c for c in cmux_calls if c[0] == "notify"]
    assert len(notify) == 1
    assert "claude: finished" in notify[0]


def test_remote_working_pill_without_notification(cmux_calls, remote_title_map):
    rstate = {"remotes": {}}
    ch.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "working")], {}
    )
    assert [c for c in cmux_calls if c[0] == "notify"] == []
    pill = [c for c in cmux_calls if c[0] == "set-status"]
    assert pill[0][2] == "1 working"
    assert "#0a84ff" in pill[0]


def test_remote_per_pane_pills(cmux_calls, remote_title_map):
    rstate = {"remotes": {}}
    ch.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "blocked")], {}
    )
    pills = [c for c in cmux_calls if c[0] == "set-status"]
    pane_pill = next(c for c in pills if c[1] == "herdr.remote.tom.w1_p1")
    assert pane_pill[2] == "claude: waiting"

    cmux_calls.clear()
    ch.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "idle")], {}
    )
    clears = [c for c in cmux_calls if c[0] == "clear-status"]
    assert [
        "clear-status",
        "herdr.remote.tom.w1_p1",
        "--workspace",
        "workspace:9",
    ] in clears
    assert ["clear-status", "herdr.remote.tom", "--workspace", "workspace:9"] in clears


def test_retire_remote_clears_per_pane_pills(
    monkeypatch, cmux_calls, remote_title_map, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    rstate = {
        "remotes": {
            "tom": {
                "panes": {"w1:p1": {"status": "blocked"}},
                "ref": "workspace:9",
                "cmux_title": "maguro:hd:tom",
            }
        }
    }
    ch.retire_remote({}, rstate, "tom", {}, {})

    clears = [c for c in cmux_calls if c[0] == "clear-status"]
    assert [
        "clear-status",
        "herdr.remote.tom.w1_p1",
        "--workspace",
        "workspace:9",
    ] in clears
    assert ["clear-status", "herdr.remote.tom", "--workspace", "workspace:9"] in clears


def test_remote_unchanged_snapshot_is_noop(cmux_calls, remote_title_map):
    rstate = {"remotes": {}}
    agents = [remote_agent("w1:p1", "blocked")]
    ch.apply_remote_snapshot({}, rstate, "tom", REMOTE_CFG, agents, {})
    cmux_calls.clear()

    changed = ch.apply_remote_snapshot({}, rstate, "tom", REMOTE_CFG, agents, {})
    assert changed is False
    assert cmux_calls == []


def test_remote_retries_deferred_notification_when_other_notice_is_read(
    monkeypatch, cmux_calls, remote_title_map
):
    visible = [{"is_read": False}]
    monkeypatch.setattr(ch, "cmux_notifications_checked", lambda cfg: visible)
    rstate = {"remotes": {}}
    agents = [remote_agent("w1:p1", "blocked")]
    ch.apply_remote_snapshot({}, rstate, "tom", REMOTE_CFG, agents, {})
    assert not [call for call in cmux_calls if call[0] == "notify"]
    visible.clear()
    ch.apply_remote_snapshot({}, rstate, "tom", REMOTE_CFG, agents, {})
    assert len([call for call in cmux_calls if call[0] == "notify"]) == 1


def test_remote_discards_deferred_notification_after_wait_ends(
    monkeypatch, cmux_calls, remote_title_map
):
    visible = [{"is_read": False}]
    monkeypatch.setattr(ch, "cmux_notifications_checked", lambda cfg: visible)
    rstate = {"remotes": {}}
    ch.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "blocked")], {}
    )
    ch.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "working")], {}
    )
    visible.clear()
    ch.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "working")], {}
    )
    assert not [call for call in cmux_calls if call[0] == "notify"]


def test_remote_does_not_retry_waiting_notification_for_missing_agent(
    monkeypatch, cmux_calls, remote_title_map
):
    visible = [{"is_read": False}]
    monkeypatch.setattr(ch, "cmux_notifications_checked", lambda cfg: visible)
    rstate = {"remotes": {}}
    ch.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "blocked")], {}
    )
    visible.clear()
    ch.apply_remote_snapshot({}, rstate, "tom", REMOTE_CFG, [], {})
    assert not [call for call in cmux_calls if call[0] == "notify"]


def test_remote_unknown_status_preserves_waiting_attention(
    cmux_calls, remote_title_map
):
    rstate = {"remotes": {}}
    ch.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "blocked")], {}
    )
    ch.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "unknown")], {}
    )
    assert rstate["remotes"]["tom"]["panes"]["w1:p1"]["status"] == "blocked"


def test_remote_missing_agent_does_not_prove_input_wait_ended(
    cmux_calls, remote_title_map
):
    rstate = {"remotes": {}}
    ch.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "blocked")], {}
    )
    ch.apply_remote_snapshot({}, rstate, "tom", REMOTE_CFG, [], {})
    assert rstate["remotes"]["tom"]["panes"]["w1:p1"]["status"] == "blocked"


def test_remote_leaving_attention_preserves_unowned_notifications(
    cmux_calls, remote_title_map
):
    rstate = {"remotes": {}}
    ch.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "blocked")], {}
    )
    cmux_calls.clear()
    ch.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "working")], {}
    )

    read = [c for c in cmux_calls if c[0] == "mark-notification-read"]
    assert read == []
    pill = [c for c in cmux_calls if c[0] == "set-status"]
    assert pill[0][2] == "1 working"


def test_remote_all_idle_clears_pill(cmux_calls, remote_title_map):
    rstate = {"remotes": {}}
    ch.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "working")], {}
    )
    cmux_calls.clear()
    ch.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "idle")], {}
    )

    clear = [c for c in cmux_calls if c[0] == "clear-status"]
    assert clear == [
        ["clear-status", "herdr.remote.tom.w1_p1", "--workspace", "workspace:9"],
        ["clear-status", "herdr.remote.tom", "--workspace", "workspace:9"],
    ]


def test_remote_aggregates_across_workspaces(cmux_calls, remote_title_map):
    rstate = {"remotes": {}}
    agents = [
        remote_agent("w1:p1", "blocked", ws_id="w1"),
        remote_agent("w2:p1", "working", agent="codex", ws_id="w2"),
    ]
    ch.apply_remote_snapshot({}, rstate, "tom", REMOTE_CFG, agents, {})

    pill = [c for c in cmux_calls if c[0] == "set-status"]
    assert pill[0][2] == "1 waiting · 1 working"


def test_remote_without_title_skips_cmux(cmux_calls):
    rstate = {"remotes": {}}
    rcfg = {"ssh_target": "maguro", "session": "tom"}
    changed = ch.apply_remote_snapshot(
        {}, rstate, "tom", rcfg, [remote_agent("w1:p1", "blocked")], {}
    )
    assert changed is True
    assert cmux_calls == []
    assert rstate["remotes"]["tom"]["panes"]["w1:p1"]["status"] == "blocked"


def test_remote_title_change_reroutes_pill(cmux_calls, remote_title_map):
    rstate = {"remotes": {}}
    ch.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "working")], {}
    )
    cmux_calls.clear()
    rcfg = dict(REMOTE_CFG, cmux_title="maguro:hd:other")
    changed = ch.apply_remote_snapshot(
        {}, rstate, "tom", rcfg, [remote_agent("w1:p1", "working")], {}
    )
    assert changed is True
    # the pill and unread notifications on the previous workspace are cleared
    clears = [c for c in cmux_calls if c[0] == "clear-status"]
    assert clears == [
        ["clear-status", "herdr.remote.tom", "--workspace", "workspace:9"],
        ["clear-status", "herdr.remote.tom.w1_p1", "--workspace", "workspace:9"],
    ]
    read = [c for c in cmux_calls if c[0] == "mark-notification-read"]
    assert read == []
    # new title is not in the map -> no stale ref reuse, no pill yet
    assert [c for c in cmux_calls if c[0] == "set-status"] == []


def test_remote_title_change_publishes_to_new_workspace(monkeypatch, cmux_calls):
    monkeypatch.setattr(
        ch,
        "cmux_workspaces_by_title",
        lambda cfg: {"maguro:hd:tom": "workspace:9", "maguro:hd:other": "workspace:10"},
    )
    rstate = {"remotes": {}}
    agents = [remote_agent("w1:p1", "working")]
    ch.apply_remote_snapshot({}, rstate, "tom", REMOTE_CFG, agents, {})
    cmux_calls.clear()

    rcfg = dict(REMOTE_CFG, cmux_title="maguro:hd:other")
    ch.apply_remote_snapshot({}, rstate, "tom", rcfg, agents, {})

    pill = [c for c in cmux_calls if c[0] == "set-status"]
    assert pill[0][1] == "herdr.remote.tom"
    assert "workspace:10" in pill[0]


def test_remote_daemon_without_remotes_cleans_up(monkeypatch, cmux_calls, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "config_dir", tmp_path)
    ch.save_remote_state(
        {
            "remotes": {
                "tom": {
                    "panes": {"w1:p1": {"status": "blocked"}},
                    "ref": "workspace:9",
                    "cmux_title": "maguro:hd:tom",
                }
            }
        }
    )
    monkeypatch.setattr(ch, "cmux_workspaces_by_title", lambda cfg: {})

    assert ch.remote_daemon({}) == 0

    read = [c for c in cmux_calls if c[0] == "mark-notification-read"]
    assert read == []
    saved = json.loads((tmp_path / "remote-state.json").read_text())
    assert saved["remotes"] == {}


def test_remote_daemon_malformed_config_does_not_clean_up(
    monkeypatch, cmux_calls, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "config_dir", tmp_path)
    (tmp_path / "config.toml").write_text("not = [valid")
    ch.save_remote_state(
        {"remotes": {"tom": {"panes": {"w1:p1": {"status": "blocked"}}}}}
    )

    assert ch.remote_daemon({}) == 1
    assert cmux_calls == []
    saved = json.loads((tmp_path / "remote-state.json").read_text())
    assert "tom" in saved["remotes"]


def test_read_pidfile_formats(tmp_path):
    p = tmp_path / "remote.pid"
    p.write_text("1234")  # pre-JSON format
    assert ch.read_pidfile(p) == {"pid": 1234}
    p.write_text('{"pid": 5, "script": "x", "mtime": 1}')
    assert ch.read_pidfile(p)["script"] == "x"
    p.write_text("garbage")
    assert ch.read_pidfile(p) == {}


def test_remote_identity_change_drops_socket_path(cmux_calls, remote_title_map):
    rstate = {"remotes": {}}
    agents = [remote_agent("w1:p1", "working")]
    ch.apply_remote_snapshot({}, rstate, "tom", REMOTE_CFG, agents, {})
    rstate["remotes"]["tom"]["socket_path"] = "/old/path.sock"
    cmux_calls.clear()

    rcfg = dict(REMOTE_CFG, ssh_target="other-host")
    changed = ch.apply_remote_snapshot({}, rstate, "tom", rcfg, agents, {})

    assert changed is True
    entry = rstate["remotes"]["tom"]
    assert "socket_path" not in entry
    assert entry["identity"] == ["other-host", "tom"]


def test_remote_mark_read_respects_other_remote_on_same_workspace(cmux_calls):
    rstate = {
        "remotes": {
            "a": {"panes": {}, "ref": "workspace:9", "cmux_title": "t"},
            "b": {"panes": {"w1:p1": {"status": "blocked"}}, "ref": "workspace:9"},
        }
    }
    ch.remote_mark_read_ref({}, rstate, "a", "workspace:9")
    assert cmux_calls == []

    ch.remote_mark_read_ref({}, rstate, "b", "workspace:9")
    assert cmux_calls == []


def test_remote_ref_clears_pill_when_title_moves(monkeypatch, cmux_calls, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(
        ch, "cmux_workspaces_by_title", lambda cfg: {"maguro:hd:tom": "workspace:10"}
    )
    entry = {"cmux_title": "maguro:hd:tom", "ref": "workspace:9", "ref_at": 0}

    ref = ch.remote_ref({}, {"remotes": {}}, "tom", entry)

    assert ref == "workspace:10"
    assert ["clear-status", "herdr.remote.tom", "--workspace", "workspace:9"] in (
        cmux_calls
    )
    assert not any(c[0] == "mark-notification-read" for c in cmux_calls)


def test_poll_interval_validation():
    assert ch.poll_interval({}) == ch.REMOTE_DEFAULT_POLL_SECONDS
    assert ch.poll_interval({"poll_seconds": 5}) == 5.0
    assert ch.poll_interval({"poll_seconds": "abc"}) == ch.REMOTE_DEFAULT_POLL_SECONDS
    assert ch.poll_interval({"poll_seconds": -1}) == ch.REMOTE_DEFAULT_POLL_SECONDS
    assert ch.poll_interval({"poll_seconds": 0.1}) == 0.5


def test_remote_republishes_pill_once_workspace_appears(monkeypatch, cmux_calls):
    titles = {}
    monkeypatch.setattr(ch, "cmux_workspaces_by_title", lambda cfg: dict(titles))
    rstate = {"remotes": {}}
    agents = [remote_agent("w1:p1", "working")]

    ch.apply_remote_snapshot({}, rstate, "tom", REMOTE_CFG, agents, {})
    assert cmux_calls == []  # title not resolvable yet

    titles["maguro:hd:tom"] = "workspace:9"
    changed = ch.apply_remote_snapshot({}, rstate, "tom", REMOTE_CFG, agents, {})
    assert changed is False  # snapshot unchanged, but the pill is retried
    pills = [c for c in cmux_calls if c[0] == "set-status"]
    assert pills[0][1] == "herdr.remote.tom"
    assert "workspace:9" in pills[0]


def test_remote_name_defaults_to_target_and_session():
    # The full target with an unambiguous separator, so distinct
    # users/hosts/sessions cannot collide.
    assert (
        ch.remote_name({"ssh_target": "tom@maguro.example.ts.net", "session": "tom"})
        == "tom@maguro.example.ts.net:tom"
    )
    assert ch.remote_name({"ssh_target": "maguro", "session": "hd"}) == "maguro:hd"
    assert ch.remote_name({"ssh_target": "maguro", "session": "hd", "name": "x"}) == "x"
    # colon-containing targets (IPv6, ssh:// URIs) fall back to a hash
    name = ch.remote_name({"ssh_target": "ssh://tom@host:2222", "session": "hd"})
    assert name.startswith("remote-") and len(name) == len("remote-") + 8


def test_remote_configs_rejects_malformed_entries(capsys):
    # any invalid entry rejects the whole config so the last valid one is kept
    bad_entries = [
        "bad",
        {"ssh_target": 42, "session": "s"},
        {"ssh_target": "a", "session": "s", "cmux_title": 1},
        {"ssh_target": "a", "session": "s", "name": ["x"], "cmux_title": "t"},
        {"ssh_target": "a", "session": "s"},  # cmux_title is required
    ]
    for bad in bad_entries:
        assert ch.remote_configs({"remotes": [bad]}) is None, bad
    assert ch.remote_configs({"remotes": "bad"}) is None
    assert ch.remote_configs({"remotes": ""}) is None
    assert ch.remote_configs({}) == []
    assert ch.remote_configs({"remotes": []}) == []
    valid = {"ssh_target": "a", "session": "s", "cmux_title": "t"}
    assert ch.remote_configs({"remotes": [valid]}) == [valid]
    assert "expected a table" in capsys.readouterr().err


def test_remote_configs_rejects_duplicate_names(capsys):
    cfg = {
        "remotes": [
            {"ssh_target": "a", "session": "s", "name": "dup", "cmux_title": "t"},
            {"ssh_target": "b", "session": "s", "name": "dup", "cmux_title": "t"},
        ]
    }
    assert ch.remote_configs(cfg) is None
    assert "duplicate remote name" in capsys.readouterr().err


def test_retire_remote_clears_pill_and_state(monkeypatch, cmux_calls, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    rstate = {
        "remotes": {
            "tom": {
                "panes": {"w1:p1": {"status": "blocked"}},
                "ref": "workspace:9",
                "cmux_title": "maguro:hd:tom",
            }
        }
    }
    ch.retire_remote({}, rstate, "tom", {}, {})

    assert rstate["remotes"] == {}
    assert ["clear-status", "herdr.remote.tom", "--workspace", "workspace:9"] in (
        cmux_calls
    )
    assert not any(c[0] == "mark-notification-read" for c in cmux_calls)
    saved = json.loads((tmp_path / "remote-state.json").read_text())
    assert saved["remotes"] == {}


def test_sweep_orphan_pills_keeps_remote_keys(monkeypatch, cfg, state):
    calls = reconcile_run(
        monkeypatch,
        {
            "list-workspaces": "workspace:1  hd:tom\n",
            ("list-status", "workspace:1"): (
                "herdr.remote.tom=1 waiting color=#ff9500 priority=20\n"
                "herdr.w9=1 waiting color=#ff9500 priority=20\n"
            ),
        },
        agents=[],
    )

    ch.reconcile(cfg, state)

    clears = [c for c in calls if c[0] == "clear-status"]
    assert clears == [["clear-status", "herdr.w9", "--workspace", "workspace:1"]]


def test_sweep_notifications_skips_with_remote_attention(
    monkeypatch, cfg, state, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    ch.save_remote_state(
        {"remotes": {"tom": {"panes": {"w1:p1": {"status": "blocked"}}}}}
    )
    notifications = json.dumps(
        [{"id": "n1", "is_read": False, "title": "claude: waiting for input"}]
    )
    calls = reconcile_run(monkeypatch, {"list-notifications": notifications}, agents=[])

    ch.reconcile(cfg, state)

    assert [c for c in calls if c[0] == "mark-notification-read"] == []


def short_sock_path(name):
    # AF_UNIX paths are limited to ~104 chars on macOS; tmp_path is too long.
    path = Path(tempfile.gettempdir()) / f"ch-{os.getpid()}-{name}.sock"
    path.unlink(missing_ok=True)
    return path


def test_socket_rpc_roundtrip():
    import threading

    sock_path = short_sock_path("rpc")
    received = []
    ready = threading.Event()

    def serve():
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(str(sock_path))
        srv.listen(1)
        ready.set()
        conn, _ = srv.accept()
        data = b""
        while b"\n" not in data:
            data += conn.recv(1 << 16)
        received.append(json.loads(data.strip()))
        conn.sendall(b'{"id":"1","result":{"type":"pong"}}\n')
        conn.close()
        srv.close()

    t = threading.Thread(target=serve, daemon=True)
    t.start()
    ready.wait(5)
    result = ch.socket_rpc(sock_path, "ping", {})
    t.join(5)

    assert result == {"type": "pong"}
    assert received == [{"id": "1", "method": "ping", "params": {}}]


def test_v1_owned_socket_request_preserves_saved_params_bytes():
    import threading

    sock_path = short_sock_path(uuid.uuid4().hex[:8])
    ready = threading.Event()
    received = []
    pending = b'{"store_id":"example", "action":"read"}'

    def serve():
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(sock_path))
            server.listen(1)
            ready.set()
            conn, _ = server.accept()
            with conn:
                data = b""
                while not data.endswith(b"\n"):
                    data += conn.recv(65536)
                received.append(data)
                request = json.loads(data)
                conn.sendall(
                    json.dumps(
                        {"id": request["id"], "ok": True, "result": {"state": None}}
                    ).encode()
                    + b"\n"
                )

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    assert ready.wait(5)
    try:
        assert ch.v1_owned_socket_request(sock_path, pending) == {"state": None}
        thread.join(5)
        assert pending in received[0]
        assert json.loads(received[0])["method"] == "notification.state.command"
    finally:
        sock_path.unlink(missing_ok=True)


@pytest.mark.parametrize(
    "response",
    [b'{"id":"wrong","ok":true,"result":{}}\n', b"not-json\n", b"{}\n"],
)
def test_v1_owned_socket_request_rejects_invalid_response(response):
    import threading

    sock_path = short_sock_path(uuid.uuid4().hex[:8])
    ready = threading.Event()

    def serve():
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(sock_path))
            server.listen(1)
            ready.set()
            conn, _ = server.accept()
            with conn:
                conn.recv(65536)
                conn.sendall(response)

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    assert ready.wait(5)
    try:
        with pytest.raises(ch.SocketError):
            ch.v1_owned_socket_request(sock_path, b"{}")
        thread.join(5)
    finally:
        sock_path.unlink(missing_ok=True)


def test_v1_owned_socket_request_preserves_error_code():
    import threading

    sock_path = short_sock_path(uuid.uuid4().hex[:8])
    ready = threading.Event()

    def serve():
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(sock_path))
            server.listen(1)
            ready.set()
            conn, _ = server.accept()
            with conn:
                request = json.loads(conn.recv(65536))
                response = {
                    "id": request["id"],
                    "ok": False,
                    "error": {"code": "wrong_epoch", "message": "secret"},
                }
                conn.sendall(json.dumps(response).encode() + b"\n")

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    assert ready.wait(5)
    try:
        with pytest.raises(ch.OwnedSocketError) as error:
            ch.v1_owned_socket_request(sock_path, b"{}")
        assert error.value.code == "wrong_epoch"
        assert "secret" not in str(error.value)
        thread.join(5)
    finally:
        sock_path.unlink(missing_ok=True)


def test_v1_owned_socket_session_reuses_connection_for_requests():
    import threading

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
                for _ in range(2):
                    request = json.loads(stream.readline())
                    received.append(request["params"])
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
        with ch.V1OwnedSocketSession(sock_path) as session:
            assert session.request(b'{"action":"register"}') == {"state": None}
            assert session.request(b'{"action":"set"}') == {"state": None}
        with pytest.raises(ch.SocketError, match="cannot be reopened"), session:
            pass
        worker.join(5)
        assert received == [{"action": "register"}, {"action": "set"}]
    finally:
        sock_path.unlink(missing_ok=True)


def test_v1_owned_socket_session_presents_capability_on_every_request():
    import threading

    sock_path = short_sock_path(uuid.uuid4().hex[:8])
    ready = threading.Event()
    prefixes = []

    def serve():
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(sock_path))
            server.listen(1)
            ready.set()
            conn, _ = server.accept()
            with conn, conn.makefile("rb") as stream:
                for _ in range(2):
                    prefix, capability, command = stream.readline().split(b" ", 2)
                    prefixes.append((prefix, capability))
                    request = json.loads(command)
                    conn.sendall(
                        json.dumps(
                            {
                                "id": request["id"],
                                "ok": True,
                                "result": {"state": None},
                            }
                        ).encode()
                        + b"\n"
                    )

    worker = threading.Thread(target=serve, daemon=True)
    worker.start()
    assert ready.wait(5)
    try:
        with ch.V1OwnedSocketSession(sock_path, capability="test-token") as session:
            session.request(b'{"action":"register"}')
            session.request(b'{"action":"set"}')
        worker.join(5)
        assert prefixes == [(b"_cmux_capability_v1", b"test-token")] * 2
    finally:
        sock_path.unlink(missing_ok=True)


def test_v1_owned_socket_session_reads_store_identity():
    import threading

    sock_path = short_sock_path(uuid.uuid4().hex[:8])
    ready = threading.Event()
    store_id = str(uuid.uuid4())
    requests = []

    def serve():
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(sock_path))
            server.listen(1)
            ready.set()
            conn, _ = server.accept()
            with conn:
                request = json.loads(conn.recv(65536))
                requests.append(request)
                conn.sendall(
                    json.dumps(
                        {
                            "id": request["id"],
                            "ok": True,
                            "result": {"store_id": store_id.upper()},
                        }
                    ).encode()
                    + b"\n"
                )

    worker = threading.Thread(target=serve, daemon=True)
    worker.start()
    assert ready.wait(5)
    try:
        with ch.V1OwnedSocketSession(sock_path) as session:
            assert session.store_identity() == store_id
        worker.join(5)
        assert requests[0]["method"] == "notification.state.info"
        assert requests[0]["params"] == {}
    finally:
        sock_path.unlink(missing_ok=True)


def test_v1_owned_backend_probe_requires_valid_store_identity(monkeypatch, tmp_path):
    events = []

    class Session:
        def __init__(self, path, **_kwargs):
            events.append(("open", path))

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            events.append(("close",))

        def store_identity(self):
            events.append(("probe",))
            return "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

        def list_notifications(self):
            events.append(("list",))
            return []

    monkeypatch.setattr(ch, "V1OwnedSocketSession", Session)
    assert ch.v1_probe_owned_backend({"CMUX_SOCKET_PATH": "/tmp/cmux"}, tmp_path)
    assert events == [
        ("open", "/tmp/cmux"),
        ("probe",),
        ("probe",),
        ("list",),
        ("close",),
    ]
    assert not ch.v1_probe_owned_backend({}, tmp_path)


def test_v1_owned_backend_probe_waits_for_unidentified_legacy_unread(
    monkeypatch, tmp_path
):
    class Session:
        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def store_identity(self):
            return str(uuid.uuid4())

        def list_notifications(self):
            return [
                {
                    "id": str(uuid.uuid4()),
                    "is_read": False,
                    "subtitle": ch.NOTIFICATION_SUBTITLE,
                }
            ]

    monkeypatch.setattr(ch, "V1OwnedSocketSession", Session)
    assert not ch.v1_probe_owned_backend({"CMUX_SOCKET_PATH": "/tmp/cmux"}, tmp_path)


def test_v1_owned_backend_rechecks_store_and_unread_notifications():
    store_id = str(uuid.uuid4())
    session = SimpleNamespace(store_identity=lambda: store_id, list_notifications=list)
    assert ch.v1_owned_backend_is_clear(session, store_id)
    session.list_notifications = lambda: [
        {
            "id": str(uuid.uuid4()),
            "is_read": False,
            "subtitle": ch.NOTIFICATION_SUBTITLE,
        }
    ]
    assert not ch.v1_owned_backend_is_clear(session, store_id)
    session.store_identity = lambda: str(uuid.uuid4())
    assert not ch.v1_owned_backend_is_clear(session, store_id)


def test_v1_supervisor_handoff_requires_complete_and_supported_backend(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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


def test_v1_supervisor_handoff_uses_existing_cmux_notification_api(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
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
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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


@pytest.mark.parametrize("visible", [[], [{"id": "old-notice", "is_read": True}]])
def test_v1_cleanup_can_skip_resolved_unproven_ordinary_notice(visible):
    state = {"v1": {"destinations": {"old": {"notification_id": "old-notice"}}}}
    resolved = ch.v1_resolved_unproven_destinations(state, visible)
    inventory = ch.v1_ordinary_cleanup_inventory(
        state, str(uuid.uuid4()), resolved_unproven=resolved
    )
    assert inventory["items"] == []


def test_v1_supervisor_attempts_handoff_after_recovery_and_discovery(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
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


def test_v1_owned_socket_send_guard_released_before_response():
    import threading

    sock_path = short_sock_path(uuid.uuid4().hex[:8])
    ready, released = threading.Event(), threading.Event()

    def serve():
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(sock_path))
            server.listen(1)
            ready.set()
            conn, _ = server.accept()
            with conn:
                request = json.loads(conn.recv(65536))
                assert released.wait(2)
                conn.sendall(
                    json.dumps(
                        {
                            "id": request["id"],
                            "ok": True,
                            "result": {"state": None},
                        }
                    ).encode()
                    + b"\n"
                )

    @contextmanager
    def guard():
        try:
            yield
        finally:
            released.set()

    worker = threading.Thread(target=serve, daemon=True)
    worker.start()
    assert ready.wait(5)
    try:
        with ch.V1OwnedSocketSession(sock_path) as session:
            assert session.request_with_send_guard(b'{"action":"set"}', guard) == {
                "state": None
            }
        worker.join(5)
        assert not worker.is_alive()
    finally:
        sock_path.unlink(missing_ok=True)


def test_v1_owned_socket_send_guard_failure_sends_nothing():
    import threading

    sock_path = short_sock_path(uuid.uuid4().hex[:8])
    ready = threading.Event()
    received = []

    def serve():
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(sock_path))
            server.listen(1)
            ready.set()
            conn, _ = server.accept()
            with conn:
                received.append(conn.recv(65536))

    @contextmanager
    def guard():
        raise ValueError("stale publication")
        yield

    worker = threading.Thread(target=serve, daemon=True)
    worker.start()
    assert ready.wait(5)
    try:
        with ch.V1OwnedSocketSession(sock_path) as session:
            with pytest.raises(ValueError, match="stale publication"):
                session.request_with_send_guard(b'{"action":"set"}', guard)
            with pytest.raises(ch.SocketError, match="session is closed"):
                session.request(b"{}")
        worker.join(5)
        assert received == [b""]
    finally:
        sock_path.unlink(missing_ok=True)


def test_v1_owned_socket_guarded_send_times_out_and_closes_connection():
    client, server = socket.socketpair()
    client.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
    session = ch.V1OwnedSocketSession("unused", timeout=5)
    session.conn = client
    released = []

    @contextmanager
    def guard():
        try:
            yield
        finally:
            released.append(True)

    try:
        start = time.monotonic()
        with pytest.raises(TimeoutError):
            session.request_with_send_guard(
                json.dumps({"data": "x" * 2_000_000}).encode(), guard
            )
        assert time.monotonic() - start < 1
        assert released == [True]
        assert session.conn is None
    finally:
        session.__exit__(None, None, None)
        server.close()


@pytest.mark.parametrize("result", [{}, {"store_id": "invalid"}, {"store_id": None}])
def test_v1_owned_socket_session_rejects_invalid_store_identity(result):
    import threading

    sock_path = short_sock_path(uuid.uuid4().hex[:8])
    ready = threading.Event()

    def serve():
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(sock_path))
            server.listen(1)
            ready.set()
            conn, _ = server.accept()
            with conn:
                request = json.loads(conn.recv(65536))
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
            with pytest.raises(ch.SocketError, match="invalid store identity"):
                session.store_identity()
            with pytest.raises(ch.SocketError, match="session is closed"):
                session.request(b"{}")
        worker.join(5)
    finally:
        sock_path.unlink(missing_ok=True)


@pytest.mark.parametrize("capability", [None, "test-token"])
def test_v1_owned_socket_session_authenticates_once_before_commands(capability):
    import threading

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
                received.append(stream.readline())
                conn.sendall(b"OK authenticated\n")
                for _ in range(2):
                    line = stream.readline()
                    if capability:
                        prefix = b"_cmux_capability_v1 test-token "
                        assert line.startswith(prefix)
                        line = line[len(prefix) :]
                    request = json.loads(line)
                    received.append(request["params"]["action"])
                    conn.sendall(
                        json.dumps(
                            {
                                "id": request["id"],
                                "ok": True,
                                "result": {"state": None},
                            }
                        ).encode()
                        + b"\n"
                    )

    worker = threading.Thread(target=serve, daemon=True)
    worker.start()
    assert ready.wait(5)
    try:
        with ch.V1OwnedSocketSession(
            sock_path, password="test-password", capability=capability
        ) as session:
            session.request(b'{"action":"register"}')
            session.request(b'{"action":"set"}')
        worker.join(5)
        prefix = b"_cmux_capability_v1 test-token " if capability else b""
        assert received == [prefix + b"auth test-password\n", "register", "set"]
    finally:
        sock_path.unlink(missing_ok=True)


def test_v1_cmux_socket_password_prefers_environment_and_reads_saved_file(tmp_path):
    directory = tmp_path / ".local" / "state" / "cmux"
    directory.mkdir(parents=True)
    (directory / "socket-control-password").write_text("saved-password\n")
    assert (
        ch.v1_cmux_socket_password({"CMUX_SOCKET_PASSWORD": "env-password\n"}, tmp_path)
        == "env-password"
    )
    assert ch.v1_cmux_socket_password({}, tmp_path) == "saved-password"
    assert (
        ch.v1_cmux_socket_password({"CMUX_SOCKET_PASSWORD": "\n"}, tmp_path)
        == "saved-password"
    )
    (directory / "socket-control-password").unlink()
    assert ch.v1_cmux_socket_password({}, tmp_path) is None


def test_v1_cmux_socket_password_normalizes_unicode_newlines(tmp_path):
    directory = tmp_path / ".local" / "state" / "cmux"
    directory.mkdir(parents=True)
    (directory / "socket-control-password").write_text("\u2028saved\u0085")
    assert (
        ch.v1_cmux_socket_password({"CMUX_SOCKET_PASSWORD": "\u2029"}, tmp_path)
        == "saved"
    )
    assert (
        ch.v1_cmux_socket_password(
            {"CMUX_SOCKET_PASSWORD": "\x0bvalue\u2028"}, tmp_path
        )
        == "value"
    )


def test_v1_cmux_socket_password_tolerates_unreadable_file(tmp_path, monkeypatch):
    def denied(*_args, **_kwargs):
        raise PermissionError("unavailable")

    monkeypatch.setattr(Path, "read_text", denied)
    assert ch.v1_cmux_socket_password({}, tmp_path) is None


def test_v1_owned_socket_publisher_pins_store_before_registering(tmp_path):
    import threading

    sock_path = short_sock_path(uuid.uuid4().hex[:8])
    ready = threading.Event()
    store_id = str(uuid.uuid4())
    requests = []

    def serve():
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(sock_path))
            server.listen(1)
            ready.set()
            conn, _ = server.accept()
            with conn, conn.makefile("rb") as stream:
                for _ in range(2):
                    request = json.loads(stream.readline())
                    requests.append(request)
                    result = (
                        {"store_id": store_id}
                        if request["method"] == "notification.state.info"
                        else {"state": None}
                    )
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
        path = tmp_path / "publisher.json"
        with ch.v1_owned_socket_transport(
            path, {"CMUX_SOCKET_PATH": str(sock_path)}, tmp_path
        ) as (session, publisher):
            assert publisher is not None
            assert session.conn is not None
        worker.join(5)
        assert requests[1]["params"]["store_id"] == store_id
        journal = ch.v1_publisher_identity(path, store_id, create=False)
        assert journal["phase"] == "active"
    finally:
        sock_path.unlink(missing_ok=True)


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


def test_v1_owned_socket_session_keeps_connection_after_rejection():
    import threading

    sock_path = short_sock_path(uuid.uuid4().hex[:8])
    ready = threading.Event()

    def serve():
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(sock_path))
            server.listen(1)
            ready.set()
            conn, _ = server.accept()
            with conn, conn.makefile("rb") as stream:
                first = json.loads(stream.readline())
                conn.sendall(
                    json.dumps(
                        {
                            "id": first["id"],
                            "ok": False,
                            "error": {"code": "target_denied"},
                        }
                    ).encode()
                    + b"\n"
                )
                second = json.loads(stream.readline())
                conn.sendall(
                    json.dumps(
                        {
                            "id": second["id"],
                            "ok": True,
                            "result": {"state": None},
                        }
                    ).encode()
                    + b"\n"
                )

    worker = threading.Thread(target=serve, daemon=True)
    worker.start()
    assert ready.wait(5)
    try:
        with ch.V1OwnedSocketSession(sock_path) as session:
            with pytest.raises(ch.OwnedSocketError, match="target_denied"):
                session.request(b'{"action":"set"}')
            assert session.request(b'{"action":"resume"}') == {"state": None}
        worker.join(5)
        assert not worker.is_alive()
    finally:
        sock_path.unlink(missing_ok=True)


def test_v1_owned_socket_session_rejects_concurrent_requests():
    import threading

    sock_path = short_sock_path(uuid.uuid4().hex[:8])
    ready = threading.Event()
    entered = threading.Event()
    release = threading.Event()
    results = []

    def serve():
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(sock_path))
            server.listen(1)
            ready.set()
            conn, _ = server.accept()
            with conn:
                request = json.loads(conn.recv(65536))
                entered.set()
                release.wait(5)
                conn.sendall(
                    json.dumps(
                        {
                            "id": request["id"],
                            "ok": True,
                            "result": {"state": None},
                        }
                    ).encode()
                    + b"\n"
                )

    worker = threading.Thread(target=serve, daemon=True)
    worker.start()
    assert ready.wait(5)
    try:
        with ch.V1OwnedSocketSession(sock_path) as session:
            request = threading.Thread(
                target=lambda: results.append(session.request(b"{}")), daemon=True
            )
            request.start()
            assert entered.wait(5)
            with pytest.raises(ch.SocketError, match="already in use"):
                session.request(b"{}")
            release.set()
            request.join(5)
            assert results == [{"state": None}]
        worker.join(5)
    finally:
        release.set()
        sock_path.unlink(missing_ok=True)


def test_v1_owned_socket_session_closes_after_lost_response():
    import threading

    sock_path = short_sock_path(uuid.uuid4().hex[:8])
    ready = threading.Event()

    def serve():
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(sock_path))
            server.listen(1)
            ready.set()
            conn, _ = server.accept()
            with conn:
                conn.recv(65536)

    worker = threading.Thread(target=serve, daemon=True)
    worker.start()
    assert ready.wait(5)
    try:
        with ch.V1OwnedSocketSession(sock_path) as session:
            with pytest.raises(ch.SocketError, match="closed the connection"):
                session.request(b'{"action":"set"}')
            with pytest.raises(ch.SocketError, match="session is closed"):
                session.request(b'{"action":"set"}')
        worker.join(5)
    finally:
        sock_path.unlink(missing_ok=True)


@pytest.mark.parametrize("malformed", ["missing_ok", "deep_json"])
def test_v1_owned_socket_request_rejects_malformed_error(malformed):
    import threading

    sock_path = short_sock_path(uuid.uuid4().hex[:8])
    ready = threading.Event()

    def serve():
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(sock_path))
            server.listen(1)
            ready.set()
            conn, _ = server.accept()
            with conn:
                request = json.loads(conn.recv(65536))
                if malformed == "missing_ok":
                    data = json.dumps(
                        {"id": request["id"], "error": {"code": "wrong_epoch"}}
                    ).encode()
                else:
                    data = b"[" * 2000 + b"]" * 2000
                conn.sendall(data + b"\n")

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    assert ready.wait(5)
    try:
        with pytest.raises(ch.SocketError) as error:
            ch.v1_owned_socket_request(sock_path, b"{}")
        assert not isinstance(error.value, ch.OwnedSocketError)
        thread.join(5)
    finally:
        sock_path.unlink(missing_ok=True)


def test_v1_owned_socket_request_uses_one_deadline():
    import threading

    sock_path = short_sock_path(uuid.uuid4().hex[:8])
    ready = threading.Event()

    def serve():
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(sock_path))
            server.listen(1)
            ready.set()
            conn, _ = server.accept()
            with conn:
                conn.recv(65536)
                with suppress(OSError):
                    for byte in b"123456789":
                        time.sleep(0.03)
                        conn.sendall(bytes([byte]))

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    assert ready.wait(5)
    try:
        with pytest.raises(TimeoutError):
            ch.v1_owned_socket_request(sock_path, b"{}", timeout=0.08)
        thread.join(5)
    finally:
        sock_path.unlink(missing_ok=True)


def test_socket_rpc_error_raises():
    import threading

    sock_path = short_sock_path("err")
    ready = threading.Event()

    def serve():
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(str(sock_path))
        srv.listen(1)
        ready.set()
        conn, _ = srv.accept()
        conn.recv(1 << 16)
        conn.sendall(b'{"id":"1","error":{"code":"bad","message":"nope"}}\n')
        conn.close()
        srv.close()

    t = threading.Thread(target=serve, daemon=True)
    t.start()
    ready.wait(5)
    with pytest.raises(ch.SocketError, match="nope"):
        ch.socket_rpc(sock_path, "ping", {})
    t.join(5)


@pytest.mark.skipif(sys.platform != "darwin", reason="requires macOS socket peer PID")
def test_v1_client_observation_reads_and_validates_private_socket():
    import threading

    path = short_sock_path("focus")
    ready = threading.Event()
    payload = {
        "version": 1,
        "pid": os.getpid(),
        "instance": "client-instance",
        "status": "ok",
        "observation": {
            "endpoint": "local",
            "boot_id": "a" * 32,
            "snapshot_revision": 4,
            "connection_generation": 2,
            "outer_focused": None,
            "pane_id": "w1:p1",
            "agent_state_change_seq": 4,
        },
    }

    def serve():
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
            listener.bind(str(path))
            listener.listen(1)
            ready.set()
            conn, _ = listener.accept()
            with conn:
                conn.sendall(json.dumps(payload).encode() + b"\n")

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    assert ready.wait(5)
    assert ch.v1_client_observation(path, os.getpid()) == payload
    thread.join(5)
    path.unlink(missing_ok=True)


def test_v1_focus_observation_requires_matching_socket_peer(monkeypatch):
    seen = []
    monkeypatch.setattr(
        ch,
        "v1_read_client_response",
        lambda path, expected_peer=None, timeout=2: seen.append(expected_peer),
    )
    assert ch.v1_client_observation("/tmp/client.sock", 42) is None
    assert seen == [42]


def test_v1_observer_response_rejects_duplicate_json_keys():
    import threading

    path = short_sock_path("duplicates")
    ready = threading.Event()

    def serve():
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
            listener.bind(str(path))
            listener.listen(1)
            ready.set()
            conn, _ = listener.accept()
            with conn:
                conn.sendall(b'{"status":"unavailable","status":"ok"}\n')

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    assert ready.wait(5)
    assert ch.v1_read_client_response(path) is None
    thread.join(5)
    path.unlink(missing_ok=True)


@pytest.mark.parametrize(
    "change",
    [
        {"version": 2},
        {"pid": 43},
        {"status": "busy"},
        {"instance": ""},
        {"observation": {"pane_id": "w1:p1", "boot_id": "a" * 32}},
    ],
)
def test_v1_client_observation_rejects_untrusted_responses(change):
    payload = {
        "version": 1,
        "pid": 42,
        "instance": "client-instance",
        "status": "ok",
        "observation": {
            "endpoint": "local",
            "boot_id": "a" * 32,
            "snapshot_revision": 4,
            "connection_generation": 2,
            "outer_focused": None,
            "pane_id": "w1:p1",
            "agent_state_change_seq": 4,
        },
    }
    assert not ch.valid_v1_client_observation({**payload, **change}, 42)


def test_v1_lsof_unix_peers_match_distinct_process_connections():
    output = "\n".join(
        [
            "p100",
            "f4",
            "d0x111",
            "n->0x222",
            "f11",
            "d0x111",
            "n->0x222",
            "p200",
            "f4",
            "d0x333",
            "n/path/herdr.sock",
            "f9",
            "d0x222",
            "n/tmp/herdr-client.sock",
            "f10",
            "d0x222",
            "n/path/herdr-client.sock",
        ]
    )
    peers = ch.v1_parse_lsof_unix_peers(output)
    assert ch.v1_lsof_processes_connected(peers, 100, 200)
    assert not ch.v1_lsof_processes_connected(peers, 100, 300)


def test_v1_lsof_client_path_accepts_equivalent_symlink(tmp_path):
    real = tmp_path / "real"
    real.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(real, target_is_directory=True)
    output = "\n".join(
        [
            "p100",
            "f4",
            "d0x111",
            "n->0x222",
            "p200",
            "f9",
            "d0x222",
            f"n{alias}/herdr-client.sock",
        ]
    )
    peers = ch.v1_parse_lsof_unix_peers(output)
    assert ch.v1_lsof_processes_connected(
        peers, 100, 200, str(real / "herdr-client.sock")
    )


def test_v1_upstream_client_inventory_requires_unique_stable_server(monkeypatch):
    monkeypatch.setattr(
        ch,
        "v1_native_process_info",
        lambda pid: {"pid": pid, "start_token": pid + 1, "tty_device": 42},
    )
    monkeypatch.setattr(ch, "v1_unix_peer_pid", lambda path, **kwargs: 200)
    monkeypatch.setattr(ch, "v1_local_generation", lambda path: "1:2")
    output = "\n".join(
        [
            "p100",
            "f4",
            "d0x111",
            "n->0x222",
            "p200",
            "f9",
            "d0x222",
            "n/tmp/herdr-client.sock",
        ]
    )
    monkeypatch.setattr(ch, "v1_bounded_command_output", lambda *args, **kwargs: output)
    result = ch.v1_upstream_local_client_inventory(100, 42, ["/tmp/herdr.sock"])
    assert result["sources"][0]["api_socket_path"] == "/tmp/herdr.sock"
    assert result["sources"][0]["socket_generation"] == "1:2"
    assert result["sources"][0]["server_pid"] == 200
    assert result["sources"][0]["server_start_token"] == 201
    assert result["observation"] is None
    assert (
        ch.v1_upstream_local_client_inventory(100, 42, ["/tmp/a.sock", "/tmp/b.sock"])
        is None
    )


def test_v1_upstream_client_inventory_rejects_api_only_connection(monkeypatch):
    monkeypatch.setattr(
        ch,
        "v1_native_process_info",
        lambda pid: {"pid": pid, "start_token": pid + 1, "tty_device": 42},
    )
    monkeypatch.setattr(ch, "v1_unix_peer_pid", lambda path, **kwargs: 200)
    monkeypatch.setattr(ch, "v1_local_generation", lambda path: "1:2")
    output = "\n".join(
        [
            "p100",
            "f4",
            "d0x111",
            "n->0x222",
            "p200",
            "f9",
            "d0x222",
            "n/tmp/herdr.sock",
        ]
    )
    monkeypatch.setattr(ch, "v1_bounded_command_output", lambda *args, **kwargs: output)
    assert ch.v1_upstream_local_client_inventory(100, 42, ["/tmp/herdr.sock"]) is None


def test_v1_upstream_client_inventory_rejects_unidentified_connection(monkeypatch):
    monkeypatch.setattr(
        ch,
        "v1_native_process_info",
        lambda pid: {"pid": pid, "start_token": pid + 1, "tty_device": 42},
    )
    monkeypatch.setattr(ch, "v1_unix_peer_pid", lambda path, **kwargs: 200)
    monkeypatch.setattr(ch, "v1_local_generation", lambda path: "1:2")
    output = "\n".join(
        [
            "p100",
            "f4",
            "d0x111",
            "n->0x222",
            "f5",
            "d0x555",
            "n->0x666",
            "p200",
            "f9",
            "d0x222",
            "n/tmp/herdr-client.sock",
        ]
    )
    monkeypatch.setattr(ch, "v1_bounded_command_output", lambda *args, **kwargs: output)
    assert ch.v1_upstream_local_client_inventory(100, 42, ["/tmp/herdr.sock"]) is None


def test_v1_upstream_client_inventory_allows_unrelated_running_server(monkeypatch):
    monkeypatch.setattr(
        ch,
        "v1_native_process_info",
        lambda pid: {"pid": pid, "start_token": pid + 1, "tty_device": 42},
    )
    monkeypatch.setattr(
        ch,
        "v1_unix_peer_pid",
        lambda path, **kwargs: 200 if path == "/tmp/a.sock" else 300,
    )
    monkeypatch.setattr(ch, "v1_local_generation", lambda path: "1:2")

    def lsof(args, **kwargs):
        server = int(args[-2].split(",")[1])
        server_socket = "0x222" if server == 200 else "0x333"
        name = "/tmp/a-client.sock" if server == 200 else "/tmp/b-client.sock"
        return f"p100\nf4\nd0x111\nn->0x222\np{server}\nf9\nd{server_socket}\nn{name}\n"

    monkeypatch.setattr(ch, "v1_bounded_command_output", lsof)
    result = ch.v1_upstream_local_client_inventory(
        100, 42, ["/tmp/b.sock", "/tmp/a.sock"]
    )
    assert result["sources"][0]["api_socket_path"] == "/tmp/a.sock"


def test_v1_upstream_client_inventory_skips_disappeared_unrelated_server(monkeypatch):
    monkeypatch.setattr(
        ch,
        "v1_native_process_info",
        lambda pid: {"pid": pid, "start_token": pid + 1, "tty_device": 42},
    )
    monkeypatch.setattr(ch, "v1_unix_peer_pid", lambda path, **kwargs: 200)

    def generation(path):
        if path == "/tmp/stale.sock":
            raise FileNotFoundError(path)
        return "1:2"

    monkeypatch.setattr(ch, "v1_local_generation", generation)
    output = "p100\nf4\nd0x111\nn->0x222\np200\nf9\nd0x222\nn/tmp/herdr-client.sock\n"
    monkeypatch.setattr(ch, "v1_bounded_command_output", lambda *args, **kwargs: output)
    result = ch.v1_upstream_local_client_inventory(
        100, 42, ["/tmp/stale.sock", "/tmp/herdr.sock"]
    )
    assert result["sources"][0]["api_socket_path"] == "/tmp/herdr.sock"


def test_v1_running_local_socket_paths_rejects_ambiguous_session_list(monkeypatch):
    monkeypatch.setattr(
        ch,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout=json.dumps(
                {
                    "sessions": [
                        {"running": True, "socket_path": "/tmp/a.sock"},
                        {"running": True, "socket_path": "/tmp/a.sock"},
                    ]
                }
            ),
            stderr="",
        ),
    )
    assert ch.v1_running_local_socket_paths() is None


def test_v1_running_local_socket_paths_preserves_usable_spelling(monkeypatch, tmp_path):
    real = tmp_path / "real"
    real.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(real, target_is_directory=True)
    path = str(alias / "herdr.sock")
    monkeypatch.setattr(
        ch,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout=json.dumps({"sessions": [{"running": True, "socket_path": path}]}),
        ),
    )
    assert ch.v1_running_local_socket_paths() == [path]


def test_v1_discovery_uses_verified_upstream_local_client(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "v1_tty_device", lambda tty: 42)
    monkeypatch.setattr(ch, "v1_observer_socket_exists", lambda path: False)
    inventory = {
        "version": 1,
        "pid": 100,
        "instance": "100:5",
        "status": "ok",
        "sources_complete": True,
        "sources": [
            {
                "endpoint": "local",
                "api_socket_path": "/tmp/herdr.sock",
                "socket_generation": "1:2",
                "connection_generation": 5,
            }
        ],
    }
    monkeypatch.setattr(
        ch, "v1_upstream_local_client_inventory", lambda *args, **kwargs: inventory
    )
    tree = {
        "windows": [
            {
                "workspaces": [
                    {
                        "id": "w",
                        "panes": [
                            {
                                "surfaces": [
                                    {
                                        "id": "s",
                                        "type": "terminal",
                                        "tty": "tty",
                                        "processes": [
                                            {
                                                "pid": 100,
                                                "name": "herdr",
                                                "tty_device": 42,
                                                "children": [],
                                            }
                                        ],
                                    }
                                ]
                            }
                        ],
                    }
                ]
            }
        ]
    }
    result = ch.v1_discover_clients_in_tree(
        tree, [tmp_path], upstream_sockets=lambda: ["/tmp/herdr.sock"]
    )
    assert result["complete"]
    assert result["clients"][0]["sources"] == inventory["sources"]
    tree["windows"][0]["workspaces"][0]["panes"][0]["surfaces"][0]["processes"][0][
        "children"
    ].append({"pid": 101, "name": "ssh", "tty_device": 0, "children": []})
    result = ch.v1_discover_clients_in_tree(
        tree, [tmp_path], upstream_sockets=lambda: ["/tmp/herdr.sock"]
    )
    assert not result["complete"]


@pytest.mark.parametrize("server_tty", [None, 42])
def test_discovery_excludes_only_verified_herdr_server(
    monkeypatch, tmp_path, server_tty
):
    monkeypatch.setattr(ch, "v1_tty_device", lambda tty: 42)
    monkeypatch.setattr(ch, "v1_observer_socket_exists", lambda path: False)
    monkeypatch.setattr(
        ch, "v1_native_process_info", lambda pid: {"pid": pid, "start_token": 5}
    )
    monkeypatch.setattr(ch, "v1_local_generation", lambda path: "1:2")
    monkeypatch.setattr(ch, "v1_unix_peer_pid", lambda path, **kwargs: 200)
    tree = {
        "windows": [
            {
                "workspaces": [
                    {
                        "id": "w",
                        "panes": [
                            {
                                "surfaces": [
                                    {
                                        "id": "s",
                                        "type": "terminal",
                                        "tty": "tty",
                                        "processes": [
                                            {
                                                "pid": 200,
                                                "name": "herdr",
                                                "tty_device": server_tty,
                                                "children": [],
                                            }
                                        ],
                                    }
                                ]
                            }
                        ],
                    }
                ]
            }
        ]
    }
    result = ch.v1_discover_clients_in_tree(
        tree, [tmp_path], upstream_sockets=lambda: ["/tmp/herdr.sock"]
    )
    assert result["complete"]
    assert result["clients"] == []
    monkeypatch.setattr(ch, "v1_unix_peer_pid", lambda path, **kwargs: 201)
    assert not ch.v1_discover_clients_in_tree(
        tree, [tmp_path], upstream_sockets=lambda: ["/tmp/herdr.sock"]
    )["complete"]


def test_server_verification_continues_past_stale_socket(monkeypatch):
    monkeypatch.setattr(
        ch, "v1_native_process_info", lambda pid: {"pid": pid, "start_token": 5}
    )

    def generation(path):
        if path == "/stale":
            raise FileNotFoundError(path)
        return "1:2"

    monkeypatch.setattr(ch, "v1_local_generation", generation)
    monkeypatch.setattr(ch, "v1_unix_peer_pid", lambda path, **kwargs: 200)
    assert ch.v1_is_verified_local_server(
        200, ["/stale", "/live"], deadline=time.monotonic() + 5
    )


def test_local_socket_provider_uses_remaining_scan_budget(monkeypatch):
    monkeypatch.setattr(ch.time, "monotonic", lambda: 100.0)
    received = []
    monkeypatch.setattr(
        ch,
        "v1_running_local_socket_paths",
        lambda *, timeout: received.append(timeout) or [],
    )
    provider = ch.v1_local_socket_provider(100.25)
    assert provider() == []
    assert provider() == []
    assert received == [0.25]


def test_v1_source_inventory_accepts_only_local_verified_socket_without_boot():
    source = {
        "endpoint": "local",
        "api_socket_path": "/tmp/herdr.sock",
        "socket_generation": "1:2",
        "connection_generation": 1,
    }
    payload = {
        "version": 1,
        "pid": 42,
        "instance": "i",
        "status": "ok",
        "sources_complete": True,
        "sources": [source],
    }
    assert ch.valid_v1_source_inventory(payload, 42)
    assert not ch.valid_v1_source_inventory(
        {**payload, "sources": [{**source, "socket_generation": "unknown"}]}, 42
    )
    remote = {
        "endpoint": "ssh:" + "a" * 32,
        "ssh_target": "tom@host",
        "session": "work",
        "socket_generation": "1:2",
        "connection_generation": 1,
    }
    assert not ch.valid_v1_source_inventory({**payload, "sources": [remote]}, 42)


def test_v1_source_inventory_accepts_unfocused_client_and_multiple_endpoints():
    payload = {
        "version": 1,
        "pid": 42,
        "instance": "instance",
        "status": "ok",
        "observation": None,
        "sources_complete": True,
        "sources": [
            {
                "endpoint": "local",
                "boot_id": "a" * 32,
                "connection_generation": 1,
                "api_socket_path": "/tmp/herdr.sock",
            },
            {
                "endpoint": "ssh:" + "b" * 32,
                "boot_id": "c" * 32,
                "connection_generation": 2,
                "ssh_target": "tom@host",
                "session": "work",
            },
        ],
    }
    assert ch.valid_v1_source_inventory(payload, 42)
    native_remote = {**payload["sources"][1], "endpoint": "local"}
    assert ch.valid_v1_source_inventory({**payload, "sources": [native_remote]}, 42)
    assert not ch.valid_v1_client_observation(payload, 42)
    assert not ch.valid_v1_source_inventory({**payload, "pid": 43}, 42)
    assert not ch.valid_v1_source_inventory({**payload, "sources_complete": 1}, 42)
    assert not ch.valid_v1_source_inventory(
        {**payload, "sources": payload["sources"] * 2}, 42
    )
    assert not ch.valid_v1_source_inventory(
        {
            **payload,
            "sources": [{**payload["sources"][0], "api_socket_path": "relative.sock"}],
        },
        42,
    )
    assert not ch.valid_v1_source_inventory(
        {
            **payload,
            "sources": [{**payload["sources"][1], "ssh_target": "-oProxyCommand=x"}],
        },
        42,
    )


def test_v1_discovery_isolates_observer_failure_between_surfaces(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "v1_tty_device", lambda tty: 100)
    monkeypatch.setattr(
        ch, "v1_native_surface_ownership", lambda *args: (set(), set(), {}, True)
    )

    def surface(name, pid):
        return {
            "id": name,
            "type": "terminal",
            "tty": "/dev/ttys001",
            "processes": [
                {"pid": pid, "name": "herdr", "tty_device": 100, "children": []}
            ],
        }

    tree = {
        "windows": [
            {
                "workspaces": [
                    {
                        "id": "w",
                        "panes": [
                            {"surfaces": [surface("good", 20), surface("bad", 30)]}
                        ],
                    }
                ]
            }
        ]
    }
    monkeypatch.setattr(
        ch, "v1_observer_socket_exists", lambda path: path.name == "20.sock"
    )
    monkeypatch.setattr(
        ch,
        "v1_client_source_inventory",
        lambda path, pid, **kwargs: {
            "pid": pid,
            "instance": "good",
            "sources_complete": True,
            "sources": [],
        },
    )
    result = ch.v1_discover_clients_in_tree(tree, [tmp_path])
    assert result["surface_complete"] == {("w", "good"): True, ("w", "bad"): False}
    assert result["complete"] is False
    tree["windows"][0]["workspaces"][0]["panes"][0]["surfaces"][1]["processes"][0][
        "pid"
    ] = 20
    assert ch.v1_discover_clients_in_tree(tree, [tmp_path]) is None


def test_v1_discovers_unfocused_clients_without_guessing_on_missing_socket(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "v1_tty_device", lambda tty: 100)
    tree = {
        "windows": [
            {
                "workspaces": [
                    {
                        "id": "w",
                        "panes": [
                            {
                                "surfaces": [
                                    {
                                        "id": "s",
                                        "type": "terminal",
                                        "tty": "/dev/ttys001",
                                        "selected": False,
                                        "processes": [
                                            {
                                                "pid": 10,
                                                "name": "zsh",
                                                "tty_device": 100,
                                                "children": [
                                                    {
                                                        "pid": 20,
                                                        "name": "herdr",
                                                        "tty_device": 100,
                                                        "children": [],
                                                    },
                                                    {
                                                        "pid": 30,
                                                        "name": "herdr",
                                                        "tty_device": 100,
                                                        "children": [],
                                                    },
                                                ],
                                            }
                                        ],
                                    }
                                ]
                            }
                        ],
                    }
                ]
            }
        ],
    }
    monkeypatch.setattr(
        ch, "v1_observer_socket_exists", lambda path: path.name == "20.sock"
    )
    monkeypatch.setattr(
        ch,
        "v1_client_source_inventory",
        lambda path, pid, **kwargs: {
            "pid": pid,
            "instance": "i",
            "sources_complete": True,
            "sources": [],
        },
    )
    result = ch.v1_discover_clients_in_tree(tree, [tmp_path])
    assert not result["complete"]
    assert result["clients"] == [
        {
            "pid": 20,
            "workspace": "w",
            "surface": "s",
            "instance": "i",
            "sources_complete": True,
            "sources": [],
        }
    ]


def test_v1_discovery_accepts_only_verified_native_transport(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "v1_tty_device", lambda tty: 100)
    client = {"pid": 42, "ppid": 40, "name": "herdr", "tty_device": 100, "children": []}
    ssh = {"pid": 43, "ppid": 40, "name": "ssh", "tty_device": 100, "children": []}
    launcher = {
        "pid": 40,
        "name": "herdr",
        "tty_device": 100,
        "children": [client, ssh],
    }
    surface = {"id": "s", "type": "terminal", "tty": "tty", "processes": [launcher]}
    tree = {
        "windows": [{"workspaces": [{"id": "w", "panes": [{"surfaces": [surface]}]}]}]
    }
    nested = {
        "pid": 42,
        "instance": "instance",
        "sources_complete": True,
        "sources": [],
    }
    claim = {"client": {"pid": 42}, "ssh_child": {"pid": 43}, "observation": nested}
    record = {"client": {"pid": 42}, "ssh_child": {"pid": 43}, "connection_id": "conn"}
    monkeypatch.setattr(
        ch,
        "v1_observer_socket_exists",
        lambda path: (
            path.parent.name == "transport-observers"
            and path.name in {"40.sock", "44.sock"}
        ),
    )
    monkeypatch.setattr(
        ch, "v1_native_transport_envelope", lambda *args, **kwargs: {"owner_pid": 40}
    )
    monkeypatch.setattr(ch, "v1_native_transport_claims", lambda value: [claim])
    monkeypatch.setattr(ch, "v1_native_transport_topology", lambda *args: [record])
    result = ch.v1_discover_clients_in_tree(
        tree, [tmp_path / "herdr" / "client-observers"]
    )
    assert result["complete"] is True
    assert result["clients"][0]["pid"] == 42
    assert result["clients"][0]["native_ownership"] == record
    surface["processes"].append(
        {"pid": 44, "name": "herdr", "tty_device": 101, "children": []}
    )
    assert (
        ch.v1_discover_clients_in_tree(tree, [tmp_path / "herdr" / "client-observers"])[
            "complete"
        ]
        is True
    )
    surface["processes"].append(
        {"pid": 45, "name": "ssh", "tty_device": 100, "children": []}
    )
    result = ch.v1_discover_clients_in_tree(
        tree, [tmp_path / "herdr" / "client-observers"]
    )
    assert result["complete"] is False


def test_v1_discovery_rejects_duplicate_pid_across_surfaces(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "v1_tty_device", lambda tty: 100)
    process = {"pid": 20, "name": "herdr", "tty_device": 100, "children": []}
    surface = {
        "id": "s1",
        "type": "terminal",
        "tty": "/dev/ttys001",
        "processes": [process],
    }
    tree = {
        "windows": [
            {
                "workspaces": [
                    {
                        "id": "w",
                        "panes": [{"surfaces": [surface, {**surface, "id": "s2"}]}],
                    }
                ]
            }
        ]
    }
    monkeypatch.setattr(ch, "v1_observer_socket_exists", lambda path: False)
    assert ch.v1_discover_clients_in_tree(tree, [tmp_path]) is None


@pytest.mark.parametrize("bad", [{"windows": {}}, {"windows": ""}])
def test_v1_discovery_rejects_malformed_window_list(bad, tmp_path):
    assert ch.v1_discover_clients_in_tree(bad, [tmp_path]) is None


@pytest.mark.parametrize("device", [None, "100", True])
def test_v1_discovery_marks_missing_process_tty_incomplete(
    monkeypatch, tmp_path, device
):
    monkeypatch.setattr(ch, "v1_tty_device", lambda tty: 100)
    process = {"pid": 20, "name": "herdr", "tty_device": device, "children": []}
    surface = {
        "id": "s",
        "type": "terminal",
        "tty": "/dev/ttys001",
        "processes": [process],
    }
    tree = {
        "windows": [{"workspaces": [{"id": "w", "panes": [{"surfaces": [surface]}]}]}]
    }
    assert ch.v1_discover_clients_in_tree(tree, [tmp_path])["complete"] is False
    surface["processes"] = {}
    assert ch.v1_discover_clients_in_tree(tree, [tmp_path]) is None


def test_v1_response_rejects_excessive_json_nesting(monkeypatch):
    class Socket:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def settimeout(self, timeout):
            pass

        def connect(self, path):
            pass

        def recv(self, size):
            return b"[" * 1100 + b"0" + b"]" * 1100 + b"\n"

    monkeypatch.setattr(ch.socket, "socket", lambda *args: Socket())
    assert ch.v1_read_client_response("/fake") is None


def test_v1_response_rejects_non_object_even_if_decoder_accepts_it(monkeypatch):
    class Socket:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def settimeout(self, timeout):
            pass

        def connect(self, path):
            pass

        def recv(self, size):
            return b"[]\n"

    monkeypatch.setattr(ch.socket, "socket", lambda *args: Socket())
    assert ch.v1_read_client_response("/fake") is None


def test_v1_response_has_absolute_deadline(monkeypatch):
    now = iter([0, 1, 3])
    reads = []

    class Socket:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def settimeout(self, timeout):
            pass

        def connect(self, path):
            pass

        def recv(self, size):
            reads.append(size)
            return b" "

    monkeypatch.setattr(ch.time, "monotonic", lambda: next(now))
    monkeypatch.setattr(ch.socket, "socket", lambda *args: Socket())
    assert ch.v1_read_client_response("/fake") is None
    assert len(reads) == 1


def test_v1_discovery_rechecks_client_instance_and_surface(monkeypatch):
    first = {
        "pid": 20,
        "workspace": "w",
        "surface": "s",
        "instance": "old",
        "sources_complete": True,
        "sources": [],
    }
    replacement = {**first, "instance": "new", "surface": "other"}
    snapshots = iter(
        [
            {"clients": [first], "complete": True},
            {"clients": [replacement], "complete": True},
        ]
    )
    monkeypatch.setattr(ch, "v1_bounded_command_output", lambda *args, **kwargs: "{}")
    monkeypatch.setattr(
        ch, "v1_discover_clients_in_tree", lambda *args, **kwargs: next(snapshots)
    )
    assert ch.v1_discover_clients({}) == {"clients": [], "complete": False}


def test_v1_discovery_preserves_independent_surface_completeness(monkeypatch):
    healthy = {
        "pid": 20,
        "workspace": "wa",
        "surface": "sa",
        "instance": "a",
        "sources_complete": True,
        "sources": [],
    }
    scan = {
        "clients": [healthy],
        "complete": False,
        "surface_complete": {("wa", "sa"): True, ("wb", "sb"): False},
    }
    snapshots = iter([scan, scan])
    monkeypatch.setattr(ch, "v1_bounded_command_output", lambda *args, **kwargs: "{}")
    monkeypatch.setattr(
        ch, "v1_discover_clients_in_tree", lambda *args, **kwargs: next(snapshots)
    )
    result = ch.v1_discover_clients({})
    assert result["complete"] is False
    assert result["surface_complete"] == {("wa", "sa"): True, ("wb", "sb"): False}
    assert result["clients"] == [healthy]


def test_v1_discovery_rejects_new_empty_surface_as_globally_complete(monkeypatch):
    first = {"clients": [], "complete": True, "surface_complete": {("w", "a"): True}}
    second = {
        "clients": [],
        "complete": True,
        "surface_complete": {("w", "a"): True, ("w", "b"): True},
    }
    snapshots = iter([first, second])
    monkeypatch.setattr(ch, "v1_bounded_command_output", lambda *args, **kwargs: "{}")
    monkeypatch.setattr(
        ch, "v1_discover_clients_in_tree", lambda *args, **kwargs: next(snapshots)
    )
    result = ch.v1_discover_clients({})
    assert not result["complete"]
    assert result["surface_complete"] == {("w", "a"): True, ("w", "b"): False}


def test_v1_discovered_targets_preserve_upstream_socket_generation():
    source = {
        "endpoint": "local",
        "api_socket_path": "/tmp/a.sock",
        "socket_generation": "123:456",
        "connection_generation": 7,
    }
    inventory = {
        "clients": [
            {
                "pid": 20,
                "instance": "i",
                "workspace": "w",
                "surface": "s",
                "sources": [source],
            }
        ]
    }
    targets = ch.v1_discovered_targets(inventory)
    target = next(iter(targets.values()))["target"]
    assert target == {
        "remote": None,
        "socket_path": "/tmp/a.sock",
        "socket_generation": "123:456",
    }


def test_v1_discovered_targets_preserve_client_source_and_boot():
    boot = "a" * 32
    inventory = {
        "complete": True,
        "clients": [
            {
                "pid": 20,
                "instance": "client-a",
                "workspace": "w",
                "surface": "s",
                "sources_complete": True,
                "sources": [
                    {
                        "endpoint": "local",
                        "api_socket_path": "/tmp/a.sock",
                        "boot_id": boot,
                        "connection_generation": 1,
                    },
                    {
                        "endpoint": "ssh:" + "b" * 32,
                        "ssh_target": "tom@host",
                        "session": "work",
                        "boot_id": boot,
                        "connection_generation": 2,
                    },
                ],
            }
        ],
    }
    targets = ch.v1_discovered_targets(inventory)
    assert len(targets) == 2
    assert {entry["target"]["server_boot_id"] for entry in targets.values()} == {boot}
    assert {entry["target"].get("remote") for entry in targets.values()} == {
        None,
        "tom@host",
    }
    assert all(
        entry["workspace"] == "w" and entry["surface"] == "s"
        for entry in targets.values()
    )
    assert all(
        entry["observed_client_pid"] == 20
        and entry["observed_client_instance"] == "client-a"
        for entry in targets.values()
    )
    assert {entry["endpoint"] for entry in targets.values()} == {
        "local",
        "ssh:" + "b" * 32,
    }
    ownership = {"connection_id": "bound"}
    inventory["clients"][0]["native_ownership"] = ownership
    native_targets = ch.v1_discovered_targets(inventory)
    assert all(
        spec["native_ownership"] == ownership for spec in native_targets.values()
    )
    state = {"v1": {"clients": {}}}
    ch.reconcile_v1_supervised_bindings(
        state, native_targets, native_targets, complete=True, supervisor_pid=1
    )
    assert all(
        record["native_ownership"] == ownership
        for record in state["v1"]["clients"].values()
    )
    del inventory["clients"][0]["native_ownership"]
    same = ch.v1_discovered_targets(inventory)
    assert targets == same
    inventory["clients"][0]["instance"] = "client-b"
    assert set(targets).isdisjoint(ch.v1_discovered_targets(inventory))


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


def test_v1_supervisor_leases_hold_prepared_bindings_and_retire_only_on_complete_scan(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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
    leases = ch.V1SupervisedLeases()
    try:
        assert leases.reconcile({"c": spec}, {"c": spec}, complete=True)
        with ch.state_lock():
            state = ch.load_state()
            ch.prune_v1_clients(state)
            assert "c" in state["v1"]["clients"]
        assert not leases.reconcile({}, {}, complete=False)
        assert "c" in ch.load_state()["v1"]["clients"]
        assert leases.reconcile({}, {}, complete=True)
        assert "c" not in ch.load_state()["v1"]["clients"]
    finally:
        leases.close()


def test_v1_supervisor_lease_conflict_keeps_existing_binding(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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
    first, second = ch.V1SupervisedLeases(), ch.V1SupervisedLeases()
    try:
        assert first.reconcile({"c": spec}, {"c": spec}, complete=True)
        assert not second.reconcile({"c": spec}, {"c": spec}, complete=True)
        assert "c" in ch.load_state()["v1"]["clients"]
    finally:
        second.close()
        first.close()


def test_v1_supervisor_leader_fences_other_supervisors(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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
    first, second = ch.V1SupervisedLeases(), ch.V1SupervisedLeases()
    try:
        assert first.reconcile({"c": spec}, {"c": spec}, complete=True)
        assert not second.reconcile({}, {}, complete=True)
        assert "c" in ch.load_state()["v1"]["clients"]
    finally:
        second.close()
        first.close()


def test_v1_supervisor_reclaims_previous_leases_before_incomplete_scan(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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
    first = ch.V1SupervisedLeases()
    assert first.reconcile({"c": spec}, {"c": spec}, complete=True)
    first.close()
    second = ch.V1SupervisedLeases()
    try:
        second.reconcile({}, {}, complete=False)
        with ch.state_lock():
            state = ch.load_state()
            ch.prune_v1_clients(state)
            assert "c" in state["v1"]["clients"]
    finally:
        second.close()


def test_v1_supervisor_releases_new_lease_if_state_write_fails(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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
    leases = ch.V1SupervisedLeases()
    monkeypatch.setattr(
        ch, "save_state", lambda state: (_ for _ in ()).throw(OSError("disk"))
    )
    with pytest.raises(OSError, match="disk"):
        leases.reconcile({"c": spec}, {"c": spec}, complete=True)
    with ch.v1_source_leader("client:c") as acquired:
        assert acquired
    leases.close()


def test_v1_supervisor_sources_prepare_once_and_keep_old_on_incomplete_discovery():
    class Worker:
        def __init__(self):
            self.ready = True
            self.alive = True

        def stop(self):
            self.alive = False

    started = []

    def start(source_id, target):
        worker = Worker()
        started.append((source_id, worker))
        return worker

    manager = ch.V1SupervisedSources(start)
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
    try:
        assert manager.reconcile({"c1": spec, "c2": spec}, complete=True) == {
            "c1": spec,
            "c2": spec,
        }
        assert len(started) == 1
        assert manager.reconcile({}, complete=False) == {}
        assert started[0][1].alive
        manager.reconcile({}, complete=True)
        assert not started[0][1].alive
    finally:
        manager.close()


def test_v1_supervisor_sources_retries_dead_worker_without_early_ready():
    class Worker:
        def __init__(self, ready):
            self.ready = ready
            self.alive = True

        def stop(self):
            self.alive = False

    started = []

    def start(source_id, target):
        worker = Worker(ready=len(started) > 0)
        started.append(worker)
        return worker

    manager = ch.V1SupervisedSources(start)
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
    try:
        assert manager.reconcile({"c": spec}, complete=True) == {}
        started[0].alive = False
        assert manager.reconcile({"c": spec}, complete=True) == {"c": spec}
        assert len(started) == 2
    finally:
        manager.close()


def test_v1_supervisor_prepares_new_boot_without_stopping_old_worker():
    class Worker:
        ready = True
        alive = True

        def stop(self):
            self.alive = False

    started = []

    def start(*args):
        worker = Worker()
        started.append(worker)
        return worker

    manager = ch.V1SupervisedSources(start)
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
    replacement = {
        **spec,
        "target": {**spec["target"], "server_boot_id": "b" * 32},
        "connection_generation": 2,
    }
    try:
        manager.reconcile({"c": spec}, complete=True)
        assert manager.reconcile({"c": replacement}, complete=False) == {
            "c": replacement
        }
        assert len(started) == 2 and all(worker.alive for worker in started)
    finally:
        manager.close()


def test_v1_supervisor_groups_symlink_aliases_for_same_source(tmp_path):
    class Worker:
        ready = True
        alive = True

        def stop(self):
            self.alive = False

    path = tmp_path / "source.sock"
    path.touch()
    alias = tmp_path / "alias.sock"
    alias.symlink_to(path)
    started = []

    def start(*args):
        worker = Worker()
        started.append(worker)
        return worker

    manager = ch.V1SupervisedSources(start)
    spec = {
        "workspace": "w",
        "surface": "s",
        "target": {
            "remote": None,
            "socket_path": str(path),
            "server_boot_id": "a" * 32,
        },
        "connection_generation": 1,
    }
    other = {**spec, "target": {**spec["target"], "socket_path": str(alias)}}
    try:
        assert manager.reconcile({"c1": spec, "c2": other}, complete=True) == {
            "c1": spec,
            "c2": other,
        }
        assert len(started) == 1
    finally:
        manager.close()


def test_v1_supervisor_preserves_discovered_socket_path(tmp_path):
    class Worker:
        ready = True
        alive = True

        def stop(self):
            self.alive = False

    path = tmp_path / "source.sock"
    path.touch()
    alias = tmp_path / "alias.sock"
    alias.symlink_to(path)
    started = []
    manager = ch.V1SupervisedSources(
        lambda key, target: started.append(target) or Worker()
    )
    spec = {
        "workspace": "w",
        "surface": "s",
        "target": {
            "remote": None,
            "socket_path": str(alias),
            "server_boot_id": "a" * 32,
        },
        "connection_generation": 1,
    }
    try:
        manager.reconcile({"c": spec}, complete=True)
        assert started[0]["socket_path"] == str(alias)
        assert manager.current_ready({"c": spec}) == {"c": spec}
    finally:
        manager.close()


def test_v1_publication_accepts_certified_surface_from_partial_discovery(monkeypatch):
    a = {"workspace": "wa", "surface": "sa", "target": {"socket_path": "/a"}}

    class Sources:
        def current_ready(self, desired):
            return desired

    monkeypatch.setattr(
        ch,
        "v1_verified_publication_targets",
        lambda desired, ready, certified: (
            {("wa", "sa")} if certified[("wa", "sa")] else set()
        ),
    )
    verification = ch.V1PublicationVerification(Sources())
    verification.update(
        {"a": a},
        complete=False,
        surface_complete={("wa", "sa"): True, ("wb", "sb"): False},
    )
    assert verification.scope() == {("wa", "sa")}
    assert verification.workspace_allowed("wa")
    assert not verification.workspace_allowed("wb")


def test_v1_publication_workspace_requires_unregistered_discovered_surface(monkeypatch):
    a = {"workspace": "w", "surface": "a", "target": {"socket_path": "/a"}}
    b = {"workspace": "w", "surface": "b", "target": {"socket_path": "/b"}}

    class Sources:
        def current_ready(self, desired):
            return {"a": a}

    monkeypatch.setattr(
        ch, "v1_verified_publication_targets", lambda desired, ready: {("w", "a")}
    )
    verification = ch.V1PublicationVerification(Sources())
    verification.update({"a": a, "b": b}, complete=True)
    assert not verification.workspace_allowed("w")


def test_v1_publication_full_scope_rejects_unsupervised_client(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    spec = {"workspace": "w", "surface": "s", "target": {"socket_path": "/socket"}}
    ch.save_state({"v1": {"clients": {"foreign": {"workspace": "w", "surface": "s"}}}})

    class Sources:
        def current_ready(self, desired):
            return desired

    monkeypatch.setattr(ch, "v1_bindings_match_clients", lambda *args, **kwargs: True)
    verification = ch.V1PublicationVerification(Sources())
    verification.update({"c": spec}, complete=True)
    assert verification.scope() == set()


def test_v1_publication_verification_revokes_on_incomplete_inventory_and_readiness(
    monkeypatch,
):
    spec = {"workspace": "w", "surface": "s", "target": {"socket_path": "/socket"}}

    class Sources:
        ready = True

        def current_ready(self, desired):
            return desired if self.ready else {}

    sources = Sources()
    monkeypatch.setattr(
        ch,
        "v1_verified_publication_targets",
        lambda desired, ready: {("w", "s")} if ready else set(),
    )
    verification = ch.V1PublicationVerification(sources)
    assert verification.eligible() == set()
    verification.update({"c": spec}, complete=True)
    assert verification.eligible() == {("w", "s")}
    sources.ready = False
    assert verification.eligible() == set()
    verification.update({"c": spec}, complete=False)
    sources.ready = True
    assert verification.eligible() == set()


def test_v1_supervisor_sources_checks_readiness_without_discovery():
    class Worker:
        ready = True
        alive = True

        def stop(self):
            self.alive = False

    worker = Worker()
    manager = ch.V1SupervisedSources(lambda *args: worker)
    spec = {
        "workspace": "w",
        "surface": "s",
        "connection_generation": 1,
        "target": {
            "remote": None,
            "socket_path": "/tmp/source.sock",
            "server_boot_id": "a" * 32,
        },
    }
    try:
        assert manager.reconcile({"c": spec}, complete=False) == {"c": spec}
        assert manager.current_ready({"c": spec}) == {"c": spec}
        worker.ready = False
        assert manager.current_ready({"c": spec}) == {}
        worker.ready = True
        assert manager.current_ready({"c": spec}) == {"c": spec}
    finally:
        manager.close()


def test_v1_supervisor_sources_retains_handle_if_stop_fails():
    class Worker:
        ready = True
        alive = True
        fail = True

        def stop(self):
            if self.fail:
                raise OSError("stop failed")
            self.alive = False

    worker = Worker()
    manager = ch.V1SupervisedSources(lambda *args: worker)
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
    assert manager.reconcile({"c": spec}, complete=True) == {"c": spec}
    with pytest.raises(OSError, match="stop failed"):
        manager.reconcile({}, complete=True)
    worker.fail = False
    manager.close()
    assert not worker.alive


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


def test_v1_supervisor_reconcile_keeps_old_source_until_binding_commit_succeeds(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")

    class Worker:
        ready = True
        alive = True

        def stop(self):
            self.alive = False

    started = []

    def start(*args):
        worker = Worker()
        started.append(worker)
        return worker

    sources, leases = ch.V1SupervisedSources(start), ch.V1SupervisedLeases()
    boot = "a" * 32

    def inventory(socket):
        return {
            "complete": True,
            "clients": [
                {
                    "pid": 1,
                    "instance": "i",
                    "workspace": "w",
                    "surface": "s",
                    "sources_complete": True,
                    "sources": [
                        {
                            "endpoint": "local",
                            "api_socket_path": socket,
                            "boot_id": boot,
                            "connection_generation": 1,
                        }
                    ],
                }
            ],
        }

    try:
        assert ch.v1_reconcile_supervisor_inventory(
            inventory("/old.sock"), sources, leases
        )
        with monkeypatch.context() as context:
            context.setattr(
                ch, "save_state", lambda state: (_ for _ in ()).throw(OSError("disk"))
            )
            with pytest.raises(OSError):
                ch.v1_reconcile_supervisor_inventory(
                    inventory("/new.sock"), sources, leases
                )
        assert started[0].alive
        assert ch.v1_reconcile_supervisor_inventory(
            inventory("/new.sock"), sources, leases
        )
        assert not started[0].alive and started[1].alive
    finally:
        sources.close()
        leases.close()


def test_v1_supervisor_publishes_ordinary_notifications_for_verified_surface(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
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

    monkeypatch.setattr(ch, "state_dir", tmp_path)
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

    monkeypatch.setattr(ch, "state_dir", tmp_path)
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


def test_v1_supervisor_publishes_while_discovery_is_stalled(monkeypatch, tmp_path):
    import threading

    monkeypatch.setattr(ch, "state_dir", tmp_path)
    (tmp_path / "v1-enabled").write_text("enabled")
    entered, release, published = (
        threading.Event(),
        threading.Event(),
        threading.Event(),
    )
    first_entered, first_release = threading.Event(), threading.Event()
    stop = threading.Event()

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

        def sync_tokens(self):
            pass

        def reconcile(self, *args, **kwargs):
            return {}

        def current_ready(self, desired):
            return {}

        def close(self):
            pass

    def discover(cfg):
        if not first_entered.is_set():
            first_entered.set()
            first_release.wait(5)
            return {"complete": True, "clients": []}
        entered.set()
        release.wait(5)
        return None

    monkeypatch.setattr(ch, "V1SupervisedLeases", Leases)
    monkeypatch.setattr(ch, "V1SupervisedSources", Sources)
    monkeypatch.setattr(ch, "v1_recover_legacy_handoff", lambda: True)
    monkeypatch.setattr(ch, "v1_reconcile_supervisor_inventory", lambda *args: None)
    monkeypatch.setattr(
        ch, "v1_supervisor_bindings_match", lambda *args, **kwargs: True
    )
    monkeypatch.setattr(ch, "v1_discover_clients", discover)
    monkeypatch.setattr(ch, "publish_v1_clients", lambda cfg, **kwargs: published.set())
    worker = threading.Thread(target=ch.v1_run_supervisor, args=({}, stop))
    worker.start()
    try:
        assert first_entered.wait(3)
        assert not published.wait(0.25), "unverified startup bindings were published"
        first_release.set()
        assert entered.wait(3)
        published.clear()
        assert published.wait(2), "publication waited for stalled discovery"
    finally:
        stop.set()
        first_release.set()
        release.set()
        worker.join(timeout=5)
    assert not worker.is_alive()


def test_v1_source_process_reads_queued_ready_after_deadline(monkeypatch):
    read_fd, write_fd = os.pipe()
    worker = object.__new__(ch.V1SupervisedSourceProcess)
    worker._process = SimpleNamespace(pid=123, stdout=os.fdopen(read_fd, "rb"))
    worker._deadline = 1
    worker._buffer = b""
    worker._ready = False
    worker._closed = False
    try:
        os.write(write_fd, b"ready\n")
        monkeypatch.setattr(ch.time, "monotonic", lambda: 2)
        monkeypatch.setattr(ch, "v1_wait_child_state", lambda pid: None)
        assert worker.alive
    finally:
        os.close(write_fd)
        worker._process.stdout.close()


def test_v1_supervisor_preserves_but_does_not_publish_unverified_bindings(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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
    previous = ch.V1SupervisedLeases()
    assert previous.reconcile({"c": spec}, {"c": spec}, complete=True)
    previous.close()
    (tmp_path / "v1-enabled").write_text("enabled")

    class Stop:
        done = False

        def is_set(self):
            return self.done

        def wait(self, seconds):
            self.done = True

    monkeypatch.setattr(ch, "v1_discover_clients", lambda cfg: None)
    seen = []

    def publish(cfg):
        with ch.state_lock():
            state = ch.load_state()
            ch.prune_v1_clients(state)
            seen.append("c" in state["v1"]["clients"])

    monkeypatch.setattr(ch, "publish_v1_clients", publish)
    ch.v1_run_supervisor({}, Stop())
    assert seen == []
    assert "c" in ch.load_state()["v1"]["clients"]


def test_v1_handoff_coverage_requires_every_legacy_source_and_ready_binding():
    target = {
        "remote": None,
        "socket_path": "/tmp/source.sock",
        "server_boot_id": "a" * 32,
    }
    spec = {
        "workspace": "w",
        "surface": "s",
        "target": target,
        "connection_generation": 1,
    }
    desired = {"c": spec}
    legacy = {
        ch.v1_source_key(target),
        ch.v1_source_key({"remote": "host", "session": "work"}),
    }

    class Worker:
        alive = True
        ready = True

        def stop(self):
            pass

    sources = ch.V1SupervisedSources(lambda *_: Worker())
    sources.reconcile(desired, complete=False)
    assert not ch.v1_handoff_has_coverage(True, desired, desired, legacy, sources)
    assert not ch.v1_handoff_has_coverage(
        False, desired, desired, {ch.v1_source_key(target)}, sources
    )
    assert not ch.v1_handoff_has_coverage(
        True, desired, {}, {ch.v1_source_key(target)}, sources
    )
    assert ch.v1_handoff_has_coverage(
        True, desired, desired, {ch.v1_source_key(target)}, sources
    )
    sources.close()


def test_v1_handoff_does_not_credit_moved_socket_alias_or_dead_worker(tmp_path):
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.touch()
    second.touch()
    alias = tmp_path / "alias"
    alias.symlink_to(first)

    class Worker:
        alive = True
        ready = True

        def stop(self):
            self.alive = False

    sources = ch.V1SupervisedSources(lambda *_: Worker())
    spec = {
        "workspace": "w",
        "surface": "s",
        "connection_generation": 1,
        "target": {"socket_path": str(alias), "server_boot_id": "a" * 32},
    }
    desired = {"c": spec}
    ready = sources.reconcile(desired, complete=False)
    assert ch.v1_handoff_has_coverage(
        True, desired, ready, {ch.v1_source_key(spec["target"])}, sources
    )
    alias.unlink()
    alias.symlink_to(second)
    assert not ch.v1_handoff_has_coverage(
        True, desired, ready, {ch.v1_source_key(spec["target"])}, sources
    )
    alias.unlink()
    alias.symlink_to(first)
    next(iter(sources._workers.values()))["worker"].alive = False
    assert not ch.v1_handoff_has_coverage(
        True, desired, ready, {ch.v1_source_key(spec["target"])}, sources
    )
    sources.close()


def test_v1_supervisor_bindings_match_requires_exact_owned_surface(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    spec = {
        "workspace": "w",
        "surface": "s",
        "connection_generation": 1,
        "target": {"socket_path": "/tmp/source.sock", "server_boot_id": "a" * 32},
    }
    desired = {"c": spec}
    state = {}
    ch.reconcile_v1_supervised_bindings(
        state, desired, desired, complete=True, supervisor_pid=os.getpid()
    )
    ch.save_state(state)
    assert ch.v1_supervisor_bindings_match(desired)
    state["v1"]["clients"]["c"]["surface"] = "other"
    ch.save_state(state)
    assert not ch.v1_supervisor_bindings_match(desired)


def test_v1_supervisor_handoff_rolls_back_when_coverage_changes(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    spec = {
        "workspace": "w",
        "surface": "s",
        "connection_generation": 1,
        "target": {"socket_path": "/tmp/source.sock", "server_boot_id": "a" * 32},
    }
    desired = {"c": spec}
    snapshots = iter(
        [{"complete": True, "clients": [1]}, {"complete": False, "clients": [1]}]
    )
    monkeypatch.setattr(ch, "v1_discover_clients", lambda cfg: next(snapshots))
    monkeypatch.setattr(ch, "v1_discovered_targets", lambda inventory: desired)
    monkeypatch.setattr(ch, "v1_legacy_source_inventory", lambda cfg: set())
    monkeypatch.setattr(ch, "v1_handoff_has_coverage", lambda complete, *args: complete)
    monkeypatch.setattr(ch, "v1_probe_source", lambda target: True)
    monkeypatch.setattr(ch, "v1_supervisor_bindings_match", lambda desired: True)
    calls = []
    monkeypatch.setattr(
        ch, "retire_v1_legacy_daemons", lambda plan: calls.append("retire") or True
    )
    monkeypatch.setattr(
        ch, "pending_v1_legacy_restores", lambda: [("watch", "work", 123)]
    )
    monkeypatch.setattr(
        ch, "restart_legacy_daemons", lambda plan: calls.append("restore")
    )

    class Sources:
        def reconcile(self, desired, *, complete):
            return desired

    assert not ch.v1_try_supervisor_handoff({}, Sources(), ch.threading.Event())
    assert "retire" not in calls
    assert "restore" in calls
    assert not (tmp_path / "v1-enabled").exists()


def test_v1_supervisor_handoff_restores_writers_if_final_validation_fails(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    desired = {"c": {"target": {"socket_path": "/tmp/a", "server_boot_id": "a" * 32}}}
    snapshots = iter([True, True, False])
    monkeypatch.setattr(
        ch, "v1_discover_clients", lambda cfg: {"complete": next(snapshots)}
    )
    monkeypatch.setattr(ch, "v1_discovered_targets", lambda inventory: desired)
    monkeypatch.setattr(ch, "v1_legacy_source_inventory", lambda cfg: {"legacy"})
    monkeypatch.setattr(ch, "v1_handoff_has_coverage", lambda complete, *args: complete)
    monkeypatch.setattr(ch, "v1_probe_source", lambda target: True)
    monkeypatch.setattr(ch, "v1_supervisor_bindings_match", lambda desired: True)
    calls = []
    monkeypatch.setattr(
        ch, "retire_v1_legacy_daemons", lambda plan: calls.append("retire") or True
    )
    monkeypatch.setattr(
        ch, "pending_v1_legacy_restores", lambda: [("watch", "work", 123)]
    )
    monkeypatch.setattr(
        ch, "restart_legacy_daemons", lambda plan: calls.append("restore")
    )

    class Sources:
        def reconcile(self, desired, *, complete):
            return desired

    assert not ch.v1_try_supervisor_handoff({}, Sources(), ch.threading.Event())
    assert calls == ["retire", "restore"]
    assert not (tmp_path / "v1-enabled").exists()


def test_v1_supervisor_handoff_commits_only_after_retirement(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    desired = {"c": {"target": {"socket_path": "/tmp/a", "server_boot_id": "a" * 32}}}
    monkeypatch.setattr(ch, "v1_discover_clients", lambda cfg: {"complete": True})
    monkeypatch.setattr(ch, "v1_discovered_targets", lambda inventory: desired)
    monkeypatch.setattr(ch, "v1_legacy_source_inventory", lambda cfg: {"legacy"})
    monkeypatch.setattr(ch, "v1_handoff_has_coverage", lambda *args: True)
    monkeypatch.setattr(ch, "v1_supervisor_bindings_match", lambda desired: True)
    monkeypatch.setattr(ch, "v1_probe_source", lambda target: True)
    monkeypatch.setattr(ch, "pending_v1_legacy_restores", lambda: [])
    calls = []

    def retire(plan):
        assert (tmp_path / "v1-enabled").read_text() == "pending"
        calls.append("retire")
        return True

    monkeypatch.setattr(ch, "retire_v1_legacy_daemons", retire)
    monkeypatch.setattr(
        ch, "update_v1_legacy_restores", lambda update: calls.append("clear journal")
    )

    class Sources:
        def reconcile(self, desired, *, complete):
            return desired

    def final_check():
        calls.append("final check")
        return True

    assert ch.v1_try_supervisor_handoff(
        {}, Sources(), ch.threading.Event(), final_check=final_check
    )
    assert calls == ["retire", "final check", "clear journal"]
    assert (tmp_path / "v1-enabled").read_text() == "enabled"


def test_v1_supervisor_handoff_rolls_back_when_final_backend_check_fails(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    desired = {"c": {"target": {"socket_path": "/tmp/a", "server_boot_id": "a" * 32}}}
    monkeypatch.setattr(ch, "v1_discover_clients", lambda cfg: {"complete": True})
    monkeypatch.setattr(ch, "v1_discovered_targets", lambda inventory: desired)
    monkeypatch.setattr(ch, "v1_legacy_source_inventory", lambda cfg: set())
    monkeypatch.setattr(ch, "v1_handoff_has_coverage", lambda *args: True)
    monkeypatch.setattr(ch, "v1_supervisor_bindings_match", lambda desired: True)
    monkeypatch.setattr(ch, "v1_probe_source", lambda target: True)
    monkeypatch.setattr(ch, "pending_v1_legacy_restores", lambda: [])
    calls = []
    monkeypatch.setattr(
        ch, "retire_v1_legacy_daemons", lambda plan: calls.append("retire") or True
    )
    monkeypatch.setattr(
        ch, "restart_legacy_daemons", lambda plan: calls.append("restore")
    )
    sources = SimpleNamespace(reconcile=lambda desired, complete: desired)

    assert not ch.v1_try_supervisor_handoff(
        {}, sources, ch.threading.Event(), final_check=lambda: False
    )
    assert calls == ["retire", "restore"]
    assert not (tmp_path / "v1-enabled").exists()


def test_v1_supervisor_handoff_recovers_interrupted_pending_marker(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    (tmp_path / "v1-enabled").write_text("pending")
    plan = [("watch", "work", 123)]
    monkeypatch.setattr(ch, "pending_v1_legacy_restores", lambda: plan)
    calls = []
    monkeypatch.setattr(ch, "restart_legacy_daemons", lambda value: calls.append(value))
    assert not ch.v1_try_supervisor_handoff({}, object(), ch.threading.Event())
    assert calls == [plan]
    assert not (tmp_path / "v1-enabled").exists()


def test_v1_handoff_rolls_back_if_hook_arrives_before_commit(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(ch, "load_config_strict", lambda: {})
    desired = {"c": {"target": {"socket_path": "/tmp/a", "server_boot_id": "a" * 32}}}
    monkeypatch.setattr(ch, "v1_discover_clients", lambda cfg: {"complete": True})
    monkeypatch.setattr(ch, "v1_discovered_targets", lambda inventory: desired)
    monkeypatch.setattr(ch, "v1_legacy_source_inventory", lambda cfg: set())
    monkeypatch.setattr(ch, "v1_handoff_has_coverage", lambda *args: True)
    monkeypatch.setattr(ch, "v1_supervisor_bindings_match", lambda desired: True)
    monkeypatch.setattr(ch, "v1_probe_source", lambda target: True)
    monkeypatch.setattr(ch, "pending_v1_legacy_restores", lambda: [])
    monkeypatch.setattr(ch, "restart_legacy_daemons", lambda plan: None)
    replayed = []
    monkeypatch.setattr(
        ch, "handle_event", lambda cfg, state, event, **kwargs: replayed.append(event)
    )
    event = status_event("p", "w", "done")

    def retire(plan):
        ch.save_state(
            {"v1_pending_legacy_events": [{"session": "work", "event": event}]}
        )
        return True

    monkeypatch.setattr(ch, "retire_v1_legacy_daemons", retire)

    class Sources:
        def reconcile(self, desired, *, complete):
            return desired

    assert not ch.v1_try_supervisor_handoff({}, Sources(), ch.threading.Event())
    assert replayed == [event]
    assert not (tmp_path / "v1-enabled").exists()


def test_v1_recovery_retries_journal_without_marker_or_client_discovery(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    plan = [("watch", "work", 123)]
    monkeypatch.setattr(ch, "pending_v1_legacy_restores", lambda: plan)
    calls = []
    monkeypatch.setattr(ch, "restart_legacy_daemons", lambda value: calls.append(value))
    assert not ch.v1_recover_legacy_handoff()
    assert calls == [plan]
    assert not (tmp_path / "v1-enabled").exists()


def test_v1_pending_events_are_replayed_on_rollback(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(ch, "load_config_strict", lambda: {})
    monkeypatch.setattr(ch, "pending_v1_legacy_restores", lambda: [])
    marker = tmp_path / "v1-enabled"
    marker.write_text("pending")
    events = [
        status_event("p", "w", "working"),
        status_event("p", "w", "done"),
    ]
    monkeypatch.setattr(sys, "argv", ["cmux_herdr.py", "event"])
    monkeypatch.setenv("HERDR_SESSION", "work")
    for event in events:
        monkeypatch.setenv("HERDR_PLUGIN_EVENT_JSON", json.dumps(event))
        assert ch.main() == 0
    captured = []
    monkeypatch.setattr(
        ch,
        "handle_event",
        lambda cfg, state, event, **kw: captured.append((kw["session"], event)),
    )
    assert ch.v1_recover_legacy_handoff()
    assert captured == [("work", event) for event in events]
    assert not marker.exists()
    assert not ch.load_state().get("v1_pending_legacy_events")


def test_v1_pending_completion_rebuilds_legacy_notification(
    monkeypatch, tmp_path, cfg, cmux_calls
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(ch, "load_config", lambda: cfg)
    monkeypatch.setattr(ch, "load_config_strict", lambda: cfg)
    monkeypatch.setattr(ch, "pending_v1_legacy_restores", lambda: [])
    (tmp_path / "v1-enabled").write_text("pending")
    monkeypatch.setattr(sys, "argv", ["cmux_herdr.py", "event"])
    monkeypatch.setenv("HERDR_SESSION", "work")
    for status in ("working", "done"):
        monkeypatch.setenv(
            "HERDR_PLUGIN_EVENT_JSON", json.dumps(status_event("p", "w1", status))
        )
        assert ch.main() == 0
    assert not cmux_calls
    monkeypatch.setenv("HERDR_SESSION", "other")
    assert ch.v1_recover_legacy_handoff()
    assert (
        ch.load_state()["sessions"]["work"]["workspaces"]["w1"]["panes"]["p"]["status"]
        == "done"
    )
    assert any(
        call[0] == "notify" and "claude: finished" in call for call in cmux_calls
    )


def test_v1_pending_watcher_idle_follows_queued_completion(
    monkeypatch, tmp_path, cfg, cmux_calls
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(ch, "load_config", lambda: cfg)
    monkeypatch.setattr(ch, "load_config_strict", lambda: cfg)
    monkeypatch.setattr(ch, "pending_v1_legacy_restores", lambda: [])
    monkeypatch.setattr(ch, "pane_is_tracked", lambda *args: False)
    monkeypatch.setattr(ch, "socket_rpc", lambda *args: {"agents": []})
    (tmp_path / "v1-enabled").write_text("pending")
    monkeypatch.setattr(sys, "argv", ["cmux_herdr.py", "event"])
    monkeypatch.setenv("HERDR_SESSION", "work")
    monkeypatch.setenv(
        "HERDR_PLUGIN_EVENT_JSON", json.dumps(status_event("p", "w1", "done"))
    )
    assert ch.main() == 0
    ch.handle_watch_event(
        cfg, "/socket", {"workspace_id": "w1", "pane_id": "p", "agent_status": "idle"}
    )
    assert ch.v1_recover_legacy_handoff()
    assert "p" not in ch.load_state()["sessions"]["work"]["workspaces"]["w1"]["panes"]


def test_v1_replay_does_not_resurrect_delayed_completion(
    monkeypatch, tmp_path, cfg, cmux_calls
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(ch, "load_config_strict", lambda: cfg)
    monkeypatch.setattr(ch, "pending_v1_legacy_restores", lambda: [])
    monkeypatch.setattr(ch, "pane_is_tracked", lambda *args: False)
    monkeypatch.setattr(ch, "socket_rpc", lambda *args: {"agents": []})
    (tmp_path / "v1-enabled").write_text("pending")
    ch.save_state(
        {
            "v1_pending_legacy_events": [
                {
                    "kind": "watch",
                    "session": "work",
                    "socket_path": "/socket",
                    "event": {
                        "workspace_id": "w1",
                        "pane_id": "p",
                        "agent_status": "idle",
                    },
                },
                {"session": "work", "event": status_event("p", "w1", "done")},
            ]
        }
    )
    assert ch.v1_recover_legacy_handoff()
    assert not any(call[0] == "notify" for call in cmux_calls)


def test_v1_replay_retains_uncertain_completion_for_retry(
    monkeypatch, tmp_path, cfg, cmux_calls
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(ch, "load_config_strict", lambda: cfg)
    monkeypatch.setattr(ch, "pending_v1_legacy_restores", lambda: [])
    monkeypatch.setattr(ch, "pane_is_tracked", lambda *args: False)

    def unavailable(*args):
        raise ch.SocketError("offline")

    monkeypatch.setattr(ch, "socket_rpc", unavailable)
    marker = tmp_path / "v1-enabled"
    marker.write_text("pending")
    ch.save_state(
        {
            "v1_pending_legacy_events": [
                {
                    "kind": "watch",
                    "session": "work",
                    "socket_path": "/socket",
                    "event": {
                        "workspace_id": "w1",
                        "pane_id": "p",
                        "agent_status": "idle",
                    },
                },
                {"session": "work", "event": status_event("p", "w1", "done")},
            ]
        }
    )
    assert not ch.v1_recover_legacy_handoff()
    assert marker.exists()
    assert len(ch.load_state()["v1_pending_legacy_events"]) == 2
    assert not any(call[0] == "notify" for call in cmux_calls)
    monkeypatch.setattr(
        ch,
        "socket_rpc",
        lambda *args: {"agents": [{"pane_id": "p", "agent_status": "done"}]},
    )
    assert ch.v1_recover_legacy_handoff()
    assert any(call[0] == "notify" for call in cmux_calls)


def test_v1_recovery_keeps_marker_when_journal_state_is_corrupt(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    (tmp_path / "v1-enabled").write_text("pending")
    (tmp_path / "state.json").write_text("{broken")
    with pytest.raises(ValueError):
        ch.v1_recover_legacy_handoff()
    assert (tmp_path / "v1-enabled").read_text() == "pending"
    assert (tmp_path / "state.json").read_text() == "{broken"


def test_v1_replay_keeps_offline_watcher_event_for_retry(monkeypatch, tmp_path, cfg):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(ch, "load_config_strict", lambda: cfg)
    monkeypatch.setattr(ch, "pending_v1_legacy_restores", lambda: [])
    monkeypatch.setattr(
        ch, "socket_rpc", lambda *args: (_ for _ in ()).throw(ch.SocketError("offline"))
    )
    (tmp_path / "v1-enabled").write_text("pending")
    ch.save_state(
        {
            "v1_pending_legacy_events": [
                {
                    "kind": "watch",
                    "session": "work",
                    "socket_path": "/socket",
                    "event": {
                        "workspace_id": "w1",
                        "pane_id": "p",
                        "agent_status": "idle",
                    },
                }
            ]
        }
    )
    assert not ch.v1_recover_legacy_handoff()
    assert (tmp_path / "v1-enabled").exists()
    assert len(ch.load_state()["v1_pending_legacy_events"]) == 1


def test_v1_replay_retains_watcher_event_for_malformed_agent_state(
    monkeypatch, tmp_path, cfg
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(ch, "load_config_strict", lambda: cfg)
    monkeypatch.setattr(ch, "pending_v1_legacy_restores", lambda: [])
    monkeypatch.setattr(ch, "socket_rpc", lambda *args: {"agents": [{"pane_id": "p"}]})
    (tmp_path / "v1-enabled").write_text("pending")
    ch.save_state(
        {
            "v1_pending_legacy_events": [
                {
                    "kind": "watch",
                    "session": "work",
                    "socket_path": "/socket",
                    "event": {
                        "workspace_id": "w1",
                        "pane_id": "p",
                        "agent_status": "idle",
                    },
                }
            ]
        }
    )
    assert not ch.v1_recover_legacy_handoff()
    assert (tmp_path / "v1-enabled").exists()


def test_v1_replay_uses_original_event_workspace(monkeypatch, tmp_path, cmux_calls):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(ch, "load_config", lambda: {})
    monkeypatch.setattr(ch, "load_config_strict", lambda: {})
    monkeypatch.setattr(ch, "pending_v1_legacy_restores", lambda: [])
    (tmp_path / "v1-enabled").write_text("pending")
    monkeypatch.setattr(sys, "argv", ["cmux_herdr.py", "event"])
    monkeypatch.setenv("HERDR_SESSION", "source")
    monkeypatch.setenv("CMUX_WORKSPACE_ID", "workspace:source")
    monkeypatch.setenv(
        "HERDR_PLUGIN_EVENT_JSON", json.dumps(status_event("p", "w1", "done"))
    )
    assert ch.main() == 0
    monkeypatch.setenv("HERDR_SESSION", "other")
    monkeypatch.setenv("CMUX_WORKSPACE_ID", "workspace:other")
    assert ch.v1_recover_legacy_handoff()
    notifications = [call for call in cmux_calls if call[0] == "notify"]
    assert notifications
    assert all(
        call[call.index("--workspace") + 1] == "workspace:source"
        for call in notifications
    )


def test_legacy_restore_ack_waits_for_stable_subscription(monkeypatch):
    class Connection:
        def makefile(self, mode):
            return iter([b'{"result":{"type":"subscription_started"}}\n'])

        def close(self):
            pass

    monkeypatch.setattr(ch, "open_subscription", lambda *args: Connection())
    ids = iter([["p"], ["other"], ["p"], ["p"]])
    monkeypatch.setattr(ch, "list_pane_ids", lambda path: next(ids))
    ready = []
    ch.watch_connection({}, "/socket", on_ready=lambda: ready.append(True))
    assert not ready
    ch.watch_connection({}, "/socket", on_ready=lambda: ready.append(True))
    assert ready == [True]


def test_legacy_restore_ack_rejects_failed_subscription(monkeypatch):
    class Connection:
        def makefile(self, mode):
            return iter([b'{"error":{"message":"rejected"}}\n'])

        def close(self):
            pass

    monkeypatch.setattr(ch, "open_subscription", lambda *args: Connection())
    monkeypatch.setattr(ch, "list_pane_ids", lambda path: ["p"])
    ready = []
    with pytest.raises(ch.SocketError):
        ch.watch_connection({}, "/socket", on_ready=lambda: ready.append(True))
    assert not ready


def test_legacy_ready_winner_acknowledges_only_its_session(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(
        ch.os, "kill", lambda *args: (_ for _ in ()).throw(ProcessLookupError())
    )
    ch.update_v1_legacy_restores(
        lambda entries: [
            ("watch", "work", 123),
            ("watch", "other", 456),
            ("remote", None, 789),
        ]
    )
    ch.ack_v1_legacy_ready("watch", "work")
    assert ch.pending_v1_legacy_restores() == [
        ("watch", "other", 456),
        ("remote", None, 789),
    ]


def test_legacy_ready_does_not_acknowledge_a_still_live_old_writer(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch.os, "kill", lambda *args: None)
    ch.update_v1_legacy_restores(lambda entries: [("watch", "work", 123)])
    ch.ack_v1_legacy_ready("watch", "work")
    assert ch.pending_v1_legacy_restores() == [("watch", "work", 123)]


def test_legacy_ready_uses_recorded_incarnation_when_pid_is_reused(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    token = [1] * 8
    ch.update_v1_legacy_restores(lambda entries: [("watch", "work", 123, token)])
    monkeypatch.setattr(
        ch,
        "v1_daemon_incarnation_exited",
        lambda pid, saved: pid == 123 and saved == token,
    )
    monkeypatch.setattr(ch.os, "kill", lambda *args: None)
    ch.ack_v1_legacy_ready("watch", "work")
    assert ch.pending_v1_legacy_restores() == []


def test_normal_watcher_start_acknowledges_old_restore_after_ready(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "current_session", lambda: "work")
    monkeypatch.delenv("CMUX_HERDR_RESTORE_PID", raising=False)
    monkeypatch.setattr(ch, "take_over_daemon", lambda *args: None)
    monkeypatch.setattr(ch.signal, "signal", lambda *args: None)
    monkeypatch.setattr(
        ch.os, "kill", lambda *args: (_ for _ in ()).throw(ProcessLookupError())
    )
    ch.update_v1_legacy_restores(lambda entries: [("watch", "work", 123)])

    def run_watch(cfg, *, on_ready):
        assert ch.pending_v1_legacy_restores() == [("watch", "work", 123)]
        on_ready()

    monkeypatch.setattr(ch, "watch_loop", run_watch)
    assert ch.watch_daemon({}) == 0
    assert ch.pending_v1_legacy_restores() == []


def test_v1_legacy_inventory_requires_verified_running_watcher_sources(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    watch = ch.watch_pid_path("work")
    watch.write_text(json.dumps({"pid": 123, "handoff_protocol": 1}))
    monkeypatch.setattr(
        ch, "live_daemon_pid", lambda path, mode: 123 if path == watch else None
    )
    monkeypatch.setattr(ch, "known_sessions", lambda **kwargs: {"work"})
    monkeypatch.setattr(
        ch, "v1_local_socket", lambda name: "/tmp/work.sock" if name == "work" else None
    )
    assert ch.v1_legacy_source_inventory({}) == {
        ch.v1_source_key({"socket_path": "/tmp/work.sock"})
    }
    watch.write_text(json.dumps({"pid": 123}))
    assert ch.v1_legacy_source_inventory({}) is None
    watch.write_text(json.dumps({"pid": 123, "handoff_protocol": 1}))
    monkeypatch.setattr(ch, "v1_local_socket", lambda name: None)
    assert ch.v1_legacy_source_inventory({}) is None


def test_v1_legacy_inventory_includes_configured_remote_writers(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(ch, "known_sessions", lambda **kwargs: set())
    monkeypatch.setattr(
        ch, "live_daemon_pid", lambda path, mode: 123 if mode == "remote" else None
    )
    cfg = {
        "remotes": [{"ssh_target": "tom@host", "session": "work", "cmux_title": "work"}]
    }
    monkeypatch.setattr(ch, "load_config_strict", lambda: cfg)
    (tmp_path / "remote.pid").write_text(
        json.dumps({"pid": 123, "inventory_epoch": "fresh"})
    )
    ch.write_v1_legacy_remote_inventory(123, "fresh", {("tom@old", "old")})
    assert ch.v1_legacy_source_inventory(cfg) == {
        ch.v1_source_key({"remote": "tom@host", "session": "work"}),
        ch.v1_source_key({"remote": "tom@old", "session": "old"}),
    }
    assert ch.v1_legacy_source_inventory({"remotes": "bad"}) is None
    (tmp_path / "remote-active-sources.json").unlink()
    assert ch.v1_legacy_source_inventory(cfg) is None
    ch.write_v1_legacy_remote_inventory(123, "stale", {("tom@old", "old")})
    assert ch.v1_legacy_source_inventory(cfg) is None
    ch.write_v1_legacy_remote_inventory(123, "fresh", {("tom@old", "old")})
    monkeypatch.setattr(ch, "load_config_strict", lambda: None)
    assert ch.v1_legacy_source_inventory(cfg) is None


def test_v1_remote_inventory_write_failure_keeps_previous_config(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    old = {"remotes": [{"ssh_target": "old", "session": "work", "cmux_title": "w"}]}
    new = {"remotes": [{"ssh_target": "new", "session": "work", "cmux_title": "w"}]}
    monkeypatch.setattr(ch, "load_config_strict", lambda: new)

    def fail(*args):
        raise OSError("disk full")

    monkeypatch.setattr(ch, "write_v1_legacy_remote_inventory", fail)
    assert (
        ch.v1_legacy_remote_next_config(old, {"a": ("old", "work", "w")}, "epoch")
        == old
    )


def test_v1_remote_transition_waits_for_handoff_barrier(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    old = {"remotes": [{"ssh_target": "old", "session": "work", "cmux_title": "w"}]}
    new = {"remotes": [{"ssh_target": "new", "session": "work", "cmux_title": "w"}]}
    monkeypatch.setattr(ch, "load_config_strict", lambda: new)
    started = ch.threading.Event()
    done = ch.threading.Event()
    result = []

    def reload():
        started.set()
        result.append(
            ch.v1_legacy_remote_next_config(old, {"a": ("old", "work", "w")}, "epoch")
        )
        done.set()

    with open(tmp_path / "remote-transition.lock", "w") as barrier:
        ch.fcntl.flock(barrier, ch.fcntl.LOCK_EX)
        worker = ch.threading.Thread(target=reload)
        worker.start()
        assert started.wait(1)
        assert not done.wait(0.1)
    assert done.wait(2)
    worker.join()
    assert result == [new]


def test_v1_handoff_barrier_blocks_new_watcher_start(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "current_session", lambda: "work")
    monkeypatch.setattr(ch, "take_over_daemon", lambda *args: None)
    monkeypatch.setattr(
        ch, "watch_loop", lambda *args, **kwargs: pytest.fail("watcher escaped barrier")
    )
    started = ch.threading.Event()
    finished = ch.threading.Event()
    result = []

    def launch():
        started.set()
        result.append(ch.watch_daemon({}))
        finished.set()

    with open(tmp_path / "v1-activation.lock", "w") as barrier:
        ch.fcntl.flock(barrier, ch.fcntl.LOCK_EX)
        worker = ch.threading.Thread(target=launch)
        worker.start()
        assert started.wait(1)
        assert not finished.wait(0.1)
        (tmp_path / "v1-enabled").write_text("pending")
    assert finished.wait(2)
    worker.join()
    assert result == [0]
    assert not ch.watch_pid_path("work").exists()


def test_v1_handoff_barrier_blocks_remote_startup_cleanup(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "load_config_strict", lambda: {})
    monkeypatch.setattr(
        ch,
        "sweep_remote_orphan_pills",
        lambda *args: pytest.fail("cleanup escaped barrier"),
    )
    started = ch.threading.Event()
    finished = ch.threading.Event()
    result = []

    def launch():
        started.set()
        result.append(ch.remote_daemon({}))
        finished.set()

    with open(tmp_path / "v1-activation.lock", "w") as barrier:
        ch.fcntl.flock(barrier, ch.fcntl.LOCK_EX)
        worker = ch.threading.Thread(target=launch)
        worker.start()
        assert started.wait(1)
        assert not finished.wait(0.1)
        (tmp_path / "v1-enabled").write_text("pending")
    assert finished.wait(2)
    worker.join()
    assert result == [0]


def test_v1_handoff_barrier_blocks_watcher_in_separate_process(tmp_path):
    script = (
        "import cmux_herdr as c; "
        "print('starting', flush=True); "
        "raise SystemExit(c.watch_daemon({}))"
    )
    env = dict(os.environ, HERDR_PLUGIN_STATE_DIR=str(tmp_path), HERDR_SESSION="work")
    with open(tmp_path / "v1-activation.lock", "w") as barrier:
        ch.fcntl.flock(barrier, ch.fcntl.LOCK_EX)
        process = subprocess.Popen(
            [sys.executable, "-c", script],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            assert process.stdout.readline().strip() == "starting"
            with pytest.raises(subprocess.TimeoutExpired):
                process.wait(timeout=0.1)
            (tmp_path / "v1-enabled").write_text("pending")
        finally:
            ch.fcntl.flock(barrier, ch.fcntl.LOCK_UN)
        try:
            assert process.wait(timeout=3) == 0
            assert not (tmp_path / "watch-work.pid").exists()
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=3)
            process.stdout.close()
            process.stderr.close()


def test_watcher_start_during_pending_resumes_after_rollback(tmp_path):
    script = (
        "import cmux_herdr as c, sys; "
        "c.watch_loop = lambda cfg, **kwargs: print('running', flush=True); "
        "sys.argv = ['cmux_herdr.py', 'watch']; "
        "print('starting', flush=True); "
        "raise SystemExit(c.main())"
    )
    env = dict(os.environ, HERDR_PLUGIN_STATE_DIR=str(tmp_path), HERDR_SESSION="work")
    marker = tmp_path / "v1-enabled"
    marker.write_text("pending")
    with open(tmp_path / "v1-activation.lock", "w") as barrier:
        ch.fcntl.flock(barrier, ch.fcntl.LOCK_EX)
        process = subprocess.Popen(
            [sys.executable, "-c", script],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            assert process.stdout.readline().strip() == "starting"
            with pytest.raises(subprocess.TimeoutExpired):
                process.wait(timeout=0.1)
            marker.unlink()
        finally:
            ch.fcntl.flock(barrier, ch.fcntl.LOCK_UN)
        try:
            assert process.wait(timeout=3) == 0
            assert process.stdout.read().strip() == "running"
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=3)
            process.stdout.close()
            process.stderr.close()


def test_busy_daemon_lock_does_not_block_other_startup(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch.signal, "signal", lambda *args: None)
    monkeypatch.setattr(ch, "take_over_daemon", lambda *args: None)
    monkeypatch.setattr(ch, "watch_loop", lambda *args, **kwargs: None)
    local = ch.threading.local()
    monkeypatch.setattr(ch, "current_session", lambda: local.session)
    first_started = ch.threading.Event()
    second_done = ch.threading.Event()

    def first():
        local.session = "first"
        first_started.set()
        ch.watch_daemon({})

    def second():
        local.session = "second"
        ch.watch_daemon({})
        second_done.set()

    with open(tmp_path / "watch-first.lock", "w") as busy:
        ch.fcntl.flock(busy, ch.fcntl.LOCK_EX)
        a = ch.threading.Thread(target=first)
        a.start()
        assert first_started.wait(1)
        # Wait until first has had an opportunity to contend for its lock.
        ch.time.sleep(0.1)
        b = ch.threading.Thread(target=second)
        b.start()
        assert second_done.wait(1)
    a.join(timeout=2)
    b.join(timeout=2)
    assert not a.is_alive()


def test_old_daemon_shutdown_wait_does_not_block_other_session(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch.signal, "signal", lambda *args: None)
    monkeypatch.setattr(ch, "watch_loop", lambda *args, **kwargs: None)
    local = ch.threading.local()
    monkeypatch.setattr(ch, "current_session", lambda: local.session)
    first_path = tmp_path / "watch-first.pid"
    first_path.write_text(json.dumps({"pid": 123, "script": "old", "mtime": 0}))
    monkeypatch.setattr(
        ch, "live_daemon_pid", lambda path, mode: 123 if path == first_path else None
    )
    monkeypatch.setattr(ch, "v1_verified_daemon_audit_token", lambda *args: [123] * 8)
    monkeypatch.setattr(ch, "v1_signal_audit_token", lambda *args: None)
    waiting = ch.threading.Event()
    release = ch.threading.Event()
    second_done = ch.threading.Event()

    def fake_kill(pid, signal_number):
        if signal_number == 0:
            waiting.set()
            release.wait(2)
            raise ProcessLookupError()

    monkeypatch.setattr(ch.os, "kill", fake_kill)

    def first():
        local.session = "first"
        ch.watch_daemon({})

    def second():
        local.session = "second"
        ch.watch_daemon({})
        second_done.set()

    a = ch.threading.Thread(target=first)
    a.start()
    try:
        assert waiting.wait(1)
        b = ch.threading.Thread(target=second)
        b.start()
        assert second_done.wait(1)
    finally:
        release.set()
        a.join(timeout=3)
    b.join(timeout=3)
    assert not a.is_alive()
    assert not b.is_alive()


def test_watcher_rechecks_handoff_after_old_daemon_shutdown(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "current_session", lambda: "work")
    monkeypatch.setattr(
        ch,
        "watch_loop",
        lambda *args, **kwargs: pytest.fail("watcher started after commit"),
    )
    pid_path = tmp_path / "watch-work.pid"
    old = {"pid": 123, "script": "old", "mtime": 0}
    pid_path.write_text(json.dumps(old))
    monkeypatch.setattr(ch, "live_daemon_pid", lambda *args: 123)
    monkeypatch.setattr(ch, "v1_verified_daemon_audit_token", lambda *args: [123] * 8)
    monkeypatch.setattr(ch, "v1_signal_audit_token", lambda *args: None)
    waiting, release = ch.threading.Event(), ch.threading.Event()

    def fake_kill(pid, signum):
        if signum == 0:
            waiting.set()
            release.wait(2)
            raise ProcessLookupError()

    monkeypatch.setattr(ch.os, "kill", fake_kill)
    result = []
    worker = ch.threading.Thread(target=lambda: result.append(ch.watch_daemon({})))
    worker.start()
    try:
        assert waiting.wait(1)
        with open(tmp_path / "v1-activation.lock", "w") as activation:
            ch.fcntl.flock(activation, ch.fcntl.LOCK_EX)
            (tmp_path / "v1-enabled").write_text("enabled")
    finally:
        release.set()
        worker.join(timeout=3)
    assert not worker.is_alive()
    assert result == [0]
    assert json.loads(pid_path.read_text()) == old


def test_dead_pid_cleanup_fails_closed_when_daemon_lock_is_busy(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    path = tmp_path / "watch-work.pid"
    path.write_text(json.dumps({"pid": 123}))
    monkeypatch.setattr(
        ch.os, "kill", lambda *args: (_ for _ in ()).throw(ProcessLookupError())
    )
    result = []
    with open(tmp_path / "watch-work.lock", "w") as busy:
        ch.fcntl.flock(busy, ch.fcntl.LOCK_EX)
        worker = ch.threading.Thread(
            target=lambda: result.append(ch.v1_remove_confirmed_dead_pidfile(path))
        )
        worker.start()
        worker.join(timeout=0.5)
        assert not worker.is_alive()
        assert result == [False]
        assert path.exists()


def test_legacy_retirement_fails_closed_when_daemon_lock_is_busy(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    path = tmp_path / "watch-work.pid"
    path.write_text(json.dumps({"pid": 123}))
    result = []
    with open(tmp_path / "watch-work.lock", "w") as busy:
        ch.fcntl.flock(busy, ch.fcntl.LOCK_EX)
        worker = ch.threading.Thread(
            target=lambda: result.append(ch.stop_v1_legacy_daemon(path, "watch", []))
        )
        worker.start()
        worker.join(timeout=0.5)
        assert not worker.is_alive()
        assert result == [False]


def test_legacy_retirement_does_not_assume_unknown_process_is_dead(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    path = tmp_path / "watch-work.pid"
    path.write_text(json.dumps({"pid": 123}))
    monkeypatch.setattr(ch, "live_daemon_pid", lambda *args: None)
    monkeypatch.setattr(
        ch.os, "kill", lambda *args: (_ for _ in ()).throw(PermissionError())
    )
    assert not ch.stop_v1_legacy_daemon(path, "watch", [])
    assert path.exists()


@pytest.mark.skipif(sys.platform != "darwin", reason="requires macOS audit tokens")
def test_v1_audit_token_signal_rejects_wrong_process_generation():
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        token = ch.v1_process_audit_token(child.pid)
        assert token is not None
        wrong = token.copy()
        wrong[7] ^= 1
        with pytest.raises(OSError):
            ch.v1_signal_audit_token(wrong, ch.signal.SIGTERM)
        assert child.poll() is None
        ch.v1_signal_audit_token(token, ch.signal.SIGTERM)
        assert child.wait(timeout=3) == -ch.signal.SIGTERM
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=3)


@pytest.mark.skipif(sys.platform != "darwin", reason="requires macOS audit tokens")
def test_v1_daemon_self_info_includes_kernel_process_identity():
    info = ch.daemon_self_info()
    assert info["audit_token"] == ch.v1_process_audit_token(os.getpid())


@pytest.mark.skipif(sys.platform != "darwin", reason="requires macOS process info")
def test_v1_native_process_identity_checks_live_kernel_incarnation():
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        info = ch.v1_native_process_info(child.pid)
        assert info is not None
        assert info["pid"] == child.pid
        assert info["ppid"] == os.getpid()
        assert info["pgid"] == os.getpgid(child.pid)
        assert info["start_token"] > 0
        assert ch.v1_native_process_info(child.pid) == info
        child.kill()
        deadline = time.monotonic() + 3
        while ch.v1_wait_child_state(child.pid) is None:
            assert time.monotonic() < deadline
            time.sleep(0.01)
        assert ch.v1_native_process_info(child.pid) is None
        child.wait(timeout=3)
        assert ch.v1_native_process_info(child.pid) is None
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=3)


@pytest.mark.parametrize("invalid_pid", [0, -1, 2**31, True, 1.5, "1"])
def test_v1_native_process_info_rejects_invalid_pid(invalid_pid):
    assert ch.v1_native_process_info(invalid_pid) is None


def test_v1_native_transport_envelope_requires_matching_live_socket_owner(monkeypatch):
    identity = {"pid": 42, "start_token": 123}
    response = {
        "version": 1,
        "owner_pid": 42,
        "owner_start_token": 123,
        "instance": "a" * 32,
        "revision": 1,
        "status": "unavailable",
        "ownership_complete": False,
        "bridges": [],
    }
    calls = []
    monkeypatch.setattr(ch, "v1_native_process_info", lambda pid: identity.copy())
    monkeypatch.setattr(
        ch,
        "v1_read_client_response",
        lambda path, expected_peer=None, **kw: (
            calls.append(expected_peer) or response.copy()
        ),
    )
    assert ch.v1_native_transport_envelope("/tmp/owner.sock", 42) == response
    assert calls == [42]
    for change in (
        {"owner_pid": 43},
        {"owner_start_token": 124},
        {"instance": "bad"},
        {"revision": True},
        {"version": True},
    ):
        monkeypatch.setattr(
            ch,
            "v1_read_client_response",
            lambda *args, change=change, **kwargs: {**response, **change},
        )
        assert ch.v1_native_transport_envelope("/tmp/owner.sock", 42) is None
    monkeypatch.setattr(ch, "v1_read_client_response", lambda *args, **kwargs: None)
    assert ch.v1_native_transport_envelope("/tmp/owner.sock", 42) is None
    identities = iter([identity, {**identity, "start_token": 124}])
    monkeypatch.setattr(ch, "v1_native_process_info", lambda pid: next(identities))
    monkeypatch.setattr(ch, "v1_read_client_response", lambda *args, **kwargs: response)
    assert ch.v1_native_transport_envelope("/tmp/owner.sock", 42) is None


def test_v1_native_transport_claims_validate_exact_authenticated_bindings():
    pid = {"pid": 42, "start_token": 1}
    ssh = {"pid": 43, "start_token": 2}
    source = {
        "endpoint": "local",
        "boot_id": "a" * 32,
        "connection_generation": 9,
        "ssh_target": "host",
        "session": "work",
    }
    client = {
        "version": 1,
        "status": "ok",
        "pid": 42,
        "instance": "b" * 32,
        "sources_complete": True,
        "sources": [source],
        "observation": None,
    }
    bridge = {
        "id": "bridge",
        "client": pid,
        "source": {"target": "host", "session": "work"},
        "connection": {
            "id": "connection",
            "consumer": pid,
            "ssh_child": ssh,
            "peer_identity_verified": False,
        },
    }
    bound = {
        "bridge_id": "bridge",
        "connection_id": "connection",
        "instance": "b" * 32,
        "generation": 9,
        "observation": client,
    }
    response = {
        "version": 1,
        "status": "ok",
        "owner_pid": 40,
        "owner_start_token": 3,
        "instance": "c" * 32,
        "revision": 5,
        "ownership_complete": True,
        "bridges": [bridge],
        "bound_observations": [bound],
    }
    claims = ch.v1_native_transport_claims(response)
    assert claims is not None and len(claims) == 1
    pane = {
        "endpoint": "local",
        "boot_id": "a" * 32,
        "connection_generation": 9,
        "snapshot_revision": 3,
        "pane_id": "pane",
        "outer_focused": None,
        "agent_state_change_seq": None,
    }
    pane_bound = {**bound, "observation": {**client, "observation": pane}}
    assert ch.v1_native_transport_claims(
        {**response, "bound_observations": [pane_bound]}
    )
    for key in ("snapshot_revision", "agent_state_change_seq"):
        invalid = {
            **pane_bound,
            "observation": {**client, "observation": {**pane, key: 2**64}},
        }
        assert (
            ch.v1_native_transport_claims({**response, "bound_observations": [invalid]})
            is None
        )
    for change in (
        {"ownership_complete": False},
        {"status": "unavailable"},
        {"bound_observations": []},
        {"bound_observations": [bound, bound]},
        {"bridges": [bridge, bridge]},
        {"bound_observations": [{**bound, "connection_id": "other"}]},
        {"bound_observations": [{**bound, "generation": 10}]},
        {"bound_observations": [{**bound, "instance": "d" * 32}]},
        {
            "bridges": [
                {**bridge, "connection": {**bridge["connection"], "consumer": ssh}}
            ]
        },
        {"bridges": [{**bridge, "source": {"target": "other", "session": "work"}}]},
    ):
        assert ch.v1_native_transport_claims({**response, **change}) is None


def test_v1_native_topology_requires_same_surface_kernel_parentage_and_tty(monkeypatch):
    owner = {
        "owner_pid": 40,
        "owner_start_token": 3,
        "instance": "c" * 32,
        "revision": 5,
    }
    claim = {
        "bridge_id": "bridge",
        "connection_id": "connection",
        "client": {"pid": 42, "start_token": 1},
        "ssh_child": {"pid": 43, "start_token": 2},
        "source": {"target": "host", "session": "work"},
        "generation": 9,
        "observation": {"instance": "b" * 32, "sources": []},
    }
    live = {
        40: {
            "pid": 40,
            "start_token": 3,
            "ppid": 10,
            "tty_device": 100,
            "path": "/bin/herdr",
        },
        42: {
            "pid": 42,
            "start_token": 1,
            "ppid": 40,
            "tty_device": 100,
            "path": "/bin/herdr",
        },
        43: {
            "pid": 43,
            "start_token": 2,
            "ppid": 40,
            "tty_device": 100,
            "path": "/usr/bin/ssh",
        },
    }
    nodes = {
        40: {"pid": 40, "ppid": 10, "name": "herdr", "tty_device": 100},
        42: {"pid": 42, "ppid": 40, "name": "herdr", "tty_device": 100},
        43: {"pid": 43, "ppid": 40, "name": "ssh", "tty_device": 100},
    }
    nodes[40]["children"] = [nodes[42], nodes[43]]
    monkeypatch.setattr(ch, "v1_native_process_info", lambda pid: live.get(pid))
    assert ch.v1_native_transport_topology(owner, [claim], nodes, 100) is not None
    for pid, key, value in (
        (42, "ppid", 10),
        (43, "tty_device", 101),
        (40, "start_token", 4),
        (42, "path", "/bin/sh"),
    ):
        original = live[pid][key]
        live[pid][key] = value
        assert ch.v1_native_transport_topology(owner, [claim], nodes, 100) is None
        live[pid][key] = original
    nodes[43]["ppid"] = 10
    assert ch.v1_native_transport_topology(owner, [claim], nodes, 100) is None
    nodes[43]["ppid"] = 40
    calls = {40: 0, 42: 0, 43: 0}

    def replaced(pid):
        calls[pid] += 1
        return (
            {**live[pid], "start_token": 999}
            if pid == 42 and calls[pid] > 1
            else live[pid].copy()
        )

    monkeypatch.setattr(ch, "v1_native_process_info", replaced)
    assert ch.v1_native_transport_topology(owner, [claim], nodes, 100) is None


def test_v1_daemon_identity_requires_matching_recorded_audit_token(
    monkeypatch, tmp_path
):
    path = tmp_path / "watch-work.pid"
    token = list(range(8))
    path.write_text(json.dumps({"pid": 123, "audit_token": token}))
    monkeypatch.setattr(ch, "v1_process_audit_token", lambda pid: token.copy())
    assert ch.v1_verified_daemon_audit_token(path, 123) == token
    monkeypatch.setattr(ch, "v1_process_audit_token", lambda pid: [0] * 8)
    assert ch.v1_verified_daemon_audit_token(path, 123) is None
    path.write_text(json.dumps({"pid": 123}))
    assert ch.v1_verified_daemon_audit_token(path, 123) is None


@pytest.mark.parametrize("invalid_pid", [2**32, True, 1.5, -1, 1])
def test_v1_daemon_pidfile_rejects_invalid_pid_without_syscall(
    monkeypatch, tmp_path, invalid_pid
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    path = tmp_path / "watch-work.pid"
    path.write_text(json.dumps({"pid": invalid_pid}))
    monkeypatch.setattr(
        ch.os, "kill", lambda *args: pytest.fail("invalid PID reached syscall")
    )
    assert ch.live_daemon_pid(path, "watch") is None
    assert not ch.v1_remove_confirmed_dead_pidfile(path)


@pytest.mark.skipif(sys.platform != "darwin", reason="requires macOS audit tokens")
def test_v1_audit_token_rejects_out_of_range_pid_and_signal():
    assert ch.v1_process_audit_token(os.getpid() + 2**32) is None
    token = ch.v1_process_audit_token(os.getpid())
    assert token is not None
    for invalid in (True, -1, 2**32 + int(ch.signal.SIGTERM)):
        with pytest.raises(ValueError):
            ch.v1_signal_audit_token(token, invalid)


def test_v1_audit_signal_fails_closed_on_unsupported_platform(monkeypatch):
    monkeypatch.setattr(ch.sys, "platform", "unsupported")
    with pytest.raises(OSError):
        ch.v1_signal_audit_token(list(range(8)), ch.signal.SIGTERM)


def test_v1_audit_signal_fails_closed_when_symbol_is_missing(monkeypatch):
    monkeypatch.setattr(ch.sys, "platform", "darwin")
    monkeypatch.setattr(ch.ctypes, "CDLL", lambda *args, **kwargs: SimpleNamespace())
    with pytest.raises(OSError):
        ch.v1_signal_audit_token(list(range(8)), ch.signal.SIGTERM)


def test_retirement_refuses_to_signal_when_pid_generation_changed(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    path = tmp_path / "watch-work.pid"
    path.write_text(json.dumps({"pid": 123, "audit_token": list(range(8))}))
    monkeypatch.setattr(ch, "live_daemon_pid", lambda *args: 123)
    monkeypatch.setattr(ch, "known_sessions", lambda: {"work"})
    monkeypatch.setattr(ch, "v1_process_audit_token", lambda pid: [1] * 8)
    monkeypatch.setattr(ch, "v1_daemon_incarnation_exited", lambda *args: False)
    monkeypatch.setattr(
        ch.os,
        "kill",
        lambda pid, sig: None if sig == 0 else pytest.fail("numeric PID was signaled"),
    )
    monkeypatch.setattr(
        ch,
        "v1_signal_audit_token",
        lambda *args: pytest.fail("replacement was signaled"),
    )
    assert not ch.stop_v1_legacy_daemon(path, "watch", [])


def test_daemon_takeover_refuses_unverified_old_process(monkeypatch, tmp_path):
    path = tmp_path / "watch-work.pid"
    path.write_text(json.dumps({"pid": 123, "script": "old", "mtime": 0}))
    monkeypatch.setattr(ch, "live_daemon_pid", lambda *args: 123)
    monkeypatch.setattr(ch, "v1_process_audit_token", lambda pid: None)
    monkeypatch.setattr(
        ch.os, "kill", lambda *args: pytest.fail("numeric PID was signaled")
    )
    monkeypatch.setattr(
        ch,
        "v1_signal_audit_token",
        lambda *args: pytest.fail("unverified PID was signaled"),
    )
    with pytest.raises(SystemExit) as failure:
        ch.take_over_daemon(path, "watch")
    assert failure.value.code == 1


def test_daemon_takeover_uses_monotonic_shutdown_deadline(monkeypatch, tmp_path):
    path = tmp_path / "watch-work.pid"
    path.write_text(json.dumps({"pid": 123, "script": "old", "mtime": 0}))
    monkeypatch.setattr(ch, "live_daemon_pid", lambda *args: 123)
    monkeypatch.setattr(ch, "v1_verified_daemon_audit_token", lambda *args: [123] * 8)
    monkeypatch.setattr(ch, "v1_signal_audit_token", lambda *args: None)
    monkeypatch.setattr(ch, "v1_daemon_incarnation_exited", lambda *args: True)
    monkeypatch.setattr(ch.time, "time", lambda: pytest.fail("wall clock used"))
    monkeypatch.setattr(ch.time, "monotonic", lambda: 100)
    ch.take_over_daemon(path, "watch")


def test_legacy_retirement_aborts_when_new_watchers_keep_arriving(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    (tmp_path / "watch-0.pid").touch()
    calls = []

    def stop(path, mode, restore_plan=None, *, deadline=None):
        if mode == "watch":
            calls.append(path)
            if len(calls) > 5:
                pytest.fail("retirement scan did not stop")
            (tmp_path / f"watch-{len(calls)}.pid").touch()
        return True

    monkeypatch.setattr(ch, "stop_v1_legacy_daemon", stop)
    clock = iter([0, 1, 2, 31, 32, 33])
    monkeypatch.setattr(ch.time, "monotonic", lambda: next(clock, 33))
    assert not ch.retire_v1_legacy_daemons()
    assert calls


def test_legacy_retirement_rejects_replacement_at_seen_pidfile(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    path = tmp_path / "watch-work.pid"
    path.write_text(json.dumps({"pid": 123}))

    def stop(target, mode, restore_plan=None, *, deadline=None):
        if mode == "watch":
            target.write_text(json.dumps({"pid": 456}))
        return True

    monkeypatch.setattr(ch, "stop_v1_legacy_daemon", stop)
    monkeypatch.setattr(ch.os, "kill", lambda *args: None)
    assert not ch.retire_v1_legacy_daemons()


def test_legacy_retirement_deadline_applies_inside_batch(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    for i in range(4):
        (tmp_path / f"watch-{i}.pid").touch()
    clock = [0]
    calls = []
    monkeypatch.setattr(ch.time, "monotonic", lambda: clock[0])

    def stop(path, mode, restore_plan=None, *, deadline=None):
        if mode == "watch":
            calls.append(path)
            clock[0] += 15
        return False

    monkeypatch.setattr(ch, "stop_v1_legacy_daemon", stop)
    assert not ch.retire_v1_legacy_daemons()
    assert len(calls) == 2
    assert clock[0] == 30


def test_legacy_retirement_does_not_signal_after_discovery_exceeds_deadline(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    path = tmp_path / "watch-work.pid"
    path.touch()
    monkeypatch.setattr(ch, "live_daemon_pid", lambda *args: 123)
    monkeypatch.setattr(ch, "v1_verified_daemon_audit_token", lambda *args: [123] * 8)
    clock = [0]
    monkeypatch.setattr(ch.time, "monotonic", lambda: clock[0])

    def sessions():
        clock[0] = 11
        return {"work"}

    monkeypatch.setattr(ch, "known_sessions", sessions)
    monkeypatch.setattr(
        ch, "v1_signal_audit_token", lambda *args: pytest.fail("signal after deadline")
    )
    plan = []
    assert not ch.stop_v1_legacy_daemon(path, "watch", plan, deadline=10)
    assert not plan


def test_legacy_retirement_cancels_new_journal_if_deadline_passes_before_signal(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    path = tmp_path / "watch-work.pid"
    path.touch()
    monkeypatch.setattr(ch, "live_daemon_pid", lambda *args: 123)
    monkeypatch.setattr(ch, "v1_verified_daemon_audit_token", lambda *args: [123] * 8)
    monkeypatch.setattr(ch, "known_sessions", lambda: {"work"})
    clock = [0]
    monkeypatch.setattr(ch.time, "monotonic", lambda: clock[0])
    record = ch.record_v1_legacy_restore

    def slow_record(plan, item):
        added = record(plan, item)
        clock[0] = 11
        return added

    monkeypatch.setattr(ch, "record_v1_legacy_restore", slow_record)
    monkeypatch.setattr(
        ch, "v1_signal_audit_token", lambda *args: pytest.fail("signal after deadline")
    )
    plan = []
    assert not ch.stop_v1_legacy_daemon(path, "watch", plan, deadline=10)
    assert not plan
    assert ch.pending_v1_legacy_restores() == []


def test_legacy_retirement_treats_reused_pid_as_original_exit(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    path = tmp_path / "watch-work.pid"
    path.touch()
    original, replacement = [1] * 7 + [10], [1] * 7 + [11]
    monkeypatch.setattr(ch, "live_daemon_pid", lambda *args: 123)
    monkeypatch.setattr(ch, "v1_verified_daemon_audit_token", lambda *args: original)
    monkeypatch.setattr(ch, "v1_signal_audit_token", lambda *args: None)
    monkeypatch.setattr(ch.os, "kill", lambda *args: None)
    monkeypatch.setattr(ch, "v1_process_audit_token", lambda pid: replacement)
    monkeypatch.setattr(ch, "v1_audit_token_pidversion", lambda token: token[7])
    assert ch.stop_v1_legacy_daemon(path, "watch")


def test_daemon_exit_is_unknown_when_audit_lookup_fails(monkeypatch):
    monkeypatch.setattr(ch.os, "kill", lambda *args: None)
    monkeypatch.setattr(ch, "v1_process_audit_token", lambda pid: None)
    assert not ch.v1_daemon_incarnation_exited(123, [1] * 8)


def test_v1_remote_daemon_monitors_when_inventory_write_fails(monkeypatch, tmp_path):
    cfg = {"remotes": [{"ssh_target": "old", "session": "work", "cmux_title": "w"}]}
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "load_config_strict", lambda: cfg)
    monkeypatch.setattr(ch, "take_over_daemon", lambda *args: None)
    monkeypatch.setattr(ch, "sweep_remote_orphan_pills", lambda *args: None)
    monkeypatch.setattr(ch.signal, "signal", lambda *args: None)
    monkeypatch.setattr(ch, "DAEMON_SHUTDOWN", ch.threading.Event())

    def fail(*args):
        raise OSError("disk full")

    monkeypatch.setattr(ch, "write_v1_legacy_remote_inventory", fail)
    polled = ch.threading.Event()
    monkeypatch.setattr(ch, "poll_remote_worker", lambda *args: polled.set())

    def stop(seconds):
        polled.wait(2)
        raise KeyboardInterrupt()

    monkeypatch.setattr(ch.time, "sleep", stop)
    assert ch.remote_daemon(cfg) == 0
    assert polled.is_set()
    assert not (tmp_path / "remote.pid").exists()


def test_v1_legacy_inventory_includes_interrupted_restore_obligations(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(ch, "known_sessions", lambda **kwargs: set())
    monkeypatch.setattr(ch, "live_daemon_pid", lambda *args: None)
    monkeypatch.setattr(ch, "v1_local_socket", lambda name: f"/tmp/{name}.sock")
    monkeypatch.setattr(
        ch, "pending_v1_legacy_restores", lambda: [("watch", "work", 123)]
    )
    assert ch.v1_legacy_source_inventory({}) == {
        ch.v1_source_key({"socket_path": "/tmp/work.sock"})
    }
    monkeypatch.setattr(
        ch, "pending_v1_legacy_restores", lambda: [("remote", None, 456)]
    )
    assert ch.v1_legacy_source_inventory({}) is None


def test_v1_legacy_inventory_discards_only_confirmed_dead_pid(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(ch, "known_sessions", lambda **kwargs: set())
    monkeypatch.setattr(ch, "live_daemon_pid", lambda *args: None)
    pidfile = tmp_path / "remote.pid"
    pidfile.write_text(json.dumps({"pid": 987654}))

    def dead(pid, sig):
        raise ProcessLookupError()

    monkeypatch.setattr(ch.os, "kill", dead)
    assert ch.v1_legacy_source_inventory({}) == set()
    assert not pidfile.exists()
    pidfile.write_text(json.dumps({"pid": 987654}))

    def denied(pid, sig):
        raise PermissionError()

    monkeypatch.setattr(ch.os, "kill", denied)
    assert ch.v1_legacy_source_inventory({}) is None
    assert pidfile.exists()


def test_dead_pidfile_cleanup_uses_incarnation_and_preserves_live_replacement(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    path = tmp_path / "watch-work.pid"
    old, new = [1] * 8, [2] * 8
    monkeypatch.setattr(ch.os, "kill", lambda *args: None)
    monkeypatch.setattr(
        ch, "v1_daemon_incarnation_exited", lambda pid, token: token == old
    )
    path.write_text(json.dumps({"pid": 123, "audit_token": old}))
    assert ch.v1_remove_confirmed_dead_pidfile(path)
    assert not path.exists()
    path.write_text(json.dumps({"pid": 123, "audit_token": new}))
    assert not ch.v1_remove_confirmed_dead_pidfile(path)
    assert path.exists()


def test_v1_legacy_inventory_includes_sources_without_daemon(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(ch, "known_sessions", lambda **kwargs: {"work"})
    monkeypatch.setattr(ch, "v1_local_socket", lambda name: f"/tmp/{name}.sock")
    monkeypatch.setattr(ch, "live_daemon_pid", lambda *args: None)
    cfg = {"remotes": [{"ssh_target": "host", "session": "remote", "cmux_title": "w"}]}
    monkeypatch.setattr(ch, "load_config_strict", lambda: cfg)
    assert ch.v1_legacy_source_inventory(cfg) == {
        ch.v1_source_key({"socket_path": "/tmp/work.sock"}),
        ch.v1_source_key({"remote": "host", "session": "remote"}),
    }
    (tmp_path / "state.json").write_text("{broken")
    assert ch.v1_legacy_source_inventory(cfg) is None


def test_v1_discovery_respects_shared_deadline(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "v1_tty_device", lambda tty: 100)
    process = {"pid": 20, "name": "herdr", "tty_device": 100, "children": []}
    surface = {
        "id": "s",
        "type": "terminal",
        "tty": "/dev/ttys001",
        "processes": [process],
    }
    tree = {
        "windows": [{"workspaces": [{"id": "w", "panes": [{"surfaces": [surface]}]}]}]
    }
    monkeypatch.setattr(ch, "v1_observer_socket_exists", lambda path: True)
    monkeypatch.setattr(
        ch,
        "v1_client_source_inventory",
        lambda *args, **kwargs: pytest.fail("deadline ignored"),
    )
    assert (
        ch.v1_discover_clients_in_tree(tree, [tmp_path], deadline=0)["complete"]
        is False
    )


def test_v1_discovery_marks_unidentified_tty_process_incomplete(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "v1_tty_device", lambda tty: 100)
    process = {"pid": 20, "tty_device": 100, "children": []}
    surface = {
        "id": "s",
        "type": "terminal",
        "tty": "/dev/ttys001",
        "processes": [process],
    }
    tree = {
        "windows": [{"workspaces": [{"id": "w", "panes": [{"surfaces": [surface]}]}]}]
    }
    assert ch.v1_discover_clients_in_tree(tree, [tmp_path])["complete"] is False


def test_v1_discovery_does_not_claim_ssh_shell_is_empty(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "v1_tty_device", lambda tty: 100)
    process = {"pid": 20, "name": "ssh", "tty_device": 100, "children": []}
    surface = {
        "id": "s",
        "type": "terminal",
        "tty": "/dev/ttys001",
        "processes": [process],
    }
    tree = {
        "windows": [{"workspaces": [{"id": "w", "panes": [{"surfaces": [surface]}]}]}]
    }
    assert ch.v1_discover_clients_in_tree(tree, [tmp_path])["complete"] is False


def test_v1_discovery_rejects_unknown_surface_type(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "v1_tty_device", lambda tty: 100)
    surface = {"id": "s", "type": "termnal", "tty": "/dev/ttys001", "processes": []}
    tree = {
        "windows": [{"workspaces": [{"id": "w", "panes": [{"surfaces": [surface]}]}]}]
    }
    assert ch.v1_discover_clients_in_tree(tree, [tmp_path]) is None


def test_v1_bounded_command_rejects_excessive_output():
    assert (
        ch.v1_bounded_command_output(
            [sys.executable, "-c", "print('x' * 10000)"], max_bytes=100
        )
        is None
    )
    assert (
        ch.v1_bounded_command_output(
            [sys.executable, "-c", "print('ok')"], max_bytes=100
        )
        == "ok\n"
    )
    assert (
        ch.v1_bounded_command_output(
            [sys.executable, "-c", "import time; time.sleep(2)"], timeout=0.05
        )
        is None
    )


def test_v1_bounded_command_terminates_its_process_group(tmp_path):
    child_file = tmp_path / "child.pid"
    code = (
        "import os, sys, time; pid=os.fork(); "
        "open(sys.argv[1], 'w').write(str(os.getpid())) if pid == 0 else None; "
        "time.sleep(30)"
    )
    assert (
        ch.v1_bounded_command_output(
            [sys.executable, "-c", code, str(child_file)], timeout=0.5
        )
        is None
    )
    pid = int(child_file.read_text())
    try:
        proc = subprocess.run(
            ["ps", "-p", str(pid), "-o", "stat="], capture_output=True, text=True
        )
        assert proc.returncode != 0 or proc.stdout.strip().startswith("Z")
    finally:
        with suppress(ProcessLookupError):
            os.kill(pid, ch.signal.SIGKILL)


def test_v1_bounded_command_reaps_descendants_after_success(tmp_path):
    child_file = tmp_path / "child.pid"
    code = (
        "import os,sys,time; pid=os.fork(); "
        "open(sys.argv[1], 'w').write(str(os.getpid())) if pid == 0 else None; "
        "os.close(1) if pid == 0 else None; "
        "time.sleep(30) if pid == 0 else time.sleep(.2)"
    )
    assert (
        ch.v1_bounded_command_output(
            [sys.executable, "-c", code, str(child_file)], timeout=2
        )
        == ""
    )
    pid = int(child_file.read_text())
    try:
        proc = subprocess.run(
            ["ps", "-p", str(pid), "-o", "stat="], capture_output=True, text=True
        )
        assert proc.returncode != 0 or proc.stdout.strip().startswith("Z")
    finally:
        with suppress(ProcessLookupError):
            os.kill(pid, ch.signal.SIGKILL)


def test_v1_discovery_stops_scanning_empty_surfaces_after_deadline(
    monkeypatch, tmp_path
):
    calls = []
    monkeypatch.setattr(ch, "v1_tty_device", lambda tty: calls.append(tty) or 100)
    surface = {"id": "s", "type": "terminal", "tty": "/dev/tty", "processes": []}
    tree = {
        "windows": [{"workspaces": [{"id": "w", "panes": [{"surfaces": [surface]}]}]}]
    }
    assert (
        ch.v1_discover_clients_in_tree(tree, [tmp_path], deadline=0)["complete"]
        is False
    )
    assert not calls


def test_v1_bounded_command_signals_group_before_reaping_failure(monkeypatch):
    calls = []

    class Selector:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def register(self, *args):
            pass

        def select(self, *args):
            return [True]

    proc = SimpleNamespace(
        pid=12345,
        stdout=SimpleNamespace(fileno=lambda: 9, close=lambda: None),
        wait=lambda **kwargs: calls.append("reap"),
    )
    monkeypatch.setattr(ch.subprocess, "Popen", lambda *args, **kwargs: proc)
    monkeypatch.setattr(ch.selectors, "DefaultSelector", Selector)
    monkeypatch.setattr(ch.os, "read", lambda *args: b"")
    monkeypatch.setattr(ch, "v1_wait_child_state", lambda pid: (ch.os.CLD_EXITED, 1))
    monkeypatch.setattr(ch.os, "killpg", lambda *args: calls.append("kill"))
    assert ch.v1_bounded_command_output(["fake"]) is None
    assert calls == ["kill", "reap"]


def test_v1_bounded_command_cleans_up_when_signal_arrives_during_spawn(monkeypatch):
    handlers = {}
    calls = []
    proc = SimpleNamespace(
        pid=12345, stdout=None, wait=lambda **kwargs: calls.append("reap")
    )

    def original(signum, frame):
        raise SystemExit(0)

    def spawn(*args, **kwargs):
        handlers[ch.signal.SIGTERM](ch.signal.SIGTERM, None)
        return proc

    monkeypatch.setattr(ch.signal, "getsignal", lambda sig: original)
    monkeypatch.setattr(
        ch.signal, "signal", lambda sig, handler: handlers.__setitem__(sig, handler)
    )
    monkeypatch.setattr(ch.subprocess, "Popen", spawn)
    monkeypatch.setattr(ch.os, "killpg", lambda *args: calls.append("kill"))
    with pytest.raises(SystemExit):
        ch.v1_bounded_command_output(["fake"])
    assert calls == ["kill", "reap"]
    assert handlers[ch.signal.SIGTERM] is original


def test_v1_bounded_command_cleans_up_before_replaying_default_signal(monkeypatch):
    handlers = {}
    calls = []
    proc = SimpleNamespace(
        pid=12345, stdout=None, wait=lambda **kwargs: calls.append("reap")
    )

    def spawn(*args, **kwargs):
        handlers[ch.signal.SIGTERM](ch.signal.SIGTERM, None)
        return proc

    monkeypatch.setattr(
        ch.signal,
        "getsignal",
        lambda sig: (
            ch.signal.SIG_DFL if sig == ch.signal.SIGTERM else ch.signal.SIG_IGN
        ),
    )
    monkeypatch.setattr(
        ch.signal, "signal", lambda sig, handler: handlers.__setitem__(sig, handler)
    )
    monkeypatch.setattr(ch.subprocess, "Popen", spawn)
    monkeypatch.setattr(ch.os, "killpg", lambda *args: calls.append("kill-group"))
    monkeypatch.setattr(ch.os, "kill", lambda *args: calls.append("replay"))
    assert ch.v1_bounded_command_output(["fake"]) is None
    assert calls == ["kill-group", "reap", "replay"]
    assert handlers[ch.signal.SIGTERM] == ch.signal.SIG_DFL


def test_v1_bounded_command_preserves_default_signal_among_multiple(monkeypatch):
    handlers = {}
    calls = []
    proc = SimpleNamespace(
        pid=12345, stdout=None, wait=lambda **kwargs: calls.append("reap")
    )

    def on_int(signum, frame):
        raise KeyboardInterrupt

    def spawn(*args, **kwargs):
        handlers[ch.signal.SIGTERM](ch.signal.SIGTERM, None)
        handlers[ch.signal.SIGINT](ch.signal.SIGINT, None)
        return proc

    monkeypatch.setattr(
        ch.signal,
        "getsignal",
        lambda sig: ch.signal.SIG_DFL if sig == ch.signal.SIGTERM else on_int,
    )
    monkeypatch.setattr(
        ch.signal, "signal", lambda sig, handler: handlers.__setitem__(sig, handler)
    )
    monkeypatch.setattr(ch.subprocess, "Popen", spawn)
    monkeypatch.setattr(ch.os, "killpg", lambda *args: calls.append("kill-group"))
    monkeypatch.setattr(ch.os, "kill", lambda pid, sig: calls.append(("replay", sig)))
    assert ch.v1_bounded_command_output(["fake"]) is None
    assert calls == ["kill-group", "reap", ("replay", ch.signal.SIGTERM)]
    assert handlers[ch.signal.SIGINT] is on_int


def test_v1_bounded_command_restores_all_handlers_if_restoration_is_interrupted(
    monkeypatch,
):
    handlers = {}
    interrupted = []

    def original(signum, frame):
        raise SystemExit(7)

    def install(signum, handler):
        handlers[signum] = handler
        if signum == ch.signal.SIGTERM and handler is original and not interrupted:
            interrupted.append(True)
            raise SystemExit(7)

    monkeypatch.setattr(ch.signal, "getsignal", lambda sig: original)
    monkeypatch.setattr(ch.signal, "signal", install)
    monkeypatch.setattr(
        ch.subprocess,
        "Popen",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("stop")),
    )
    with pytest.raises(SystemExit) as error:
        ch.v1_bounded_command_output(["fake"])
    assert error.value.code == 7
    assert handlers[ch.signal.SIGTERM] is original
    assert handlers[ch.signal.SIGINT] is original


def test_v1_bounded_command_restores_handlers_after_other_thread_signal():
    code = """
import signal, threading, time, sys
import cmux_herdr as c
stop = threading.Event()
thread = threading.Thread(target=stop.wait)
thread.start()
real_signal = signal.signal
old_term = signal.getsignal(signal.SIGTERM)
old_int = signal.getsignal(signal.SIGINT)
def original(sig, frame):
    raise SystemExit(7)
real_signal(signal.SIGTERM, original)
triggered = False
def intercept(sig, handler):
    global triggered
    result = real_signal(sig, handler)
    if sig == signal.SIGTERM and handler is original and not triggered:
        triggered = True
        signal.pthread_kill(thread.ident, signal.SIGTERM)
        time.sleep(.05)
    return result
try:
    signal.signal = intercept
    try:
        c.v1_bounded_command_output([sys.executable, '-c', 'pass'])
    except SystemExit as error:
        assert error.code == 7
    else:
        raise AssertionError('signal was not propagated')
    assert signal.getsignal(signal.SIGINT) is old_int
finally:
    signal.signal = real_signal
    real_signal(signal.SIGTERM, old_term)
    real_signal(signal.SIGINT, old_int)
    stop.set()
    thread.join()
"""
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=10
    )
    assert result.returncode == 0, result.stderr


def test_v1_bounded_command_cleans_up_on_real_default_sigterm(tmp_path):
    child_file = tmp_path / "child.pid"
    child_code = (
        "import os,sys,time; "
        "open(sys.argv[1], 'w').write(str(os.getpid())); time.sleep(30)"
    )
    parent_code = (
        "import cmux_herdr as c,sys; "
        "c.v1_bounded_command_output([sys.executable,'-c',sys.argv[1],sys.argv[2]],timeout=30)"
    )
    parent = subprocess.Popen(
        [sys.executable, "-c", parent_code, child_code, str(child_file)]
    )
    child_pid = None
    try:
        deadline = ch.time.monotonic() + 5
        while not child_file.exists() and ch.time.monotonic() < deadline:
            ch.time.sleep(0.02)
        child_pid = int(child_file.read_text())
        parent.terminate()
        assert parent.wait(timeout=5) == -ch.signal.SIGTERM
        proc = subprocess.run(
            ["ps", "-p", str(child_pid), "-o", "stat="], capture_output=True, text=True
        )
        assert proc.returncode != 0 or proc.stdout.strip().startswith("Z")
    finally:
        if parent.poll() is None:
            parent.kill()
            parent.wait()
        if child_pid is not None:
            with suppress(ProcessLookupError):
                os.kill(child_pid, ch.signal.SIGKILL)


def test_v1_discovery_checks_deadline_after_tree_walk(monkeypatch):
    now = [0.0]
    tree = {
        "windows": [
            {
                "workspaces": [
                    {
                        "id": "w",
                        "panes": [
                            {
                                "surfaces": [
                                    {
                                        "id": "s",
                                        "type": "terminal",
                                        "tty": "/dev/tty",
                                        "processes": [],
                                    }
                                ]
                            }
                        ],
                    }
                ]
            }
        ]
    }
    monkeypatch.setattr(ch.time, "monotonic", lambda: now[0])

    def command(*args, **kwargs):
        now[0] += 9.95
        return json.dumps(tree)

    def tty(*args):
        now[0] += 0.2
        return 100

    monkeypatch.setattr(ch, "v1_bounded_command_output", command)
    monkeypatch.setattr(ch, "v1_tty_device", tty)
    result = ch.v1_discover_clients({})
    assert result is None or result["complete"] is False


def test_v1_discovery_rejects_excessive_tree_nesting(monkeypatch):
    nested = "[" * 1100 + "0" + "]" * 1100
    monkeypatch.setattr(ch, "v1_bounded_command_output", lambda *args, **kwargs: nested)
    assert ch.v1_discover_clients({}) is None


def test_v1_focus_episode_requires_matching_server_and_current_completion():
    boot = "a" * 32
    observation = {
        "boot_id": boot,
        "pane_id": "p",
        "outer_focused": True,
        "agent_state_change_seq": 7,
    }
    snapshot = {
        "server_boot_id": boot,
        "agents": [
            {
                "pane_id": "p",
                "terminal_id": "t",
                "agent_status": "idle",
                "state_change_seq": 7,
            }
        ],
    }
    state = {
        "v1": {
            "sources": {
                "source": {
                    "attention": {
                        "p": {
                            "status": "done",
                            "episode": "gen:t:7",
                            "generation": "gen",
                            "terminal_id": "t",
                            "sequence": 7,
                            "server_boot_id": boot,
                        }
                    }
                }
            }
        }
    }
    assert (
        ch.v1_verified_focus_episode(state, "source", observation, snapshot)
        == "gen:t:7"
    )
    assert (
        ch.v1_verified_focus_episode(
            state, "source", {**observation, "boot_id": "b" * 32}, snapshot
        )
        is None
    )
    assert (
        ch.v1_verified_focus_episode(
            state, "source", {**observation, "outer_focused": False}, snapshot
        )
        is None
    )
    stale = {**snapshot, "agents": [{**snapshot["agents"][0], "state_change_seq": 8}]}
    assert ch.v1_verified_focus_episode(state, "source", observation, stale) is None
    assert (
        ch.v1_verified_focus_episode(
            state, "source", {**observation, "agent_state_change_seq": 6}, snapshot
        )
        is None
    )
    blocked = {
        **snapshot,
        "agents": [{**snapshot["agents"][0], "agent_status": "blocked"}],
    }
    assert ch.v1_verified_focus_episode(state, "source", observation, blocked) is None
    entry = state["v1"]["sources"]["source"]["attention"]["p"]
    entry["server_boot_id"] = "b" * 32
    assert ch.v1_verified_focus_episode(state, "source", observation, snapshot) is None


def test_v1_focus_episode_after_agent_release_requires_presented_release():
    boot = "a" * 32
    entry = {
        "status": "done",
        "episode": "gen:t:7",
        "generation": "gen",
        "terminal_id": "t",
        "sequence": 7,
        "server_boot_id": boot,
    }
    state = {"v1": {"sources": {"source": {"attention": {"p": entry}}}}}
    observation = {
        "boot_id": boot,
        "pane_id": "p",
        "outer_focused": True,
        "agent_state_change_seq": None,
        "pane_state": {
            "terminal_id": "t",
            "state_change_seq": 8,
            "agent_present": False,
            "released_completion_seq": 7,
        },
    }
    snapshot = {
        "server_boot_id": boot,
        "agents": [],
        "panes": [{"pane_id": "p", "terminal_id": "t", "state_change_seq": 8}],
    }
    assert (
        ch.v1_verified_focus_episode(state, "source", observation, snapshot)
        == "gen:t:7"
    )
    simultaneous = {
        **observation,
        "pane_state": {**observation["pane_state"], "state_change_seq": 7},
    }
    simultaneous_snapshot = {
        **snapshot,
        "panes": [{**snapshot["panes"][0], "state_change_seq": 7}],
    }
    assert (
        ch.v1_verified_focus_episode(
            state, "source", simultaneous, simultaneous_snapshot
        )
        == "gen:t:7"
    )
    missing_provenance = {
        **simultaneous,
        "pane_state": {**simultaneous["pane_state"], "released_completion_seq": None},
    }
    assert (
        ch.v1_verified_focus_episode(
            state, "source", missing_provenance, simultaneous_snapshot
        )
        is None
    )
    for pane_state in (
        None,
        {**observation["pane_state"], "agent_present": True},
        {**observation["pane_state"], "terminal_id": "other"},
        {**observation["pane_state"], "state_change_seq": 7},
        {**observation["pane_state"], "state_change_seq": 9},
    ):
        assert (
            ch.v1_verified_focus_episode(
                state, "source", {**observation, "pane_state": pane_state}, snapshot
            )
            is None
        )
    assert (
        ch.v1_verified_focus_episode(
            state, "source", observation, {**snapshot, "panes": snapshot["panes"] * 2}
        )
        is None
    )
    assert (
        ch.v1_verified_focus_episode(
            state, "source", observation, {**snapshot, "agents": [{"pane_id": "p"}]}
        )
        is None
    )
    later = {
        **observation,
        "pane_state": {
            **observation["pane_state"],
            "state_change_seq": 10,
            "released_completion_seq": 9,
        },
    }
    later_snapshot = {
        **snapshot,
        "panes": [{**snapshot["panes"][0], "state_change_seq": 10}],
    }
    assert ch.v1_verified_focus_episode(state, "source", later, later_snapshot) is None
    later["pane_state"]["released_completion_seq"] = 7
    assert (
        ch.v1_verified_focus_episode(state, "source", later, later_snapshot)
        == "gen:t:7"
    )


def test_v1_surface_processes_select_only_foreground_process_group(monkeypatch):
    monkeypatch.setattr(ch, "v1_tty_device", lambda tty: 100, raising=False)
    tree = {
        "windows": [
            {
                "workspaces": [
                    {
                        "id": "W",
                        "ref": "workspace:1",
                        "panes": [
                            {
                                "surfaces": [
                                    {
                                        "id": "S",
                                        "ref": "surface:1",
                                        "tty": "/dev/ttys001",
                                        "processes": [
                                            {
                                                "pid": 10,
                                                "tty_device": 100,
                                                "pgid": 10,
                                                "tpgid": 20,
                                                "children": [
                                                    {
                                                        "pid": 20,
                                                        "tty_device": 100,
                                                        "pgid": 20,
                                                        "tpgid": 20,
                                                        "children": [
                                                            {
                                                                "pid": 21,
                                                                "tty_device": 100,
                                                                "pgid": 20,
                                                                "tpgid": 20,
                                                                "children": [],
                                                            }
                                                        ],
                                                    }
                                                ],
                                            },
                                            {
                                                "pid": 30,
                                                "tty_device": 100,
                                                "pgid": 30,
                                                "tpgid": 20,
                                                "children": [],
                                            },
                                        ],
                                    }
                                ]
                            }
                        ],
                    }
                ]
            }
        ]
    }
    assert ch.v1_foreground_surface_pids(tree, "w", "s") == {20, 21}
    assert ch.v1_foreground_surface_pids(tree, "workspace:1", "surface:1") == {20, 21}
    assert ch.v1_foreground_surface_pids(tree, "w", "missing") is None
    tree["windows"][0]["workspaces"][0]["panes"][0]["surfaces"][0]["processes"][0][
        "children"
    ][0]["tpgid"] = None
    assert ch.v1_foreground_surface_pids(tree, "w", "s") == {21}
    child = tree["windows"][0]["workspaces"][0]["panes"][0]["surfaces"][0]["processes"][
        0
    ]["children"][0]["children"][0]
    child["tty_device"] = 999
    assert ch.v1_foreground_surface_pids(tree, "w", "s") == set()


def test_v1_surface_processes_reject_inconsistent_terminal_foreground_groups(
    monkeypatch,
):
    monkeypatch.setattr(ch, "v1_tty_device", lambda tty: 100)
    surface = {
        "id": "s",
        "tty": "/dev/ttys001",
        "processes": [
            {"pid": 20, "pgid": 20, "tpgid": 20, "tty_device": 100, "children": []},
            {"pid": 30, "pgid": 30, "tpgid": 30, "tty_device": 100, "children": []},
        ],
    }
    tree = {
        "windows": [{"workspaces": [{"id": "w", "panes": [{"surfaces": [surface]}]}]}]
    }
    assert ch.v1_foreground_surface_pids(tree, "w", "s") is None


def test_v1_foreground_observation_requires_one_observable_herdr(monkeypatch, tmp_path):
    processes = {
        10: {"pid": 10, "name": "zsh", "path": "/bin/zsh"},
        20: {"pid": 20, "name": "herdr", "path": "/bin/herdr"},
    }
    available = {20}
    monkeypatch.setattr(
        ch, "v1_observer_socket_exists", lambda path: int(path.stem) in available
    )
    monkeypatch.setattr(
        ch,
        "v1_client_observation",
        lambda path, pid, **kwargs: {"pid": pid, "instance": "i"},
    )
    assert ch.v1_foreground_observation(processes, [tmp_path]) == {
        "pid": 20,
        "instance": "i",
    }
    processes[30] = {"pid": 30, "name": "herdr", "path": "/bin/herdr"}
    assert ch.v1_foreground_observation(processes, [tmp_path]) is None
    available.add(30)
    assert ch.v1_foreground_observation(processes, [tmp_path]) is None
    del processes[30]
    monkeypatch.setattr(ch, "v1_client_observation", lambda path, pid, **kwargs: None)
    assert ch.v1_foreground_observation(processes, [tmp_path]) is None


def test_v1_foreground_observation_accepts_remote_parent_and_client(
    monkeypatch, tmp_path
):
    processes = {
        20: {"pid": 20, "ppid": 10, "name": "herdr", "path": "/bin/herdr"},
        21: {"pid": 21, "ppid": 20, "name": "herdr", "path": "/bin/herdr"},
    }
    monkeypatch.setattr(ch, "v1_observer_socket_exists", lambda path: path.stem == "21")
    monkeypatch.setattr(
        ch, "v1_client_observation", lambda path, pid, **kwargs: {"pid": pid}
    )
    assert ch.v1_foreground_observation(processes, [tmp_path]) == {"pid": 21}
    processes[20]["ppid"] = 21
    processes[21]["ppid"] = 10
    assert ch.v1_foreground_observation(processes, [tmp_path]) is None


@pytest.mark.parametrize("server_tty", [None, 42])
def test_v1_foreground_observation_ignores_verified_server(
    monkeypatch, tmp_path, server_tty
):
    processes = {
        20: {"pid": 20, "ppid": 1, "name": "herdr", "tty_device": server_tty},
        21: {"pid": 21, "ppid": 10, "name": "herdr", "tty_device": 42},
    }
    monkeypatch.setattr(ch, "v1_observer_socket_exists", lambda path: path.stem == "21")
    monkeypatch.setattr(
        ch, "v1_client_observation", lambda path, pid, **kwargs: {"pid": pid}
    )
    monkeypatch.setattr(
        ch, "v1_is_verified_local_server", lambda pid, paths, **kwargs: pid == 20
    )
    assert ch.v1_foreground_observation(
        processes,
        [tmp_path],
        upstream_sockets=lambda: ["/source"],
        deadline=time.monotonic() + 5,
    ) == {"pid": 21}


def test_v1_foreground_observation_does_not_query_after_deadline(monkeypatch, tmp_path):
    processes = {21: {"pid": 21, "ppid": 10, "name": "herdr", "tty_device": 42}}
    monkeypatch.setattr(ch, "v1_observer_socket_exists", lambda path: True)
    calls = []
    monkeypatch.setattr(
        ch, "v1_client_observation", lambda *args, **kwargs: calls.append(args)
    )
    assert (
        ch.v1_foreground_observation(
            processes, [tmp_path], deadline=time.monotonic() - 1
        )
        is None
    )
    assert calls == []


def test_v1_surface_observation_requests_both_cmux_ids(monkeypatch):
    calls = []
    monkeypatch.setattr(
        ch,
        "run",
        lambda command, **kwargs: (
            calls.append(command) or SimpleNamespace(returncode=0, stdout="{}")
        ),
    )
    monkeypatch.setattr(ch, "v1_foreground_surface_processes", lambda *args: {})
    monkeypatch.setattr(ch, "v1_foreground_observation", lambda *args, **kwargs: None)
    ch.v1_surface_observation({}, "workspace-uuid", "surface-uuid")
    assert (
        "--id-format" in calls[0]
        and calls[0][calls[0].index("--id-format") + 1] == "both"
    )


def test_v1_surface_observation_requires_current_native_ownership(monkeypatch):
    tree = {
        "windows": [
            {"workspaces": [{"id": "w", "panes": [{"surfaces": [{"id": "s"}]}]}]}
        ]
    }
    monkeypatch.setattr(
        ch,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=json.dumps(tree)),
    )
    monkeypatch.setattr(ch, "v1_foreground_surface_processes", lambda *args: {})
    observation = {"pid": 42, "instance": "i", "observation": {}}
    monkeypatch.setattr(
        ch, "v1_foreground_observation", lambda *args, **kwargs: observation
    )
    record = {"connection_id": "bound"}
    client = {
        "pid": 42,
        "instance": "i",
        "workspace": "w",
        "surface": "s",
        "native_ownership": record,
    }
    scan = {"complete": True, "clients": [client]}
    monkeypatch.setattr(ch, "v1_discover_clients_in_tree", lambda *args, **kwargs: scan)
    assert ch.v1_surface_observation({}, "w", "s") == {
        **observation,
        "native_ownership": record,
    }
    scan["complete"] = False
    assert ch.v1_surface_observation({}, "w", "s") is None
    scan["complete"] = True
    client["instance"] = "different"
    assert ch.v1_surface_observation({}, "w", "s") is None


def test_v1_surface_observation_ignores_unrelated_surfaces(monkeypatch):
    surface = {"id": "s", "type": "terminal", "tty": "tty", "processes": []}
    other = {
        "id": "other",
        "type": "terminal",
        "tty": "tty2",
        "processes": [{"pid": 99, "name": "ssh", "children": []}],
    }
    tree = {
        "windows": [
            {"workspaces": [{"id": "w", "panes": [{"surfaces": [surface, other]}]}]}
        ]
    }
    observation = {"pid": 42, "instance": "i", "observation": {}}
    client = {"pid": 42, "instance": "i", "workspace": "w", "surface": "s"}
    monkeypatch.setattr(
        ch,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=json.dumps(tree)),
    )
    monkeypatch.setattr(ch, "v1_foreground_surface_processes", lambda *args: {})
    monkeypatch.setattr(
        ch, "v1_foreground_observation", lambda *args, **kwargs: observation
    )

    def scan(value, directories, **kwargs):
        surfaces = [
            s
            for w in value["windows"]
            for ws in w["workspaces"]
            for pane in ws["panes"]
            for s in pane["surfaces"]
        ]
        return {
            "complete": len(surfaces) == 1 and surfaces[0]["id"] == "s",
            "clients": [client],
        }

    monkeypatch.setattr(ch, "v1_discover_clients_in_tree", scan)
    assert ch.v1_surface_observation({}, "w", "s") == observation


def test_v1_surface_processes_reject_ambiguous_surface():
    surface = {"id": "s", "processes": []}
    tree = {
        "windows": [
            {"workspaces": [{"id": "w", "panes": [{"surfaces": [surface, surface]}]}]}
        ]
    }
    assert ch.v1_foreground_surface_pids(tree, "w", "s") is None


# --- Watch subscriber --------------------------------------------------------


@pytest.fixture
def watch_state(monkeypatch, tmp_path):
    """State on disk, since the subscriber loads and saves it under the lock."""
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    return tmp_path / "state.json"


def seed_state(path, panes, label="proj"):
    path.write_text(
        json.dumps(
            {"sessions": {"": {"workspaces": {"w1": {"label": label, "panes": panes}}}}}
        )
    )


def done_pane(agent="claude"):
    return {"status": "done", "agent": agent, "title": "task"}


def socket_event(pane_id, ws_id, status):
    return {"pane_id": pane_id, "workspace_id": ws_id, "agent_status": status}


def test_watch_clears_pane_herdr_no_longer_tracks(
    monkeypatch, cfg, cmux_calls, watch_state
):
    seed_state(watch_state, {"w1:p1": done_pane()})
    monkeypatch.setattr(ch, "pane_is_tracked", lambda sock, pane_id: False)

    ch.handle_watch_event(cfg, "/tmp/x.sock", socket_event("w1:p1", "w1", "idle"))

    assert ["clear-status", "herdr.w1", "--workspace", "workspace:1"] in cmux_calls
    saved = json.loads(watch_state.read_text())
    assert saved["sessions"][""]["workspaces"]["w1"]["panes"] == {}


def test_watch_leaves_unowned_notifications_when_attention_drains(
    monkeypatch, cfg, cmux_calls, watch_state
):
    seed_state(watch_state, {"w1:p1": done_pane()})
    monkeypatch.setattr(ch, "pane_is_tracked", lambda sock, pane_id: False)

    ch.handle_watch_event(cfg, "/tmp/x.sock", socket_event("w1:p1", "w1", "idle"))

    assert not any(c[0] == "mark-notification-read" for c in cmux_calls)


def test_watch_ignores_tracked_status(monkeypatch, cfg, cmux_calls, watch_state):
    seed_state(watch_state, {"w1:p1": done_pane()})
    monkeypatch.setattr(
        ch, "pane_is_tracked", lambda sock, pane_id: pytest.fail("must not confirm")
    )

    for status in sorted(ch.TRACKED_STATUSES):
        ch.handle_watch_event(cfg, "/tmp/x.sock", socket_event("w1:p1", "w1", status))

    assert cmux_calls == []


def test_watch_keeps_pane_a_newer_transition_reclaimed(
    monkeypatch, cfg, cmux_calls, watch_state
):
    # The hook wrote `working` between the event and the lock; the in-lock
    # confirmation sees it and the stale idle event must not remove the pane.
    seed_state(watch_state, {"w1:p1": {**done_pane(), "status": "working"}})
    monkeypatch.setattr(ch, "pane_is_tracked", lambda sock, pane_id: True)

    ch.handle_watch_event(cfg, "/tmp/x.sock", socket_event("w1:p1", "w1", "idle"))

    assert cmux_calls == []
    saved = json.loads(watch_state.read_text())
    assert "w1:p1" in saved["sessions"][""]["workspaces"]["w1"]["panes"]


def test_watch_ignores_pane_the_plugin_does_not_track(
    monkeypatch, cfg, cmux_calls, watch_state
):
    seed_state(watch_state, {"w1:p1": done_pane()})
    monkeypatch.setattr(
        ch, "pane_is_tracked", lambda sock, pane_id: pytest.fail("must not confirm")
    )

    ch.handle_watch_event(cfg, "/tmp/x.sock", socket_event("w1:p9", "w1", "idle"))

    assert cmux_calls == []


def test_watch_keeps_pane_when_confirmation_fails(cfg, cmux_calls, watch_state):
    seed_state(watch_state, {"w1:p1": done_pane()})

    # pane_is_tracked fails safe: an unreachable server must not drop pills.
    ch.handle_watch_event(cfg, "/nonexistent.sock", socket_event("w1:p1", "w1", "idle"))

    assert cmux_calls == []
    saved = json.loads(watch_state.read_text())
    assert "w1:p1" in saved["sessions"][""]["workspaces"]["w1"]["panes"]


def test_watch_clears_per_pane_pill(monkeypatch, cfg, cmux_calls, watch_state):
    seed_state(watch_state, {"w1:p1": done_pane()})
    monkeypatch.setattr(ch, "pane_is_tracked", lambda sock, pane_id: False)

    ch.handle_watch_event(cfg, "/tmp/x.sock", socket_event("w1:p1", "w1", "idle"))

    assert [
        "clear-status",
        "herdr.w1.w1_p1",
        "--workspace",
        "workspace:1",
    ] in cmux_calls


def test_watch_subscriptions_cover_panes_and_lifecycle():
    subs = ch.watch_subscriptions(["w1:p1", "w2:p3"])

    status = [s for s in subs if s["type"] == "pane.agent_status_changed"]
    assert [s["pane_id"] for s in status] == ["w1:p1", "w2:p3"]
    wildcards = {s["type"] for s in subs if "pane_id" not in s}
    assert wildcards == {"pane.created", "pane.closed"}


def test_pane_is_tracked_reads_current_status(monkeypatch):
    agents = [{"pane_id": "w1:p1", "agent_status": "working"}]
    monkeypatch.setattr(ch, "socket_rpc", lambda *a, **k: {"agents": agents})

    assert ch.pane_is_tracked("/tmp/x.sock", "w1:p1") is True
    assert ch.pane_is_tracked("/tmp/x.sock", "w1:p2") is False


def test_pane_is_tracked_fails_safe(monkeypatch):
    def boom(*a, **k):
        raise ch.SocketError("gone")

    monkeypatch.setattr(ch, "socket_rpc", boom)

    # Unverified means "leave it alone", never "remove it".
    assert ch.pane_is_tracked("/tmp/x.sock", "w1:p1") is True


def test_pid_is_daemon_distinguishes_modes(monkeypatch):
    def fake_ps(cmd, capture_output, text, timeout):
        return SimpleNamespace(stdout="python3 /plugin/cmux_herdr.py watch\n")

    monkeypatch.setattr(ch.subprocess, "run", fake_ps)

    assert ch.pid_is_daemon(123, "watch") is True
    assert ch.pid_is_daemon(123, "remote") is False


def test_watch_pid_path_is_per_session(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)

    assert ch.watch_pid_path("tom") != ch.watch_pid_path("coten")
    # Session names reach the filesystem; keep them to safe characters.
    assert "/" not in ch.watch_pid_path("a/b").name


def test_reconcile_keeps_state_when_agent_snapshot_fails(
    monkeypatch, state, cmux_calls
):
    state["legacy_log_cleared"] = True
    bucket = ch.session_bucket(state)
    bucket["workspaces"]["w1"] = {
        "label": "project",
        "panes": {"p": {"status": "done", "agent": "claude", "title": "task"}},
    }
    before = json.loads(json.dumps(state))

    def fail(_args):
        raise RuntimeError("temporary outage")

    monkeypatch.setattr(ch, "herdr_cli", fail)
    ch.reconcile({}, state)
    assert state == before
    assert not cmux_calls


def test_attention_ignores_stale_forced_acknowledgement():
    attention = {}
    ch.apply_attention_status(attention, "pane", "done", "episode-a")
    ch.apply_attention_status(attention, "pane", "working", "working-b")
    ch.apply_attention_status(attention, "pane", "done", "episode-b")
    assert not ch.force_acknowledge_attention(attention, "pane", "episode-a")
    assert attention["pane"] == {"status": "done", "episode": "episode-b"}
    assert ch.force_acknowledge_attention(attention, "pane", "episode-b")
    assert not ch.has_pending_attention(attention, "pane")


def test_attention_preserves_done_on_idle_and_never_forces_blocked():
    attention = {}
    ch.apply_attention_status(attention, "pane", "done", "episode-a")
    ch.apply_attention_status(attention, "pane", "idle", "idle-a")
    assert attention["pane"]["episode"] == "episode-a"
    ch.apply_attention_status(attention, "pane", "blocked", "episode-b")
    assert not ch.force_acknowledge_attention(attention, "pane", "episode-b")
    assert attention["pane"]["status"] == "blocked"
    ch.apply_attention_status(attention, "pane", "working", "working-b")
    assert "pane" not in attention


def test_acknowledged_episode_is_not_resurrected_by_duplicate_event():
    attention = {}
    ch.apply_attention_status(attention, "pane", "done", "episode-a")
    assert ch.force_acknowledge_attention(attention, "pane", "episode-a")
    ch.apply_attention_status(attention, "pane", "done", "episode-a")
    assert not ch.has_pending_attention(attention, "pane")
    ch.apply_attention_status(attention, "pane", "done", "episode-b")
    assert ch.has_pending_attention(attention, "pane")


def test_attention_rejects_out_of_order_status_after_new_completion():
    attention = {}
    ch.apply_attention_status(attention, "pane", "done", "episode-a", sequence=1)
    ch.apply_attention_status(attention, "pane", "working", "working", sequence=2)
    ch.apply_attention_status(attention, "pane", "done", "episode-b", sequence=3)
    ch.apply_attention_status(attention, "pane", "working", "old", sequence=2)
    assert attention["pane"]["episode"] == "episode-b"


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
    ch.reconcile_aggregate_notification(
        cfg, "w", "s", {"p": {"status": "blocked", "episode": "1"}}, notifications
    )
    assert cmux_calls == []


def test_aggregate_notification_restores_unread_silently(cfg, cmux_calls):
    ch.reconcile_aggregate_notification(
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

    monkeypatch.setattr(ch, "run", fake_run)
    result = ch.reconcile_aggregate_notification(
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
            "subtitle": ch.NOTIFICATION_SUBTITLE,
        }
    ]
    attention = {
        "p1": {"status": "blocked", "episode": "1"},
        "p2": {"status": "done", "episode": "2"},
    }
    ch.reconcile_aggregate_notification(cfg, "w", "s", attention, previous, "our-id")
    assert cmux_calls[0][:2] == ["rpc", "notification.create_for_target"]
    assert json.loads(cmux_calls[0][2])["title"] == "herdr: 1 waiting · 1 finished"


def test_aggregate_notification_marks_only_its_own_id_read(cfg, cmux_calls):
    notifications = [
        {
            "id": "our-id",
            "workspace_id": "w",
            "surface_id": "s",
            "is_read": False,
            "subtitle": ch.NOTIFICATION_SUBTITLE,
        },
        {"id": "foreign", "workspace_id": "w", "surface_id": "other", "is_read": False},
    ]
    ch.reconcile_aggregate_notification(cfg, "w", "s", {}, notifications, "our-id")
    assert cmux_calls == [["mark-notification-read", "--id", "our-id"]]


def test_attention_sidebar_uses_pending_completion_after_idle():
    panes = {"p": {"status": "idle", "agent": "claude", "title": "task"}}
    attention = {"p": {"status": "done", "episode": "a"}}
    visible = ch.sidebar_panes(panes, attention)
    assert visible["p"]["status"] == "done"
    assert panes["p"]["status"] == "idle"


def test_attention_sidebar_preserves_pending_blocked_when_agent_state_unknown():
    panes = {"p": {"status": "unknown", "agent": "letta", "title": "task"}}
    attention = {"p": {"status": "blocked", "episode": "a"}}
    assert ch.sidebar_panes(panes, attention)["p"]["status"] == "blocked"


def test_attention_sidebar_prefers_current_work_over_old_completion():
    panes = {"p": {"status": "working", "agent": "claude", "title": "task"}}
    attention = {"p": {"status": "done", "episode": "a"}}
    assert ch.sidebar_panes(panes, attention)["p"]["status"] == "working"


def test_attention_sidebar_hides_acknowledged_done():
    panes = {"p": {"status": "done", "agent": "claude", "title": "task"}}
    attention = {"p": {"acknowledged": "a"}}
    assert ch.sidebar_panes(panes, attention) == {}


def test_unsequenced_duplicate_preserves_ordering_watermark():
    attention = {}
    ch.apply_attention_status(attention, "pane", "done", "episode-b", sequence=30)
    ch.apply_attention_status(attention, "pane", "done", "episode-b")
    ch.apply_attention_status(attention, "pane", "working", "old", sequence=20)
    assert attention["pane"]["episode"] == "episode-b"
    assert attention["pane"]["sequence"] == 30


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        ([], {"session": "", "remote": None}),
        (["--session", "work"], {"session": "work", "remote": None}),
        (["session", "attach", "work"], {"session": "work", "remote": None}),
        (
            ["--remote", "tom@host", "--session", "work"],
            {"session": "work", "remote": "tom@host"},
        ),
        (["agent", "list"], None),
        (["--session", "work", "pane", "list"], None),
        (["--help"], None),
    ],
)
def test_interactive_client_arguments(args, expected):
    assert ch.interactive_client_target(args) == expected


def test_snapshot_reconciles_attention_with_seen_and_new_work():
    attention = {}

    def agent(status, sequence):
        return {
            "pane_id": "p",
            "terminal_id": "t",
            "workspace_id": "w",
            "agent_status": status,
            "state_change_seq": sequence,
            "agent": "claude",
        }

    ch.reconcile_agent_snapshot(attention, [agent("done", 1)], "generation")
    original = attention["p"]["episode"]
    ch.reconcile_agent_snapshot(attention, [agent("idle", 1)], "generation")
    assert attention["p"]["episode"] == original
    ch.reconcile_agent_snapshot(attention, [agent("working", 2)], "generation")
    assert not ch.has_pending_attention(attention, "p")
    ch.reconcile_agent_snapshot(attention, [agent("done", 3)], "generation")
    assert attention["p"]["episode"] != original


def test_snapshot_new_server_generation_accepts_reset_sequence():
    attention = {}
    first = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 100,
    }
    second = {**first, "state_change_seq": 1}
    ch.reconcile_agent_snapshot(attention, [first], "old")
    ch.reconcile_agent_snapshot(attention, [second], "new")
    assert attention["p"]["episode"] != "old:t:100"
    assert attention["p"]["sequence"] == 1


def test_snapshot_replaced_terminal_does_not_reuse_old_episode():
    attention = {}
    first = {
        "pane_id": "p",
        "terminal_id": "old",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 9,
    }
    second = {
        **first,
        "terminal_id": "new",
        "agent_status": "working",
        "state_change_seq": 0,
    }
    ch.reconcile_agent_snapshot(attention, [first], "generation")
    ch.reconcile_agent_snapshot(attention, [second], "generation")
    assert not ch.has_pending_attention(attention, "p")


def test_v1_snapshot_persists_ledger_and_agent_metadata():
    state = {}
    agents = [
        {
            "pane_id": "p",
            "terminal_id": "t",
            "workspace_id": "w",
            "agent_status": "done",
            "state_change_seq": 1,
            "agent": "codex",
        }
    ]
    ch.apply_v1_snapshot(state, "local:tom", "boot", agents, ["p"])
    source = state["v1"]["sources"]["local:tom"]
    assert source["attention"]["p"]["status"] == "done"
    assert source["panes"]["p"]["agent"] == "codex"
    before = json.loads(json.dumps(state))
    ch.apply_v1_snapshot(state, "local:tom", "boot", None, None)
    assert state == before


def test_v1_snapshot_only_removes_panes_from_authoritative_list():
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 1,
    }
    ch.apply_v1_snapshot(state, "remote:one", "boot", [agent], ["p"])
    ch.apply_v1_snapshot(state, "remote:one", "boot", [], None)
    assert ch.has_pending_attention(
        state["v1"]["sources"]["remote:one"]["attention"], "p"
    )
    ch.apply_v1_snapshot(state, "remote:one", "boot", [], [])
    assert "p" not in state["v1"]["sources"]["remote:one"]["attention"]


def test_unsequenced_event_keeps_generation_for_restart():
    attention = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "agent_status": "done",
        "state_change_seq": 30,
    }
    ch.reconcile_agent_snapshot(attention, [agent], "old")
    ch.apply_attention_status(attention, "p", "working", "work")
    ch.reconcile_agent_snapshot(attention, [{**agent, "state_change_seq": 1}], "new")
    assert attention["p"]["status"] == "done"
    assert attention["p"]["generation"] == "new"


def test_stale_snapshot_does_not_replace_sidebar_pane_status():
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 30,
    }
    ch.apply_v1_snapshot(state, "source", "boot", [agent], ["p"])
    ch.apply_v1_snapshot(
        state,
        "source",
        "boot",
        [{**agent, "agent_status": "working", "state_change_seq": 20}],
        ["p"],
    )
    source = state["v1"]["sources"]["source"]
    assert (
        ch.sidebar_panes(source["panes"], source["attention"])["p"]["status"] == "done"
    )


@pytest.mark.parametrize(
    "args", [["--session="], ["--remote="], ["--session"], ["--remote"], ["--version"]]
)
def test_interactive_client_rejects_empty_or_noninteractive_args(args):
    assert ch.interactive_client_target(args) is None


def test_v1_refresh_reads_agent_and_pane_snapshots(monkeypatch):
    calls = []

    def fake_rpc(path, method, params):
        calls.append(method)
        if method == "agent.list":
            return {
                "agents": [
                    {
                        "pane_id": "p",
                        "terminal_id": "t",
                        "workspace_id": "w",
                        "agent_status": "blocked",
                        "state_change_seq": 2,
                    }
                ]
            }
        return {"panes": [{"pane_id": "p"}]}

    monkeypatch.setattr(ch, "socket_rpc", fake_rpc)
    state = {}
    assert ch.refresh_v1_source(state, "source", "boot", "/socket")
    assert calls == ["session.snapshot", "agent.list", "pane.list", "session.snapshot"]
    assert state["v1"]["sources"]["source"]["attention"]["p"]["status"] == "blocked"


def test_v1_refresh_binds_completion_to_same_server_instance(monkeypatch):
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "agent_status": "done",
        "state_change_seq": 7,
    }

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": boot, "agents": [agent]}}
        if method == "agent.list":
            return {"agents": [agent]}
        return {"panes": [{"pane_id": "p"}]}

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    state = {}
    assert ch.refresh_v1_source(state, "source", "gen", "/socket")
    assert state["v1"]["sources"]["source"]["attention"]["p"]["server_boot_id"] == boot


def test_v1_refresh_binds_quick_done_to_idle_event_for_known_server(monkeypatch):
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 6,
    }

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": boot, "agents": [dict(agent)]}}
        return (
            {"agents": [dict(agent)]}
            if method == "agent.list"
            else {"panes": [{"pane_id": "p"}]}
        )

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    state = {}
    assert ch.refresh_v1_source(state, "source", "gen", "/socket")
    assert ch.apply_v1_status_event(
        state,
        "source",
        {
            "pane_id": "p",
            "terminal_id": "t",
            "state_change_seq": 7,
            "agent_status": "done",
        },
        event_boot=boot,
    )
    agent.update(agent_status="idle", state_change_seq=7)
    assert ch.refresh_v1_source(state, "source", "gen", "/socket")
    entry = state["v1"]["sources"]["source"]["attention"]["p"]
    assert entry["status"] == "done" and entry["server_boot_id"] == boot


def test_v1_status_event_rejects_a_stale_subscription_server():
    state = {
        "v1": {
            "sources": {
                "source": {
                    "server_boot_id": "a" * 32,
                    "panes": {"p": {"status": "working"}},
                    "attention": {},
                }
            }
        }
    }
    before = json.loads(json.dumps(state))
    assert not ch.apply_v1_status_event(
        state, "source", {"pane_id": "p", "agent_status": "done"}, event_boot="b" * 32
    )
    assert state == before


@pytest.mark.parametrize("next_status", ["working", "idle", "done"])
def test_v1_unknown_keeps_blocked_until_known_transition(next_status):
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "blocked",
        "state_change_seq": 7,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"], boot_id=boot)
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    unknown = {
        "pane_id": "p",
        "terminal_id": "t",
        "agent_status": "unknown",
        "state_change_seq": 8,
    }
    assert ch.apply_v1_status_event(state, "source", unknown, event_boot=boot)
    assert source["attention"]["p"]["status"] == "blocked"
    known = {**unknown, "agent_status": next_status, "state_change_seq": 9}
    assert ch.apply_v1_status_event(state, "source", known, event_boot=boot)
    assert source["attention"]["p"].get("status") != "blocked"


def test_v1_missing_agent_after_generation_change_retires_old_blocked():
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "blocked",
        "state_change_seq": 7,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "old", [agent], ["p"])
    absent = {**agent, "agent_status": "unknown", "state_change_seq": 1}
    ch.apply_v1_snapshot(
        state, "source", "new", [], ["p"], pane_records=[absent], reconnect=True
    )
    source = state["v1"]["sources"]["source"]
    assert not ch.has_pending_attention(source["attention"], "p")
    assert ch.sidebar_panes(source["panes"], source["attention"]) == {}
    snapshot = {"agents": [], "panes": [absent]}
    ch.v1_reconcile_absent_agents(
        state, "source", "new", snapshot, [], [absent], snapshot
    )
    assert ch.sidebar_panes(source["panes"], source["attention"]) == {}


def test_v1_unknown_snapshot_after_generation_change_retires_old_blocked():
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "blocked",
        "state_change_seq": 7,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "old", [agent], ["p"])
    unknown = {**agent, "agent_status": "unknown", "state_change_seq": 1}
    ch.apply_v1_snapshot(state, "source", "new", [unknown], ["p"], reconnect=True)
    source = state["v1"]["sources"]["source"]
    assert not ch.has_pending_attention(source["attention"], "p")
    assert ch.sidebar_panes(source["panes"], source["attention"]) == {}


def test_v1_legacy_unknown_absence_without_identity_stays_blocked(monkeypatch):
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "blocked",
        "state_change_seq": 7,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    assert ch.apply_v1_status_event(
        state, "source", {"pane_id": "p", "agent_status": "unknown"}
    )
    pane = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "unknown",
    }

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": {"agents": [], "panes": [pane]}}
        return {"agents": []} if method == "agent.list" else {"panes": [pane]}

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    assert ch.refresh_v1_source(state, "source", "g", "/socket", reconnect=True)
    source = state["v1"]["sources"]["source"]
    assert source["attention"]["p"]["status"] == "blocked"


def test_v1_unknown_snapshot_of_present_agent_preserves_blocked():
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "blocked",
        "state_change_seq": 7,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"], boot_id=boot)
    source = state["v1"]["sources"]["source"]
    unknown = {**agent, "agent_status": "unknown", "state_change_seq": 8}
    ch.apply_v1_snapshot(
        state, "source", "g", [unknown], ["p"], boot_id=boot, reconnect=True
    )
    assert source["attention"]["p"]["status"] == "blocked"
    assert source["attention"]["p"]["sequence"] == 8


def test_v1_unknown_keeps_blocked_until_confirmed_absence():
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "blocked",
        "state_change_seq": 7,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"], boot_id=boot)
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    unknown = {
        "pane_id": "p",
        "terminal_id": "t",
        "agent_status": "unknown",
        "state_change_seq": 8,
    }
    assert ch.apply_v1_status_event(state, "source", unknown, event_boot=boot)
    assert source["attention"]["p"]["status"] == "blocked"
    assert source["attention"]["p"]["sequence"] == 8
    absent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "unknown",
        "state_change_seq": 8,
    }
    snapshot = {"server_boot_id": boot, "agents": [], "panes": [absent]}
    ch.v1_reconcile_absent_agents(
        state, "source", "g", snapshot, [], [absent], snapshot
    )
    assert not ch.has_pending_attention(source["attention"], "p")


def test_v1_unknown_event_preserves_completion_identity_and_ordering():
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 7,
        "completion_seq": 7,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"], boot_id=boot)
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    source["attention"]["p"]["server_boot_id"] = boot
    event = {
        "pane_id": "p",
        "terminal_id": "t",
        "state_change_seq": 8,
        "agent_status": "unknown",
    }
    assert ch.apply_v1_status_event(state, "source", event, event_boot=boot)
    observation = {
        "boot_id": boot,
        "pane_id": "p",
        "outer_focused": True,
        "agent_state_change_seq": None,
        "pane_state": {
            "terminal_id": "t",
            "state_change_seq": 9,
            "agent_present": False,
            "released_completion_seq": 7,
        },
    }
    snapshot = {
        "server_boot_id": boot,
        "agents": [],
        "panes": [{"pane_id": "p", "terminal_id": "t", "state_change_seq": 9}],
    }
    assert (
        ch.v1_verified_focus_episode(state, "source", observation, snapshot) == "g:t:7"
    )
    assert ch.apply_v1_status_event(
        state,
        "source",
        {**event, "state_change_seq": 7, "agent_status": "blocked"},
        event_boot=boot,
    )
    assert source["attention"]["p"]["status"] == "done"
    assert source["attention"]["p"]["sequence"] == 8


def test_v1_saved_unknown_state_recovers_exact_completion_identity():
    boot = "a" * 32
    entry = {
        "status": "done",
        "episode": "g:t:7",
        "sequence": 8,
        "generation": "g",
        "terminal_id": "t",
        "server_boot_id": boot,
    }
    state = {
        "v1": {
            "sources": {
                "source": {
                    "attention": {"p": entry},
                    "panes": {"p": {"terminal_id": "t", "status": "unknown"}},
                    "server_boot_id": boot,
                    "cursor_generation": "g",
                }
            }
        }
    }
    observation = {
        "boot_id": boot,
        "pane_id": "p",
        "outer_focused": True,
        "agent_state_change_seq": None,
        "pane_state": {
            "terminal_id": "t",
            "state_change_seq": 10,
            "agent_present": False,
            "released_completion_seq": 7,
        },
    }
    snapshot = {
        "server_boot_id": boot,
        "agents": [],
        "panes": [{"pane_id": "p", "terminal_id": "t", "state_change_seq": 10}],
    }
    assert (
        ch.v1_verified_focus_episode(state, "source", observation, snapshot) == "g:t:7"
    )
    assert ch.apply_v1_status_event(
        state,
        "source",
        {
            "pane_id": "p",
            "terminal_id": "t",
            "state_change_seq": 9,
            "agent_status": "unknown",
        },
        event_boot=boot,
    )
    assert (
        ch.v1_verified_focus_episode(state, "source", observation, snapshot) == "g:t:7"
    )
    entry["episode"] = "other:t:7"
    assert ch.v1_verified_focus_episode(state, "source", observation, snapshot) is None


@pytest.mark.parametrize("release_status", ["done", "idle", "unknown"])
def test_v1_release_event_preserves_the_exact_completed_episode(release_status):
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 7,
        "completion_seq": 7,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"], boot_id=boot)
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    source["attention"]["p"]["server_boot_id"] = boot
    event = {
        "pane_id": "p",
        "terminal_id": "t",
        "agent_status": release_status,
        "state_change_seq": 8,
        "completion_seq": None,
        "released_completion_seq": 7,
    }
    assert ch.apply_v1_status_event(state, "source", event, event_boot=boot)
    entry = source["attention"]["p"]
    assert entry["status"] == "done"
    assert entry["episode"] == "g:t:7"
    assert entry["sequence"] == 8
    observation = {
        "boot_id": boot,
        "pane_id": "p",
        "outer_focused": True,
        "agent_state_change_seq": None,
        "pane_state": {
            "terminal_id": "t",
            "state_change_seq": 8,
            "agent_present": False,
            "released_completion_seq": 7,
        },
    }
    snapshot = {
        "server_boot_id": boot,
        "agents": [],
        "panes": [{"pane_id": "p", "terminal_id": "t", "state_change_seq": 8}],
    }
    assert (
        ch.v1_verified_focus_episode(state, "source", observation, snapshot) == "g:t:7"
    )


def test_v1_release_event_keeps_acknowledgment_and_rejects_stale_provenance():
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 7,
        "completion_seq": 7,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"], boot_id=boot)
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    assert ch.force_acknowledge_attention(source["attention"], "p", "g:t:7")
    event = {
        "pane_id": "p",
        "terminal_id": "t",
        "agent_status": "done",
        "state_change_seq": 8,
        "released_completion_seq": 7,
    }
    assert ch.apply_v1_status_event(state, "source", event, event_boot=boot)
    assert source["attention"]["p"]["acknowledged"] == "g:t:7"
    assert source["attention"]["p"]["sequence"] == 8
    assert ch.apply_v1_status_event(state, "source", event, event_boot=boot)
    assert not ch.has_pending_attention(source["attention"], "p")
    assert not ch.apply_v1_status_event(
        state,
        "source",
        {**event, "state_change_seq": 9, "released_completion_seq": 6},
        event_boot=boot,
    )
    assert not ch.has_pending_attention(source["attention"], "p")


def test_v1_old_release_does_not_restore_completion_after_new_work():
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 7,
        "completion_seq": 7,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"], boot_id=boot)
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    working = {
        "pane_id": "p",
        "terminal_id": "t",
        "agent_status": "working",
        "state_change_seq": 9,
    }
    assert ch.apply_v1_status_event(state, "source", working, event_boot=boot)
    release = {
        **working,
        "agent_status": "done",
        "state_change_seq": 8,
        "released_completion_seq": 7,
    }
    assert not ch.apply_v1_status_event(state, "source", release, event_boot=boot)
    assert not ch.has_pending_attention(source["attention"], "p")


@pytest.mark.parametrize("present", [False, True])
@pytest.mark.parametrize("acknowledged", [False, True])
def test_v1_release_snapshot_preserves_exact_episode(
    monkeypatch, present, acknowledged
):
    boot = "a" * 32
    generation = json.dumps(["g", boot], separators=(",", ":"))
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 7,
        "completion_seq": 7,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", generation, [agent], ["p"], boot_id=boot)
    source = state["v1"]["sources"]["source"]
    source.update(server_boot_id=boot, socket_generation="g")
    source["attention"]["p"]["server_boot_id"] = boot
    episode = f"{generation}:t:7"
    if acknowledged:
        assert ch.force_acknowledge_attention(source["attention"], "p", episode)
    pane = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "idle" if present else "unknown",
        "state_change_seq": 8,
        "released_completion_seq": 7,
    }
    agents = (
        [
            {
                **agent,
                "agent_status": "idle",
                "state_change_seq": 8,
                "completion_seq": None,
            }
        ]
        if present
        else []
    )
    snapshot = {"server_boot_id": boot, "agents": agents, "panes": [pane]}

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": snapshot}
        return {"agents": agents} if method == "agent.list" else {"panes": [pane]}

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    assert ch.refresh_v1_source(state, "source", "g", "/socket", reconnect=True)
    entry = state["v1"]["sources"]["source"]["attention"]["p"]
    assert entry.get("acknowledged" if acknowledged else "episode") == episode
    assert entry["sequence"] == 8
    assert ch.has_pending_attention({"p": entry}, "p") is not acknowledged
    release = {
        "pane_id": "p",
        "terminal_id": "t",
        "agent_status": "done",
        "state_change_seq": 8,
        "released_completion_seq": 7,
    }
    assert ch.apply_v1_status_event(state, "source", release, event_boot=boot)
    entry = state["v1"]["sources"]["source"]["attention"]["p"]
    assert entry.get("acknowledged" if acknowledged else "episode") == episode
    assert ch.has_pending_attention({"p": entry}, "p") is not acknowledged
    if present and not acknowledged:
        observation = {
            "boot_id": boot,
            "pane_id": "p",
            "outer_focused": True,
            "agent_state_change_seq": 8,
            "pane_state": {
                "terminal_id": "t",
                "state_change_seq": 8,
                "agent_present": True,
                "released_completion_seq": 7,
            },
        }
        assert (
            ch.v1_verified_focus_episode(state, "source", observation, snapshot)
            == episode
        )


@pytest.mark.parametrize("fresh", [False, True])
def test_v1_release_snapshot_does_not_invent_completion_from_release_sequence(
    monkeypatch, fresh
):
    boot = "a" * 32
    generation = json.dumps(["g", boot], separators=(",", ":"))
    initial = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 6,
    }
    state = {}
    if not fresh:
        ch.apply_v1_snapshot(
            state, "source", generation, [initial], ["p"], boot_id=boot
        )
        state["v1"]["sources"]["source"].update(
            server_boot_id=boot, socket_generation="g"
        )
    pane = {
        **initial,
        "agent_status": "done",
        "state_change_seq": 8,
        "released_completion_seq": 7,
    }
    agent = {**pane, "completion_seq": None}
    snapshot = {"server_boot_id": boot, "agents": [agent], "panes": [pane]}

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": snapshot}
        return {"agents": [agent]} if method == "agent.list" else {"panes": [pane]}

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    assert ch.refresh_v1_source(state, "source", "g", "/socket", reconnect=True)
    assert not ch.has_pending_attention(
        state["v1"]["sources"]["source"]["attention"], "p"
    )


def test_v1_release_during_snapshot_does_not_replace_pending_completion(monkeypatch):
    boot = "a" * 32
    generation = json.dumps(["g", boot], separators=(",", ":"))
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 7,
        "completion_seq": 7,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", generation, [agent], ["p"], boot_id=boot)
    state["v1"]["sources"]["source"].update(server_boot_id=boot, socket_generation="g")
    before = {"server_boot_id": boot, "panes": [agent]}
    released = {
        **agent,
        "state_change_seq": 8,
        "completion_seq": None,
        "released_completion_seq": 7,
    }
    after = {"server_boot_id": boot, "panes": [released]}
    snapshots = iter([before, after])

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": next(snapshots)}
        return (
            {"agents": [released]} if method == "agent.list" else {"panes": [released]}
        )

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    assert not ch.refresh_v1_source(state, "source", "g", "/socket", reconnect=True)
    entry = state["v1"]["sources"]["source"]["attention"]["p"]
    assert entry["episode"] == f"{generation}:t:7"


@pytest.mark.parametrize("acknowledged", [False, True])
@pytest.mark.parametrize("present", [False, True])
@pytest.mark.parametrize("fresh", [False, True])
@pytest.mark.parametrize("initial_status", ["working", "blocked"])
def test_v1_delayed_completion_after_release_snapshot_is_restored(
    monkeypatch, acknowledged, present, fresh, initial_status
):
    boot = "a" * 32
    generation = json.dumps(["g", boot], separators=(",", ":"))
    initial = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": initial_status,
        "state_change_seq": 6,
    }
    state = {}
    if not fresh:
        ch.apply_v1_snapshot(
            state, "source", generation, [initial], ["p"], boot_id=boot
        )
        state["v1"]["sources"]["source"].update(
            server_boot_id=boot, socket_generation="g"
        )
    pane = {
        **initial,
        "agent_status": "unknown" if initial_status == "blocked" else "done",
        "state_change_seq": 8,
        "completion_seq": None,
        "released_completion_seq": 7,
    }
    if acknowledged:
        pane["tokens"] = {"cmux_herdr_ack": f"{generation}:t:7"}
    agents = [pane] if present else []
    snapshot = {"server_boot_id": boot, "agents": agents, "panes": [pane]}

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": snapshot}
        return {"agents": agents} if method == "agent.list" else {"panes": [pane]}

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    assert ch.refresh_v1_source(state, "source", "g", "/socket", reconnect=True)
    event = {
        **initial,
        "agent_status": "done",
        "state_change_seq": 7,
        "completion_seq": 7,
    }
    assert ch.apply_v1_status_event(state, "source", event, event_boot=boot)
    entry = state["v1"]["sources"]["source"]["attention"]["p"]
    assert (
        entry.get("acknowledged" if acknowledged else "episode") == f"{generation}:t:7"
    )
    assert entry["sequence"] == 8
    assert ch.has_pending_attention({"p": entry}, "p") is not acknowledged
    if not acknowledged:
        invalidation = {**pane, "released_completion_seq": None}
        assert ch.apply_v1_status_event(state, "source", invalidation, event_boot=boot)
        assert not ch.has_pending_attention(
            state["v1"]["sources"]["source"]["attention"], "p"
        )


@pytest.mark.parametrize("new_sequence", [8, 9])
def test_v1_new_state_invalidates_old_release_provenance(new_sequence):
    boot = "a" * 32
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 6,
    }
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"], boot_id=boot)
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    source["attention"]["p"].update(sequence=8, released_completion_seq=7)
    assert ch.apply_v1_status_event(
        state,
        "source",
        {**agent, "agent_status": "unknown", "state_change_seq": new_sequence},
        event_boot=boot,
    )
    assert ch.apply_v1_status_event(
        state,
        "source",
        {**agent, "agent_status": "idle", "state_change_seq": 7, "completion_seq": 7},
        event_boot=boot,
    )
    assert not ch.has_pending_attention(source["attention"], "p")


@pytest.mark.parametrize("present", [False, True])
def test_v1_new_snapshot_invalidates_old_release_provenance(monkeypatch, present):
    boot = "a" * 32
    pane = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "unknown",
        "state_change_seq": 8,
        "released_completion_seq": 7,
    }
    state = {}

    def rpc(path, method, params):
        agents = [pane] if present else []
        if method == "session.snapshot":
            return {
                "snapshot": {"server_boot_id": boot, "agents": agents, "panes": [pane]}
            }
        return {"agents": agents} if method == "agent.list" else {"panes": [pane]}

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    assert ch.refresh_v1_source(state, "source", "g", "/socket")
    pane.update(state_change_seq=9, released_completion_seq=None)
    assert ch.refresh_v1_source(state, "source", "g", "/socket")
    event = {**pane, "agent_status": "done", "state_change_seq": 7, "completion_seq": 7}
    assert ch.apply_v1_status_event(state, "source", event, event_boot=boot)
    assert not ch.has_pending_attention(
        state["v1"]["sources"]["source"]["attention"], "p"
    )


@pytest.mark.parametrize("sequence,status", [(8, "idle"), (9, "unknown")])
def test_v1_release_invalidation_retires_unfocusable_completion(sequence, status):
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 7,
        "completion_seq": 7,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"], boot_id=boot)
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    release = {
        **agent,
        "agent_status": "idle",
        "state_change_seq": 8,
        "completion_seq": None,
        "released_completion_seq": 7,
    }
    assert ch.apply_v1_status_event(state, "source", release, event_boot=boot)
    assert ch.has_pending_attention(source["attention"], "p")
    assert ch.apply_v1_status_event(
        state,
        "source",
        {
            **release,
            "released_completion_seq": None,
            "state_change_seq": sequence,
            "agent_status": status,
        },
        event_boot=boot,
    )
    assert not ch.has_pending_attention(source["attention"], "p")


def test_v1_repeated_unknown_without_release_preserves_completion(monkeypatch):
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 7,
        "completion_seq": 7,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"], boot_id=boot)
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    event = {
        **agent,
        "agent_status": "unknown",
        "state_change_seq": 8,
        "completion_seq": None,
    }
    assert ch.apply_v1_status_event(state, "source", event, event_boot=boot)
    assert ch.apply_v1_status_event(state, "source", event, event_boot=boot)
    assert ch.has_pending_attention(source["attention"], "p")
    pane = {**event, "released_completion_seq": None}
    snapshot = {"server_boot_id": boot, "agents": [pane], "panes": [pane]}
    monkeypatch.setattr(
        ch,
        "socket_rpc",
        lambda path, method, params: (
            {"snapshot": snapshot}
            if method == "session.snapshot"
            else {"agents": [pane]}
            if method == "agent.list"
            else {"panes": [pane]}
        ),
    )
    # Keep the same boot-derived generation used by a source refresh.
    generation = json.dumps(["g", boot], separators=(",", ":"))
    source["cursor_generation"] = generation
    source["attention"]["p"]["generation"] = generation
    source["attention"]["p"]["episode"] = f"{generation}:t:7"
    source["socket_generation"] = "g"
    assert ch.refresh_v1_source(state, "source", "g", "/socket")
    assert ch.has_pending_attention(state["v1"]["sources"]["source"]["attention"], "p")


def test_v1_acknowledged_release_invalidation_advances_watermark(monkeypatch):
    boot = "a" * 32
    generation = json.dumps(["g", boot], separators=(",", ":"))
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 7,
        "completion_seq": 7,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", generation, [agent], ["p"], boot_id=boot)
    source = state["v1"]["sources"]["source"]
    source.update(server_boot_id=boot, socket_generation="g")
    assert ch.apply_v1_status_event(
        state,
        "source",
        {
            **agent,
            "agent_status": "idle",
            "state_change_seq": 8,
            "completion_seq": None,
            "released_completion_seq": 7,
        },
        event_boot=boot,
    )
    assert ch.force_acknowledge_attention(source["attention"], "p", f"{generation}:t:7")
    pane = {
        **agent,
        "agent_status": "unknown",
        "state_change_seq": 12,
        "completion_seq": None,
    }
    snapshot = {"server_boot_id": boot, "agents": [], "panes": [pane]}
    monkeypatch.setattr(
        ch,
        "socket_rpc",
        lambda path, method, params: (
            {"snapshot": snapshot}
            if method == "session.snapshot"
            else {"agents": []}
            if method == "agent.list"
            else {"panes": [pane]}
        ),
    )
    assert ch.refresh_v1_source(state, "source", "g", "/socket")
    assert ch.apply_v1_status_event(
        state,
        "source",
        {**agent, "state_change_seq": 10, "completion_seq": 10},
        event_boot=boot,
    )
    assert not ch.has_pending_attention(
        state["v1"]["sources"]["source"]["attention"], "p"
    )


def test_v1_newer_noncompletion_idle_event_retires_previous_done():
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 7,
        "completion_seq": 7,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    event = {
        "pane_id": "p",
        "terminal_id": "t",
        "state_change_seq": 8,
        "completion_seq": None,
        "agent_status": "idle",
    }
    assert ch.apply_v1_status_event(state, "source", event, event_boot=boot)
    assert not ch.has_pending_attention(source["attention"], "p")
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [
            {
                **agent,
                "agent_status": "idle",
                "state_change_seq": 8,
                "completion_seq": None,
            }
        ],
        ["p"],
        reconnect=True,
    )
    assert not ch.has_pending_attention(source["attention"], "p")


def test_v1_sequenced_events_do_not_resurrect_a_newer_acknowledged_completion():
    boot = "a" * 32
    generation = "generation"
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 6,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", generation, [agent], ["p"])
    state["v1"]["sources"]["source"]["server_boot_id"] = boot

    def event(status, sequence):
        return {
            "pane_id": "p",
            "terminal_id": "t",
            "state_change_seq": sequence,
            "agent_status": status,
        }

    assert ch.apply_v1_status_event(state, "source", event("done", 7), event_boot=boot)
    ch.apply_v1_snapshot(
        state,
        "source",
        generation,
        [{**agent, "agent_status": "done", "state_change_seq": 9}],
        ["p"],
    )
    attention = state["v1"]["sources"]["source"]["attention"]
    episode = attention["p"]["episode"]
    assert ch.force_acknowledge_attention(attention, "p", episode)
    assert ch.apply_v1_status_event(
        state, "source", event("working", 8), event_boot=boot
    )
    assert ch.apply_v1_status_event(state, "source", event("done", 9), event_boot=boot)
    assert attention["p"]["acknowledged"] == episode
    assert not ch.has_pending_attention(attention, "p")


def test_v1_sequenced_done_event_has_exact_episode_before_snapshot():
    boot = "a" * 32
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 6,
    }
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    assert ch.apply_v1_status_event(
        state,
        "source",
        {
            "pane_id": "p",
            "terminal_id": "t",
            "state_change_seq": 7,
            "agent_status": "done",
        },
        event_boot=boot,
    )
    assert source["attention"]["p"]["episode"] == "g:t:7"
    assert not source["attention"]["p"].get("provisional")


def test_v1_idle_snapshot_does_not_skip_unprocessed_later_completion():
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 6,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot

    def event(status, sequence):
        return {
            "pane_id": "p",
            "terminal_id": "t",
            "state_change_seq": sequence,
            "agent_status": status,
        }

    assert ch.apply_v1_status_event(state, "source", event("done", 7), event_boot=boot)
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "idle", "state_change_seq": 9}],
        ["p"],
    )
    assert ch.apply_v1_status_event(
        state, "source", event("working", 8), event_boot=boot
    )
    assert ch.apply_v1_status_event(state, "source", event("done", 9), event_boot=boot)
    assert source["attention"]["p"]["episode"] == "g:t:9"


def test_v1_seen_idle_event_still_records_a_completed_turn():
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 6,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    event = {
        "pane_id": "p",
        "terminal_id": "t",
        "state_change_seq": 7,
        "completion_seq": 7,
        "agent_status": "idle",
    }
    assert ch.apply_v1_status_event(state, "source", event, event_boot=boot)
    assert source["attention"]["p"]["status"] == "done"
    assert source["attention"]["p"]["episode"] == "g:t:7"


def test_v1_reconnect_recovers_completion_already_marked_idle(monkeypatch):
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 6,
        "completion_seq": None,
    }

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": boot, "agents": [dict(agent)]}}
        return (
            {"agents": [dict(agent)]}
            if method == "agent.list"
            else {"panes": [{"pane_id": "p"}]}
        )

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    state = {}
    assert ch.refresh_v1_source(state, "source", "gen", "/socket")
    agent.update(agent_status="idle", state_change_seq=7, completion_seq=7)
    assert ch.refresh_v1_source(state, "source", "gen", "/socket", reconnect=True)
    entry = state["v1"]["sources"]["source"]["attention"]["p"]
    assert entry["status"] == "done" and entry["server_boot_id"] == boot
    assert ch.force_acknowledge_attention(
        state["v1"]["sources"]["source"]["attention"], "p", entry["episode"]
    )
    assert ch.refresh_v1_source(state, "source", "gen", "/socket", reconnect=True)
    assert not ch.has_pending_attention(
        state["v1"]["sources"]["source"]["attention"], "p"
    )


def test_v1_focus_accepts_completion_when_snapshot_changes_done_to_idle(monkeypatch):
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 7,
        "completion_seq": 7,
    }
    calls = 0

    def rpc(path, method, params):
        nonlocal calls
        if method == "session.snapshot":
            calls += 1
            status = "done" if calls == 1 else "idle"
            return {
                "snapshot": {
                    "server_boot_id": boot,
                    "agents": [{**agent, "agent_status": status}],
                }
            }
        return (
            {"agents": [dict(agent)]}
            if method == "agent.list"
            else {"panes": [{"pane_id": "p"}]}
        )

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    state = {}
    assert ch.refresh_v1_source(state, "source", "gen", "/socket")
    idle = {**agent, "agent_status": "idle"}
    assert ch.apply_v1_status_event(state, "source", idle, boot)
    observation = {
        "boot_id": boot,
        "pane_id": "p",
        "agent_state_change_seq": 7,
        "outer_focused": True,
    }
    assert ch.v1_verified_focus_episode(
        state, "source", observation, {"server_boot_id": boot, "agents": [idle]}
    )


def test_v1_queued_seen_completion_survives_ahead_idle_snapshot():
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 6,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "idle", "state_change_seq": 7, "completion_seq": 7}],
        ["p"],
    )
    event = {
        "pane_id": "p",
        "terminal_id": "t",
        "state_change_seq": 7,
        "completion_seq": 7,
        "agent_status": "idle",
    }
    assert ch.apply_v1_status_event(state, "source", event, event_boot=boot)
    assert source["attention"]["p"]["episode"] == "g:t:7"


def test_v1_queued_done_survives_ahead_seen_idle_snapshot():
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 6,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "idle", "state_change_seq": 7, "completion_seq": 7}],
        ["p"],
    )
    event = {
        "pane_id": "p",
        "terminal_id": "t",
        "state_change_seq": 7,
        "completion_seq": 7,
        "agent_status": "done",
    }
    assert ch.apply_v1_status_event(state, "source", event, event_boot=boot)
    assert source["attention"]["p"]["episode"] == "g:t:7"


def test_v1_sequenced_done_survives_failed_refresh_and_idle_reconnect(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 6,
    }

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": boot, "agents": [dict(agent)]}}
        return (
            {"agents": [dict(agent)]}
            if method == "agent.list"
            else {"panes": [{"pane_id": "p"}]}
        )

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    assert ch.sync_v1_source("source", "gen", "/socket")
    event = {
        "pane_id": "p",
        "terminal_id": "t",
        "state_change_seq": 7,
        "completion_seq": 7,
        "agent_status": "done",
    }
    assert ch.persist_v1_status_event("source", event, boot)
    monkeypatch.setattr(
        ch, "socket_rpc", lambda *args: (_ for _ in ()).throw(OSError("disconnected"))
    )
    assert not ch.sync_v1_source("source", "gen", "/socket")
    agent.update(agent_status="idle", state_change_seq=7, completion_seq=7)
    monkeypatch.setattr(ch, "socket_rpc", rpc)
    assert ch.sync_v1_source("source", "gen", "/socket", reset_predictions=True)
    entry = ch.load_state()["v1"]["sources"]["source"]["attention"]["p"]
    assert entry["status"] == "done" and not entry.get("provisional")
    assert entry["sequence"] == 7 and entry["server_boot_id"] == boot


def test_v1_reconnect_retires_superseded_completion_when_identity_was_reset(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 7,
        "completion_seq": 7,
    }

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": boot, "agents": [dict(agent)]}}
        return (
            {"agents": [dict(agent)]}
            if method == "agent.list"
            else {"panes": [{"pane_id": "p"}]}
        )

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    assert ch.sync_v1_source("source", "gen", "/socket")
    agent.update(agent_status="idle", state_change_seq=9, completion_seq=None)
    assert ch.sync_v1_source("source", "gen", "/socket", reset_predictions=True)
    attention = ch.load_state()["v1"]["sources"]["source"]["attention"]
    assert not ch.has_pending_attention(attention, "p")
    event = {
        "pane_id": "p",
        "terminal_id": "t",
        "state_change_seq": 9,
        "completion_seq": 9,
        "agent_status": "done",
    }
    assert ch.persist_v1_status_event("source", event, boot)
    attention = ch.load_state()["v1"]["sources"]["source"]["attention"]
    assert not ch.has_pending_attention(attention, "p")


def test_v1_idle_snapshot_new_completion_survives_old_events_after_ack():
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 6,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot

    def event(status, sequence):
        return {
            "pane_id": "p",
            "terminal_id": "t",
            "state_change_seq": sequence,
            "agent_status": status,
        }

    assert ch.apply_v1_status_event(state, "source", event("done", 7), event_boot=boot)
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "idle", "state_change_seq": 9, "completion_seq": 9}],
        ["p"],
    )
    attention = source["attention"]
    assert attention["p"]["episode"] == "g:t:9"
    assert ch.force_acknowledge_attention(attention, "p", "g:t:9")
    assert ch.apply_v1_status_event(
        state, "source", event("working", 8), event_boot=boot
    )
    assert ch.apply_v1_status_event(state, "source", event("done", 9), event_boot=boot)
    assert attention["p"]["acknowledged"] == "g:t:9"


def test_v1_ack_event_rejects_another_server_subscription(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    state = {
        "v1": {
            "sources": {
                "source": {
                    "server_boot_id": "a" * 32,
                    "attention": {"p": {"status": "done", "episode": "episode"}},
                }
            }
        }
    }
    ch.save_state(state)
    data = {"pane": {"pane_id": "p", "tokens": {"cmux_herdr_ack": "episode"}}}
    assert not ch.persist_v1_ack_event("source", data, "b" * 32)
    assert ch.load_state()["v1"] == state["v1"]


def test_v1_refresh_rejects_server_restart_during_snapshot(monkeypatch):
    boots = iter(["a" * 32, "b" * 32])

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": next(boots), "agents": []}}
        return {"agents": []} if method == "agent.list" else {"panes": []}

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    state = {}
    assert not ch.refresh_v1_source(state, "source", "gen", "/socket")
    assert state == {}


def test_v1_supervisor_refresh_rejects_new_boot_without_overwriting_old_state(
    monkeypatch,
):
    state = {
        "v1": {
            "sources": {
                "old": {
                    "server_boot_id": "a" * 32,
                    "attention": {"p": {"status": "done", "episode": "old"}},
                }
            }
        }
    }
    before = json.loads(json.dumps(state))

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": "b" * 32, "agents": []}}
        return {"agents": []} if method == "agent.list" else {"panes": []}

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    assert not ch.refresh_v1_source(
        state, "old", "gen", "/socket", expected_boot="a" * 32
    )
    assert state == before


def test_v1_refresh_distinguishes_server_restart_when_socket_generation_repeats(
    monkeypatch,
):
    boot = ["a" * 32]
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "agent_status": "done",
        "state_change_seq": 7,
    }

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": boot[0], "agents": [agent]}}
        return (
            {"agents": [agent]}
            if method == "agent.list"
            else {"panes": [{"pane_id": "p"}]}
        )

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    state = {}
    assert ch.refresh_v1_source(state, "source", "gen", "/socket")
    old = state["v1"]["sources"]["source"]["attention"]["p"]["episode"]
    boot[0] = "b" * 32
    assert ch.refresh_v1_source(state, "source", "gen", "/socket")
    attention = state["v1"]["sources"]["source"]["attention"]
    assert attention["p"]["episode"] != old
    assert not ch.force_acknowledge_attention(attention, "p", old)


def test_v1_refresh_preserves_known_identity_if_boot_id_temporarily_missing(
    monkeypatch,
):
    boot = ["a" * 32]
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "agent_status": "done",
        "state_change_seq": 7,
    }

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": boot[0], "agents": [agent]}}
        return (
            {"agents": [agent]}
            if method == "agent.list"
            else {"panes": [{"pane_id": "p"}]}
        )

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    state = {}
    assert ch.refresh_v1_source(state, "source", "gen", "/socket")
    before = json.loads(json.dumps(state))
    boot[0] = None
    assert not ch.refresh_v1_source(state, "source", "gen", "/socket")
    assert state == before


def test_v1_refresh_retains_state_on_pane_list_failure(monkeypatch):
    state = {
        "v1": {
            "sources": {
                "source": {
                    "attention": {"p": {"status": "done", "episode": "a"}},
                    "panes": {},
                }
            }
        }
    }
    before = json.loads(json.dumps(state))

    def fake_rpc(path, method, params):
        if method == "pane.list":
            raise OSError("disconnected")
        return {"agents": []}

    monkeypatch.setattr(ch, "socket_rpc", fake_rpc)
    assert not ch.refresh_v1_source(state, "source", "boot", "/socket")
    assert state == before


def test_v1_refresh_rejects_malformed_agent_snapshot_atomically(monkeypatch):
    state = {}

    def fake_rpc(path, method, params):
        return (
            {
                "agents": [
                    {
                        "pane_id": "p",
                        "terminal_id": "t",
                        "state_change_seq": 1,
                        "agent_status": "done",
                    },
                    None,
                ]
            }
            if method == "agent.list"
            else {"panes": [{"pane_id": "p"}]}
        )

    monkeypatch.setattr(ch, "socket_rpc", fake_rpc)
    assert not ch.refresh_v1_source(state, "source", "boot", "/socket")
    assert state == {}


def test_v1_source_refresh_persists_success_and_keeps_failed_state(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "blocked",
        "state_change_seq": 1,
    }
    monkeypatch.setattr(
        ch,
        "socket_rpc",
        lambda path, method, params: (
            {"agents": [agent]}
            if method == "agent.list"
            else {"panes": [{"pane_id": "p"}]}
        ),
    )
    assert ch.sync_v1_source("local:tom", "boot", "/socket")
    saved = json.loads(ch.STATE_PATH.read_text())
    assert saved["v1"]["sources"]["local:tom"]["attention"]["p"]["status"] == "blocked"

    def fail(*args):
        raise OSError("disconnected")

    monkeypatch.setattr(ch, "socket_rpc", fail)
    assert not ch.sync_v1_source("local:tom", "boot", "/socket")
    assert json.loads(ch.STATE_PATH.read_text()) == saved


def test_v1_plugin_paths_follow_herdr_xdg_directories():
    state_dir, config_dir = ch.v1_plugin_paths(
        {"HOME": "/home/test", "XDG_STATE_HOME": "/state", "XDG_CONFIG_HOME": "/config"}
    )
    assert state_dir == Path("/state/herdr/plugins/cmux-herdr")
    assert config_dir == Path("/config/herdr/plugins/config/cmux-herdr")


def test_v1_plugin_paths_use_explicit_plugin_environment():
    state_dir, config_dir = ch.v1_plugin_paths(
        {
            "HOME": "/home/test",
            "HERDR_PLUGIN_STATE_DIR": "/explicit/state",
            "HERDR_PLUGIN_CONFIG_DIR": "/explicit/config",
        }
    )
    assert state_dir == Path("/explicit/state")
    assert config_dir == Path("/explicit/config")


def test_v1_refresh_invalid_status_does_not_partially_update(monkeypatch):
    state = {
        "v1": {
            "sources": {
                "s": {
                    "attention": {
                        "p": {
                            "status": "done",
                            "episode": "old",
                            "sequence": 1,
                            "generation": "g",
                            "terminal_id": "t",
                        }
                    },
                    "panes": {},
                }
            }
        }
    }
    before = json.loads(json.dumps(state))
    agents = [
        {
            "pane_id": "p",
            "terminal_id": "t",
            "agent_status": "working",
            "state_change_seq": 2,
        },
        {"pane_id": "q", "terminal_id": "u", "agent_status": [], "state_change_seq": 1},
    ]
    monkeypatch.setattr(
        ch,
        "socket_rpc",
        lambda path, method, params: (
            {"agents": agents}
            if method == "agent.list"
            else {"panes": [{"pane_id": "p"}, {"pane_id": "q"}]}
        ),
    )
    assert not ch.refresh_v1_source(state, "s", "g", "/socket")
    assert state == before


def test_v1_subscriptions_cover_pane_status_focus_and_metadata():
    subscriptions = ch.v1_subscriptions(["p1", "p2"])
    assert {"type": "pane.agent_status_changed", "pane_id": "p1"} in subscriptions
    assert {"type": "pane.agent_status_changed", "pane_id": "p2"} in subscriptions
    assert {"type": "pane.updated"} in subscriptions
    assert {"type": "pane.focused"} in subscriptions
    assert {"type": "pane.created"} in subscriptions
    assert {"type": "pane.closed"} in subscriptions


def test_v1_status_events_keep_each_connected_completion_distinct(monkeypatch):
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 1,
    }
    ch.apply_v1_snapshot(state, "source", "boot", [agent], ["p"])
    ids = iter(["episode-a", "episode-b"])
    monkeypatch.setattr(ch, "new_v1_episode_id", lambda: next(ids))
    ch.apply_v1_status_event(state, "source", {"pane_id": "p", "agent_status": "done"})
    a = state["v1"]["sources"]["source"]["attention"]["p"]["episode"]
    ch.apply_v1_status_event(
        state, "source", {"pane_id": "p", "agent_status": "working"}
    )
    ch.apply_v1_status_event(state, "source", {"pane_id": "p", "agent_status": "done"})
    b = state["v1"]["sources"]["source"]["attention"]["p"]["episode"]
    assert (a, b) == ("episode-a", "episode-b")
    assert not ch.force_acknowledge_attention(
        state["v1"]["sources"]["source"]["attention"], "p", a
    )


def test_v1_duplicate_done_after_seen_does_not_resurrect_acknowledged(monkeypatch):
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 1,
    }
    ch.apply_v1_snapshot(state, "source", "boot", [agent], ["p"])
    monkeypatch.setattr(ch, "new_v1_episode_id", lambda: "episode-a")
    ch.apply_v1_status_event(state, "source", {"pane_id": "p", "agent_status": "done"})
    attention = state["v1"]["sources"]["source"]["attention"]
    ch.apply_v1_snapshot(
        state,
        "source",
        "boot",
        [{**agent, "agent_status": "done", "state_change_seq": 2}],
        ["p"],
    )
    assert ch.force_acknowledge_attention(attention, "p", attention["p"]["episode"])
    ch.apply_v1_status_event(state, "source", {"pane_id": "p", "agent_status": "idle"})
    ch.apply_v1_status_event(state, "source", {"pane_id": "p", "agent_status": "done"})
    assert not ch.has_pending_attention(attention, "p")


def test_v1_status_event_is_persisted_for_existing_source(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 1,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", "boot", [agent], ["p"])
    ch.save_state(state)
    assert ch.persist_v1_status_event(
        "source", {"pane_id": "p", "agent_status": "blocked"}
    )
    saved = json.loads(ch.STATE_PATH.read_text())
    assert saved["v1"]["sources"]["source"]["attention"]["p"]["status"] == "blocked"


def test_v1_watch_subscribes_then_processes_ordered_events(monkeypatch):
    import io

    events = [
        {"id": "watch", "result": {"type": "subscription_started"}},
        {
            "event": "pane_agent_status_changed",
            "data": {"pane_id": "p", "agent_status": "blocked"},
        },
        {
            "event": "pane_agent_status_changed",
            "data": {"pane_id": "p", "agent_status": "working"},
        },
        {"event": "pane_created", "data": {}},
    ]

    class Connection:
        def makefile(self, mode):
            return io.BytesIO(b"".join((json.dumps(e) + "\n").encode() for e in events))

        def close(self):
            pass

    order = []
    monkeypatch.setattr(ch, "list_pane_ids", lambda path: ["p"])
    monkeypatch.setattr(ch, "open_v1_subscription", lambda path, subs: Connection())
    monkeypatch.setattr(
        ch,
        "sync_v1_source",
        lambda source, generation, path, **kwargs: order.append("snapshot") or True,
    )
    monkeypatch.setattr(
        ch,
        "persist_v1_status_event",
        lambda source, data, *args: order.append(data["agent_status"]) or True,
    )
    ch.v1_watch_connection("source", "boot", "/socket")
    assert order == ["snapshot", "blocked", "working"]


def test_v1_watch_retries_release_snapshot_without_losing_buffered_events(monkeypatch):
    import io

    events = [
        {"result": {"type": "subscription_started"}},
        {"event": "pane_agent_status_changed", "data": {"agent_status": "working"}},
    ]
    calls = []

    class Connection:
        def makefile(self, mode):
            return io.BytesIO(b"".join((json.dumps(e) + "\n").encode() for e in events))

        def close(self):
            calls.append("close")

    def sync(*args, **kwargs):
        calls.append("snapshot")
        if calls == ["snapshot"]:
            raise ch.V1SnapshotRace("release changed")
        return True

    monkeypatch.setattr(ch, "list_pane_ids", lambda path: ["p"])
    monkeypatch.setattr(ch, "open_v1_subscription", lambda path, subs: Connection())
    monkeypatch.setattr(ch, "sync_v1_source", sync)
    monkeypatch.setattr(ch.time, "sleep", lambda delay: None)
    monkeypatch.setattr(
        ch, "persist_v1_status_event", lambda *args: calls.append("event") or True
    )
    ch.v1_watch_connection("source", "generation", "/socket")
    assert calls == ["snapshot", "snapshot", "event", "close"]


def test_v1_watch_refreshes_on_agent_release_without_trusting_payload(monkeypatch):
    import io

    events = [
        {"id": "watch", "result": {"type": "subscription_started"}},
        {"event": "pane.agent_detected", "data": {"pane_id": "p", "released": True}},
    ]

    class Connection:
        def makefile(self, mode):
            return io.BytesIO(
                b"".join((json.dumps(event) + "\n").encode() for event in events)
            )

        def close(self):
            pass

    subscriptions = []
    refreshes = []
    monkeypatch.setattr(ch, "list_pane_ids", lambda path: ["p"])
    monkeypatch.setattr(
        ch,
        "open_v1_subscription",
        lambda path, subs: subscriptions.extend(subs) or Connection(),
    )
    monkeypatch.setattr(
        ch, "sync_v1_source", lambda *args, **kwargs: refreshes.append(True) or True
    )
    ch.v1_watch_connection("source", "generation", "/socket")
    assert {"type": "pane.agent_detected"} in subscriptions
    assert len(refreshes) == 2


def test_v1_watch_reconnects_if_subscription_and_snapshot_servers_differ(monkeypatch):
    import io

    class Connection:
        def makefile(self, mode):
            return io.BytesIO(
                (
                    json.dumps(
                        {
                            "result": {
                                "type": "subscription_started",
                                "server_boot_id": "a" * 32,
                            }
                        }
                    )
                    + "\n"
                ).encode()
            )

        def close(self):
            pass

    monkeypatch.setattr(ch, "list_pane_ids", lambda path: [])
    monkeypatch.setattr(ch, "open_v1_subscription", lambda *args: Connection())
    monkeypatch.setattr(ch, "sync_v1_source", lambda *args, **kwargs: True)
    monkeypatch.setattr(ch, "v1_subscription_matches_source", lambda *args: False)
    with pytest.raises(ch.SocketError):
        ch.v1_watch_connection("source", "generation", "/socket")


def test_v1_supervisor_watch_rejects_changed_boot_before_ready(monkeypatch):
    import io

    class Connection:
        def makefile(self, mode):
            return io.BytesIO(
                (
                    json.dumps(
                        {
                            "result": {
                                "type": "subscription_started",
                                "server_boot_id": "b" * 32,
                            }
                        }
                    )
                    + "\n"
                ).encode()
            )

        def close(self):
            pass

    monkeypatch.setattr(ch, "list_pane_ids", lambda path: [])
    monkeypatch.setattr(ch, "open_v1_subscription", lambda *args: Connection())
    monkeypatch.setattr(
        ch,
        "sync_v1_source",
        lambda *args, **kwargs: pytest.fail("stale source was synchronized"),
    )
    with pytest.raises(ch.SocketError):
        ch.v1_watch_connection(
            "source", "generation", "/socket", expected_boot="a" * 32
        )


def test_v1_supervisor_watch_does_not_report_ready_for_stale_pane_set(monkeypatch):
    import io

    class Connection:
        def makefile(self, mode):
            return io.BytesIO(
                (
                    json.dumps(
                        {
                            "result": {
                                "type": "subscription_started",
                                "server_boot_id": "a" * 32,
                            }
                        }
                    )
                    + "\n"
                ).encode()
            )

        def close(self):
            pass

    panes = iter([["p"], ["p", "q"]])
    monkeypatch.setattr(ch, "list_pane_ids", lambda path: next(panes))
    monkeypatch.setattr(ch, "open_v1_subscription", lambda *args: Connection())
    monkeypatch.setattr(ch, "sync_v1_source", lambda *args, **kwargs: True)
    monkeypatch.setattr(ch, "v1_subscription_matches_source", lambda *args: True)
    ready = []
    ch.v1_watch_connection(
        "source",
        "generation",
        "/socket",
        expected_boot="a" * 32,
        on_change=lambda: ready.append(True),
    )
    assert not ready


def test_v1_supervisor_source_reports_ready_only_once_after_subscription(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "v1_local_generation", lambda path: "generation")
    calls = []

    def watch(source, generation, socket_path, *, on_change, expected_boot):
        calls.append((source, generation, socket_path, expected_boot))
        on_change()
        on_change()

    monkeypatch.setattr(ch, "v1_watch_connection", watch)
    ready = []
    target = {"remote": None, "socket_path": "/source.sock", "server_boot_id": "a" * 32}
    assert ch.v1_watch_supervised_source("source", target, lambda: ready.append(True))
    assert ready == [True]
    assert calls == [("source", "generation", "/source.sock", "a" * 32)]


@pytest.mark.parametrize("failure", ["connect", "send"])
def test_v1_subscription_bounds_connect_and_closes_on_failure(monkeypatch, failure):
    events = []

    class Connection:
        def settimeout(self, value):
            events.append(("timeout", value))

        def connect(self, path):
            events.append(("connect", path))
            if failure == "connect":
                raise TimeoutError("connect stalled")

        def sendall(self, data):
            events.append(("send",))
            raise TimeoutError("send stalled")

        def close(self):
            events.append(("close",))

    monkeypatch.setattr(ch.socket, "socket", lambda *args: Connection())
    with pytest.raises(TimeoutError):
        ch.open_v1_subscription("/socket", [])
    assert events == [
        ("timeout", ch.V1_SUBSCRIPTION_IDLE_TIMEOUT),
        ("connect", "/socket"),
        *([("send",)] if failure == "send" else []),
        ("close",),
    ]


def test_v1_subscription_reconnects_after_silent_stream(monkeypatch):
    client, server = socket.socketpair()
    monkeypatch.setattr(ch, "V1_SUBSCRIPTION_IDLE_TIMEOUT", 0.1)

    def open_socket(*args, timeout):
        client.settimeout(timeout)
        return client

    monkeypatch.setattr(ch, "open_subscription", open_socket)
    monkeypatch.setattr(ch, "list_pane_ids", lambda path: [])
    monkeypatch.setattr(ch, "sync_v1_source", lambda *args, **kwargs: True)
    monkeypatch.setattr(ch, "v1_subscription_matches_source", lambda *args: True)
    server.sendall(
        (
            json.dumps(
                {"result": {"type": "subscription_started", "server_boot_id": "a" * 32}}
            )
            + "\n"
        ).encode()
    )
    try:
        with pytest.raises(TimeoutError):
            ch.v1_watch_connection(
                "source", "generation", "/socket", expected_boot="a" * 32
            )
    finally:
        server.close()


def test_v1_supervised_source_retries_after_subscription_timeout(monkeypatch):
    stop = ch.threading.Event()
    attempts, ready = [], []

    def watch(source, target, on_ready, *, on_not_ready):
        attempts.append(source)
        if len(attempts) == 1:
            on_ready()
            raise TimeoutError("silent subscription")
        on_ready()
        stop.set()
        return True

    monkeypatch.setattr(ch, "v1_watch_supervised_source", watch)
    assert ch.run_v1_supervised_source(
        "source",
        {},
        lambda: ready.append(True),
        stop,
        on_not_ready=lambda: ready.append(False),
    )
    assert attempts == ["source", "source"]
    assert ready == [True, False, True, False]


@pytest.mark.parametrize("marker", ["pending", "enabled"])
@pytest.mark.parametrize("boot", ["a" * 32, None])
def test_v1_supervised_source_syncs_tokens_without_supervisor_discovery(
    monkeypatch, tmp_path, marker, boot
):
    import threading

    monkeypatch.setattr(ch, "state_dir", tmp_path)
    (tmp_path / "v1-enabled").write_text(marker)
    monkeypatch.setattr(ch, "v1_local_generation", lambda path: "generation")
    monkeypatch.setattr(ch, "v1_unix_peer_pid", lambda path: 123)
    monkeypatch.setattr(ch, "v1_native_process_info", lambda pid: {"start_token": 5})
    synced = threading.Event()
    calls = []

    def sync(source, path, *, expected_boot):
        calls.append((source, path, expected_boot))
        synced.set()

    monkeypatch.setattr(ch, "sync_v1_episode_tokens", sync)

    def watch(source, generation, path, *, on_change, expected_boot):
        on_change()
        if marker == "pending":
            assert not synced.wait(0.2)
            (tmp_path / "v1-enabled").write_text("enabled")
        assert synced.wait(3), "token synchronization depended on supervisor discovery"

    monkeypatch.setattr(ch, "v1_watch_connection", watch)
    target = {"remote": None, "socket_path": "/source.sock", "server_boot_id": boot}
    if boot is None:
        target["socket_generation"] = "generation"
    assert ch.v1_watch_supervised_source("source", target, lambda: None)
    assert calls and calls[0] == ("source", "/source.sock", boot)


def test_v1_supervised_upstream_source_rejects_changed_socket(monkeypatch):
    target = {"remote": None, "socket_path": "/source.sock", "socket_generation": "1:2"}
    monkeypatch.setattr(ch, "v1_local_generation", lambda path: "1:3")
    events = []
    monkeypatch.setattr(
        ch, "v1_watch_connection", lambda *args, **kwargs: events.append(1)
    )
    assert not ch.v1_watch_supervised_source("source", target, lambda: None)
    assert events == []


def test_v1_supervised_upstream_source_rejects_replaced_server(monkeypatch):
    target = {
        "remote": None,
        "socket_path": "/source.sock",
        "socket_generation": "1:2",
        "server_pid": 123,
        "server_start_token": 5,
    }
    monkeypatch.setattr(ch, "v1_local_generation", lambda path: "1:2")
    monkeypatch.setattr(ch, "v1_unix_peer_pid", lambda path: 123)
    monkeypatch.setattr(ch, "v1_native_process_info", lambda pid: {"start_token": 6})
    calls = []
    monkeypatch.setattr(
        ch, "v1_watch_connection", lambda *args, **kwargs: calls.append(1)
    )
    assert not ch.v1_watch_supervised_source("source", target, lambda: None)
    assert calls == []


def test_v1_upstream_source_fence_rejects_replaced_socket(monkeypatch):
    monkeypatch.setattr(ch, "v1_local_generation", lambda path: "1:3")
    monkeypatch.setattr(ch, "v1_native_process_info", lambda pid: {"start_token": 5})
    token = ch.HERDR_SOURCE_FENCE.set(
        {"path": "/source.sock", "generation": "1:2", "pid": 123, "start_token": 5}
    )
    try:
        with pytest.raises(ch.SocketError, match="source socket changed"):
            ch.v1_check_herdr_source_fence("/source.sock")
    finally:
        ch.HERDR_SOURCE_FENCE.reset(token)


def test_v1_upstream_source_fence_rejects_reused_server_pid(monkeypatch):
    monkeypatch.setattr(ch, "v1_local_generation", lambda path: "1:2")
    monkeypatch.setattr(ch, "v1_native_process_info", lambda pid: {"start_token": 6})
    token = ch.HERDR_SOURCE_FENCE.set(
        {"path": "/source.sock", "generation": "1:2", "pid": 123, "start_token": 5}
    )
    try:
        with pytest.raises(ch.SocketError, match="source process changed"):
            ch.v1_check_herdr_source_fence("/source.sock")
    finally:
        ch.HERDR_SOURCE_FENCE.reset(token)


def test_v1_supervisor_source_entrypoint_reports_readiness(
    monkeypatch, tmp_path, capsys
):
    monkeypatch.setenv("HERDR_PLUGIN_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("HERDR_PLUGIN_CONFIG_DIR", str(tmp_path / "config"))
    target = {"remote": None, "socket_path": "/source.sock", "server_boot_id": "a" * 32}
    source_id = ch.v1_supervised_source_key(target)
    monkeypatch.setattr(
        sys,
        "argv",
        ["cmux_herdr.py", "supervisor-source", source_id, json.dumps(target)],
    )
    monkeypatch.setattr(ch.signal, "signal", lambda *args: None)

    def watch(source, given_target, ready, stop, **kwargs):
        assert source == source_id and given_target == target
        ready()
        return True

    monkeypatch.setattr(ch, "run_v1_supervised_source", watch)
    assert ch.main() == 0
    assert capsys.readouterr().out == "ready\n"


def test_v1_supervisor_source_entrypoint_monitors_parent(monkeypatch, tmp_path):
    monkeypatch.setenv("HERDR_PLUGIN_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("HERDR_PLUGIN_CONFIG_DIR", str(tmp_path / "config"))
    target = {"remote": None, "socket_path": "/source.sock", "server_boot_id": "a" * 32}
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "cmux_herdr.py",
            "supervisor-source",
            ch.v1_supervised_source_key(target),
            json.dumps(target),
        ],
    )
    monkeypatch.setattr(ch.signal, "signal", lambda *args: None)
    monkeypatch.setattr(ch, "run_v1_supervised_source", lambda *args, **kwargs: True)
    started = []

    class Thread:
        def __init__(self, *, target, args, daemon):
            started.append((target, args, daemon))

        def start(self):
            pass

    monkeypatch.setattr(ch.threading, "Thread", Thread)
    assert ch.main() == 0
    assert started and started[0][0] is ch.v1_watch_parent
    assert started[0][1][0] == os.getppid()
    assert started[0][1][1].is_set()


def test_v1_supervisor_source_process_requires_ready_and_stops_group(
    monkeypatch, tmp_path
):
    read_fd, write_fd = os.pipe()
    stream = os.fdopen(read_fd, "rb", buffering=0)
    calls = []
    dead = [False]
    process = SimpleNamespace(
        pid=12345, stdout=stream, wait=lambda **kwargs: calls.append("reap")
    )
    monkeypatch.setattr(ch.subprocess, "Popen", lambda *args, **kwargs: process)
    monkeypatch.setattr(
        ch, "v1_wait_child_state", lambda pid: (1, 0) if dead[0] else None
    )

    def killpg(pid, sig):
        calls.append(sig)
        dead[0] = True

    monkeypatch.setattr(ch.os, "killpg", killpg)
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "config_dir", tmp_path)
    source = ch.V1SupervisedSourceProcess("source", {"remote": None})
    try:
        assert source.alive and not source.ready
        os.write(write_fd, b"ready\n")
        assert source.ready
        os.write(write_fd, b"not-ready\n")
        assert not source.ready
        os.write(write_fd, b"ready\n")
        assert source.ready
        source.stop()
        assert not source.alive
        assert calls == [ch.signal.SIGTERM, ch.signal.SIGKILL, "reap"]
    finally:
        os.close(write_fd)
        source.stop()


def test_v1_watch_reconnects_when_done_refresh_fails(monkeypatch):
    import io

    events = [
        {"result": {"type": "subscription_started"}},
        {
            "event": "pane_agent_status_changed",
            "data": {"pane_id": "p", "agent_status": "done"},
        },
    ]

    class Connection:
        def makefile(self, mode):
            return io.BytesIO(b"".join((json.dumps(e) + "\n").encode() for e in events))

        def close(self):
            pass

    calls = iter([True, False])
    monkeypatch.setattr(ch, "list_pane_ids", lambda path: ["p"])
    monkeypatch.setattr(ch, "open_v1_subscription", lambda *args: Connection())
    monkeypatch.setattr(ch, "sync_v1_source", lambda *args, **kwargs: next(calls))
    monkeypatch.setattr(ch, "persist_v1_status_event", lambda *args: True)
    with pytest.raises(ch.SocketError):
        ch.v1_watch_connection("source", "generation", "/socket")


def test_v1_remote_ack_event_only_accepts_matching_done_episode():
    state = {
        "v1": {
            "sources": {
                "remote": {
                    "attention": {"p": {"status": "done", "episode": "b"}},
                    "panes": {"p": {"status": "done"}},
                }
            }
        }
    }
    assert not ch.apply_v1_ack_event(
        state, "remote", {"pane": {"pane_id": "p", "tokens": {"cmux_herdr_ack": "a"}}}
    )
    assert ch.apply_v1_ack_event(
        state, "remote", {"pane": {"pane_id": "p", "tokens": {"cmux_herdr_ack": "b"}}}
    )
    assert not ch.has_pending_attention(
        state["v1"]["sources"]["remote"]["attention"], "p"
    )


@pytest.mark.parametrize("token,expected_pending", [("g:t:1", False), ("old", True)])
def test_v1_snapshot_recovers_only_matching_force_read(token, expected_pending):
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 1,
    }
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [agent],
        ["p"],
        [{"pane_id": "p", "tokens": {"cmux_herdr_ack": token}}],
        reconnect=True,
    )
    assert (
        ch.has_pending_attention(state["v1"]["sources"]["source"]["attention"], "p")
        == expected_pending
    )


def test_v1_ack_event_persists_matching_completion(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    state = {
        "v1": {
            "sources": {
                "remote": {
                    "attention": {"p": {"status": "done", "episode": "b"}},
                    "panes": {"p": {"status": "done"}},
                }
            }
        }
    }
    ch.save_state(state)
    assert ch.persist_v1_ack_event(
        "remote", {"pane": {"pane_id": "p", "tokens": {"cmux_herdr_ack": "b"}}}
    )
    saved = json.loads(ch.STATE_PATH.read_text())
    assert not ch.has_pending_attention(
        saved["v1"]["sources"]["remote"]["attention"], "p"
    )


def test_v1_snapshot_does_not_resurrect_event_acknowledged_completion(monkeypatch):
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 1,
    }
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    monkeypatch.setattr(ch, "new_v1_episode_id", lambda: "event-a")
    ch.apply_v1_status_event(state, "source", {"pane_id": "p", "agent_status": "done"})
    source = state["v1"]["sources"]["source"]
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "done", "state_change_seq": 2}],
        ["p"],
    )
    assert ch.force_acknowledge_attention(
        source["attention"], "p", source["attention"]["p"]["episode"]
    )
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "done", "state_change_seq": 2}],
        ["p"],
    )
    assert not ch.has_pending_attention(source["attention"], "p")


def test_v1_reconnect_does_not_acknowledge_unseen_new_completion(monkeypatch):
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 1,
    }
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    monkeypatch.setattr(ch, "new_v1_episode_id", lambda: "event-a")
    ch.apply_v1_status_event(state, "source", {"pane_id": "p", "agent_status": "done"})
    source = state["v1"]["sources"]["source"]
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "done", "state_change_seq": 2}],
        ["p"],
    )
    assert ch.force_acknowledge_attention(
        source["attention"], "p", source["attention"]["p"]["episode"]
    )
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "done", "state_change_seq": 4}],
        ["p"],
    )
    assert ch.has_pending_attention(source["attention"], "p")


def test_v1_duplicate_event_preserves_provisional_acknowledgment(monkeypatch):
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 1,
    }
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    monkeypatch.setattr(ch, "new_v1_episode_id", lambda: "event-a")
    event = {"pane_id": "p", "agent_status": "done"}
    ch.apply_v1_status_event(state, "source", event)
    ch.apply_v1_status_event(state, "source", event)
    source = state["v1"]["sources"]["source"]
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "done", "state_change_seq": 2}],
        ["p"],
    )
    assert ch.force_acknowledge_attention(
        source["attention"], "p", source["attention"]["p"]["episode"]
    )
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "done", "state_change_seq": 2}],
        ["p"],
    )
    assert not ch.has_pending_attention(source["attention"], "p")


def test_v1_source_has_single_leader_and_recovers_after_exit(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    with ch.v1_source_leader("remote:server:session") as first:
        assert first is True
        with ch.v1_source_leader("remote:server:session") as second:
            assert second is False
        with ch.v1_source_leader("remote:other:session") as other:
            assert other is True
    with ch.v1_source_leader("remote:server:session") as replacement:
        assert replacement is True


def test_v1_observed_transitions_correlate_confirmed_completion(monkeypatch):
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 1,
    }
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    ids = iter(["blocked-a", "done-b"])
    monkeypatch.setattr(ch, "new_v1_episode_id", lambda: next(ids))
    for status in ("blocked", "working", "done"):
        ch.apply_v1_status_event(
            state, "source", {"pane_id": "p", "agent_status": status}
        )
    source = state["v1"]["sources"]["source"]
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "done", "state_change_seq": 4}],
        ["p"],
    )
    assert ch.force_acknowledge_attention(
        source["attention"], "p", source["attention"]["p"]["episode"]
    )
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "done", "state_change_seq": 4}],
        ["p"],
    )
    assert not ch.has_pending_attention(source["attention"], "p")


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
    monkeypatch.setattr(ch, "cmux_notifications_checked", lambda cfg: [])

    def reconcile(
        cfg, workspace, surface, attention, notifications, notification_id=None
    ):
        calls.append(sorted(item["status"] for item in attention.values()))
        return "created-id"

    monkeypatch.setattr(ch, "reconcile_aggregate_notification", reconcile)
    assert ch.reconcile_v1_destination({}, state, "w", "s", ["local", "remote"])
    assert calls == [["blocked", "done"]]
    assert (
        state["v1"]["destinations"][ch.v1_destination_key("w", "s")]["notification_id"]
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
    monkeypatch.setattr(ch, "cmux_notifications_checked", lambda cfg: None)
    monkeypatch.setattr(
        ch,
        "reconcile_aggregate_notification",
        lambda *args: pytest.fail("must not publish"),
    )
    assert not ch.reconcile_v1_destination({}, state, "w", "s", ["local"])
    assert state == before


def test_v1_observed_events_across_panes_correlate_global_sequence(monkeypatch):
    state = {}
    a = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 1,
    }
    b = {
        "pane_id": "q",
        "terminal_id": "u",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 2,
    }
    ch.apply_v1_snapshot(state, "source", "g", [a, b], ["p", "q"])
    ids = iter(["done-a", "blocked-b"])
    monkeypatch.setattr(ch, "new_v1_episode_id", lambda: next(ids))
    ch.apply_v1_status_event(state, "source", {"pane_id": "p", "agent_status": "done"})
    ch.apply_v1_status_event(
        state, "source", {"pane_id": "q", "agent_status": "blocked"}
    )
    source = state["v1"]["sources"]["source"]
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [
            {**a, "agent_status": "done", "state_change_seq": 3},
            {**b, "agent_status": "blocked", "state_change_seq": 4},
        ],
        ["p", "q"],
    )
    assert ch.force_acknowledge_attention(
        source["attention"], "p", source["attention"]["p"]["episode"]
    )
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [
            {**a, "agent_status": "done", "state_change_seq": 3},
            {**b, "agent_status": "blocked", "state_change_seq": 4},
        ],
        ["p", "q"],
    )
    assert not ch.has_pending_attention(source["attention"], "p")


def test_v1_refresh_tracks_empty_panes_before_agent_starts(monkeypatch):
    def rpc(path, method, params):
        return (
            {"agents": []}
            if method == "agent.list"
            else {
                "panes": [
                    {"pane_id": "p", "workspace_id": "w", "agent_status": "unknown"}
                ]
            }
        )

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    state = {}
    assert ch.refresh_v1_source(state, "source", "g", "/socket")
    assert ch.apply_v1_status_event(
        state, "source", {"pane_id": "p", "agent_status": "blocked"}
    )
    assert state["v1"]["sources"]["source"]["attention"]["p"]["status"] == "blocked"


def test_v1_reconnect_retires_missing_agent_and_rejects_queued_old_events(monkeypatch):
    boot = "a" * 32
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "blocked",
        "state_change_seq": 7,
        "agent": "codex",
    }
    present = True

    def rpc(path, method, params):
        pane = {
            "pane_id": "p",
            "terminal_id": "t",
            "workspace_id": "w",
            "agent_status": "blocked" if present else "unknown",
            "state_change_seq": 7 if present else 8,
        }
        agents = [dict(agent)] if present else []
        if method == "session.snapshot":
            return {
                "snapshot": {"server_boot_id": boot, "agents": agents, "panes": [pane]}
            }
        return {"agents": agents} if method == "agent.list" else {"panes": [pane]}

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    state = {}
    assert ch.refresh_v1_source(state, "source", "g", "/socket")
    present = False
    assert ch.refresh_v1_source(state, "source", "g", "/socket", reconnect=True)
    source = state["v1"]["sources"]["source"]
    assert not ch.has_pending_attention(source["attention"], "p")
    assert source["panes"]["p"]["status"] == "unknown"
    assert ch.apply_v1_status_event(state, "source", agent, event_boot=boot)
    assert not ch.has_pending_attention(source["attention"], "p")
    assert ch.apply_v1_status_event(
        state, "source", {**agent, "state_change_seq": 9}, event_boot=boot
    )
    assert ch.has_pending_attention(source["attention"], "p")


def test_v1_missing_agent_blocks_stale_blocked_but_preserves_queued_done(monkeypatch):
    boot = "a" * 32
    pane = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "unknown",
        "state_change_seq": 8,
    }

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": boot, "agents": [], "panes": [pane]}}
        return {"agents": []} if method == "agent.list" else {"panes": [pane]}

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    state = {}
    assert ch.refresh_v1_source(state, "source", "g", "/socket")
    source = state["v1"]["sources"]["source"]
    blocked = {
        "pane_id": "p",
        "terminal_id": "t",
        "agent_status": "blocked",
        "state_change_seq": 6,
    }
    assert ch.apply_v1_status_event(state, "source", blocked, event_boot=boot)
    assert not ch.has_pending_attention(source["attention"], "p")
    done = {
        **blocked,
        "agent_status": "done",
        "state_change_seq": 7,
        "completion_seq": 7,
    }
    assert ch.apply_v1_status_event(state, "source", done, event_boot=boot)
    assert source["attention"]["p"]["status"] == "done"


def test_v1_absent_agent_with_replaced_terminal_rejects_old_events(monkeypatch):
    boot = "a" * 32
    pane = {
        "pane_id": "p",
        "terminal_id": "new",
        "workspace_id": "w",
        "agent_status": "unknown",
        "state_change_seq": 9,
    }
    old = {
        "pane_id": "p",
        "terminal_id": "old",
        "workspace_id": "w",
        "agent_status": "blocked",
        "state_change_seq": 7,
        "agent": "codex",
    }
    state = {}
    generation = json.dumps(["g", boot], separators=(",", ":"))
    ch.apply_v1_snapshot(state, "source", generation, [old], ["p"])
    state["v1"]["sources"]["source"]["server_boot_id"] = boot

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": boot, "agents": [], "panes": [pane]}}
        return {"agents": []} if method == "agent.list" else {"panes": [pane]}

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    assert ch.refresh_v1_source(state, "source", "g", "/socket", reconnect=True)
    source = state["v1"]["sources"]["source"]
    assert not ch.has_pending_attention(source["attention"], "p")
    assert source["panes"]["p"]["terminal_id"] == "new"
    assert not ch.apply_v1_status_event(state, "source", old, event_boot=boot)
    assert ch.apply_v1_status_event(
        state,
        "source",
        {**old, "terminal_id": "new", "state_change_seq": 10},
        event_boot=boot,
    )
    assert ch.has_pending_attention(source["attention"], "p")


def test_v1_absent_agent_preserves_known_unread_completion_and_label(monkeypatch):
    boot = "a" * 32
    generation = json.dumps(["g", boot], separators=(",", ":"))
    old = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 7,
        "agent": "codex",
    }
    pane = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "unknown",
        "state_change_seq": 8,
    }
    state = {}
    ch.apply_v1_snapshot(state, "source", generation, [old], ["p"])
    state["v1"]["sources"]["source"]["server_boot_id"] = boot

    def rpc(path, method, params):
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": boot, "agents": [], "panes": [pane]}}
        return {"agents": []} if method == "agent.list" else {"panes": [pane]}

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    assert ch.refresh_v1_source(state, "source", "g", "/socket", reconnect=True)
    source = state["v1"]["sources"]["source"]
    assert source["attention"]["p"]["status"] == "done"
    assert source["panes"]["p"]["agent"] == "codex"


@pytest.mark.parametrize("difference", ["boot", "terminal", "sequence", "agent"])
def test_v1_missing_agent_keeps_attention_when_snapshots_disagree(
    monkeypatch, difference
):
    boot = "a" * 32
    pane = {
        "pane_id": "p",
        "terminal_id": "t",
        "agent_status": "unknown",
        "state_change_seq": 8,
    }
    old = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "blocked",
        "state_change_seq": 7,
        "agent": "codex",
    }
    state = {}
    ch.apply_v1_snapshot(
        state, "source", json.dumps(["g", boot], separators=(",", ":")), [old], ["p"]
    )
    source = state["v1"]["sources"]["source"]
    source["server_boot_id"] = boot
    source["socket_generation"] = "g"
    calls = 0

    def rpc(path, method, params):
        nonlocal calls
        if method == "session.snapshot":
            calls += 1
            changed = dict(pane)
            if calls == 2 and difference == "terminal":
                changed["terminal_id"] = "other"
            if calls == 2 and difference == "sequence":
                changed["state_change_seq"] = 9
            agents = [old] if calls == 2 and difference == "agent" else []
            return {
                "snapshot": {
                    "server_boot_id": "b" * 32
                    if calls == 2 and difference == "boot"
                    else boot,
                    "agents": agents,
                    "panes": [changed],
                }
            }
        return {"agents": []} if method == "agent.list" else {"panes": [pane]}

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    ch.refresh_v1_source(state, "source", "g", "/socket", reconnect=True)
    assert ch.has_pending_attention(state["v1"]["sources"]["source"]["attention"], "p")


def test_v1_reconnect_discards_untrusted_provisional_sequence(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "done",
        "state_change_seq": 3,
    }
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    ch.apply_v1_status_event(
        state, "source", {"pane_id": "p", "agent_status": "working"}
    )
    ch.apply_v1_status_event(state, "source", {"pane_id": "p", "agent_status": "done"})
    episode = state["v1"]["sources"]["source"]["attention"]["p"]["episode"]
    assert not ch.force_acknowledge_attention(
        state["v1"]["sources"]["source"]["attention"], "p", episode
    )
    ch.save_state(state)
    latest = {**agent, "state_change_seq": 5}
    monkeypatch.setattr(
        ch,
        "socket_rpc",
        lambda path, method, params: (
            {"agents": [latest]}
            if method == "agent.list"
            else {"panes": [{"pane_id": "p"}]}
        ),
    )
    assert ch.sync_v1_source("source", "g", "/socket", reset_predictions=True)
    saved = json.loads(ch.STATE_PATH.read_text())
    assert ch.has_pending_attention(saved["v1"]["sources"]["source"]["attention"], "p")


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


def test_v1_force_ack_requires_authoritative_episode(monkeypatch):
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 1,
    }
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    monkeypatch.setattr(ch, "new_v1_episode_id", lambda: "unverified")
    ch.apply_v1_status_event(state, "source", {"pane_id": "p", "agent_status": "done"})
    attention = state["v1"]["sources"]["source"]["attention"]
    assert not ch.force_acknowledge_attention(attention, "p", "unverified")
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "done", "state_change_seq": 2}],
        ["p"],
    )
    confirmed = attention["p"]["episode"]
    assert ch.force_acknowledge_attention(attention, "p", confirmed)


def test_v1_seen_snapshot_confirms_provisional_completion(monkeypatch):
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 1,
    }
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    monkeypatch.setattr(ch, "new_v1_episode_id", lambda: "unverified")
    ch.apply_v1_status_event(state, "source", {"pane_id": "p", "agent_status": "done"})
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "idle", "state_change_seq": 2}],
        ["p"],
    )
    attention = state["v1"]["sources"]["source"]["attention"]
    assert ch.force_acknowledge_attention(attention, "p", attention["p"]["episode"])


def test_v1_watch_confirms_done_from_snapshot_before_acknowledgment(monkeypatch):
    import io

    events = [
        {"id": "watch", "result": {"type": "subscription_started"}},
        {
            "event": "pane_agent_status_changed",
            "data": {"pane_id": "p", "agent_status": "done"},
        },
    ]

    class Connection:
        def makefile(self, mode):
            return io.BytesIO(b"".join((json.dumps(e) + "\n").encode() for e in events))

        def close(self):
            pass

    order = []
    monkeypatch.setattr(ch, "list_pane_ids", lambda path: ["p"])
    monkeypatch.setattr(ch, "open_v1_subscription", lambda path, subs: Connection())
    monkeypatch.setattr(
        ch,
        "sync_v1_source",
        lambda source, generation, path, **kwargs: order.append("snapshot") or True,
    )
    monkeypatch.setattr(
        ch,
        "persist_v1_status_event",
        lambda source, data, *args: order.append(data["agent_status"]) or True,
    )
    ch.v1_watch_connection("source", "boot", "/socket")
    assert order == ["snapshot", "done", "snapshot"]


def test_v1_seen_event_then_snapshot_confirms_completion(monkeypatch):
    state = {}
    agent = {
        "pane_id": "p",
        "terminal_id": "t",
        "workspace_id": "w",
        "agent_status": "working",
        "state_change_seq": 1,
    }
    ch.apply_v1_snapshot(state, "source", "g", [agent], ["p"])
    monkeypatch.setattr(ch, "new_v1_episode_id", lambda: "pending")
    ch.apply_v1_status_event(state, "source", {"pane_id": "p", "agent_status": "done"})
    ch.apply_v1_status_event(state, "source", {"pane_id": "p", "agent_status": "idle"})
    ch.apply_v1_snapshot(
        state,
        "source",
        "g",
        [{**agent, "agent_status": "idle", "state_change_seq": 2}],
        ["p"],
    )
    attention = state["v1"]["sources"]["source"]["attention"]
    assert ch.force_acknowledge_attention(attention, "p", attention["p"]["episode"])


def test_v1_watch_notifies_observer_after_each_state_change(monkeypatch):
    import io

    events = [
        {"id": "watch", "result": {"type": "subscription_started"}},
        {
            "event": "pane_agent_status_changed",
            "data": {"pane_id": "p", "agent_status": "blocked"},
        },
        {
            "event": "pane_updated",
            "data": {"pane": {"pane_id": "p", "tokens": {"cmux_herdr_ack": "a"}}},
        },
    ]

    class Connection:
        def makefile(self, mode):
            return io.BytesIO(b"".join((json.dumps(e) + "\n").encode() for e in events))

        def close(self):
            pass

    changes = []
    monkeypatch.setattr(ch, "list_pane_ids", lambda path: ["p"])
    monkeypatch.setattr(ch, "open_v1_subscription", lambda path, subs: Connection())
    monkeypatch.setattr(ch, "sync_v1_source", lambda *args, **kwargs: True)
    monkeypatch.setattr(ch, "persist_v1_status_event", lambda *args: True)
    monkeypatch.setattr(ch, "persist_v1_ack_event", lambda *args: True)
    ch.v1_watch_connection(
        "source", "boot", "/socket", on_change=lambda: changes.append(True)
    )
    assert len(changes) == 3


def test_v1_verified_publication_targets_require_complete_surface(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    spec_a = {"workspace": "wa", "surface": "sa"}
    spec_b = {"workspace": "wb", "surface": "sb"}
    spec_c = {"workspace": "wc", "surface": "sc"}
    desired = {"a": spec_a, "b": spec_b, "c": spec_c, "d": spec_c}
    ch.save_state(
        {
            "v1": {
                "clients": {
                    key: {"workspace": spec["workspace"], "surface": spec["surface"]}
                    for key, spec in desired.items()
                }
            }
        }
    )
    monkeypatch.setattr(ch, "v1_bindings_match_clients", lambda *args, **kwargs: True)
    assert ch.v1_verified_publication_targets(desired, {"a": spec_a, "c": spec_c}) == {
        ("wa", "sa")
    }
    state = ch.load_state()
    state["v1"]["clients"]["extra"] = {"workspace": "wa", "surface": "sa"}
    ch.save_state(state)
    assert (
        ch.v1_verified_publication_targets(desired, {"a": spec_a, "c": spec_c}) == set()
    )


def test_v1_partial_publication_preserves_unverified_destinations(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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


def test_v1_publication_backend_logs_unknown_state(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    marker = tmp_path / "v1-notification-backend"
    marker.write_text("unknown")
    marker.chmod(0o600)
    monkeypatch.setattr(ch, "retire_v1_legacy_daemons", lambda: True)
    logs = []
    monkeypatch.setattr(ch, "log", logs.append)
    monkeypatch.setattr(ch, "publish_v1_clients_once", lambda *args, **kwargs: False)
    ch.publish_v1_clients({})
    assert any("invalid notification backend" in item for item in logs)


def test_v1_publication_backend_read_failure_keeps_sidebar(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "retire_v1_legacy_daemons", lambda: True)
    open_file = os.open

    def read(path, *args, **kwargs):
        if Path(path).name == "v1-notification-backend":
            raise PermissionError("unavailable")
        return open_file(path, *args, **kwargs)

    monkeypatch.setattr(os, "open", read)
    calls = []
    monkeypatch.setattr(
        ch,
        "publish_v1_clients_once",
        lambda *args, **kwargs: calls.append(kwargs["notifications"]) or False,
    )
    ch.publish_v1_clients({})
    assert calls == [False]


def test_v1_prepare_owned_backend_is_durable_and_idempotent(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
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


def test_v1_activation_marker_preserves_previous_value_on_sync_failure(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    ch.v1_write_activation_marker("pending")
    marker = tmp_path / "v1-enabled"
    assert marker.read_text() == "pending"
    assert marker.stat().st_mode & 0o077 == 0
    fsync = ch.os.fsync
    calls = []

    def fail_first(fd):
        calls.append(fd)
        if len(calls) == 1:
            raise OSError("sync failed")
        return fsync(fd)

    monkeypatch.setattr(ch.os, "fsync", fail_first)
    with pytest.raises(OSError, match="sync failed"):
        ch.v1_write_activation_marker("enabled")
    assert marker.read_text() == "pending"
    ch.v1_write_activation_marker("enabled")
    assert marker.read_text() == "enabled"


def test_v1_activation_rollback_requires_directory_sync(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    (tmp_path / "v1-enabled").write_text("pending")
    with ch.v1_activation_lock():

        def fail_sync(_fd):
            raise OSError("directory sync failed")

        monkeypatch.setattr(ch.os, "fsync", fail_sync)
        with pytest.raises(OSError, match="directory sync failed"):
            ch.v1_remove_activation_marker()


def test_v1_recovery_syncs_absent_marker_before_restarting(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    calls = []
    monkeypatch.setattr(ch, "pending_v1_legacy_restores", lambda: [("watch", "a", 1)])
    monkeypatch.setattr(
        ch, "restart_legacy_daemons", lambda _plan: calls.append("restart")
    )
    sync = ch.os.fsync

    def record_sync(fd):
        calls.append("sync")
        return sync(fd)

    monkeypatch.setattr(ch.os, "fsync", record_sync)
    assert not ch.v1_recover_legacy_handoff()
    assert calls == ["sync", "restart"]


def test_v1_recovery_syncs_absent_marker_without_restore_plan(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "pending_v1_legacy_restores", lambda: [])
    sync = ch.os.fsync
    calls = []

    def record_sync(fd):
        calls.append(True)
        return sync(fd)

    monkeypatch.setattr(ch.os, "fsync", record_sync)
    assert ch.v1_recover_legacy_handoff()
    assert calls == [True]


def test_v1_daemon_startup_requires_absent_marker_sync(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)

    def fail_sync(_fd):
        raise OSError("sync failed")

    monkeypatch.setattr(ch.os, "fsync", fail_sync)
    with (
        pytest.raises(OSError, match="sync failed"),
        ch.v1_daemon_startup_lock(tmp_path / "watch-test.pid"),
    ):
        pytest.fail("daemon must not start after failed directory sync")


def test_v1_daemon_takeover_syncs_after_reacquiring_activation(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    path = tmp_path / "watch-work.pid"
    path.write_text(json.dumps({"pid": 123, "script": "old", "mtime": 0}))
    monkeypatch.setattr(ch, "live_daemon_pid", lambda *_args: 123)
    monkeypatch.setattr(ch, "v1_verified_daemon_audit_token", lambda *_args: [123] * 8)
    monkeypatch.setattr(ch, "v1_signal_audit_token", lambda *_args: None)
    monkeypatch.setattr(ch, "v1_daemon_incarnation_exited", lambda *_args: True)
    calls = []
    monkeypatch.setattr(
        ch, "_v1_sync_absent_activation_marker", lambda _lock: calls.append("sync")
    )
    with (tmp_path / "v1-activation.lock").open("w") as activation:
        ch.fcntl.flock(activation, ch.fcntl.LOCK_EX)
        ch.take_over_daemon(path, "watch", activation)
    assert calls == ["sync"]


def test_v1_activation_rollback_does_not_modify_replaced_directory(
    monkeypatch, tmp_path
):
    state = tmp_path / "state"
    state.mkdir()
    monkeypatch.setattr(ch, "state_dir", state)
    (state / "v1-enabled").write_text("pending")
    with ch.v1_activation_lock():
        state.rename(tmp_path / "original")
        state.mkdir()
        marker = state / "v1-enabled"
        marker.write_text("enabled")
        with pytest.raises(ValueError, match="activation directory changed"):
            ch.v1_remove_activation_marker()
        assert marker.read_text() == "enabled"


def test_v1_activation_lock_rejects_fifo(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    os.mkfifo(tmp_path / "v1-activation.lock", 0o600)
    with pytest.raises((OSError, ValueError)), ch.v1_activation_lock():
        pytest.fail("FIFO must not be used as an activation lock")


def test_v1_prepare_owned_backend_accepts_explicit_ordinary(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    (tmp_path / "v1-enabled").write_text("enabled")
    marker = tmp_path / "v1-notification-backend"
    marker.write_text("ordinary")
    marker.chmod(0o600)
    assert ch.v1_prepare_owned_backend()
    assert marker.read_text() == "preparing-owned"


def test_v1_prepare_owned_backend_retries_after_directory_sync_failure(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
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


@pytest.mark.parametrize("kind", ["symlink", "fifo"])
def test_v1_publication_backend_rejects_unsafe_marker(monkeypatch, tmp_path, kind):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "retire_v1_legacy_daemons", lambda: True)
    marker = tmp_path / "v1-notification-backend"
    if kind == "symlink":
        marker.symlink_to(tmp_path / "missing")
    else:
        os.mkfifo(marker, 0o600)
    calls = []
    monkeypatch.setattr(
        ch,
        "publish_v1_clients_once",
        lambda *args, **kwargs: calls.append(kwargs["notifications"]) or False,
    )
    ch.publish_v1_clients({})
    assert calls == [False]


def test_v1_prepare_owned_backend_waits_for_ordinary_publication(monkeypatch, tmp_path):
    import threading

    monkeypatch.setattr(ch, "state_dir", tmp_path)
    (tmp_path / "v1-enabled").write_text("enabled")
    monkeypatch.setattr(ch, "retire_v1_legacy_daemons", lambda: True)
    started, release, transitioned = (threading.Event() for _ in range(3))

    def publish(*_args, **_kwargs):
        started.set()
        assert release.wait(2)
        return False

    monkeypatch.setattr(ch, "publish_v1_clients_once", publish)
    writer = threading.Thread(target=lambda: ch.publish_v1_clients({}))
    transition = threading.Thread(
        target=lambda: (ch.v1_prepare_owned_backend(), transitioned.set())
    )
    writer.start()
    assert started.wait(2)
    transition.start()
    try:
        assert not transitioned.wait(0.05)
        assert not (tmp_path / "v1-notification-backend").exists()
    finally:
        release.set()
        writer.join(2)
        transition.join(2)
    assert transitioned.is_set()
    assert (tmp_path / "v1-notification-backend").read_text() == "preparing-owned"


def test_v1_prepare_owned_backend_waits_for_handoff_lock(monkeypatch, tmp_path):
    import fcntl
    import threading

    monkeypatch.setattr(ch, "state_dir", tmp_path)
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
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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


def test_v1_prepare_ordinary_cleanup_stage_rejects_unproven_store(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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


def test_v1_old_unread_notice_keeps_publication_until_read_then_fences(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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


def test_v1_cleanup_stage_merges_unfenced_inventory_after_interruption(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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


@pytest.mark.parametrize("bad_inventory", [None, {}, {"items": [None]}])
def test_v1_cleanup_stage_rejects_invalid_saved_inventory(
    monkeypatch, tmp_path, bad_inventory
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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


def test_v1_cleanup_stage_rejects_saved_inventory_from_another_store(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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


def test_v1_cleanup_stage_rejects_unidentified_legacy_notification(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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


def test_v1_cleanup_stage_recognizes_owned_row_after_interrupted_publish(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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


def test_v1_cleanup_stage_does_not_adopt_another_producers_notification(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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


def test_v1_cleanup_stage_lists_after_publication_is_fenced(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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


def test_v1_cleanup_stage_drains_and_retries_from_durable_inventory(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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
    monkeypatch.setattr(ch, "state_dir", tmp_path)
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
    monkeypatch.setattr(ch, "state_dir", tmp_path)
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


def test_v1_owned_transition_runs_over_one_socket(monkeypatch, tmp_path):
    import threading

    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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


def test_v1_cleanup_stage_rejects_missing_state_before_fencing(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    (tmp_path / "v1-enabled").write_text("enabled")
    ch.save_state({"v1": {"clients": {}, "sources": {}}})
    store = str(uuid.uuid4())
    session = SimpleNamespace(store_identity=lambda: store, list_notifications=list)
    assert ch.v1_prepare_ordinary_cleanup_stage(session) == {
        "store_id": store,
        "items": [],
    }


@pytest.mark.parametrize("backend", ["preparing-owned", "owned"])
def test_v1_legacy_recovery_does_not_restart_writers_after_backend_fence(
    monkeypatch, tmp_path, backend
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    marker = tmp_path / "v1-notification-backend"
    marker.write_text(backend)
    marker.chmod(0o600)
    (tmp_path / "v1-enabled").write_text("pending")
    restarts = []
    monkeypatch.setattr(ch, "pending_v1_legacy_restores", lambda: [("watch", "a", 123)])
    monkeypatch.setattr(ch, "restart_legacy_daemons", restarts.append)
    assert not ch.v1_recover_legacy_handoff()
    assert (tmp_path / "v1-enabled").read_text() == "pending"
    assert not restarts


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
    monkeypatch.setattr(ch, "cmux_cli", lambda cfg, args: effects.append(args) or True)
    ch.reconcile_v1_sidebar({}, state, {"W": {"a"}})
    effects.clear()
    ch.reconcile_v1_sidebar({}, state, {"w": {"a"}})
    assert not any(args[0] == "clear-status" for args in effects)


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


def test_v1_ordinary_cleanup_inventory_rejects_object_destination_key():
    workspace, surface = str(uuid.uuid4()), str(uuid.uuid4())
    key = json.dumps({workspace: "ignored", surface: "ignored"})
    store = str(uuid.uuid4())
    state = {
        "v1": {"destinations": {key: {"notification_id": "owned", "store_id": store}}}
    }
    with pytest.raises(ValueError, match="invalid notification target"):
        ch.v1_ordinary_cleanup_inventory(state, store)


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


def test_v1_partial_publication_preserves_active_destination_across_case_changes(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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


def test_v1_partial_publication_clears_certified_sidebar_case_insensitively(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    ch.save_state({"v1": {"sidebar_keys": {"W": ["old"]}}})
    monkeypatch.setattr(ch, "prune_v1_clients", lambda state: None)
    monkeypatch.setattr(ch, "retire_v1_legacy_daemons", lambda: True)
    effects = []
    monkeypatch.setattr(ch, "cmux_cli", lambda cfg, args: effects.append(args) or True)
    ch.publish_v1_clients({}, eligible_destinations={("w", "s")})
    assert ["clear-status", "old", "--workspace", "W"] in effects


def test_v1_partial_publication_retires_certified_empty_surface(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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


def test_v1_publication_preserves_orphan_sidebar_when_full_scope_is_revoked(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    ch.save_state({"v1": {"sidebar_keys": {"w": ["old-key"]}}})
    monkeypatch.setattr(ch, "prune_v1_clients", lambda state: None)
    monkeypatch.setattr(ch, "retire_v1_legacy_daemons", lambda: True)
    monkeypatch.setattr(ch, "load_remote_state", lambda: {})
    effects = []
    monkeypatch.setattr(ch, "cmux_cli", lambda *args: effects.append(args) or True)
    ch.publish_v1_clients({}, eligibility_check=lambda: set())
    assert not effects
    assert ch.load_state()["v1"]["sidebar_keys"] == {"w": ["old-key"]}


def test_v1_sidebar_preserves_remaining_keys_when_readiness_is_lost(monkeypatch):
    state = {"v1": {"sidebar_keys": {"w": ["a", "b"]}}}
    ready = [True]
    calls = []

    def clear(cfg, args):
        calls.append(args)
        ready[0] = False
        return True

    monkeypatch.setattr(ch, "cmux_cli", clear)
    ch.reconcile_v1_sidebar({}, state, {}, is_current=lambda workspace: ready[0])
    assert len(calls) == 1
    assert len(state["v1"]["sidebar_keys"]["w"]) == 1


def test_v1_destination_validation_detects_client_with_different_id_case(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    snapshot = {"v1": {"clients": {}}}
    ch.save_state(snapshot)
    assert ch.v1_destination_inputs_current(snapshot, "W", "S")
    live = ch.load_state()
    live["v1"]["clients"]["new"] = {"workspace": "w", "surface": "s", "source": "a"}
    ch.save_state(live)
    assert not ch.v1_destination_inputs_current(snapshot, "W", "S")


def test_v1_workspace_validation_detects_replaced_binding(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    snapshot = {
        "v1": {
            "clients": {"c": {"workspace": "w", "surface": "s", "source": "old"}},
            "sources": {"old": {}, "new": {}},
        }
    }
    ch.save_state(snapshot)
    assert ch.v1_workspace_inputs_current(snapshot, "w")
    live = ch.load_state()
    live["v1"]["clients"]["c"]["source"] = "new"
    ch.save_state(live)
    assert not ch.v1_workspace_inputs_current(snapshot, "w")


def test_v1_partial_publication_freezes_mixed_workspace_sidebar(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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


def test_v1_source_observer_publishes_on_change_and_respects_leader(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    calls = []
    monkeypatch.setattr(
        ch, "publish_v1_clients", lambda cfg, **kwargs: calls.append("publish")
    )
    monkeypatch.setattr(
        ch,
        "v1_watch_connection",
        lambda source, generation, path, on_change: on_change(),
    )
    assert ch.observe_v1_source({}, "source", "boot", "/socket")
    assert calls == ["publish"]
    with ch.v1_source_leader("source"):
        assert not ch.observe_v1_source({}, "source", "boot", "/socket")
    assert calls == ["publish"]


def test_v1_client_registration_persists_and_unregistration_preserves_other_clients(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    ch.persist_v1_client("a", "source", "w", "s", 10)
    ch.persist_v1_client("b", "source", "w2", "s2", 11)
    ch.remove_v1_client("a")
    saved = json.loads(ch.STATE_PATH.read_text())
    assert set(saved["v1"]["clients"]) == {"b"}


def test_v1_local_socket_resolves_running_named_and_default_session(monkeypatch):
    sessions = {
        "sessions": [
            {
                "name": "default",
                "default": True,
                "running": True,
                "socket_path": "/default.sock",
            },
            {"name": "work", "running": True, "socket_path": "/work.sock"},
            {"name": "off", "running": False, "socket_path": "/off.sock"},
        ]
    }
    monkeypatch.setattr(
        ch,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0, stdout=json.dumps(sessions), stderr=""
        ),
    )
    assert ch.v1_local_socket("") == "/default.sock"
    assert ch.v1_local_socket("work") == "/work.sock"
    assert ch.v1_local_socket("off") is None


def test_v1_source_key_distinguishes_remote_and_default_session():
    assert ch.v1_source_key({"session": "", "remote": None}) == ch.v1_source_key(
        {"session": "default", "remote": None}
    )
    assert ch.v1_source_key({"session": "default", "remote": None}) != ch.v1_source_key(
        {"session": "default", "remote": "tom@host"}
    )
    assert ch.v1_source_key({"session": "a:b", "remote": "c"}) != ch.v1_source_key(
        {"session": "b", "remote": "c:a"}
    )


def test_v1_local_generation_changes_when_socket_identity_changes(tmp_path):
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.touch()
    second.touch()
    assert ch.v1_local_generation(first) != ch.v1_local_generation(second)


def test_v1_remote_generation_quotes_socket_path(monkeypatch):
    calls = []

    def fake_run(cmd, timeout=10):
        calls.append(cmd)
        return SimpleNamespace(returncode=0, stdout="12:34\n", stderr="")

    monkeypatch.setattr(ch, "run", fake_run)
    assert ch.v1_remote_generation("tom@host", "/tmp/socket with spaces") == "12:34"
    assert calls[0][-2] == "tom@host"
    assert "'/tmp/socket with spaces'" in calls[0][-1]


def test_v1_client_registers_observes_and_unregisters(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    stop = ch.threading.Event()
    calls = []
    monkeypatch.setattr(ch, "v1_publish_loop", lambda cfg, event: None)
    monkeypatch.setattr(
        ch, "publish_v1_clients", lambda cfg, **kwargs: calls.append("publish")
    )
    monkeypatch.setattr(ch, "v1_local_socket", lambda session: "/socket")
    monkeypatch.setattr(ch, "v1_local_generation", lambda path: "boot")
    monkeypatch.setattr(ch, "v1_probe_source", lambda target: True)

    def observe(cfg, source, generation, path):
        calls.append((source, generation, path))
        stop.set()
        return True

    monkeypatch.setattr(ch, "observe_v1_source", observe)
    target = {"session": "work", "remote": None}
    ch.run_v1_client({}, target, "w", "s", stop)
    assert (
        ch.v1_source_key({**target, "socket_path": "/socket"}),
        "boot",
        "/socket",
    ) in calls
    assert json.loads(ch.STATE_PATH.read_text())["v1"]["clients"] == {}


def test_v1_client_unregisters_when_worker_start_fails(monkeypatch):
    calls = []
    monkeypatch.setattr(ch, "resolve_v1_source_target", lambda target: target)
    monkeypatch.setattr(ch, "v1_probe_source", lambda target: True)
    monkeypatch.setattr(ch, "activate_v1_client", lambda *args: True)
    monkeypatch.setattr(ch, "remove_v1_client", lambda client: calls.append(client))
    monkeypatch.setattr(ch, "publish_v1_clients", lambda cfg, **kwargs: None)
    monkeypatch.setattr(
        ch.threading.Thread,
        "start",
        lambda self: (_ for _ in ()).throw(RuntimeError("cannot start")),
    )
    with pytest.raises(RuntimeError, match="cannot start"):
        ch.run_v1_client_with_lease(
            {},
            {"remote": None, "socket_path": "/socket"},
            "w",
            "s",
            ch.threading.Event(),
            "c",
        )
    assert calls == ["c"]


def test_v1_client_unregisters_if_activation_is_interrupted(monkeypatch):
    calls = []
    monkeypatch.setattr(ch, "resolve_v1_source_target", lambda target: target)
    monkeypatch.setattr(ch, "v1_probe_source", lambda target: True)
    monkeypatch.setattr(
        ch,
        "activate_v1_client",
        lambda *args: (_ for _ in ()).throw(SystemExit(0)),
    )
    monkeypatch.setattr(ch, "remove_v1_client", lambda client: calls.append(client))
    monkeypatch.setattr(
        ch,
        "publish_v1_clients",
        lambda cfg, **kwargs: pytest.fail("published after rollback"),
    )
    with pytest.raises(SystemExit):
        ch.run_v1_client_with_lease(
            {},
            {"remote": None, "socket_path": "/socket"},
            "w",
            "s",
            ch.threading.Event(),
            "c",
        )
    assert calls == ["c"]


def test_v1_client_retries_remote_timeout(monkeypatch):
    stop = ch.threading.Event()
    calls = []
    monkeypatch.setattr(ch, "resolve_v1_source_target", lambda target: target)
    monkeypatch.setattr(ch, "v1_probe_source", lambda target: True)
    monkeypatch.setattr(ch, "activate_v1_client", lambda *args: True)
    monkeypatch.setattr(ch, "remove_v1_client", lambda client: None)
    monkeypatch.setattr(ch, "publish_v1_clients", lambda cfg, **kwargs: None)
    monkeypatch.setattr(ch, "v1_publish_loop", lambda *args: None)
    monkeypatch.setattr(ch, "v1_focus_loop", lambda *args: None)

    def observe(*args):
        calls.append(1)
        if len(calls) == 1:
            raise subprocess.TimeoutExpired("ssh", 1)
        stop.set()

    monkeypatch.setattr(ch, "observe_v1_remote", observe)
    ch.run_v1_client_with_lease(
        {}, {"remote": "host", "session": "work"}, "w", "s", stop, "c"
    )
    assert len(calls) == 2


def test_v1_client_entrypoint_does_not_activate_partial_handoff(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "config_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setenv("HERDR_PLUGIN_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("HERDR_PLUGIN_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("CMUX_WORKSPACE_ID", "workspace-id")
    monkeypatch.setenv("CMUX_SURFACE_ID", "surface-id")
    monkeypatch.setattr(
        ch.sys, "argv", ["cmux_herdr.py", "client", "--session", "work"]
    )
    monkeypatch.setattr(ch.signal, "signal", lambda *args: None)
    monkeypatch.setattr(ch, "v1_cmux_focus", lambda *args: (False, "com.cmuxterm.app"))
    calls = []
    monkeypatch.setattr(
        ch,
        "run_v1_client",
        lambda cfg, target, workspace, surface, stop: calls.append(
            (target, workspace, surface)
        ),
    )
    assert ch.main() == 1
    assert calls == []


def test_supervisor_entry_runs_only_for_cmux_interactive_client(monkeypatch, tmp_path):
    monkeypatch.setenv("CMUX_WORKSPACE_ID", "w")
    monkeypatch.setenv("CMUX_SURFACE_ID", "s")
    monkeypatch.setenv("HERDR_PLUGIN_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("HERDR_PLUGIN_CONFIG_DIR", str(tmp_path))
    monkeypatch.setattr(
        sys, "argv", ["cmux_herdr.py", "supervisor", "--session", "work"]
    )
    monkeypatch.setattr(ch.signal, "signal", lambda *args: None)
    monkeypatch.setattr(ch, "load_config", lambda: {})
    calls = []
    monkeypatch.setattr(
        ch, "v1_run_supervisor", lambda cfg, stop, **kwargs: calls.append(cfg)
    )
    assert ch.main() == 0
    assert calls == [{}]
    calls.clear()
    monkeypatch.setattr(sys, "argv", ["cmux_herdr.py", "supervisor", "api", "schema"])
    assert ch.main() == 0
    assert not calls


def test_supervisor_entry_reloads_configuration_after_change(monkeypatch, tmp_path):
    monkeypatch.setenv("CMUX_WORKSPACE_ID", "w")
    monkeypatch.setenv("CMUX_SURFACE_ID", "s")
    monkeypatch.setenv("HERDR_PLUGIN_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("HERDR_PLUGIN_CONFIG_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["cmux_herdr.py", "supervisor"])
    monkeypatch.setattr(ch.signal, "signal", lambda *args: None)
    configs = iter([{"value": "old"}, {"value": "new"}, {"value": "new"}])
    monkeypatch.setattr(ch, "load_config_strict", lambda: next(configs))
    calls = []

    def run(cfg, stop, **kwargs):
        calls.append(cfg)
        return "config-changed" if kwargs["config_changed"]() else True

    monkeypatch.setattr(ch, "v1_run_supervisor", run)
    assert ch.main() == 0
    assert calls == [{"value": "old"}, {"value": "new"}]


def test_supervisor_keeps_running_on_invalid_remote_configuration(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("CMUX_WORKSPACE_ID", "w")
    monkeypatch.setenv("CMUX_SURFACE_ID", "s")
    monkeypatch.setenv("HERDR_PLUGIN_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("HERDR_PLUGIN_CONFIG_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["cmux_herdr.py", "supervisor"])
    monkeypatch.setattr(ch.signal, "signal", lambda *args: None)
    configs = iter([{}, {"remotes": "invalid"}])
    monkeypatch.setattr(ch, "load_config_strict", lambda: next(configs))

    def run(cfg, stop, *, config_changed):
        assert not config_changed()
        return True

    monkeypatch.setattr(ch, "v1_run_supervisor", run)
    assert ch.main() == 0


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


def test_supervisor_waits_for_valid_initial_configuration(monkeypatch, tmp_path):
    monkeypatch.setenv("CMUX_WORKSPACE_ID", "w")
    monkeypatch.setenv("CMUX_SURFACE_ID", "s")
    monkeypatch.setenv("HERDR_PLUGIN_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("HERDR_PLUGIN_CONFIG_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["cmux_herdr.py", "supervisor"])
    monkeypatch.setattr(ch.signal, "signal", lambda *args: None)
    configs = iter([{"cmux_bin": 5}, {"cmux_bin": "/valid"}])
    monkeypatch.setattr(ch, "load_config_strict", lambda: next(configs))

    class Stop:
        def is_set(self):
            return False

        def wait(self, timeout):
            return False

    monkeypatch.setattr(ch.threading, "Event", Stop)
    calls = []
    monkeypatch.setattr(
        ch, "v1_run_supervisor", lambda cfg, stop, **kwargs: calls.append(cfg)
    )
    assert ch.main() == 0
    assert calls == [{"cmux_bin": "/valid"}]


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


def test_zsh_integration_runs_supervisor_only_in_cmux_and_preserves_exit(tmp_path):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    marker = tmp_path / "observer-args"
    fake_python = fake_bin / "python3"
    fake_python.write_text(
        '#!/bin/sh\nprintf "%s\\n" "$@" > "$OBSERVER_MARKER"\n'
        'printf "%s\\n" "$HERDR_BIN_PATH" > "$OBSERVER_MARKER.bin"\nexit 0\n'
    )
    fake_python.chmod(0o755)
    fake_herdr = fake_bin / "herdr"
    fake_herdr.write_text("#!/bin/sh\nsleep 0.3\nexit 23\n")
    fake_herdr.chmod(0o755)
    env = dict(
        os.environ,
        PATH=f"{fake_bin}:{os.environ.get('PATH', '')}",
        OBSERVER_MARKER=str(marker),
        HERDR_BIN_PATH="/stale/herdr",
        CMUX_WORKSPACE_ID="w",
        CMUX_SURFACE_ID="s",
    )
    script = Path(ch.__file__).with_name("cmux_herdr.zsh")
    result = subprocess.run(
        ["zsh", "-c", 'source "$1"; herdr --session work', "test", str(script)],
        env=env,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 23
    assert marker.read_text().splitlines()[-3:] == ["supervisor", "--session", "work"]
    assert marker.with_name(marker.name + ".bin").read_text().strip() == str(fake_herdr)
    marker.unlink()
    env.pop("CMUX_SURFACE_ID")
    result = subprocess.run(
        ["zsh", "-c", 'source "$1"; herdr --session work', "test", str(script)],
        env=env,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 23
    assert not marker.exists()


def test_v1_client_stops_when_launching_shell_exits(monkeypatch):
    class Stop:
        def wait(self, timeout):
            assert timeout == 1
            return False

    sent = []
    monkeypatch.setattr(ch.os, "getppid", lambda: 1)
    monkeypatch.setattr(ch.os, "getpid", lambda: 123)
    monkeypatch.setattr(ch.os, "kill", lambda pid, sig: sent.append((pid, sig)))
    ch.v1_watch_parent(42, Stop())
    assert sent == [(123, ch.signal.SIGTERM)]


def test_v1_prunes_only_clients_without_live_lease(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
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
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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


@pytest.fixture
def force_action_identity(monkeypatch):
    monkeypatch.setattr(ch, "v1_herdr_process_identity", lambda pid: {"start_token": 1})
    monkeypatch.setattr(ch, "v1_local_generation", lambda path: "1:2")


@pytest.mark.usefixtures("force_action_identity")
def test_v1_force_read_action_targets_only_context_pane(monkeypatch):
    monkeypatch.setenv(
        "HERDR_PLUGIN_CONTEXT_JSON", json.dumps({"focused_pane_id": "p"})
    )
    monkeypatch.setenv("HERDR_SOCKET_PATH", "/socket")
    calls = []

    def rpc(path, method, params, **kwargs):
        calls.append((path, method, params))
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": "a" * 32}}
        if method == "pane.get":
            return {
                "pane": {
                    "pane_id": "p",
                    "agent_status": "idle",
                    "tokens": {"cmux_herdr_done": "episode-id"},
                }
            }
        return {"type": "ok"}

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    assert ch.v1_force_read_action()
    assert calls[-1][1] == "pane.report_metadata_if_boot"
    assert calls[-1][2]["pane_id"] == "p"
    assert calls[-1][2]["tokens"] == {"cmux_herdr_ack": "episode-id"}


@pytest.mark.usefixtures("force_action_identity")
def test_v1_force_read_action_does_not_acknowledge_blocked(monkeypatch):
    monkeypatch.setenv(
        "HERDR_PLUGIN_CONTEXT_JSON", json.dumps({"focused_pane_id": "p"})
    )
    monkeypatch.setenv("HERDR_SOCKET_PATH", "/socket")
    calls = []

    def rpc(path, method, params, **kwargs):
        calls.append(method)
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": "a" * 32}}
        return {
            "pane": {
                "pane_id": "p",
                "agent_status": "blocked",
                "tokens": {"cmux_herdr_done": "old"},
            }
        }

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    assert not ch.v1_force_read_action()
    assert calls == ["session.snapshot", "pane.get"]


@pytest.mark.usefixtures("force_action_identity")
def test_v1_force_read_upstream_requires_original_server_peer(monkeypatch):
    monkeypatch.setenv(
        "HERDR_PLUGIN_CONTEXT_JSON", json.dumps({"focused_pane_id": "p"})
    )
    monkeypatch.setenv("HERDR_SOCKET_PATH", "/socket")
    monkeypatch.setattr(ch.os, "getppid", lambda: 1234)
    calls = []

    def rpc(path, method, params, **kwargs):
        calls.append((method, kwargs))
        if method == "session.snapshot":
            return {"snapshot": {}}
        if method == "pane.get":
            return {
                "pane": {
                    "pane_id": "p",
                    "agent_status": "idle",
                    "tokens": {"cmux_herdr_done": "episode-id"},
                }
            }
        return {"type": "ok"}

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    assert ch.v1_force_read_action()
    assert calls == [
        ("session.snapshot", {"expected_peer": 1234}),
        ("pane.get", {"expected_peer": 1234}),
        ("pane.report_metadata", {"expected_peer": 1234}),
    ]


def test_v1_socket_rpc_rejects_changed_server_before_sending(monkeypatch):
    events = []

    class Socket:
        def settimeout(self, timeout):
            pass

        def connect(self, path):
            pass

        def getsockopt(self, level, option):
            return 999

        def sendall(self, data):
            events.append("sent")

        def close(self):
            events.append("closed")

    monkeypatch.setattr(ch.sys, "platform", "darwin")
    monkeypatch.setattr(ch.socket, "socket", lambda *args: Socket())
    with pytest.raises(ch.SocketError, match="incarnation changed"):
        ch.socket_rpc("/socket", "pane.get", {}, expected_peer=123)
    assert events == ["closed"]


@pytest.mark.usefixtures("force_action_identity")
def test_v1_force_read_action_accepts_retained_completion_after_agent_exit(monkeypatch):
    monkeypatch.setenv(
        "HERDR_PLUGIN_CONTEXT_JSON", json.dumps({"focused_pane_id": "p"})
    )
    monkeypatch.setenv("HERDR_SOCKET_PATH", "/socket")
    calls = []

    def rpc(path, method, params, **kwargs):
        calls.append((method, params))
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": "a" * 32}}
        if method == "pane.get":
            return {
                "pane": {
                    "pane_id": "p",
                    "agent": None,
                    "agent_status": "unknown",
                    "tokens": {"cmux_herdr_done": "episode"},
                }
            }
        return {"type": "ok"}

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    assert ch.v1_force_read_action()
    assert calls[-1][1]["tokens"] == {"cmux_herdr_ack": "episode"}


@pytest.mark.usefixtures("force_action_identity")
def test_v1_force_read_action_rejects_unknown_state_for_present_agent(monkeypatch):
    monkeypatch.setenv(
        "HERDR_PLUGIN_CONTEXT_JSON", json.dumps({"focused_pane_id": "p"})
    )
    monkeypatch.setenv("HERDR_SOCKET_PATH", "/socket")
    monkeypatch.setattr(
        ch,
        "socket_rpc",
        lambda path, method, params, **kwargs: (
            {"snapshot": {"server_boot_id": "a" * 32}}
            if method == "session.snapshot"
            else {
                "pane": {
                    "pane_id": "p",
                    "agent": "codex",
                    "agent_status": "unknown",
                    "tokens": {"cmux_herdr_done": "old"},
                }
            }
        ),
    )
    assert not ch.v1_force_read_action()


@pytest.mark.usefixtures("force_action_identity")
def test_v1_force_read_writes_only_with_verified_server_boot(monkeypatch):
    monkeypatch.setenv(
        "HERDR_PLUGIN_CONTEXT_JSON", json.dumps({"focused_pane_id": "p"})
    )
    monkeypatch.setenv("HERDR_SOCKET_PATH", "/socket")
    calls = []
    boot = "a" * 32

    def rpc(path, method, params, **kwargs):
        calls.append((method, params))
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": boot}}
        if method == "pane.get":
            return {
                "pane": {
                    "pane_id": "p",
                    "agent_status": "idle",
                    "tokens": {"cmux_herdr_done": "episode"},
                }
            }
        return {"type": "ok"}

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    assert ch.v1_force_read_action()
    assert calls[-1][0] == "pane.report_metadata_if_boot"
    assert calls[-1][1]["expected_server_boot_id"] == boot


@pytest.mark.usefixtures("force_action_identity")
def test_v1_force_read_rejects_missing_or_restarted_server_identity(monkeypatch):
    monkeypatch.setenv(
        "HERDR_PLUGIN_CONTEXT_JSON", json.dumps({"focused_pane_id": "p"})
    )
    monkeypatch.setenv("HERDR_SOCKET_PATH", "/socket")
    calls = []

    def no_boot(path, method, params, **kwargs):
        calls.append(method)
        return {"snapshot": {}}

    monkeypatch.setattr(ch, "socket_rpc", no_boot)
    assert not ch.v1_force_read_action()
    assert calls == ["session.snapshot", "pane.get"]

    def restarted(path, method, params, **kwargs):
        if method == "session.snapshot":
            return {"snapshot": {"server_boot_id": "a" * 32}}
        if method == "pane.get":
            return {
                "pane": {
                    "pane_id": "p",
                    "agent_status": "idle",
                    "tokens": {"cmux_herdr_done": "episode"},
                }
            }
        raise ch.SocketError("server boot changed")

    monkeypatch.setattr(ch, "socket_rpc", restarted)
    assert not ch.v1_force_read_action()


@pytest.mark.parametrize("changed_source", ["a", "b"])
def test_v1_slow_source_refresh_does_not_lock_other_updates(
    monkeypatch, tmp_path, changed_source
):
    import threading

    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    ch.save_state({"v1": {"sources": {"a": {"value": 1}, "b": {"value": 1}}}})
    entered, release, updated = threading.Event(), threading.Event(), threading.Event()

    def refresh(state, *args, **kwargs):
        entered.set()
        assert release.wait(5)
        state["v1"]["sources"]["a"]["value"] = 2
        return True

    monkeypatch.setattr(ch, "refresh_v1_source", refresh)
    result = []
    worker = threading.Thread(
        target=lambda: result.append(ch.sync_v1_source("a", "g", "/socket"))
    )

    def update():
        with ch.state_lock():
            state = ch.load_state()
            state["v1"]["sources"][changed_source]["value"] = 3
            ch.save_state(state)
        updated.set()

    writer = threading.Thread(target=update)
    worker.start()
    try:
        assert entered.wait(3)
        writer.start()
        assert updated.wait(1), "remote refresh held the shared state lock"
    finally:
        release.set()
        worker.join(5)
        if writer.ident is not None:
            writer.join(5)
    state = ch.load_state()["v1"]["sources"]
    assert state[changed_source]["value"] == 3
    assert result == [changed_source != "a"]
    if changed_source == "b":
        assert state["a"]["value"] == 2


def test_v1_slow_publisher_does_not_hold_shared_state_lock(monkeypatch, tmp_path):
    import threading

    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    ch.save_state({"v1": {"sources": {"a": {"value": 1}}, "clients": {}}})
    entered, release, updated = threading.Event(), threading.Event(), threading.Event()
    monkeypatch.setattr(ch, "retire_v1_legacy_daemons", lambda: True)
    monkeypatch.setattr(ch, "prune_v1_clients", lambda state: None)
    monkeypatch.setattr(ch, "load_remote_state", lambda: {})
    monkeypatch.setattr(ch, "reconcile_v1_legacy_sidebar", lambda *args: None)

    def sidebar(cfg, state, workspaces):
        entered.set()
        assert release.wait(5)
        state["v1"]["sidebar_keys"] = {"w": ["herdr.v1.test"]}

    monkeypatch.setattr(ch, "reconcile_v1_sidebar", sidebar)
    worker = threading.Thread(target=lambda: ch.publish_v1_clients({}))

    def update():
        with ch.state_lock():
            state = ch.load_state()
            state["v1"]["sources"]["a"]["value"] = 2
            ch.save_state(state)
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
    state = ch.load_state()
    assert state["v1"]["sources"]["a"]["value"] == 2
    assert state["v1"]["sidebar_keys"] == {"w": ["herdr.v1.test"]}


def test_v1_publisher_does_not_restore_acknowledged_attention_from_stale_snapshot(
    monkeypatch, tmp_path
):
    import threading

    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
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


def test_v1_publisher_repairs_attention_acknowledged_during_notification_creation(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    ch.save_state(
        {
            "v1": {
                "sources": {"a": {"attention": {"p": {"status": "done"}}}},
                "clients": {"c": {"workspace": "w", "surface": "s", "source": "a"}},
            }
        }
    )
    monkeypatch.setattr(ch, "retire_v1_legacy_daemons", lambda: True)
    monkeypatch.setattr(ch, "prune_v1_clients", lambda state: None)
    monkeypatch.setattr(ch, "load_remote_state", lambda: {})
    monkeypatch.setattr(ch, "reconcile_v1_legacy_sidebar", lambda *args: None)
    monkeypatch.setattr(ch, "reconcile_v1_sidebar", lambda *args: None)
    notices = []
    monkeypatch.setattr(ch, "cmux_notifications_checked", lambda cfg: list(notices))

    def legacy(*args):
        with ch.state_lock():
            live = ch.load_state()
            live["v1"]["sources"]["a"]["attention"].clear()
            ch.save_state(live)
        return None

    monkeypatch.setattr(ch, "find_v1_legacy_notification", legacy)

    def run(cmd, **kwargs):
        notices.append(
            {
                "id": "new",
                "workspace_id": "w",
                "surface_id": "s",
                "is_read": False,
                "subtitle": ch.NOTIFICATION_SUBTITLE,
            }
        )
        return SimpleNamespace(returncode=0, stdout='{"id":"new"}', stderr="")

    monkeypatch.setattr(ch, "run", run)
    monkeypatch.setattr(
        ch, "cmux_cli", lambda cfg, cmd: notices[0].update(is_read=True) or True
    )
    ch.publish_v1_clients({})
    assert not notices or notices[0]["is_read"]


def test_v1_final_publisher_waits_for_inflight_publication(monkeypatch, tmp_path):
    import threading

    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    ch.save_state(
        {
            "v1": {
                "sources": {"a": {}},
                "clients": {"c": {"workspace": "w", "surface": "s", "source": "a"}},
            }
        }
    )
    entered, release = threading.Event(), threading.Event()
    seen = []
    monkeypatch.setattr(ch, "retire_v1_legacy_daemons", lambda: True)
    monkeypatch.setattr(ch, "prune_v1_clients", lambda state: None)
    monkeypatch.setattr(ch, "load_remote_state", lambda: {})
    monkeypatch.setattr(ch, "reconcile_v1_legacy_sidebar", lambda *args: None)

    def sidebar(cfg, state, workspaces):
        seen.append(set(workspaces))
        if len(seen) == 1:
            entered.set()
            assert release.wait(5)

    monkeypatch.setattr(ch, "reconcile_v1_sidebar", sidebar)
    monkeypatch.setattr(ch, "reconcile_v1_destination", lambda *args, **kwargs: True)
    first = threading.Thread(target=lambda: ch.publish_v1_clients({}))
    final = threading.Thread(target=lambda: ch.publish_v1_clients({}, wait=True))
    first.start()
    try:
        assert entered.wait(3)
        with ch.state_lock():
            state = ch.load_state()
            state["v1"]["clients"].clear()
            ch.save_state(state)
        final.start()
        release.set()
        final.join(5)
    finally:
        release.set()
        first.join(5)
        if final.ident is not None:
            final.join(5)
    assert seen == [{"w"}, set(), set()]


def test_v1_slow_token_write_does_not_block_updates_or_commit_stale_episode(
    monkeypatch, tmp_path
):
    import threading

    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    ch.save_state(
        {
            "v1": {
                "sources": {
                    "a": {
                        "server_boot_id": "boot",
                        "attention": {"p": {"status": "done", "episode": "old"}},
                        "panes": {"p": {}},
                    },
                    "b": {"value": 1},
                }
            }
        }
    )
    entered, release, updated = threading.Event(), threading.Event(), threading.Event()
    writes = []

    def rpc(path, method, params):
        value = params["tokens"]["cmux_herdr_done"]
        if value == "old":
            entered.set()
            assert release.wait(5)
        writes.append(value)
        return {}

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    worker = threading.Thread(
        target=lambda: ch.sync_v1_episode_tokens("a", "/socket", expected_boot="boot")
    )

    def update():
        with ch.state_lock():
            state = ch.load_state()
            state["v1"]["sources"]["a"]["attention"]["p"]["episode"] = "new"
            state["v1"]["sources"]["b"]["value"] = 2
            ch.save_state(state)
        updated.set()

    writer = threading.Thread(target=update)
    worker.start()
    try:
        assert entered.wait(3)
        writer.start()
        assert updated.wait(1), "remote token write held the shared state lock"
    finally:
        release.set()
        worker.join(5)
        if writer.ident is not None:
            writer.join(5)
    state = ch.load_state()["v1"]["sources"]
    assert state["b"]["value"] == 2
    assert state["a"]["published_tokens"]["p"] != "old"
    ch.sync_v1_episode_tokens("a", "/socket", expected_boot="boot")
    assert writes == ["old", "new"]
    assert ch.load_state()["v1"]["sources"]["a"]["published_tokens"]["p"] == "new"


@pytest.mark.parametrize("new_generation", [False, True])
def test_v1_source_refresh_merges_concurrent_token_bookkeeping(
    monkeypatch, tmp_path, new_generation
):
    import threading

    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    ch.save_state(
        {
            "v1": {
                "sources": {
                    "a": {
                        "value": 1,
                        "cursor_generation": "g",
                        "published_tokens": {"p": {"pending": True, "id": "old"}},
                    }
                }
            }
        }
    )
    entered, release = threading.Event(), threading.Event()

    def refresh(state, *args, **kwargs):
        entered.set()
        assert release.wait(5)
        state["v1"]["sources"]["a"]["value"] = 2
        if new_generation:
            state["v1"]["sources"]["a"]["cursor_generation"] = "next"
            state["v1"]["sources"]["a"].pop("published_tokens")
        return True

    monkeypatch.setattr(ch, "refresh_v1_source", refresh)
    result = []
    worker = threading.Thread(
        target=lambda: result.append(ch.sync_v1_source("a", "g", "/socket"))
    )
    worker.start()
    try:
        assert entered.wait(3)
        with ch.state_lock():
            state = ch.load_state()
            state["v1"]["sources"]["a"]["published_tokens"]["p"]["id"] = "new"
            ch.save_state(state)
    finally:
        release.set()
        worker.join(5)
    assert result == [True]
    source = ch.load_state()["v1"]["sources"]["a"]
    assert source["value"] == 2
    if new_generation:
        assert "published_tokens" not in source
    else:
        assert source["published_tokens"]["p"]["id"] == "new"


def test_v1_episode_tokens_publish_only_confirmed_completions(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    state = {
        "v1": {
            "sources": {
                "source": {
                    "attention": {
                        "p": {"status": "done", "episode": "confirmed"},
                        "q": {
                            "status": "done",
                            "episode": "pending",
                            "provisional": True,
                        },
                    },
                    "panes": {"p": {"status": "done"}, "q": {"status": "done"}},
                }
            }
        }
    }
    ch.save_state(state)
    calls = []
    monkeypatch.setattr(
        ch,
        "socket_rpc",
        lambda path, method, params: calls.append((method, params)) or {"type": "ok"},
    )
    ch.sync_v1_episode_tokens("source", "/socket")
    assert len(calls) == 1
    assert calls[0][1]["pane_id"] == "p"
    assert calls[0][1]["tokens"] == {"cmux_herdr_done": "confirmed"}
    ch.sync_v1_episode_tokens("source", "/socket")
    assert len(calls) == 1


def test_v1_episode_tokens_guard_writes_with_server_boot(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    ch.save_state(
        {
            "v1": {
                "sources": {
                    "source": {
                        "server_boot_id": "old",
                        "attention": {"p": {"status": "done", "episode": "episode"}},
                        "panes": {"p": {}},
                    }
                }
            }
        }
    )
    calls = []

    def rpc(path, method, params):
        calls.append((method, params))
        raise ch.SocketError("server boot identity changed")

    monkeypatch.setattr(ch, "socket_rpc", rpc)
    ch.sync_v1_episode_tokens("source", "/socket", expected_boot="old")
    assert calls[0][0] == "pane.report_metadata_if_boot"
    assert calls[0][1]["expected_server_boot_id"] == "old"
    assert (
        ch.load_state()["v1"]["sources"]["source"]["published_tokens"]["p"] != "episode"
    )


def test_v1_episode_tokens_retry_clear_after_lost_write_response(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    ch.save_state(
        {
            "v1": {
                "sources": {
                    "source": {
                        "server_boot_id": "boot",
                        "attention": {"p": {"status": "done", "episode": "episode"}},
                        "panes": {"p": {}},
                    }
                }
            }
        }
    )
    remote = {}

    def lost_response(path, method, params):
        remote.update(params["tokens"])
        raise ch.SocketError("response lost")

    monkeypatch.setattr(ch, "socket_rpc", lost_response)
    ch.sync_v1_episode_tokens("source", "/socket", expected_boot="boot")
    state = ch.load_state()
    state["v1"]["sources"]["source"]["attention"] = {}
    ch.save_state(state)
    monkeypatch.setattr(
        ch,
        "socket_rpc",
        lambda path, method, params: remote.update(params["tokens"]) or {},
    )
    ch.sync_v1_episode_tokens("source", "/socket", expected_boot="boot")
    assert remote["cmux_herdr_done"] is None


def test_v1_episode_tokens_use_stored_boot_for_legacy_caller(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    ch.save_state(
        {
            "v1": {
                "sources": {
                    "s": {
                        "server_boot_id": "boot",
                        "attention": {"p": {"status": "done", "episode": "episode"}},
                        "panes": {"p": {}},
                    }
                }
            }
        }
    )
    calls = []
    monkeypatch.setattr(
        ch,
        "socket_rpc",
        lambda path, method, params: calls.append((method, params)) or {},
    )
    ch.sync_v1_episode_tokens("s", "/socket")
    assert calls[0][0] == "pane.report_metadata_if_boot"
    assert calls[0][1]["expected_server_boot_id"] == "boot"


def test_v1_episode_tokens_clear_after_acknowledgment_or_pane_close(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    ch.save_state(
        {
            "v1": {
                "sources": {
                    "source": {
                        "attention": {},
                        "panes": {"p": {"status": "idle"}},
                        "published_tokens": {"p": "old", "closed": "old"},
                    }
                }
            }
        }
    )
    calls = []
    monkeypatch.setattr(
        ch, "socket_rpc", lambda path, method, params: calls.append(params) or {}
    )
    ch.sync_v1_episode_tokens("source", "/socket")
    assert [(call["pane_id"], call["tokens"]) for call in calls] == [
        ("p", {"cmux_herdr_done": None})
    ]
    assert ch.load_state()["v1"]["sources"]["source"]["published_tokens"] == {}


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
    monkeypatch.setattr(ch, "cmux_cli", lambda cfg, args: calls.append(args) or True)
    ch.reconcile_v1_sidebar({}, state, {"cmux-w": {"source"}})
    keys = [call[1] for call in calls if call[0] == "set-status"]
    assert len(keys) == len(set(keys)) == 3
    assert all(call[call.index("--workspace") + 1] == "cmux-w" for call in calls)
    calls.clear()
    state["v1"]["sources"]["source"]["panes"].pop("p:1")
    monkeypatch.setattr(
        ch,
        "cmux_cli",
        lambda cfg, args: calls.append(args) or args[0] != "clear-status",
    )
    ch.reconcile_v1_sidebar({}, state, {"cmux-w": {"source"}})
    stale = [call[1] for call in calls if call[0] == "clear-status"]
    assert len(stale) == 1
    assert stale[0] in state["v1"]["sidebar_keys"]["cmux-w"]
    calls.clear()
    monkeypatch.setattr(ch, "cmux_cli", lambda cfg, args: calls.append(args) or True)
    ch.reconcile_v1_sidebar({}, state, {})
    assert {call[1] for call in calls if call[0] == "clear-status"} == set(keys)
    assert not state["v1"]["sidebar_keys"]


def test_v1_token_retry_loop_retries_without_an_event(monkeypatch):
    calls = []

    class Stop:
        def is_set(self):
            return len(calls) == 2

        def wait(self, delay):
            assert delay == 1

    monkeypatch.setattr(
        ch, "sync_v1_episode_tokens", lambda source, sock: calls.append((source, sock))
    )
    ch.v1_token_retry_loop("source", "/socket", Stop())
    assert calls == [("source", "/socket"), ("source", "/socket")]


def test_v1_token_retry_starts_only_after_watcher_snapshot(monkeypatch):
    starts = []

    class Worker:
        def __init__(self, **kwargs):
            pass

        def start(self):
            starts.append("start")

        def join(self):
            starts.append("join")

    def watch(source, generation, socket, *, on_change):
        assert starts == []
        on_change()
        on_change()

    monkeypatch.setattr(ch.threading, "Thread", Worker)
    monkeypatch.setattr(ch, "v1_watch_connection", watch)
    monkeypatch.setattr(ch, "publish_v1_source", lambda *args: None)
    ch.watch_v1_with_token_retry({}, "source", "generation", "/socket")
    assert starts == ["start", "join"]


def test_v1_force_read_main_dispatches_without_loading_config(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["cmux_herdr.py", "force-read"])
    calls = []
    monkeypatch.setattr(ch, "v1_force_read_action", lambda: calls.append("ack") or True)
    monkeypatch.setattr(
        ch, "load_config", lambda: pytest.fail("unexpected config read")
    )
    assert ch.main() == 0
    assert calls == ["ack"]


def test_v1_force_read_action_selects_supported_python(tmp_path):
    manifest = ch.tomllib.loads(
        (Path(ch.__file__).parent / "herdr-plugin.toml").read_text()
    )
    action = next(
        item for item in manifest["actions"] if item["id"] == "mark-agent-read"
    )
    old = tmp_path / "python3"
    old.write_text("#!/bin/sh\nexit 1\n")
    old.chmod(0o755)
    supported = tmp_path / "python3.12"
    supported.write_text(
        '#!/bin/sh\nif [ "$1" = -c ]; then exit 0; fi\nprintf \'%s\\n\' "$@"\n'
    )
    supported.chmod(0o755)
    proc = subprocess.run(
        action["command"],
        env={
            "PATH": str(tmp_path),
            "ZDOTDIR": str(tmp_path),
            "HERDR_PLUGIN_ROOT": str(tmp_path),
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0
    assert proc.stdout.splitlines() == [str(tmp_path / "cmux_herdr.py"), "force-read"]


def test_v1_force_read_rejects_old_python_even_with_tomllib(tmp_path):
    manifest = ch.tomllib.loads(
        (Path(ch.__file__).parent / "herdr-plugin.toml").read_text()
    )
    action = next(
        item for item in manifest["actions"] if item["id"] == "mark-agent-read"
    )
    old_python = tmp_path / "python3"
    old_python.write_text(
        '#!/bin/sh\nif [ "$1" != -c ]; then exit 99; fi\n'
        """exec "$TEST_REAL_PYTHON" -c 'import sys; """
        """sys.version_info=(3, 9, 0); exec(sys.argv[1])' "$2"\n"""
    )
    old_python.chmod(0o755)
    supported = tmp_path / "python3.12"
    supported.write_text(
        '#!/bin/sh\nif [ "$1" = -c ]; then exit 0; fi\nprintf \'%s\\n\' "$@"\n'
    )
    supported.chmod(0o755)
    proc = subprocess.run(
        action["command"],
        env={
            "PATH": str(tmp_path),
            "TEST_REAL_PYTHON": sys.executable,
            "ZDOTDIR": str(tmp_path),
            "HERDR_PLUGIN_ROOT": str(tmp_path),
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0
    assert proc.stdout.splitlines() == [str(tmp_path / "cmux_herdr.py"), "force-read"]


def test_v1_focus_acknowledges_only_verified_focused_done():
    state = {
        "v1": {
            "sources": {
                "source": {
                    "attention": {
                        "p": {"status": "done", "episode": "done-1"},
                        "q": {"status": "blocked", "episode": "blocked-1"},
                        "r": {"status": "done", "episode": "done-2"},
                    }
                }
            }
        }
    }
    evidence = {
        "single_client": True,
        "foreground": True,
        "surface_focused": True,
        "pane_id": "p",
        "episode": "done-1",
    }
    for condition in ("single_client", "foreground", "surface_focused"):
        assert not ch.apply_v1_focus_ack(
            state, "source", {**evidence, condition: False}
        )
    assert not ch.apply_v1_focus_ack(state, "source", {**evidence, "episode": "stale"})
    assert not ch.apply_v1_focus_ack(
        state, "source", {**evidence, "pane_id": "q", "episode": "blocked-1"}
    )
    assert ch.apply_v1_focus_ack(state, "source", evidence)
    attention = state["v1"]["sources"]["source"]["attention"]
    assert attention["p"]["acknowledged"] == "done-1"
    assert attention["q"]["status"] == "blocked"
    assert attention["r"]["status"] == "done"


@pytest.mark.parametrize("release_sequence", [7, 11])
def test_v1_focus_reconciliation_acknowledges_presented_release(
    monkeypatch, tmp_path, release_sequence
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    boot = "a" * 32
    episode = "generation:t:7"
    state = {
        "v1": {
            "clients": {
                "c": {
                    "source": "src",
                    "workspace": "w",
                    "surface": "s",
                    "observed_client_pid": 42,
                    "observed_client_instance": "instance",
                    "endpoint": "local",
                    "connection_generation": 4,
                    "server_boot_id": boot,
                }
            },
            "sources": {
                "src": {
                    "attention": {
                        "p": {
                            "status": "done",
                            "episode": episode,
                            "generation": "generation",
                            "server_boot_id": boot,
                            "terminal_id": "t",
                            "sequence": 7,
                        },
                        "q": {"status": "blocked", "episode": "blocked"},
                    }
                }
            },
        }
    }
    ch.save_state(state)
    monkeypatch.setattr(ch, "prune_v1_clients", lambda state: None)
    monkeypatch.setattr(ch, "v1_cmux_is_foreground", lambda *args: True)
    observation = {
        "pid": 42,
        "instance": "instance",
        "observation": {
            "boot_id": boot,
            "endpoint": "local",
            "connection_generation": 4,
            "pane_id": "p",
            "outer_focused": True,
            "agent_state_change_seq": None,
            "pane_state": {
                "terminal_id": "t",
                "state_change_seq": release_sequence,
                "released_completion_seq": 7,
                "agent_present": False,
            },
        },
    }
    snapshot = {
        "server_boot_id": boot,
        "agents": [],
        "panes": [
            {"pane_id": "p", "terminal_id": "t", "state_change_seq": release_sequence}
        ],
    }
    monkeypatch.setattr(
        ch, "socket_rpc", lambda *args, **kwargs: {"snapshot": snapshot}
    )
    monkeypatch.setattr(ch, "v1_surface_observation", lambda *args: observation)

    observation["observation"]["pane_state"]["released_completion_seq"] = 9
    assert not ch.v1_reconcile_focus({}, "c", "src", "w", "s", "/socket")
    assert (
        "acknowledged" not in ch.load_state()["v1"]["sources"]["src"]["attention"]["p"]
    )

    observation["observation"]["pane_state"]["released_completion_seq"] = 7
    assert ch.v1_reconcile_focus({}, "c", "src", "w", "s", "/socket")
    attention = ch.load_state()["v1"]["sources"]["src"]["attention"]
    assert attention["p"]["acknowledged"] == episode
    assert attention["q"]["status"] == "blocked"
    assert "acknowledged" not in attention["q"]


def test_v1_focus_reconciliation_acknowledges_only_stable_current_target(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    boot = "a" * 32
    episode = "generation:t:7"
    state = {
        "v1": {
            "clients": {
                "c": {
                    "source": "src",
                    "workspace": "w",
                    "surface": "s",
                    "observed_client_pid": 42,
                    "observed_client_instance": "instance",
                    "endpoint": "local",
                    "connection_generation": 4,
                    "server_boot_id": boot,
                }
            },
            "sources": {
                "src": {
                    "attention": {
                        "p": {
                            "status": "done",
                            "episode": episode,
                            "generation": "generation",
                            "server_boot_id": boot,
                            "terminal_id": "t",
                            "sequence": 7,
                        },
                        "q": {"status": "blocked", "episode": "blocked"},
                    }
                }
            },
        }
    }
    ch.save_state(state)
    monkeypatch.setattr(ch, "prune_v1_clients", lambda state: None)
    monkeypatch.setattr(ch, "v1_cmux_is_foreground", lambda *args: True)
    observation = {
        "pid": 42,
        "instance": "instance",
        "observation": {
            "boot_id": boot,
            "endpoint": "local",
            "connection_generation": 4,
            "snapshot_revision": 1,
            "pane_id": "p",
            "outer_focused": True,
            "agent_state_change_seq": 7,
        },
    }
    observed = []
    monkeypatch.setattr(ch, "v1_surface_observation", lambda *args: observed.pop(0))
    monkeypatch.setattr(
        ch,
        "socket_rpc",
        lambda *args, **kwargs: {
            "snapshot": {
                "server_boot_id": boot,
                "agents": [
                    {
                        "pane_id": "p",
                        "terminal_id": "t",
                        "agent_status": "done",
                        "state_change_seq": 7,
                    }
                ],
            }
        },
    )
    for wrong in (
        {**observation, "pid": 43},
        {**observation, "instance": "other"},
        {
            **observation,
            "observation": {
                **observation["observation"],
                "endpoint": "ssh:" + "b" * 32,
            },
        },
        {
            **observation,
            "observation": {**observation["observation"], "connection_generation": 5},
        },
    ):
        observed[:] = [wrong, wrong]
        assert not ch.v1_reconcile_focus({}, "c", "src", "w", "s", "/socket")
    ownership = {"connection_id": "original"}
    state["v1"]["clients"]["c"]["native_ownership"] = ownership
    ch.save_state(state)
    observed[:] = [observation, observation]
    assert not ch.v1_reconcile_focus({}, "c", "src", "w", "s", "/socket")
    wrong = {**observation, "native_ownership": {"connection_id": "replacement"}}
    observed[:] = [wrong, wrong]
    assert not ch.v1_reconcile_focus({}, "c", "src", "w", "s", "/socket")
    observation["native_ownership"] = ownership
    observed[:] = [
        {
            **observation,
            "observation": {**observation["observation"], "snapshot_revision": 2},
        },
        observation,
    ]
    assert not ch.v1_reconcile_focus({}, "c", "src", "w", "s", "/socket")
    observed[:] = [
        observation,
        {
            **observation,
            "observation": {**observation["observation"], "snapshot_revision": 2},
        },
    ]
    assert ch.v1_reconcile_focus({}, "c", "src", "w", "s", "/socket")
    attention = ch.load_state()["v1"]["sources"]["src"]["attention"]
    assert attention["p"]["acknowledged"] == episode
    assert attention["q"]["status"] == "blocked"
    ch.save_state(state)
    observed[:] = [observation, {**observation, "instance": "other"}]
    assert not ch.v1_reconcile_focus({}, "c", "src", "w", "s", "/socket")
    assert ch.load_state()["v1"] == state["v1"]


def test_v1_focus_loop_checks_registered_client_and_uses_remote_forward(monkeypatch):
    calls = []
    monkeypatch.setattr(ch, "v1_reconcile_focus", lambda *args: calls.append(args))

    class Stop:
        def __init__(self):
            self.stopped = False

        def is_set(self):
            return self.stopped

        def wait(self, seconds):
            self.stopped = True

    target = {"remote": "tom@host", "session": "work"}
    ch.v1_focus_loop({}, "client", "source", "w", "s", target, Stop())
    name = "v1-" + ch.hashlib.sha256(b"source").hexdigest()[:16]
    assert calls == [({}, "client", "source", "w", "s", ch.remote_sock_path(name))]


def test_v1_focus_reconciliation_combines_real_cmux_and_client_checks(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(ch, "prune_v1_clients", lambda state: None)
    boot = "a" * 32
    state = {
        "v1": {
            "clients": {
                "c": {
                    "source": "src",
                    "workspace": "w",
                    "surface": "s",
                    "observed_client_pid": 20,
                    "observed_client_instance": "i",
                    "endpoint": "local",
                    "connection_generation": 4,
                    "server_boot_id": boot,
                }
            },
            "sources": {
                "src": {
                    "attention": {
                        "p": {
                            "status": "done",
                            "episode": "g:t:7",
                            "generation": "g",
                            "server_boot_id": boot,
                            "terminal_id": "t",
                            "sequence": 7,
                        }
                    }
                }
            },
        }
    }
    ch.save_state(state)
    caller = {
        "window_ref": "window:1",
        "workspace_ref": "workspace:1",
        "surface_ref": "surface:1",
    }
    terminal = {
        "workspace_id": "w",
        "surface_id": "s",
        "mapped": True,
        "workspace_selected": True,
        "window_key": True,
        "window_visible": True,
        "surface_view_first_responder": True,
    }
    surface = {
        "id": "s",
        "type": "terminal",
        "tty": "/dev/ttys001",
        "processes": [
            {
                "pid": 20,
                "ppid": 10,
                "name": "herdr",
                "path": "/bin/herdr",
                "tty_device": 100,
                "pgid": 20,
                "tpgid": 20,
                "children": [],
            }
        ],
    }
    tree = {
        "windows": [{"workspaces": [{"id": "w", "panes": [{"surfaces": [surface]}]}]}]
    }

    def fake_run(command, **kwargs):
        if "identify" in command:
            result = {
                "caller": caller,
                "focused": caller,
                "bundle_identifier": "com.cmuxterm.app",
                "socket_path": "/cmux.sock",
            }
        elif "debug.terminals" in command:
            result = {"terminals": [terminal]}
        else:
            result = tree
        return SimpleNamespace(returncode=0, stdout=json.dumps(result))

    monkeypatch.setattr(ch, "run", fake_run)
    monkeypatch.setattr(ch, "v1_frontmost_app", lambda: ("com.cmuxterm.app", 12))
    monkeypatch.setattr(ch, "v1_unix_peer_pid", lambda path: 12)
    monkeypatch.setattr(ch, "v1_tty_device", lambda tty: 100)
    monkeypatch.setattr(
        ch,
        "v1_observer_socket_exists",
        lambda path: (
            path.parent.name == "client-observers"
            and path.parent.parent.name == "herdr"
        ),
    )
    monkeypatch.setattr(
        ch,
        "v1_client_source_inventory",
        lambda *args, **kwargs: {
            "pid": 20,
            "instance": "i",
            "sources_complete": True,
            "sources": [],
        },
    )
    observation = {
        "pid": 20,
        "instance": "i",
        "observation": {
            "boot_id": boot,
            "endpoint": "local",
            "connection_generation": 4,
            "pane_id": "p",
            "outer_focused": None,
            "agent_state_change_seq": 7,
        },
    }
    monkeypatch.setattr(
        ch, "v1_client_observation", lambda path, pid, **kwargs: observation
    )
    monkeypatch.setattr(
        ch,
        "socket_rpc",
        lambda *args, **kwargs: {
            "snapshot": {
                "server_boot_id": boot,
                "agents": [
                    {
                        "pane_id": "p",
                        "terminal_id": "t",
                        "agent_status": "done",
                        "state_change_seq": 7,
                    }
                ],
            }
        },
    )
    terminal["surface_view_first_responder"] = False
    assert not ch.v1_reconcile_focus({}, "c", "src", "w", "s", "/socket")
    terminal["surface_view_first_responder"] = True
    assert ch.v1_reconcile_focus({}, "c", "src", "w", "s", "/socket")
    assert (
        ch.load_state()["v1"]["sources"]["src"]["attention"]["p"]["acknowledged"]
        == "g:t:7"
    )


def test_v1_cmux_surface_focus_requires_matching_window_workspace_and_surface(
    monkeypatch,
):
    caller = {
        "window_ref": "window:1",
        "workspace_ref": "workspace:1",
        "surface_ref": "surface:1",
    }
    data = {
        "caller": caller,
        "focused": dict(caller),
        "bundle_identifier": "com.cmuxterm.app",
    }
    commands = []

    def identify(command, **kwargs):
        commands.append(command)
        return SimpleNamespace(returncode=0, stdout=json.dumps(data))

    monkeypatch.setattr(ch, "run", identify)
    assert ch.v1_cmux_focus({}, "workspace-id", "surface-id") == (
        True,
        "com.cmuxterm.app",
    )
    assert commands[0][-4:] == [
        "--workspace",
        "workspace-id",
        "--surface",
        "surface-id",
    ]
    data["focused"]["surface_ref"] = "surface:2"
    assert ch.v1_cmux_focus({}, "workspace-id", "surface-id") == (
        False,
        "com.cmuxterm.app",
    )
    data["focused"].pop("window_ref")
    assert ch.v1_cmux_focus({}, "workspace-id", "surface-id") == (
        False,
        "com.cmuxterm.app",
    )
    monkeypatch.setattr(
        ch, "run", lambda *args, **kwargs: SimpleNamespace(returncode=1, stdout="")
    )
    assert ch.v1_cmux_focus({}, "workspace-id", "surface-id") == (False, None)


def test_v1_foreground_cmux_requires_the_socket_server_process(monkeypatch):
    identity = {
        "focused": True,
        "bundle": "com.cmuxterm.app",
        "socket_path": "/cmux.sock",
    }
    monkeypatch.setattr(ch, "v1_cmux_identity", lambda *args: identity)
    monkeypatch.setattr(ch, "v1_frontmost_app", lambda: ("com.cmuxterm.app", 12))
    monkeypatch.setattr(ch, "v1_cmux_terminal_focus", lambda *args: True, raising=False)
    monkeypatch.setattr(ch, "v1_unix_peer_pid", lambda path: 13)
    assert not ch.v1_cmux_is_foreground({}, "w", "s")
    monkeypatch.setattr(ch, "v1_unix_peer_pid", lambda path: 12)
    assert ch.v1_cmux_is_foreground({}, "w", "s")
    identity["focused"] = False
    assert not ch.v1_cmux_is_foreground({}, "w", "s")


def test_v1_cmux_terminal_focus_requires_key_window_and_first_responder(monkeypatch):
    terminal = {
        "workspace_id": "W",
        "surface_id": "S",
        "mapped": True,
        "workspace_selected": True,
        "window_key": True,
        "window_visible": True,
        "surface_view_first_responder": True,
    }
    monkeypatch.setattr(
        ch,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0, stdout=json.dumps({"terminals": [terminal]})
        ),
    )
    assert ch.v1_cmux_terminal_focus({}, "w", "s")
    for key in (
        "window_key",
        "surface_view_first_responder",
        "workspace_selected",
        "window_visible",
        "mapped",
    ):
        terminal[key] = False
        assert not ch.v1_cmux_terminal_focus({}, "w", "s")
        terminal[key] = True


def test_v1_interactive_client_respects_herdr_session_environment(monkeypatch):
    monkeypatch.setenv("HERDR_SESSION", "work")
    monkeypatch.delenv("HERDR_SOCKET_PATH", raising=False)
    assert ch.interactive_client_target([]) == {"session": "work", "remote": None}
    assert ch.interactive_client_target(["--session", "other"]) == {
        "session": "other",
        "remote": None,
    }


def test_v1_interactive_client_tracks_local_socket_override(monkeypatch):
    monkeypatch.setenv("HERDR_SESSION", "work")
    monkeypatch.setenv("HERDR_SOCKET_PATH", "/tmp/custom-herdr.sock")
    target = ch.interactive_client_target([])
    assert target == {
        "session": "work",
        "remote": None,
        "socket_path": "/tmp/custom-herdr.sock",
    }
    assert ch.v1_source_key(target) != ch.v1_source_key(
        {"session": "work", "remote": None}
    )
    assert ch.interactive_client_target(["--session", "other"]) == {
        "session": "other",
        "remote": None,
    }


def test_v1_socket_override_source_identity_ignores_inherited_session():
    one = {"remote": None, "session": "one", "socket_path": "/tmp/custom-herdr.sock"}
    two = {"remote": None, "session": "two", "socket_path": "/tmp/custom-herdr.sock"}
    assert ch.v1_source_key(one) == ch.v1_source_key(two)


def test_v1_migration_stops_only_verified_legacy_daemons(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    for filename in ("remote.pid", "watch-default.pid", "watch-work.pid"):
        (tmp_path / filename).write_text("{}")
    probes = []

    def find(path, mode):
        probes.append((path.name, mode))
        return {"remote.pid": 123, "watch-default.pid": None, "watch-work.pid": 456}[
            path.name
        ]

    signals = []

    def kill(pid, sig):
        if sig == 0:
            raise ProcessLookupError()
        signals.append((pid, sig))

    monkeypatch.setattr(ch, "live_daemon_pid", find)
    monkeypatch.setattr(
        ch, "v1_verified_daemon_audit_token", lambda path, pid: [pid] * 8
    )
    monkeypatch.setattr(
        ch, "v1_signal_audit_token", lambda token, sig: signals.append((token[0], sig))
    )
    monkeypatch.setattr(ch.os, "kill", kill)
    assert not ch.retire_v1_legacy_daemons()
    assert set(probes) == {
        ("remote.pid", "remote"),
        ("watch-default.pid", "watch"),
        ("watch-work.pid", "watch"),
    }
    assert signals == [(123, ch.signal.SIGTERM), (456, ch.signal.SIGTERM)]


def test_v1_legacy_dispatch_is_disabled_after_opt_in(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    (tmp_path / "v1-enabled").touch()
    monkeypatch.setattr(sys, "argv", ["cmux_herdr.py", "reconcile"])
    monkeypatch.setattr(ch, "load_config", lambda: pytest.fail("legacy writer ran"))
    assert ch.main() == 0


def test_v1_local_source_identity_is_the_resolved_socket(monkeypatch):
    monkeypatch.setattr(ch, "v1_local_generation", lambda path: "generation")
    monkeypatch.setattr(ch, "v1_local_socket", lambda session: "/tmp/shared-herdr.sock")
    named = ch.resolve_v1_source_target({"remote": None, "session": "work"})
    override = ch.resolve_v1_source_target(
        {"remote": None, "session": "other", "socket_path": "/tmp/shared-herdr.sock"}
    )
    assert ch.v1_source_key(named) == ch.v1_source_key(override)


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
        ch, "cmux_herdr_status_keys_checked", lambda cfg, ref: list(visible[ref])
    )

    def clear(cfg, args):
        calls.append(args)
        if args[1] == "herdr.w1.w1_p1" and len(calls) <= 6:
            return False
        visible[args[-1]].discard(args[1])
        return True

    monkeypatch.setattr(ch, "cmux_cli", clear)
    ch.reconcile_v1_legacy_sidebar({}, state, remote)
    assert len(calls) == 6
    assert "foreign" in visible["workspace:1"]
    ch.reconcile_v1_legacy_sidebar({}, state, remote)
    assert calls[-1] == ["clear-status", "herdr.w1.w1_p1", "--workspace", "workspace:1"]
    visible["workspace:1"].add("herdr.w1")
    ch.reconcile_v1_legacy_sidebar({}, state, remote)
    assert calls[-1] == ["clear-status", "herdr.w1", "--workspace", "workspace:1"]


def test_v1_migration_discovers_watchers_created_during_scan(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    checked = []

    def probe(path, mode):
        checked.append(path.name)
        if mode == "remote":
            (tmp_path / "watch-new.pid").touch()
        return None

    monkeypatch.setattr(ch, "live_daemon_pid", probe)
    assert not ch.retire_v1_legacy_daemons()
    assert checked == ["remote.pid", "watch-new.pid"]


def test_v1_publish_does_not_compete_with_unstopped_legacy_writer(monkeypatch):
    monkeypatch.setattr(ch, "retire_v1_legacy_daemons", lambda: False)
    monkeypatch.setattr(ch, "load_state", lambda: pytest.fail("publication must wait"))
    ch.publish_v1_clients({})


def test_v1_legacy_sidebar_finds_old_keys_in_active_and_label_workspaces(monkeypatch):
    state = {
        "sessions": {
            "work": {"workspaces": {"w1": {"label": "Old title", "panes": {"p1": {}}}}}
        },
        "v1": {"clients": {"client": {"workspace": "workspace:active"}}},
    }
    monkeypatch.setattr(ch, "cmux_workspace_refs", lambda cfg: {"workspace:old"})
    monkeypatch.setattr(
        ch,
        "cmux_herdr_status_keys_checked",
        lambda cfg, ref: ["herdr.w1", "herdr.w1.p1"],
    )
    calls = []
    monkeypatch.setattr(ch, "cmux_cli", lambda cfg, args: calls.append(args) or True)
    ch.reconcile_v1_legacy_sidebar({}, state, {})
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
        ch.find_v1_legacy_notification(state, ["source"], "w", "s", notifications)
        == "legacy"
    )
    assert (
        ch.find_v1_legacy_notification(state, ["source"], "W", "S", notifications)
        == "legacy"
    )
    state["v1"]["sources"]["source"]["attention"] = {}
    assert (
        ch.find_v1_legacy_notification(state, ["source"], "w", "s", notifications)
        is None
    )
    state["v1"]["sources"]["source"]["attention"] = {
        "p": {"status": "done", "episode": "e"}
    }
    assert (
        ch.find_v1_legacy_notification(state, ["source"], "w", "s", notifications)
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
        ch.find_v1_legacy_notification(state, ["source"], "w", "s", notifications)
        is None
    )


def test_v1_legacy_sidebar_finds_known_keys_in_inactive_workspaces(monkeypatch):
    state = {"sessions": {"old": {"workspaces": {"w1": {"panes": {"p1": {}}}}}}}
    monkeypatch.setattr(ch, "cmux_workspace_refs", lambda cfg: {"workspace:old"})
    monkeypatch.setattr(
        ch,
        "cmux_herdr_status_keys_checked",
        lambda cfg, ref: ["herdr.w1", "herdr.w1.p1", "herdr.unrelated"],
    )
    calls = []
    monkeypatch.setattr(ch, "cmux_cli", lambda cfg, args: calls.append(args) or True)
    ch.reconcile_v1_legacy_sidebar({}, state, {})
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
        ch.find_v1_legacy_notification(
            state, ["remote-source"], "w", "s", [notification]
        )
        == "legacy"
    )
    entry["identity"] = ["tom@other", "work"]
    assert (
        ch.find_v1_legacy_notification(
            state, ["remote-source"], "w", "s", [notification]
        )
        is None
    )
    entry["identity"] = ["tom@host", "work"]
    entry["panes"] = {"other": {"agent": "codex", "status": "blocked"}}
    assert (
        ch.find_v1_legacy_notification(
            state, ["remote-source"], "w", "s", [notification]
        )
        is None
    )


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


def test_local_discovery_does_not_guess_legacy_session_from_socket(monkeypatch):
    monkeypatch.setattr(
        ch, "run", lambda *args, **kwargs: pytest.fail("discovery must not invoke CLI")
    )
    inventory = {
        "clients": [
            {
                "pid": 42,
                "instance": "i",
                "workspace": "w",
                "surface": "s",
                "sources": [
                    {
                        "endpoint": "local",
                        "boot_id": "a" * 32,
                        "connection_generation": 1,
                        "api_socket_path": "/tmp/work.sock",
                    }
                ],
            }
        ]
    }
    target = next(iter(ch.v1_discovered_targets(inventory).values()))
    assert "legacy_target" not in target


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
    monkeypatch.setattr(ch, "load_remote_state", lambda: {"remotes": {}})
    notification = {
        "id": "legacy",
        "workspace_id": "w",
        "surface_id": "s",
        "subtitle": "herdr",
        "title": "codex: waiting for input",
        "is_read": False,
    }
    assert (
        ch.find_v1_legacy_notification(state, ["source"], "w", "s", [notification])
        is None
    )


def test_v1_legacy_workspace_scan_keeps_duplicate_titles(monkeypatch):
    monkeypatch.setattr(
        ch,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0, stdout="* workspace:1 same [selected]\n  workspace:2 same\n"
        ),
    )
    assert ch.cmux_workspace_refs({}) == {"workspace:1", "workspace:2"}


def test_v1_client_does_not_disable_legacy_when_cmux_unreachable(monkeypatch, tmp_path):
    monkeypatch.setenv("HERDR_PLUGIN_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("HERDR_PLUGIN_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("CMUX_WORKSPACE_ID", "workspace-id")
    monkeypatch.setenv("CMUX_SURFACE_ID", "surface-id")
    monkeypatch.setattr(sys, "argv", ["cmux_herdr.py", "client"])
    monkeypatch.setattr(ch, "v1_cmux_focus", lambda *args: (False, None))
    monkeypatch.setattr(
        ch, "run_v1_client", lambda *args: pytest.fail("client should not run")
    )
    assert ch.main() == 1
    assert not (tmp_path / "v1-enabled").exists()


def test_v1_does_not_activate_until_herdr_source_is_resolved(monkeypatch, tmp_path):
    monkeypatch.setenv("HERDR_PLUGIN_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("HERDR_PLUGIN_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("CMUX_WORKSPACE_ID", "workspace-id")
    monkeypatch.setenv("CMUX_SURFACE_ID", "surface-id")
    monkeypatch.setattr(sys, "argv", ["cmux_herdr.py", "client"])
    monkeypatch.setattr(ch, "v1_cmux_focus", lambda *args: (False, "com.cmuxterm.app"))
    monkeypatch.setattr(ch.signal, "signal", lambda *args: None)
    monkeypatch.setattr(ch, "run_v1_client", lambda *args: None)
    assert ch.main() == 1
    assert not (tmp_path / "v1-enabled").exists()


def test_v1_remote_target_waits_until_socket_is_available(monkeypatch):
    target = {"remote": "tom@host", "session": "work"}
    calls = []
    monkeypatch.setattr(
        ch, "resolve_remote_socket", lambda cfg: calls.append(cfg) or "/remote.sock"
    )

    def inaccessible(host, path):
        raise ch.SocketError("not running")

    monkeypatch.setattr(ch, "v1_remote_generation", inaccessible)
    assert ch.resolve_v1_source_target(target) is None
    monkeypatch.setattr(ch, "v1_remote_generation", lambda host, path: "generation")
    assert ch.resolve_v1_source_target(target) == target
    assert calls[0] == {"ssh_target": "tom@host", "session": "work"}


def test_v1_remote_source_preserves_configured_herdr_binary(monkeypatch):
    config = {
        "remotes": [
            {
                "ssh_target": "tom@host",
                "session": "work",
                "cmux_title": "work",
                "herdr_bin": "/opt/bin/herdr",
            }
        ]
    }
    target = ch.v1_target_with_remote_config(
        config, {"remote": "tom@host", "session": "work"}
    )
    seen = []
    monkeypatch.setattr(
        ch, "resolve_remote_socket", lambda cfg: seen.append(cfg) or "/remote.sock"
    )
    monkeypatch.setattr(ch, "v1_remote_generation", lambda *args: "generation")
    assert ch.resolve_v1_source_target(target) == target
    assert seen == [
        {"ssh_target": "tom@host", "session": "work", "herdr_bin": "/opt/bin/herdr"}
    ]


def test_v1_remote_source_rejects_conflicting_configured_binaries():
    target = {"remote": "tom@host", "session": "work"}
    config = {
        "remotes": [
            {
                "name": name,
                "ssh_target": "tom@host",
                "session": "work",
                "cmux_title": name,
                "herdr_bin": binary,
            }
            for name, binary in (("a", "/one/herdr"), ("b", "/two/herdr"))
        ]
    }
    assert ch.v1_target_with_remote_config(config, target) is None


@pytest.mark.parametrize(
    "extra",
    [
        {"ssh_target": "other", "session": "other"},
        {
            "ssh_target": "tom@host",
            "session": "work",
            "cmux_title": "other",
            "herdr_bin": "/another/herdr",
        },
    ],
)
def test_v1_remote_source_rejects_invalid_remote_config(extra):
    target = {"remote": "tom@host", "session": "work"}
    config = {
        "remotes": [
            {
                "ssh_target": "tom@host",
                "session": "work",
                "cmux_title": "work",
                "herdr_bin": "/opt/bin/herdr",
            },
            extra,
        ]
    }
    assert ch.v1_target_with_remote_config(config, target) is None


def test_v1_remote_probe_forwards_configured_binary(monkeypatch):
    calls = []
    monkeypatch.setattr(
        ch, "ensure_forward", lambda name, cfg, *args: calls.append(cfg) or "/local"
    )
    monkeypatch.setattr(ch, "drop_forward", lambda *args: None)
    monkeypatch.setattr(ch, "socket_rpc", lambda *args: {"snapshot": {}})
    assert ch.v1_probe_source(
        {"remote": "tom@host", "session": "work", "herdr_bin": "/opt/bin/herdr"}
    )
    assert calls == [
        {"ssh_target": "tom@host", "session": "work", "herdr_bin": "/opt/bin/herdr"}
    ]


def test_forward_records_child_before_delivering_sigterm(monkeypatch, tmp_path):
    sock = tmp_path / "forward.sock"
    procs = {}
    child = SimpleNamespace(poll=lambda: None)
    handlers = []
    monkeypatch.setattr(ch, "remote_sock_path", lambda name: sock)
    monkeypatch.setattr(ch, "resolve_remote_socket", lambda cfg: "/remote.sock")

    def spawn(*args, **kwargs):
        sock.touch()
        handlers[-1](ch.signal.SIGTERM, None)
        return child

    def original(signum, frame):
        assert procs["test"] is child
        raise SystemExit(0)

    monkeypatch.setattr(ch.subprocess, "Popen", spawn)
    monkeypatch.setattr(ch.signal, "getsignal", lambda signum: original)
    monkeypatch.setattr(
        ch.signal, "signal", lambda signum, handler: handlers.append(handler)
    )
    with pytest.raises(SystemExit):
        ch.ensure_forward("test", {"ssh_target": "host"}, {}, procs)
    assert handlers[-1] is original


def test_v1_custom_socket_target_waits_when_socket_is_missing(monkeypatch):
    monkeypatch.setattr(
        ch,
        "v1_local_generation",
        lambda path: (_ for _ in ()).throw(FileNotFoundError()),
    )
    assert (
        ch.resolve_v1_source_target(
            {"remote": None, "session": "", "socket_path": "/missing.sock"}
        )
        is None
    )


def test_v1_source_probe_rejects_non_socket_and_invalid_snapshot(monkeypatch, tmp_path):
    path = tmp_path / "not-socket"
    path.touch()
    assert not ch.v1_probe_source({"remote": None, "socket_path": str(path)})
    monkeypatch.setattr(
        ch, "socket_rpc", lambda *args: {"snapshot": {"focused_pane_id": "p"}}
    )
    assert ch.v1_probe_source({"remote": None, "socket_path": str(path)})
    monkeypatch.setattr(ch, "socket_rpc", lambda *args: {})
    assert not ch.v1_probe_source({"remote": None, "socket_path": str(path)})


def test_v1_source_probe_requires_discovered_server_incarnation(monkeypatch, tmp_path):
    path = tmp_path / "source.sock"
    expected = "a" * 32
    target = {"remote": None, "socket_path": str(path), "server_boot_id": expected}
    monkeypatch.setattr(
        ch, "socket_rpc", lambda *args: {"snapshot": {"server_boot_id": "b" * 32}}
    )
    assert not ch.v1_probe_source(target)
    monkeypatch.setattr(
        ch, "socket_rpc", lambda *args: {"snapshot": {"server_boot_id": expected}}
    )
    assert ch.v1_probe_source(target)


def test_v1_activation_rolls_back_new_marker_if_retirement_never_succeeds(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(
        ch,
        "resolve_v1_source_target",
        lambda target: {**target, "socket_path": "/socket"},
    )
    monkeypatch.setattr(ch, "v1_probe_source", lambda target: True)
    stop = ch.threading.Event()

    def retirement(plan=None):
        stop.set()
        return False

    monkeypatch.setattr(ch, "retire_v1_legacy_daemons", retirement)
    ch.run_v1_client_with_lease(
        {}, {"remote": None, "session": "work"}, "w", "s", stop, "c"
    )
    assert not (tmp_path / "v1-enabled").exists()


def test_v1_activation_retries_retirement_before_registering(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    attempts = []
    monkeypatch.setattr(
        ch,
        "retire_v1_legacy_daemons",
        lambda plan=None: attempts.append("retire") or len(attempts) == 2,
    )

    class Stop:
        def is_set(self):
            return False

        def wait(self, seconds):
            assert seconds == 1

    assert ch.activate_v1_client("c", "source", "w", "s", None, Stop())
    assert attempts == ["retire", "retire"]
    assert ch.load_state()["v1"]["clients"]["c"]["source"] == "source"
    assert (tmp_path / "v1-enabled").exists()


def test_v1_remote_probe_closes_forward_on_failure(monkeypatch):
    calls = []
    monkeypatch.setattr(ch, "ensure_forward", lambda *args: "/forwarded.sock")
    monkeypatch.setattr(
        ch, "socket_rpc", lambda *args: (_ for _ in ()).throw(ch.SocketError("offline"))
    )
    monkeypatch.setattr(
        ch, "drop_forward", lambda name, entry, procs: calls.append(name)
    )
    assert not ch.v1_probe_source({"remote": "tom@host", "session": "work"})
    assert len(calls) == 1 and calls[0].startswith("v1-probe-")


def test_v1_activation_rolls_back_if_interrupted_while_creating_marker(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    lock_class = ch.state_lock

    class InterruptOnce(lock_class):
        interrupted = False

        def __exit__(self, *args):
            result = super().__exit__(*args)
            if not InterruptOnce.interrupted:
                InterruptOnce.interrupted = True
                raise SystemExit(0)
            return result

    monkeypatch.setattr(ch, "state_lock", InterruptOnce)
    with pytest.raises(SystemExit):
        ch.activate_v1_client("c", "source", "w", "s", None, ch.threading.Event())
    assert not (tmp_path / "v1-enabled").exists()


def test_v1_activation_restores_stopped_legacy_daemons_on_interruption(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    plan = [("remote", None, 123), ("watch", "work", 456)]

    def interrupted_retirement(journal):
        for item in plan:
            ch.record_v1_legacy_restore(journal, item)
        raise SystemExit(0)

    monkeypatch.setattr(ch, "retire_v1_legacy_daemons", interrupted_retirement)
    restored = []
    monkeypatch.setattr(
        ch, "restart_legacy_daemons", lambda items: restored.extend(items)
    )
    with pytest.raises(SystemExit):
        ch.activate_v1_client("c", "source", "w", "s", None, ch.threading.Event())
    assert not (tmp_path / "v1-enabled").exists()
    assert restored == plan


def test_v1_activation_releases_lock_when_retirement_is_busy(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    pid_path = tmp_path / "remote.pid"
    monkeypatch.setattr(
        ch,
        "retire_v1_legacy_daemons",
        lambda plan: ch.stop_v1_legacy_daemon(pid_path, "remote", plan),
    )
    monkeypatch.setattr(ch, "restart_legacy_daemons", lambda plan: None)
    result = []

    def activate():
        result.append(
            ch._activate_v1_client_once(
                "c", "source", "w", "s", None, ch.threading.Event()
            )
        )

    with open(tmp_path / "remote.lock", "w") as daemon_lock:
        ch.fcntl.flock(daemon_lock, ch.fcntl.LOCK_EX)
        worker = ch.threading.Thread(target=activate, daemon=True)
        worker.start()
        worker.join(timeout=2)
        assert not worker.is_alive()
        assert result == [False]
        with ch.v1_activation_lock():
            assert not (tmp_path / "v1-enabled").exists()


def test_v1_activation_keeps_pending_marker_when_event_replay_fails(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(ch, "retire_v1_legacy_daemons", lambda plan: False)
    monkeypatch.setattr(
        ch, "_v1_rollback_legacy_events_and_marker_locked", lambda: False
    )
    restored = []
    monkeypatch.setattr(
        ch, "restart_legacy_daemons", lambda plan: restored.append(plan)
    )
    assert not ch._activate_v1_client_once(
        "c", "source", "w", "s", None, ch.threading.Event()
    )
    assert (tmp_path / "v1-enabled").read_text() == "pending"
    assert restored == []


def test_v1_activation_replays_event_when_retirement_fails(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(ch, "load_config_strict", lambda: {})
    monkeypatch.setattr(ch, "restart_legacy_daemons", lambda plan: None)
    replayed = []
    monkeypatch.setattr(
        ch, "handle_event", lambda cfg, state, event, **kwargs: replayed.append(event)
    )
    event = status_event("p", "w", "done")

    def retire(plan):
        ch.save_state(
            {"v1_pending_legacy_events": [{"session": "work", "event": event}]}
        )
        return False

    monkeypatch.setattr(ch, "retire_v1_legacy_daemons", retire)
    assert not ch._activate_v1_client_once(
        "c", "source", "w", "s", None, ch.threading.Event()
    )
    assert replayed == [event]
    assert not (tmp_path / "v1-enabled").exists()
    assert not ch.load_state().get("v1_pending_legacy_events")


def test_v1_interrupted_cutovers_preserve_pending_restore(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    old = ("watch", "work", 456)
    attempts = 0

    def interrupted_retirement(journal):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            ch.record_v1_legacy_restore(journal, old)
        raise SystemExit(0)

    monkeypatch.setattr(ch, "retire_v1_legacy_daemons", interrupted_retirement)
    restored = []
    monkeypatch.setattr(
        ch, "restart_legacy_daemons", lambda plan: restored.append(plan)
    )
    for _ in range(2):
        with pytest.raises(SystemExit):
            ch.activate_v1_client("c", "source", "w", "s", None, ch.threading.Event())
    assert restored == [[old], [old]]


def test_v1_recovers_a_cutover_abandoned_after_marker_creation(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")
    marker = tmp_path / "v1-enabled"
    marker.write_text("pending")
    old = ("watch", "work", 456)
    ch.record_v1_legacy_restore([], old)
    restored = []
    monkeypatch.setattr(
        ch, "restart_legacy_daemons", lambda plan: restored.append(plan)
    )
    stop = ch.threading.Event()
    stop.set()
    assert not ch.activate_v1_client("c", "source", "w", "s", None, stop)
    assert not marker.exists()
    assert restored == [[old]]


def test_v1_restore_acknowledges_only_the_matching_obligation(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    journal = []
    old = ("watch", "work", 456)
    newer = ("watch", "work", 789)
    ch.record_v1_legacy_restore(journal, old)
    ch.record_v1_legacy_restore(journal, newer)
    ch.ack_v1_legacy_restore("watch", "work", "456")
    assert ch.pending_v1_legacy_restores() == [newer]


def test_v1_legacy_restore_does_not_inherit_another_cmux_surface(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "config_dir", tmp_path)
    monkeypatch.setenv("CMUX_WORKSPACE_ID", "wrong-workspace")
    monkeypatch.setenv("CMUX_SURFACE_ID", "wrong-surface")
    monkeypatch.setenv("HERDR_SOCKET_PATH", "/wrong.sock")
    calls = []
    monkeypatch.setattr(
        ch.subprocess, "Popen", lambda *args, **kwargs: calls.append(kwargs)
    )
    ch.restart_legacy_daemons([("watch", "work", 456)])
    assert calls[0]["env"]["HERDR_SESSION"] == "work"
    assert all(
        key not in calls[0]["env"]
        for key in (*ch.CMUX_CALLER_IDENTITY_VARS, "HERDR_SOCKET_PATH")
    )


def test_v1_legacy_restore_does_not_spawn_duplicate_waiters(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "config_dir", tmp_path)
    original_popen = subprocess.Popen
    children = []

    def spawn_waiter(*args, **kwargs):
        child = original_popen(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            pass_fds=kwargs["pass_fds"],
        )
        children.append(child)
        return child

    monkeypatch.setattr(ch.subprocess, "Popen", spawn_waiter)
    try:
        ch.restart_legacy_daemons([("watch", "work", 456)])
        ch.restart_legacy_daemons([("watch", "work", 456)])
        assert len(children) == 1
        children[0].terminate()
        children[0].wait(timeout=5)
        ch.restart_legacy_daemons([("watch", "work", 456)])
        assert len(children) == 2
    finally:
        for child in children:
            if child.poll() is None:
                child.terminate()
                child.wait(timeout=5)


def test_legacy_retirement_removes_reused_daemon_pidfile(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    path = ch.watch_pid_path("work")
    path.write_text(json.dumps({"pid": 456, "audit_token": [1] * 8}))
    monkeypatch.setattr(ch, "live_daemon_pid", lambda *args: 456)
    monkeypatch.setattr(ch, "v1_verified_daemon_audit_token", lambda *args: None)
    monkeypatch.setattr(ch.os, "kill", lambda *args: None)
    monkeypatch.setattr(ch, "v1_daemon_incarnation_exited", lambda *args: True)
    assert ch.stop_v1_legacy_daemon(path, "watch", [])
    assert not path.exists()


def test_v1_retirement_journals_watchers_discovered_after_cutover(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    path = ch.watch_pid_path("new")
    path.touch()
    monkeypatch.setattr(ch, "known_sessions", lambda: {"new"})
    monkeypatch.setattr(ch, "live_daemon_pid", lambda path, mode: 456)
    monkeypatch.setattr(ch, "v1_verified_daemon_audit_token", lambda *args: [456] * 8)
    observed = []
    journal = []

    def kill(pid, sig):
        if sig == 0:
            raise ProcessLookupError()
        observed.append(list(journal))

    monkeypatch.setattr(ch.os, "kill", kill)
    monkeypatch.setattr(
        ch, "v1_signal_audit_token", lambda token, sig: observed.append(list(journal))
    )
    assert ch.stop_v1_legacy_daemon(path, "watch", journal)
    assert observed == [[("watch", "new", 456, [456] * 8)]]


def test_v1_retirement_keeps_watcher_when_its_session_is_unknown(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    path = ch.watch_pid_path("unknown")
    path.touch()
    monkeypatch.setattr(ch, "known_sessions", lambda: None)
    monkeypatch.setattr(ch, "live_daemon_pid", lambda path, mode: 456)
    monkeypatch.setattr(ch, "v1_verified_daemon_audit_token", lambda *args: [456] * 8)
    monkeypatch.setattr(
        ch, "v1_signal_audit_token", lambda *args: pytest.fail("signaled watcher")
    )
    monkeypatch.setattr(ch.os, "kill", lambda *args: pytest.fail("signaled watcher"))
    journal = []
    assert not ch.stop_v1_legacy_daemon(path, "watch", journal)
    assert journal == []


def test_v1_retirement_can_restore_a_watcher_for_a_stopped_session(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    path = ch.watch_pid_path("stopped")
    path.touch()
    monkeypatch.setattr(
        ch,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout=json.dumps({"sessions": [{"name": "stopped", "running": False}]}),
        ),
    )
    monkeypatch.setattr(ch, "live_daemon_pid", lambda path, mode: 456)
    monkeypatch.setattr(ch, "v1_verified_daemon_audit_token", lambda *args: [456] * 8)
    monkeypatch.setattr(ch, "v1_signal_audit_token", lambda *args: None)
    monkeypatch.setattr(
        ch.os,
        "kill",
        lambda pid, sig: (
            (_ for _ in ()).throw(ProcessLookupError()) if sig == 0 else None
        ),
    )
    journal = []
    assert ch.stop_v1_legacy_daemon(path, "watch", journal)
    assert journal == [("watch", "stopped", 456, [456] * 8)]


def test_v1_retirement_recovers_deleted_session_from_watcher_environment(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    path = ch.watch_pid_path("deleted.session")
    path.touch()
    monkeypatch.setattr(ch, "known_sessions", lambda: {"default"})
    monkeypatch.setattr(ch, "live_daemon_pid", lambda path, mode: 456)
    monkeypatch.setattr(ch, "v1_verified_daemon_audit_token", lambda *args: [456] * 8)
    monkeypatch.setattr(ch, "v1_signal_audit_token", lambda *args: None)
    monkeypatch.setattr(
        ch.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout="python cmux_herdr.py watch HERDR_SESSION=deleted.session",
        ),
    )
    monkeypatch.setattr(
        ch.os,
        "kill",
        lambda pid, sig: (
            (_ for _ in ()).throw(ProcessLookupError()) if sig == 0 else None
        ),
    )
    journal = []
    assert ch.stop_v1_legacy_daemon(path, "watch", journal)
    assert journal == [("watch", "deleted.session", 456, [456] * 8)]


def test_v1_restore_waits_for_signaled_daemon_before_restarting(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setenv("CMUX_HERDR_RESTORE_PID", "456")
    pids = iter([456, 456, None])
    monkeypatch.setattr(ch, "live_daemon_pid", lambda path, mode: next(pids))
    waits = []
    monkeypatch.setattr(ch.time, "sleep", lambda seconds: waits.append(seconds))
    ch.wait_for_v1_legacy_restore(tmp_path / "watch-new.pid", "watch")
    assert len(waits) == 2


def test_v1_restore_does_not_wait_for_reused_pid(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setenv("CMUX_HERDR_RESTORE_PID", "456")
    monkeypatch.setenv("CMUX_HERDR_RESTORE_TOKEN", json.dumps([1] * 8))
    monkeypatch.setattr(ch, "live_daemon_pid", lambda *args: 456)
    monkeypatch.setattr(ch, "v1_daemon_incarnation_exited", lambda pid, token: True)
    monkeypatch.setattr(
        ch.time, "sleep", lambda *args: pytest.fail("waited for reused PID")
    )
    ch.wait_for_v1_legacy_restore(tmp_path / "watch-new.pid", "watch")


def test_v1_restore_gives_up_when_another_cutover_starts(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setenv("CMUX_HERDR_RESTORE_PID", "456")
    monkeypatch.setattr(ch, "live_daemon_pid", lambda path, mode: 456)
    monkeypatch.setattr(
        ch.time, "sleep", lambda seconds: (tmp_path / "v1-enabled").touch()
    )
    with pytest.raises(SystemExit):
        ch.wait_for_v1_legacy_restore(tmp_path / "watch-new.pid", "watch")


def test_v1_restore_does_not_hold_startup_lock_while_waiting(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setenv("HERDR_SESSION", "work")
    monkeypatch.setenv("CMUX_HERDR_RESTORE_PID", "456")
    monkeypatch.setattr(ch, "live_daemon_pid", lambda path, mode: 456)

    def check_lock(seconds):
        path = tmp_path / "watch-work.lock"
        with path.open("w") as lock:
            ch.fcntl.flock(lock, ch.fcntl.LOCK_EX | ch.fcntl.LOCK_NB)
        raise SystemExit(0)

    monkeypatch.setattr(ch.time, "sleep", check_lock)
    with pytest.raises(SystemExit):
        ch.watch_daemon({})
