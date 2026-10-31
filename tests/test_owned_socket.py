import json
import socket
import time
import uuid
from contextlib import contextmanager, suppress
from pathlib import Path
from types import SimpleNamespace

import pytest
from helpers import short_sock_path

import cmux_herdr as ch


def test_v1_owned_supervisor_reconnects_after_socket_loss(tmp_path, monkeypatch):
    import threading
    from contextlib import contextmanager

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
