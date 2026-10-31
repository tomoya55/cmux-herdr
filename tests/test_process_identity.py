import json
import os
import socket
import subprocess
import sys
import time

import pytest
from helpers import short_sock_path

import cmux_herdr as ch


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
