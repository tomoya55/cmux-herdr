import json
import os
import selectors
import signal
import subprocess
import sys
import time
from contextlib import suppress
from types import SimpleNamespace

import pytest

from cmux_herdr_core import cmux, core, discovery, focus, process_identity, publication


def test_v1_discovery_uses_verified_upstream_local_client(monkeypatch, tmp_path):
    monkeypatch.setattr(process_identity, "v1_tty_device", lambda tty: 42)
    monkeypatch.setattr(
        process_identity, "v1_observer_socket_exists", lambda path: False
    )
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
        process_identity,
        "v1_upstream_local_client_inventory",
        lambda *args, **kwargs: inventory,
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
    result = discovery.v1_discover_clients_in_tree(
        tree, [tmp_path], upstream_sockets=lambda: ["/tmp/herdr.sock"]
    )
    assert result["complete"]
    assert result["clients"][0]["sources"] == inventory["sources"]
    tree["windows"][0]["workspaces"][0]["panes"][0]["surfaces"][0]["processes"][0][
        "children"
    ].append({"pid": 101, "name": "ssh", "tty_device": 0, "children": []})
    result = discovery.v1_discover_clients_in_tree(
        tree, [tmp_path], upstream_sockets=lambda: ["/tmp/herdr.sock"]
    )
    assert not result["complete"]


@pytest.mark.parametrize("server_tty", [None, 42])
def test_discovery_excludes_only_verified_herdr_server(
    monkeypatch, tmp_path, server_tty
):
    monkeypatch.setattr(process_identity, "v1_tty_device", lambda tty: 42)
    monkeypatch.setattr(
        process_identity, "v1_observer_socket_exists", lambda path: False
    )
    monkeypatch.setattr(
        process_identity,
        "v1_native_process_info",
        lambda pid: {"pid": pid, "start_token": 5},
    )
    monkeypatch.setattr(core, "v1_local_generation", lambda path: "1:2")
    monkeypatch.setattr(focus, "v1_unix_peer_pid", lambda path, **kwargs: 200)
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
    result = discovery.v1_discover_clients_in_tree(
        tree, [tmp_path], upstream_sockets=lambda: ["/tmp/herdr.sock"]
    )
    assert result["complete"]
    assert result["clients"] == []
    monkeypatch.setattr(focus, "v1_unix_peer_pid", lambda path, **kwargs: 201)
    assert not discovery.v1_discover_clients_in_tree(
        tree, [tmp_path], upstream_sockets=lambda: ["/tmp/herdr.sock"]
    )["complete"]


def test_v1_discovery_isolates_observer_failure_between_surfaces(monkeypatch, tmp_path):
    monkeypatch.setattr(process_identity, "v1_tty_device", lambda tty: 100)
    monkeypatch.setattr(
        process_identity,
        "v1_native_surface_ownership",
        lambda *args: (set(), set(), {}, True),
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
        process_identity,
        "v1_observer_socket_exists",
        lambda path: path.name == "20.sock",
    )
    monkeypatch.setattr(
        process_identity,
        "v1_client_source_inventory",
        lambda path, pid, **kwargs: {
            "pid": pid,
            "instance": "good",
            "sources_complete": True,
            "sources": [],
        },
    )
    result = discovery.v1_discover_clients_in_tree(tree, [tmp_path])
    assert result["surface_complete"] == {("w", "good"): True, ("w", "bad"): False}
    assert result["complete"] is False
    tree["windows"][0]["workspaces"][0]["panes"][0]["surfaces"][1]["processes"][0][
        "pid"
    ] = 20
    assert discovery.v1_discover_clients_in_tree(tree, [tmp_path]) is None


def test_v1_discovers_unfocused_clients_without_guessing_on_missing_socket(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(process_identity, "v1_tty_device", lambda tty: 100)
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
        process_identity,
        "v1_observer_socket_exists",
        lambda path: path.name == "20.sock",
    )
    monkeypatch.setattr(
        process_identity,
        "v1_client_source_inventory",
        lambda path, pid, **kwargs: {
            "pid": pid,
            "instance": "i",
            "sources_complete": True,
            "sources": [],
        },
    )
    result = discovery.v1_discover_clients_in_tree(tree, [tmp_path])
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
    monkeypatch.setattr(process_identity, "v1_tty_device", lambda tty: 100)
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
        process_identity,
        "v1_observer_socket_exists",
        lambda path: (
            path.parent.name == "transport-observers"
            and path.name in {"40.sock", "44.sock"}
        ),
    )
    monkeypatch.setattr(
        process_identity,
        "v1_native_transport_envelope",
        lambda *args, **kwargs: {"owner_pid": 40},
    )
    monkeypatch.setattr(
        process_identity, "v1_native_transport_claims", lambda value: [claim]
    )
    monkeypatch.setattr(
        process_identity, "v1_native_transport_topology", lambda *args: [record]
    )
    result = discovery.v1_discover_clients_in_tree(
        tree, [tmp_path / "herdr" / "client-observers"]
    )
    assert result["complete"] is True
    assert result["clients"][0]["pid"] == 42
    assert result["clients"][0]["native_ownership"] == record
    surface["processes"].append(
        {"pid": 44, "name": "herdr", "tty_device": 101, "children": []}
    )
    assert (
        discovery.v1_discover_clients_in_tree(
            tree, [tmp_path / "herdr" / "client-observers"]
        )["complete"]
        is True
    )
    surface["processes"].append(
        {"pid": 45, "name": "ssh", "tty_device": 100, "children": []}
    )
    result = discovery.v1_discover_clients_in_tree(
        tree, [tmp_path / "herdr" / "client-observers"]
    )
    assert result["complete"] is False


def test_v1_discovery_rejects_duplicate_pid_across_surfaces(monkeypatch, tmp_path):
    monkeypatch.setattr(process_identity, "v1_tty_device", lambda tty: 100)
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
    monkeypatch.setattr(
        process_identity, "v1_observer_socket_exists", lambda path: False
    )
    assert discovery.v1_discover_clients_in_tree(tree, [tmp_path]) is None


@pytest.mark.parametrize("bad", [{"windows": {}}, {"windows": ""}])
def test_v1_discovery_rejects_malformed_window_list(bad, tmp_path):
    assert discovery.v1_discover_clients_in_tree(bad, [tmp_path]) is None


@pytest.mark.parametrize("device", [None, "100", True])
def test_v1_discovery_marks_missing_process_tty_incomplete(
    monkeypatch, tmp_path, device
):
    monkeypatch.setattr(process_identity, "v1_tty_device", lambda tty: 100)
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
    assert discovery.v1_discover_clients_in_tree(tree, [tmp_path])["complete"] is False
    surface["processes"] = {}
    assert discovery.v1_discover_clients_in_tree(tree, [tmp_path]) is None


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
    monkeypatch.setattr(
        discovery, "v1_bounded_command_output", lambda *args, **kwargs: "{}"
    )
    monkeypatch.setattr(
        discovery,
        "v1_discover_clients_in_tree",
        lambda *args, **kwargs: next(snapshots),
    )
    assert discovery.v1_discover_clients({}) == {"clients": [], "complete": False}


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
    monkeypatch.setattr(
        discovery, "v1_bounded_command_output", lambda *args, **kwargs: "{}"
    )
    monkeypatch.setattr(
        discovery,
        "v1_discover_clients_in_tree",
        lambda *args, **kwargs: next(snapshots),
    )
    result = discovery.v1_discover_clients({})
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
    monkeypatch.setattr(
        discovery, "v1_bounded_command_output", lambda *args, **kwargs: "{}"
    )
    monkeypatch.setattr(
        discovery,
        "v1_discover_clients_in_tree",
        lambda *args, **kwargs: next(snapshots),
    )
    result = discovery.v1_discover_clients({})
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
    targets = discovery.v1_discovered_targets(inventory)
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
    targets = discovery.v1_discovered_targets(inventory)
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
    native_targets = discovery.v1_discovered_targets(inventory)
    assert all(
        spec["native_ownership"] == ownership for spec in native_targets.values()
    )
    state = {"v1": {"clients": {}}}
    publication.reconcile_v1_supervised_bindings(
        state, native_targets, native_targets, complete=True, supervisor_pid=1
    )
    assert all(
        record["native_ownership"] == ownership
        for record in state["v1"]["clients"].values()
    )
    del inventory["clients"][0]["native_ownership"]
    same = discovery.v1_discovered_targets(inventory)
    assert targets == same
    inventory["clients"][0]["instance"] = "client-b"
    assert set(targets).isdisjoint(discovery.v1_discovered_targets(inventory))


def test_v1_discovery_respects_shared_deadline(monkeypatch, tmp_path):
    monkeypatch.setattr(process_identity, "v1_tty_device", lambda tty: 100)
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
    monkeypatch.setattr(
        process_identity, "v1_observer_socket_exists", lambda path: True
    )
    monkeypatch.setattr(
        process_identity,
        "v1_client_source_inventory",
        lambda *args, **kwargs: pytest.fail("deadline ignored"),
    )
    assert (
        discovery.v1_discover_clients_in_tree(tree, [tmp_path], deadline=0)["complete"]
        is False
    )


def test_v1_discovery_marks_unidentified_tty_process_incomplete(monkeypatch, tmp_path):
    monkeypatch.setattr(process_identity, "v1_tty_device", lambda tty: 100)
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
    assert discovery.v1_discover_clients_in_tree(tree, [tmp_path])["complete"] is False


def test_v1_discovery_does_not_claim_ssh_shell_is_empty(monkeypatch, tmp_path):
    monkeypatch.setattr(process_identity, "v1_tty_device", lambda tty: 100)
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
    assert discovery.v1_discover_clients_in_tree(tree, [tmp_path])["complete"] is False


def test_v1_discovery_rejects_unknown_surface_type(monkeypatch, tmp_path):
    monkeypatch.setattr(process_identity, "v1_tty_device", lambda tty: 100)
    surface = {"id": "s", "type": "termnal", "tty": "/dev/ttys001", "processes": []}
    tree = {
        "windows": [{"workspaces": [{"id": "w", "panes": [{"surfaces": [surface]}]}]}]
    }
    assert discovery.v1_discover_clients_in_tree(tree, [tmp_path]) is None


def test_v1_bounded_command_rejects_excessive_output():
    assert (
        discovery.v1_bounded_command_output(
            [sys.executable, "-c", "print('x' * 10000)"], max_bytes=100
        )
        is None
    )
    assert (
        discovery.v1_bounded_command_output(
            [sys.executable, "-c", "print('ok')"], max_bytes=100
        )
        == "ok\n"
    )
    assert (
        discovery.v1_bounded_command_output(
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
        discovery.v1_bounded_command_output(
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
            os.kill(pid, signal.SIGKILL)


def test_v1_bounded_command_reaps_descendants_after_success(tmp_path):
    child_file = tmp_path / "child.pid"
    code = (
        "import os,sys,time; pid=os.fork(); "
        "open(sys.argv[1], 'w').write(str(os.getpid())) if pid == 0 else None; "
        "os.close(1) if pid == 0 else None; "
        "time.sleep(30) if pid == 0 else time.sleep(.2)"
    )
    assert (
        discovery.v1_bounded_command_output(
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
            os.kill(pid, signal.SIGKILL)


def test_v1_discovery_stops_scanning_empty_surfaces_after_deadline(
    monkeypatch, tmp_path
):
    calls = []
    monkeypatch.setattr(
        process_identity, "v1_tty_device", lambda tty: calls.append(tty) or 100
    )
    surface = {"id": "s", "type": "terminal", "tty": "/dev/tty", "processes": []}
    tree = {
        "windows": [{"workspaces": [{"id": "w", "panes": [{"surfaces": [surface]}]}]}]
    }
    assert (
        discovery.v1_discover_clients_in_tree(tree, [tmp_path], deadline=0)["complete"]
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
    monkeypatch.setattr(subprocess, "Popen", lambda *args, **kwargs: proc)
    monkeypatch.setattr(selectors, "DefaultSelector", Selector)
    monkeypatch.setattr(os, "read", lambda *args: b"")
    monkeypatch.setattr(
        discovery, "v1_wait_child_state", lambda pid: (os.CLD_EXITED, 1)
    )
    monkeypatch.setattr(os, "killpg", lambda *args: calls.append("kill"))
    assert discovery.v1_bounded_command_output(["fake"]) is None
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
        handlers[signal.SIGTERM](signal.SIGTERM, None)
        return proc

    monkeypatch.setattr(signal, "getsignal", lambda sig: original)
    monkeypatch.setattr(
        signal, "signal", lambda sig, handler: handlers.__setitem__(sig, handler)
    )
    monkeypatch.setattr(subprocess, "Popen", spawn)
    monkeypatch.setattr(os, "killpg", lambda *args: calls.append("kill"))
    with pytest.raises(SystemExit):
        discovery.v1_bounded_command_output(["fake"])
    assert calls == ["kill", "reap"]
    assert handlers[signal.SIGTERM] is original


def test_v1_bounded_command_cleans_up_before_replaying_default_signal(monkeypatch):
    handlers = {}
    calls = []
    proc = SimpleNamespace(
        pid=12345, stdout=None, wait=lambda **kwargs: calls.append("reap")
    )

    def spawn(*args, **kwargs):
        handlers[signal.SIGTERM](signal.SIGTERM, None)
        return proc

    monkeypatch.setattr(
        signal,
        "getsignal",
        lambda sig: signal.SIG_DFL if sig == signal.SIGTERM else signal.SIG_IGN,
    )
    monkeypatch.setattr(
        signal, "signal", lambda sig, handler: handlers.__setitem__(sig, handler)
    )
    monkeypatch.setattr(subprocess, "Popen", spawn)
    monkeypatch.setattr(os, "killpg", lambda *args: calls.append("kill-group"))
    monkeypatch.setattr(os, "kill", lambda *args: calls.append("replay"))
    assert discovery.v1_bounded_command_output(["fake"]) is None
    assert calls == ["kill-group", "reap", "replay"]
    assert handlers[signal.SIGTERM] == signal.SIG_DFL


def test_v1_bounded_command_preserves_default_signal_among_multiple(monkeypatch):
    handlers = {}
    calls = []
    proc = SimpleNamespace(
        pid=12345, stdout=None, wait=lambda **kwargs: calls.append("reap")
    )

    def on_int(signum, frame):
        raise KeyboardInterrupt

    def spawn(*args, **kwargs):
        handlers[signal.SIGTERM](signal.SIGTERM, None)
        handlers[signal.SIGINT](signal.SIGINT, None)
        return proc

    monkeypatch.setattr(
        signal,
        "getsignal",
        lambda sig: signal.SIG_DFL if sig == signal.SIGTERM else on_int,
    )
    monkeypatch.setattr(
        signal, "signal", lambda sig, handler: handlers.__setitem__(sig, handler)
    )
    monkeypatch.setattr(subprocess, "Popen", spawn)
    monkeypatch.setattr(os, "killpg", lambda *args: calls.append("kill-group"))
    monkeypatch.setattr(os, "kill", lambda pid, sig: calls.append(("replay", sig)))
    assert discovery.v1_bounded_command_output(["fake"]) is None
    assert calls == ["kill-group", "reap", ("replay", signal.SIGTERM)]
    assert handlers[signal.SIGINT] is on_int


def test_v1_bounded_command_restores_all_handlers_if_restoration_is_interrupted(
    monkeypatch,
):
    handlers = {}
    interrupted = []

    def original(signum, frame):
        raise SystemExit(7)

    def install(signum, handler):
        handlers[signum] = handler
        if signum == signal.SIGTERM and handler is original and not interrupted:
            interrupted.append(True)
            raise SystemExit(7)

    monkeypatch.setattr(signal, "getsignal", lambda sig: original)
    monkeypatch.setattr(signal, "signal", install)
    monkeypatch.setattr(
        subprocess,
        "Popen",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("stop")),
    )
    with pytest.raises(SystemExit) as error:
        discovery.v1_bounded_command_output(["fake"])
    assert error.value.code == 7
    assert handlers[signal.SIGTERM] is original
    assert handlers[signal.SIGINT] is original


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
    monkeypatch.setattr(time, "monotonic", lambda: now[0])

    def command(*args, **kwargs):
        now[0] += 9.95
        return json.dumps(tree)

    def tty(*args):
        now[0] += 0.2
        return 100

    monkeypatch.setattr(discovery, "v1_bounded_command_output", command)
    monkeypatch.setattr(process_identity, "v1_tty_device", tty)
    result = discovery.v1_discover_clients({})
    assert result is None or result["complete"] is False


def test_v1_discovery_rejects_excessive_tree_nesting(monkeypatch):
    nested = "[" * 1100 + "0" + "]" * 1100
    monkeypatch.setattr(
        discovery, "v1_bounded_command_output", lambda *args, **kwargs: nested
    )
    assert discovery.v1_discover_clients({}) is None


def test_local_discovery_does_not_guess_legacy_session_from_socket(monkeypatch):
    monkeypatch.setattr(
        cmux,
        "run",
        lambda *args, **kwargs: pytest.fail("discovery must not invoke CLI"),
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
    target = next(iter(discovery.v1_discovered_targets(inventory).values()))
    assert "legacy_target" not in target


def test_v1_process_named_matches_name_or_executable_basename():
    assert discovery._v1_process_named("herdr", None, "herdr")
    assert discovery._v1_process_named(None, "/usr/local/bin/herdr", "herdr")
    assert not discovery._v1_process_named("zsh", "/bin/zsh", "herdr")
    assert not discovery._v1_process_named("herdr-helper", None, "herdr")


def test_v1_is_verified_upstream_server_requires_known_socket_paths(monkeypatch):
    calls = []
    monkeypatch.setattr(
        process_identity,
        "v1_is_verified_local_server",
        lambda pid, paths, *, deadline: calls.append((pid, paths)) or True,
    )
    assert not discovery._v1_is_verified_upstream_server(1, None, 0)
    assert not discovery._v1_is_verified_upstream_server(1, lambda: None, 0)
    assert discovery._v1_is_verified_upstream_server(1, lambda: ["/s"], 0)
    assert calls == [(1, ["/s"])]
