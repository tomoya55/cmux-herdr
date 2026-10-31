import json
import time
from types import SimpleNamespace

import pytest

from cmux_herdr_core import cmux, discovery, focus, process_identity


def test_v1_foreground_observation_requires_one_observable_herdr(monkeypatch, tmp_path):
    processes = {
        10: {"pid": 10, "name": "zsh", "path": "/bin/zsh"},
        20: {"pid": 20, "name": "herdr", "path": "/bin/herdr"},
    }
    available = {20}
    monkeypatch.setattr(
        process_identity,
        "v1_observer_socket_exists",
        lambda path: int(path.stem) in available,
    )
    monkeypatch.setattr(
        process_identity,
        "v1_client_observation",
        lambda path, pid, **kwargs: {"pid": pid, "instance": "i"},
    )
    assert discovery.v1_foreground_observation(processes, [tmp_path]) == {
        "pid": 20,
        "instance": "i",
    }
    processes[30] = {"pid": 30, "name": "herdr", "path": "/bin/herdr"}
    assert discovery.v1_foreground_observation(processes, [tmp_path]) is None
    available.add(30)
    assert discovery.v1_foreground_observation(processes, [tmp_path]) is None
    del processes[30]
    monkeypatch.setattr(
        process_identity, "v1_client_observation", lambda path, pid, **kwargs: None
    )
    assert discovery.v1_foreground_observation(processes, [tmp_path]) is None


def test_v1_foreground_observation_accepts_remote_parent_and_client(
    monkeypatch, tmp_path
):
    processes = {
        20: {"pid": 20, "ppid": 10, "name": "herdr", "path": "/bin/herdr"},
        21: {"pid": 21, "ppid": 20, "name": "herdr", "path": "/bin/herdr"},
    }
    monkeypatch.setattr(
        process_identity, "v1_observer_socket_exists", lambda path: path.stem == "21"
    )
    monkeypatch.setattr(
        process_identity,
        "v1_client_observation",
        lambda path, pid, **kwargs: {"pid": pid},
    )
    assert discovery.v1_foreground_observation(processes, [tmp_path]) == {"pid": 21}
    processes[20]["ppid"] = 21
    processes[21]["ppid"] = 10
    assert discovery.v1_foreground_observation(processes, [tmp_path]) is None


@pytest.mark.parametrize("server_tty", [None, 42])
def test_v1_foreground_observation_ignores_verified_server(
    monkeypatch, tmp_path, server_tty
):
    processes = {
        20: {"pid": 20, "ppid": 1, "name": "herdr", "tty_device": server_tty},
        21: {"pid": 21, "ppid": 10, "name": "herdr", "tty_device": 42},
    }
    monkeypatch.setattr(
        process_identity, "v1_observer_socket_exists", lambda path: path.stem == "21"
    )
    monkeypatch.setattr(
        process_identity,
        "v1_client_observation",
        lambda path, pid, **kwargs: {"pid": pid},
    )
    monkeypatch.setattr(
        process_identity,
        "v1_is_verified_local_server",
        lambda pid, paths, **kwargs: pid == 20,
    )
    assert discovery.v1_foreground_observation(
        processes,
        [tmp_path],
        upstream_sockets=lambda: ["/source"],
        deadline=time.monotonic() + 5,
    ) == {"pid": 21}


def test_v1_foreground_observation_does_not_query_after_deadline(monkeypatch, tmp_path):
    processes = {21: {"pid": 21, "ppid": 10, "name": "herdr", "tty_device": 42}}
    monkeypatch.setattr(
        process_identity, "v1_observer_socket_exists", lambda path: True
    )
    calls = []
    monkeypatch.setattr(
        process_identity,
        "v1_client_observation",
        lambda *args, **kwargs: calls.append(args),
    )
    assert (
        discovery.v1_foreground_observation(
            processes, [tmp_path], deadline=time.monotonic() - 1
        )
        is None
    )
    assert calls == []


def test_v1_surface_observation_requests_both_cmux_ids(monkeypatch):
    calls = []
    monkeypatch.setattr(
        cmux,
        "run",
        lambda command, **kwargs: (
            calls.append(command) or SimpleNamespace(returncode=0, stdout="{}")
        ),
    )
    monkeypatch.setattr(
        process_identity, "v1_foreground_surface_processes", lambda *args: {}
    )
    monkeypatch.setattr(
        discovery, "v1_foreground_observation", lambda *args, **kwargs: None
    )
    focus.v1_surface_observation({}, "workspace-uuid", "surface-uuid")
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
        cmux,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=json.dumps(tree)),
    )
    monkeypatch.setattr(
        process_identity, "v1_foreground_surface_processes", lambda *args: {}
    )
    observation = {"pid": 42, "instance": "i", "observation": {}}
    monkeypatch.setattr(
        discovery, "v1_foreground_observation", lambda *args, **kwargs: observation
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
    monkeypatch.setattr(
        discovery, "v1_discover_clients_in_tree", lambda *args, **kwargs: scan
    )
    assert focus.v1_surface_observation({}, "w", "s") == {
        **observation,
        "native_ownership": record,
    }
    scan["complete"] = False
    assert focus.v1_surface_observation({}, "w", "s") is None
    scan["complete"] = True
    client["instance"] = "different"
    assert focus.v1_surface_observation({}, "w", "s") is None


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
        cmux,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=json.dumps(tree)),
    )
    monkeypatch.setattr(
        process_identity, "v1_foreground_surface_processes", lambda *args: {}
    )
    monkeypatch.setattr(
        discovery, "v1_foreground_observation", lambda *args, **kwargs: observation
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

    monkeypatch.setattr(discovery, "v1_discover_clients_in_tree", scan)
    assert focus.v1_surface_observation({}, "w", "s") == observation


def test_v1_foreground_cmux_requires_the_socket_server_process(monkeypatch):
    identity = {
        "focused": True,
        "bundle": "com.cmuxterm.app",
        "socket_path": "/cmux.sock",
    }
    monkeypatch.setattr(focus, "v1_cmux_identity", lambda *args: identity)
    monkeypatch.setattr(
        process_identity, "v1_frontmost_app", lambda: ("com.cmuxterm.app", 12)
    )
    monkeypatch.setattr(
        focus, "v1_cmux_terminal_focus", lambda *args: True, raising=False
    )
    monkeypatch.setattr(focus, "v1_unix_peer_pid", lambda path: 13)
    assert not focus.v1_cmux_is_foreground({}, "w", "s")
    monkeypatch.setattr(focus, "v1_unix_peer_pid", lambda path: 12)
    assert focus.v1_cmux_is_foreground({}, "w", "s")
    identity["focused"] = False
    assert not focus.v1_cmux_is_foreground({}, "w", "s")


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
        cmux,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0, stdout=json.dumps({"terminals": [terminal]})
        ),
    )
    assert focus.v1_cmux_terminal_focus({}, "w", "s")
    for key in (
        "window_key",
        "surface_view_first_responder",
        "workspace_selected",
        "window_visible",
        "mapped",
    ):
        terminal[key] = False
        assert not focus.v1_cmux_terminal_focus({}, "w", "s")
        terminal[key] = True
