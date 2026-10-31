import json
import os

import pytest

from cmux_herdr_core import focus, remote_bridge


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

    monkeypatch.setattr(remote_bridge, "socket_rpc", rpc)
    assert focus.v1_force_read_action()
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

    monkeypatch.setattr(remote_bridge, "socket_rpc", rpc)
    assert not focus.v1_force_read_action()
    assert calls == ["session.snapshot", "pane.get"]


@pytest.mark.usefixtures("force_action_identity")
def test_v1_force_read_upstream_requires_original_server_peer(monkeypatch):
    monkeypatch.setenv(
        "HERDR_PLUGIN_CONTEXT_JSON", json.dumps({"focused_pane_id": "p"})
    )
    monkeypatch.setenv("HERDR_SOCKET_PATH", "/socket")
    monkeypatch.setattr(os, "getppid", lambda: 1234)
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

    monkeypatch.setattr(remote_bridge, "socket_rpc", rpc)
    assert focus.v1_force_read_action()
    assert calls == [
        ("session.snapshot", {"expected_peer": 1234}),
        ("pane.get", {"expected_peer": 1234}),
        ("pane.report_metadata", {"expected_peer": 1234}),
    ]


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

    monkeypatch.setattr(remote_bridge, "socket_rpc", rpc)
    assert focus.v1_force_read_action()
    assert calls[-1][1]["tokens"] == {"cmux_herdr_ack": "episode"}


@pytest.mark.usefixtures("force_action_identity")
def test_v1_force_read_action_rejects_unknown_state_for_present_agent(monkeypatch):
    monkeypatch.setenv(
        "HERDR_PLUGIN_CONTEXT_JSON", json.dumps({"focused_pane_id": "p"})
    )
    monkeypatch.setenv("HERDR_SOCKET_PATH", "/socket")
    monkeypatch.setattr(
        remote_bridge,
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
    assert not focus.v1_force_read_action()


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

    monkeypatch.setattr(remote_bridge, "socket_rpc", rpc)
    assert focus.v1_force_read_action()
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

    monkeypatch.setattr(remote_bridge, "socket_rpc", no_boot)
    assert not focus.v1_force_read_action()
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
        raise remote_bridge.SocketError("server boot changed")

    monkeypatch.setattr(remote_bridge, "socket_rpc", restarted)
    assert not focus.v1_force_read_action()
