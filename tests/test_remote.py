import json
import signal
import socket
import subprocess
import sys
from types import SimpleNamespace

import pytest
from helpers import remote_agent, short_sock_path

from cmux_herdr_core import cmux, core, process_identity, remote_bridge, watcher

# --- Remote daemon -----------------------------------------------------------


REMOTE_CFG = {"ssh_target": "maguro", "session": "tom", "cmux_title": "maguro:hd:tom"}


def test_remote_blocked_notifies_and_sets_pill(cmux_calls, remote_title_map):
    rstate = {"remotes": {}}
    changed = remote_bridge.apply_remote_snapshot(
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
    remote_bridge.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "done")], {}
    )
    notify = [c for c in cmux_calls if c[0] == "notify"]
    assert len(notify) == 1
    assert "claude: finished" in notify[0]


def test_remote_working_pill_without_notification(cmux_calls, remote_title_map):
    rstate = {"remotes": {}}
    remote_bridge.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "working")], {}
    )
    assert [c for c in cmux_calls if c[0] == "notify"] == []
    pill = [c for c in cmux_calls if c[0] == "set-status"]
    assert pill[0][2] == "1 working"
    assert "#0a84ff" in pill[0]


def test_remote_per_pane_pills(cmux_calls, remote_title_map):
    rstate = {"remotes": {}}
    remote_bridge.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "blocked")], {}
    )
    pills = [c for c in cmux_calls if c[0] == "set-status"]
    pane_pill = next(c for c in pills if c[1] == "herdr.remote.tom.w1_p1")
    assert pane_pill[2] == "claude: waiting"

    cmux_calls.clear()
    remote_bridge.apply_remote_snapshot(
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
    rstate = {
        "remotes": {
            "tom": {
                "panes": {"w1:p1": {"status": "blocked"}},
                "ref": "workspace:9",
                "cmux_title": "maguro:hd:tom",
            }
        }
    }
    remote_bridge.retire_remote({}, rstate, "tom", {}, {})

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
    remote_bridge.apply_remote_snapshot({}, rstate, "tom", REMOTE_CFG, agents, {})
    cmux_calls.clear()

    changed = remote_bridge.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, agents, {}
    )
    assert changed is False
    assert cmux_calls == []


def test_remote_retries_deferred_notification_when_other_notice_is_read(
    monkeypatch, cmux_calls, remote_title_map
):
    visible = [{"is_read": False}]
    monkeypatch.setattr(cmux, "cmux_notifications_checked", lambda cfg: visible)
    rstate = {"remotes": {}}
    agents = [remote_agent("w1:p1", "blocked")]
    remote_bridge.apply_remote_snapshot({}, rstate, "tom", REMOTE_CFG, agents, {})
    assert not [call for call in cmux_calls if call[0] == "notify"]
    visible.clear()
    remote_bridge.apply_remote_snapshot({}, rstate, "tom", REMOTE_CFG, agents, {})
    assert len([call for call in cmux_calls if call[0] == "notify"]) == 1


def test_remote_discards_deferred_notification_after_wait_ends(
    monkeypatch, cmux_calls, remote_title_map
):
    visible = [{"is_read": False}]
    monkeypatch.setattr(cmux, "cmux_notifications_checked", lambda cfg: visible)
    rstate = {"remotes": {}}
    remote_bridge.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "blocked")], {}
    )
    remote_bridge.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "working")], {}
    )
    visible.clear()
    remote_bridge.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "working")], {}
    )
    assert not [call for call in cmux_calls if call[0] == "notify"]


def test_remote_does_not_retry_waiting_notification_for_missing_agent(
    monkeypatch, cmux_calls, remote_title_map
):
    visible = [{"is_read": False}]
    monkeypatch.setattr(cmux, "cmux_notifications_checked", lambda cfg: visible)
    rstate = {"remotes": {}}
    remote_bridge.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "blocked")], {}
    )
    visible.clear()
    remote_bridge.apply_remote_snapshot({}, rstate, "tom", REMOTE_CFG, [], {})
    assert not [call for call in cmux_calls if call[0] == "notify"]


def test_remote_unknown_status_preserves_waiting_attention(
    cmux_calls, remote_title_map
):
    rstate = {"remotes": {}}
    remote_bridge.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "blocked")], {}
    )
    remote_bridge.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "unknown")], {}
    )
    assert rstate["remotes"]["tom"]["panes"]["w1:p1"]["status"] == "blocked"


def test_remote_missing_agent_does_not_prove_input_wait_ended(
    cmux_calls, remote_title_map
):
    rstate = {"remotes": {}}
    remote_bridge.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "blocked")], {}
    )
    remote_bridge.apply_remote_snapshot({}, rstate, "tom", REMOTE_CFG, [], {})
    assert rstate["remotes"]["tom"]["panes"]["w1:p1"]["status"] == "blocked"


def test_remote_leaving_attention_preserves_unowned_notifications(
    cmux_calls, remote_title_map
):
    rstate = {"remotes": {}}
    remote_bridge.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "blocked")], {}
    )
    cmux_calls.clear()
    remote_bridge.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "working")], {}
    )

    read = [c for c in cmux_calls if c[0] == "mark-notification-read"]
    assert read == []
    pill = [c for c in cmux_calls if c[0] == "set-status"]
    assert pill[0][2] == "1 working"


def test_remote_all_idle_clears_pill(cmux_calls, remote_title_map):
    rstate = {"remotes": {}}
    remote_bridge.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "working")], {}
    )
    cmux_calls.clear()
    remote_bridge.apply_remote_snapshot(
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
    remote_bridge.apply_remote_snapshot({}, rstate, "tom", REMOTE_CFG, agents, {})

    pill = [c for c in cmux_calls if c[0] == "set-status"]
    assert pill[0][2] == "1 waiting · 1 working"


def test_remote_without_title_skips_cmux(cmux_calls):
    rstate = {"remotes": {}}
    rcfg = {"ssh_target": "maguro", "session": "tom"}
    changed = remote_bridge.apply_remote_snapshot(
        {}, rstate, "tom", rcfg, [remote_agent("w1:p1", "blocked")], {}
    )
    assert changed is True
    assert cmux_calls == []
    assert rstate["remotes"]["tom"]["panes"]["w1:p1"]["status"] == "blocked"


def test_remote_title_change_reroutes_pill(cmux_calls, remote_title_map):
    rstate = {"remotes": {}}
    remote_bridge.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, [remote_agent("w1:p1", "working")], {}
    )
    cmux_calls.clear()
    rcfg = dict(REMOTE_CFG, cmux_title="maguro:hd:other")
    changed = remote_bridge.apply_remote_snapshot(
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
        cmux,
        "cmux_workspaces_by_title",
        lambda cfg: {"maguro:hd:tom": "workspace:9", "maguro:hd:other": "workspace:10"},
    )
    rstate = {"remotes": {}}
    agents = [remote_agent("w1:p1", "working")]
    remote_bridge.apply_remote_snapshot({}, rstate, "tom", REMOTE_CFG, agents, {})
    cmux_calls.clear()

    rcfg = dict(REMOTE_CFG, cmux_title="maguro:hd:other")
    remote_bridge.apply_remote_snapshot({}, rstate, "tom", rcfg, agents, {})

    pill = [c for c in cmux_calls if c[0] == "set-status"]
    assert pill[0][1] == "herdr.remote.tom"
    assert "workspace:10" in pill[0]


def test_remote_identity_change_drops_socket_path(cmux_calls, remote_title_map):
    rstate = {"remotes": {}}
    agents = [remote_agent("w1:p1", "working")]
    remote_bridge.apply_remote_snapshot({}, rstate, "tom", REMOTE_CFG, agents, {})
    rstate["remotes"]["tom"]["socket_path"] = "/old/path.sock"
    cmux_calls.clear()

    rcfg = dict(REMOTE_CFG, ssh_target="other-host")
    changed = remote_bridge.apply_remote_snapshot({}, rstate, "tom", rcfg, agents, {})

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
    remote_bridge.remote_mark_read_ref({}, rstate, "a", "workspace:9")
    assert cmux_calls == []

    remote_bridge.remote_mark_read_ref({}, rstate, "b", "workspace:9")
    assert cmux_calls == []


def test_remote_ref_clears_pill_when_title_moves(monkeypatch, cmux_calls, tmp_path):
    monkeypatch.setattr(
        cmux, "cmux_workspaces_by_title", lambda cfg: {"maguro:hd:tom": "workspace:10"}
    )
    entry = {"cmux_title": "maguro:hd:tom", "ref": "workspace:9", "ref_at": 0}

    ref = remote_bridge.remote_ref({}, {"remotes": {}}, "tom", entry)

    assert ref == "workspace:10"
    assert ["clear-status", "herdr.remote.tom", "--workspace", "workspace:9"] in (
        cmux_calls
    )
    assert not any(c[0] == "mark-notification-read" for c in cmux_calls)


def test_poll_interval_validation():
    assert remote_bridge.poll_interval({}) == core.REMOTE_DEFAULT_POLL_SECONDS
    assert remote_bridge.poll_interval({"poll_seconds": 5}) == 5.0
    assert (
        remote_bridge.poll_interval({"poll_seconds": "abc"})
        == core.REMOTE_DEFAULT_POLL_SECONDS
    )
    assert (
        remote_bridge.poll_interval({"poll_seconds": -1})
        == core.REMOTE_DEFAULT_POLL_SECONDS
    )
    assert remote_bridge.poll_interval({"poll_seconds": 0.1}) == 0.5


def test_remote_republishes_pill_once_workspace_appears(monkeypatch, cmux_calls):
    titles = {}
    monkeypatch.setattr(cmux, "cmux_workspaces_by_title", lambda cfg: dict(titles))
    rstate = {"remotes": {}}
    agents = [remote_agent("w1:p1", "working")]

    remote_bridge.apply_remote_snapshot({}, rstate, "tom", REMOTE_CFG, agents, {})
    assert cmux_calls == []  # title not resolvable yet

    titles["maguro:hd:tom"] = "workspace:9"
    changed = remote_bridge.apply_remote_snapshot(
        {}, rstate, "tom", REMOTE_CFG, agents, {}
    )
    assert changed is False  # snapshot unchanged, but the pill is retried
    pills = [c for c in cmux_calls if c[0] == "set-status"]
    assert pills[0][1] == "herdr.remote.tom"
    assert "workspace:9" in pills[0]


def test_remote_name_defaults_to_target_and_session():
    # The full target with an unambiguous separator, so distinct
    # users/hosts/sessions cannot collide.
    assert (
        remote_bridge.remote_name(
            {"ssh_target": "tom@maguro.example.ts.net", "session": "tom"}
        )
        == "tom@maguro.example.ts.net:tom"
    )
    assert (
        remote_bridge.remote_name({"ssh_target": "maguro", "session": "hd"})
        == "maguro:hd"
    )
    assert (
        remote_bridge.remote_name(
            {"ssh_target": "maguro", "session": "hd", "name": "x"}
        )
        == "x"
    )
    # colon-containing targets (IPv6, ssh:// URIs) fall back to a hash
    name = remote_bridge.remote_name(
        {"ssh_target": "ssh://tom@host:2222", "session": "hd"}
    )
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
        assert remote_bridge.remote_configs({"remotes": [bad]}) is None, bad
    assert remote_bridge.remote_configs({"remotes": "bad"}) is None
    assert remote_bridge.remote_configs({"remotes": ""}) is None
    assert remote_bridge.remote_configs({}) == []
    assert remote_bridge.remote_configs({"remotes": []}) == []
    valid = {"ssh_target": "a", "session": "s", "cmux_title": "t"}
    assert remote_bridge.remote_configs({"remotes": [valid]}) == [valid]
    assert "expected a table" in capsys.readouterr().err


def test_remote_configs_rejects_duplicate_names(capsys):
    cfg = {
        "remotes": [
            {"ssh_target": "a", "session": "s", "name": "dup", "cmux_title": "t"},
            {"ssh_target": "b", "session": "s", "name": "dup", "cmux_title": "t"},
        ]
    }
    assert remote_bridge.remote_configs(cfg) is None
    assert "duplicate remote name" in capsys.readouterr().err


def test_retire_remote_clears_pill_and_state(monkeypatch, cmux_calls, tmp_path):
    rstate = {
        "remotes": {
            "tom": {
                "panes": {"w1:p1": {"status": "blocked"}},
                "ref": "workspace:9",
                "cmux_title": "maguro:hd:tom",
            }
        }
    }
    remote_bridge.retire_remote({}, rstate, "tom", {}, {})

    assert rstate["remotes"] == {}
    assert ["clear-status", "herdr.remote.tom", "--workspace", "workspace:9"] in (
        cmux_calls
    )
    assert not any(c[0] == "mark-notification-read" for c in cmux_calls)
    saved = json.loads((tmp_path / "remote-state.json").read_text())
    assert saved["remotes"] == {}


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
    result = remote_bridge.socket_rpc(sock_path, "ping", {})
    t.join(5)

    assert result == {"type": "pong"}
    assert received == [{"id": "1", "method": "ping", "params": {}}]


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
    with pytest.raises(remote_bridge.SocketError, match="nope"):
        remote_bridge.socket_rpc(sock_path, "ping", {})
    t.join(5)


def test_v1_upstream_source_fence_rejects_replaced_socket(monkeypatch):
    monkeypatch.setattr(core, "v1_local_generation", lambda path: "1:3")
    monkeypatch.setattr(
        process_identity, "v1_native_process_info", lambda pid: {"start_token": 5}
    )
    token = core.HERDR_SOURCE_FENCE.set(
        {"path": "/source.sock", "generation": "1:2", "pid": 123, "start_token": 5}
    )
    try:
        with pytest.raises(remote_bridge.SocketError, match="source socket changed"):
            remote_bridge.v1_check_herdr_source_fence("/source.sock")
    finally:
        core.HERDR_SOURCE_FENCE.reset(token)


def test_v1_upstream_source_fence_rejects_reused_server_pid(monkeypatch):
    monkeypatch.setattr(core, "v1_local_generation", lambda path: "1:2")
    monkeypatch.setattr(
        process_identity, "v1_native_process_info", lambda pid: {"start_token": 6}
    )
    token = core.HERDR_SOURCE_FENCE.set(
        {"path": "/source.sock", "generation": "1:2", "pid": 123, "start_token": 5}
    )
    try:
        with pytest.raises(remote_bridge.SocketError, match="source process changed"):
            remote_bridge.v1_check_herdr_source_fence("/source.sock")
    finally:
        core.HERDR_SOURCE_FENCE.reset(token)


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

    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(socket, "socket", lambda *args: Socket())
    with pytest.raises(remote_bridge.SocketError, match="incarnation changed"):
        remote_bridge.socket_rpc("/socket", "pane.get", {}, expected_peer=123)
    assert events == ["closed"]


def test_forward_records_child_before_delivering_sigterm(monkeypatch, tmp_path):
    sock = tmp_path / "forward.sock"
    procs = {}
    child = SimpleNamespace(poll=lambda: None)
    handlers = []
    monkeypatch.setattr(remote_bridge, "remote_sock_path", lambda name: sock)
    monkeypatch.setattr(
        remote_bridge, "resolve_remote_socket", lambda cfg: "/remote.sock"
    )

    def spawn(*args, **kwargs):
        sock.touch()
        handlers[-1](signal.SIGTERM, None)
        return child

    def original(signum, frame):
        assert procs["test"] is child
        raise SystemExit(0)

    monkeypatch.setattr(subprocess, "Popen", spawn)
    monkeypatch.setattr(signal, "getsignal", lambda signum: original)
    monkeypatch.setattr(
        signal, "signal", lambda signum, handler: handlers.append(handler)
    )
    with pytest.raises(SystemExit):
        remote_bridge.ensure_forward("test", {"ssh_target": "host"}, {}, procs)
    assert handlers[-1] is original


def test_v1_remote_probe_closes_forward_on_failure(monkeypatch):
    calls = []
    monkeypatch.setattr(
        remote_bridge, "ensure_forward", lambda *args: "/forwarded.sock"
    )
    monkeypatch.setattr(
        remote_bridge,
        "socket_rpc",
        lambda *args: (_ for _ in ()).throw(remote_bridge.SocketError("offline")),
    )
    monkeypatch.setattr(
        remote_bridge, "drop_forward", lambda name, entry, procs: calls.append(name)
    )
    assert not watcher.v1_probe_source({"remote": "tom@host", "session": "work"})
    assert len(calls) == 1 and calls[0].startswith("v1-probe-")
