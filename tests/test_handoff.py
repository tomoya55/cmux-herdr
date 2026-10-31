import fcntl
import json
import os
import threading
from types import SimpleNamespace

import pytest
from helpers import status_event

from cmux_herdr_core import (
    core,
    discovery,
    legacy_daemons,
    legacy_events,
    legacy_handoff,
    remote_bridge,
    supervisor,
    watcher,
)


def test_v1_supervisor_handoff_rolls_back_when_coverage_changes(monkeypatch, tmp_path):
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
    monkeypatch.setattr(discovery, "v1_discover_clients", lambda cfg: next(snapshots))
    monkeypatch.setattr(discovery, "v1_discovered_targets", lambda inventory: desired)
    monkeypatch.setattr(legacy_handoff, "v1_legacy_source_inventory", lambda cfg: set())
    monkeypatch.setattr(
        supervisor, "v1_handoff_has_coverage", lambda complete, *args: complete
    )
    monkeypatch.setattr(watcher, "v1_probe_source", lambda target: True)
    monkeypatch.setattr(
        supervisor, "v1_supervisor_bindings_match", lambda desired: True
    )
    calls = []
    monkeypatch.setattr(
        legacy_daemons,
        "retire_v1_legacy_daemons",
        lambda plan: calls.append("retire") or True,
    )
    monkeypatch.setattr(
        legacy_daemons, "pending_v1_legacy_restores", lambda: [("watch", "work", 123)]
    )
    monkeypatch.setattr(
        watcher, "restart_legacy_daemons", lambda plan: calls.append("restore")
    )

    class Sources:
        def reconcile(self, desired, *, complete):
            return desired

    assert not legacy_handoff.v1_try_supervisor_handoff(
        {}, Sources(), threading.Event()
    )
    assert "retire" not in calls
    assert "restore" in calls
    assert not (tmp_path / "v1-enabled").exists()


def test_v1_supervisor_handoff_restores_writers_if_final_validation_fails(
    monkeypatch, tmp_path
):
    desired = {"c": {"target": {"socket_path": "/tmp/a", "server_boot_id": "a" * 32}}}
    snapshots = iter([True, True, False])
    monkeypatch.setattr(
        discovery, "v1_discover_clients", lambda cfg: {"complete": next(snapshots)}
    )
    monkeypatch.setattr(discovery, "v1_discovered_targets", lambda inventory: desired)
    monkeypatch.setattr(
        legacy_handoff, "v1_legacy_source_inventory", lambda cfg: {"legacy"}
    )
    monkeypatch.setattr(
        supervisor, "v1_handoff_has_coverage", lambda complete, *args: complete
    )
    monkeypatch.setattr(watcher, "v1_probe_source", lambda target: True)
    monkeypatch.setattr(
        supervisor, "v1_supervisor_bindings_match", lambda desired: True
    )
    calls = []
    monkeypatch.setattr(
        legacy_daemons,
        "retire_v1_legacy_daemons",
        lambda plan: calls.append("retire") or True,
    )
    monkeypatch.setattr(
        legacy_daemons, "pending_v1_legacy_restores", lambda: [("watch", "work", 123)]
    )
    monkeypatch.setattr(
        watcher, "restart_legacy_daemons", lambda plan: calls.append("restore")
    )

    class Sources:
        def reconcile(self, desired, *, complete):
            return desired

    assert not legacy_handoff.v1_try_supervisor_handoff(
        {}, Sources(), threading.Event()
    )
    assert calls == ["retire", "restore"]
    assert not (tmp_path / "v1-enabled").exists()


def test_v1_supervisor_handoff_commits_only_after_retirement(monkeypatch, tmp_path):
    desired = {"c": {"target": {"socket_path": "/tmp/a", "server_boot_id": "a" * 32}}}
    monkeypatch.setattr(
        discovery, "v1_discover_clients", lambda cfg: {"complete": True}
    )
    monkeypatch.setattr(discovery, "v1_discovered_targets", lambda inventory: desired)
    monkeypatch.setattr(
        legacy_handoff, "v1_legacy_source_inventory", lambda cfg: {"legacy"}
    )
    monkeypatch.setattr(supervisor, "v1_handoff_has_coverage", lambda *args: True)
    monkeypatch.setattr(
        supervisor, "v1_supervisor_bindings_match", lambda desired: True
    )
    monkeypatch.setattr(watcher, "v1_probe_source", lambda target: True)
    monkeypatch.setattr(legacy_daemons, "pending_v1_legacy_restores", lambda: [])
    calls = []

    def retire(plan):
        assert (tmp_path / "v1-enabled").read_text() == "pending"
        calls.append("retire")
        return True

    monkeypatch.setattr(legacy_daemons, "retire_v1_legacy_daemons", retire)
    monkeypatch.setattr(
        legacy_daemons,
        "update_v1_legacy_restores",
        lambda update: calls.append("clear journal"),
    )

    class Sources:
        def reconcile(self, desired, *, complete):
            return desired

    def final_check():
        calls.append("final check")
        return True

    assert legacy_handoff.v1_try_supervisor_handoff(
        {}, Sources(), threading.Event(), final_check=final_check
    )
    assert calls == ["retire", "final check", "clear journal"]
    assert (tmp_path / "v1-enabled").read_text() == "enabled"


def test_v1_supervisor_handoff_rolls_back_when_final_backend_check_fails(
    monkeypatch, tmp_path
):
    desired = {"c": {"target": {"socket_path": "/tmp/a", "server_boot_id": "a" * 32}}}
    monkeypatch.setattr(
        discovery, "v1_discover_clients", lambda cfg: {"complete": True}
    )
    monkeypatch.setattr(discovery, "v1_discovered_targets", lambda inventory: desired)
    monkeypatch.setattr(legacy_handoff, "v1_legacy_source_inventory", lambda cfg: set())
    monkeypatch.setattr(supervisor, "v1_handoff_has_coverage", lambda *args: True)
    monkeypatch.setattr(
        supervisor, "v1_supervisor_bindings_match", lambda desired: True
    )
    monkeypatch.setattr(watcher, "v1_probe_source", lambda target: True)
    monkeypatch.setattr(legacy_daemons, "pending_v1_legacy_restores", lambda: [])
    calls = []
    monkeypatch.setattr(
        legacy_daemons,
        "retire_v1_legacy_daemons",
        lambda plan: calls.append("retire") or True,
    )
    monkeypatch.setattr(
        watcher, "restart_legacy_daemons", lambda plan: calls.append("restore")
    )
    sources = SimpleNamespace(reconcile=lambda desired, complete: desired)

    assert not legacy_handoff.v1_try_supervisor_handoff(
        {}, sources, threading.Event(), final_check=lambda: False
    )
    assert calls == ["retire", "restore"]
    assert not (tmp_path / "v1-enabled").exists()


def test_v1_supervisor_handoff_recovers_interrupted_pending_marker(
    monkeypatch, tmp_path
):
    (tmp_path / "v1-enabled").write_text("pending")
    plan = [("watch", "work", 123)]
    monkeypatch.setattr(legacy_daemons, "pending_v1_legacy_restores", lambda: plan)
    calls = []
    monkeypatch.setattr(
        watcher, "restart_legacy_daemons", lambda value: calls.append(value)
    )
    assert not legacy_handoff.v1_try_supervisor_handoff({}, object(), threading.Event())
    assert calls == [plan]
    assert not (tmp_path / "v1-enabled").exists()


def test_v1_handoff_rolls_back_if_hook_arrives_before_commit(monkeypatch, tmp_path):
    monkeypatch.setattr(core, "load_config_strict", lambda: {})
    desired = {"c": {"target": {"socket_path": "/tmp/a", "server_boot_id": "a" * 32}}}
    monkeypatch.setattr(
        discovery, "v1_discover_clients", lambda cfg: {"complete": True}
    )
    monkeypatch.setattr(discovery, "v1_discovered_targets", lambda inventory: desired)
    monkeypatch.setattr(legacy_handoff, "v1_legacy_source_inventory", lambda cfg: set())
    monkeypatch.setattr(supervisor, "v1_handoff_has_coverage", lambda *args: True)
    monkeypatch.setattr(
        supervisor, "v1_supervisor_bindings_match", lambda desired: True
    )
    monkeypatch.setattr(watcher, "v1_probe_source", lambda target: True)
    monkeypatch.setattr(legacy_daemons, "pending_v1_legacy_restores", lambda: [])
    monkeypatch.setattr(watcher, "restart_legacy_daemons", lambda plan: None)
    replayed = []
    monkeypatch.setattr(
        legacy_events,
        "handle_event",
        lambda cfg, state, event, **kwargs: replayed.append(event),
    )
    event = status_event("p", "w", "done")

    def retire(plan):
        core.save_state(
            {"v1_pending_legacy_events": [{"session": "work", "event": event}]}
        )
        return True

    monkeypatch.setattr(legacy_daemons, "retire_v1_legacy_daemons", retire)

    class Sources:
        def reconcile(self, desired, *, complete):
            return desired

    assert not legacy_handoff.v1_try_supervisor_handoff(
        {}, Sources(), threading.Event()
    )
    assert replayed == [event]
    assert not (tmp_path / "v1-enabled").exists()


def test_v1_recovery_retries_journal_without_marker_or_client_discovery(
    monkeypatch, tmp_path
):
    plan = [("watch", "work", 123)]
    monkeypatch.setattr(legacy_daemons, "pending_v1_legacy_restores", lambda: plan)
    calls = []
    monkeypatch.setattr(
        watcher, "restart_legacy_daemons", lambda value: calls.append(value)
    )
    assert not legacy_handoff.v1_recover_legacy_handoff()
    assert calls == [plan]
    assert not (tmp_path / "v1-enabled").exists()


def test_v1_replay_does_not_resurrect_delayed_completion(
    monkeypatch, tmp_path, cfg, cmux_calls
):
    monkeypatch.setattr(core, "load_config_strict", lambda: cfg)
    monkeypatch.setattr(legacy_daemons, "pending_v1_legacy_restores", lambda: [])
    monkeypatch.setattr(watcher, "pane_is_tracked", lambda *args: False)
    monkeypatch.setattr(remote_bridge, "socket_rpc", lambda *args: {"agents": []})
    (tmp_path / "v1-enabled").write_text("pending")
    core.save_state(
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
    assert legacy_handoff.v1_recover_legacy_handoff()
    assert not any(call[0] == "notify" for call in cmux_calls)


def test_v1_replay_retains_uncertain_completion_for_retry(
    monkeypatch, tmp_path, cfg, cmux_calls
):
    monkeypatch.setattr(core, "load_config_strict", lambda: cfg)
    monkeypatch.setattr(legacy_daemons, "pending_v1_legacy_restores", lambda: [])
    monkeypatch.setattr(watcher, "pane_is_tracked", lambda *args: False)

    def unavailable(*args):
        raise remote_bridge.SocketError("offline")

    monkeypatch.setattr(remote_bridge, "socket_rpc", unavailable)
    marker = tmp_path / "v1-enabled"
    marker.write_text("pending")
    core.save_state(
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
    assert not legacy_handoff.v1_recover_legacy_handoff()
    assert marker.exists()
    assert len(core.load_state()["v1_pending_legacy_events"]) == 2
    assert not any(call[0] == "notify" for call in cmux_calls)
    monkeypatch.setattr(
        remote_bridge,
        "socket_rpc",
        lambda *args: {"agents": [{"pane_id": "p", "agent_status": "done"}]},
    )
    assert legacy_handoff.v1_recover_legacy_handoff()
    assert any(call[0] == "notify" for call in cmux_calls)


def test_v1_recovery_keeps_marker_when_journal_state_is_corrupt(monkeypatch, tmp_path):
    (tmp_path / "v1-enabled").write_text("pending")
    (tmp_path / "state.json").write_text("{broken")
    with pytest.raises(ValueError):
        legacy_handoff.v1_recover_legacy_handoff()
    assert (tmp_path / "v1-enabled").read_text() == "pending"
    assert (tmp_path / "state.json").read_text() == "{broken"


def test_v1_replay_retains_watcher_event_for_malformed_agent_state(
    monkeypatch, tmp_path, cfg
):
    monkeypatch.setattr(core, "load_config_strict", lambda: cfg)
    monkeypatch.setattr(legacy_daemons, "pending_v1_legacy_restores", lambda: [])
    monkeypatch.setattr(
        remote_bridge, "socket_rpc", lambda *args: {"agents": [{"pane_id": "p"}]}
    )
    (tmp_path / "v1-enabled").write_text("pending")
    core.save_state(
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
    assert not legacy_handoff.v1_recover_legacy_handoff()
    assert (tmp_path / "v1-enabled").exists()


def test_v1_legacy_inventory_requires_verified_running_watcher_sources(
    monkeypatch, tmp_path
):
    watch = watcher.watch_pid_path("work")
    watch.write_text(json.dumps({"pid": 123, "handoff_protocol": 1}))
    monkeypatch.setattr(
        legacy_daemons,
        "live_daemon_pid",
        lambda path, mode: 123 if path == watch else None,
    )
    monkeypatch.setattr(legacy_events, "known_sessions", lambda **kwargs: {"work"})
    monkeypatch.setattr(
        watcher,
        "v1_local_socket",
        lambda name: "/tmp/work.sock" if name == "work" else None,
    )
    assert legacy_handoff.v1_legacy_source_inventory({}) == {
        core.v1_source_key({"socket_path": "/tmp/work.sock"})
    }
    watch.write_text(json.dumps({"pid": 123}))
    assert legacy_handoff.v1_legacy_source_inventory({}) is None
    watch.write_text(json.dumps({"pid": 123, "handoff_protocol": 1}))
    monkeypatch.setattr(watcher, "v1_local_socket", lambda name: None)
    assert legacy_handoff.v1_legacy_source_inventory({}) is None


def test_v1_legacy_inventory_includes_configured_remote_writers(monkeypatch, tmp_path):
    monkeypatch.setattr(legacy_events, "known_sessions", lambda **kwargs: set())
    monkeypatch.setattr(
        legacy_daemons,
        "live_daemon_pid",
        lambda path, mode: 123 if mode == "remote" else None,
    )
    cfg = {
        "remotes": [{"ssh_target": "tom@host", "session": "work", "cmux_title": "work"}]
    }
    monkeypatch.setattr(core, "load_config_strict", lambda: cfg)
    (tmp_path / "remote.pid").write_text(
        json.dumps({"pid": 123, "inventory_epoch": "fresh"})
    )
    legacy_handoff.write_v1_legacy_remote_inventory(123, "fresh", {("tom@old", "old")})
    assert legacy_handoff.v1_legacy_source_inventory(cfg) == {
        core.v1_source_key({"remote": "tom@host", "session": "work"}),
        core.v1_source_key({"remote": "tom@old", "session": "old"}),
    }
    assert legacy_handoff.v1_legacy_source_inventory({"remotes": "bad"}) is None
    (tmp_path / "remote-active-sources.json").unlink()
    assert legacy_handoff.v1_legacy_source_inventory(cfg) is None
    legacy_handoff.write_v1_legacy_remote_inventory(123, "stale", {("tom@old", "old")})
    assert legacy_handoff.v1_legacy_source_inventory(cfg) is None
    legacy_handoff.write_v1_legacy_remote_inventory(123, "fresh", {("tom@old", "old")})
    monkeypatch.setattr(core, "load_config_strict", lambda: None)
    assert legacy_handoff.v1_legacy_source_inventory(cfg) is None


def test_v1_remote_inventory_write_failure_keeps_previous_config(monkeypatch, tmp_path):
    old = {"remotes": [{"ssh_target": "old", "session": "work", "cmux_title": "w"}]}
    new = {"remotes": [{"ssh_target": "new", "session": "work", "cmux_title": "w"}]}
    monkeypatch.setattr(core, "load_config_strict", lambda: new)

    def fail(*args):
        raise OSError("disk full")

    monkeypatch.setattr(legacy_handoff, "write_v1_legacy_remote_inventory", fail)
    assert (
        legacy_handoff.v1_legacy_remote_next_config(
            old, {"a": ("old", "work", "w")}, "epoch"
        )
        == old
    )


def test_v1_remote_transition_waits_for_handoff_barrier(monkeypatch, tmp_path):
    old = {"remotes": [{"ssh_target": "old", "session": "work", "cmux_title": "w"}]}
    new = {"remotes": [{"ssh_target": "new", "session": "work", "cmux_title": "w"}]}
    monkeypatch.setattr(core, "load_config_strict", lambda: new)
    started = threading.Event()
    done = threading.Event()
    result = []

    def reload():
        started.set()
        result.append(
            legacy_handoff.v1_legacy_remote_next_config(
                old, {"a": ("old", "work", "w")}, "epoch"
            )
        )
        done.set()

    with open(tmp_path / "remote-transition.lock", "w") as barrier:
        fcntl.flock(barrier, fcntl.LOCK_EX)
        worker = threading.Thread(target=reload)
        worker.start()
        assert started.wait(1)
        assert not done.wait(0.1)
    assert done.wait(2)
    worker.join()
    assert result == [new]


def test_dead_pid_cleanup_fails_closed_when_daemon_lock_is_busy(monkeypatch, tmp_path):
    path = tmp_path / "watch-work.pid"
    path.write_text(json.dumps({"pid": 123}))
    monkeypatch.setattr(
        os, "kill", lambda *args: (_ for _ in ()).throw(ProcessLookupError())
    )
    result = []
    with open(tmp_path / "watch-work.lock", "w") as busy:
        fcntl.flock(busy, fcntl.LOCK_EX)
        worker = threading.Thread(
            target=lambda: result.append(
                legacy_handoff.v1_remove_confirmed_dead_pidfile(path)
            )
        )
        worker.start()
        worker.join(timeout=0.5)
        assert not worker.is_alive()
        assert result == [False]
        assert path.exists()


def test_v1_legacy_inventory_includes_interrupted_restore_obligations(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(legacy_events, "known_sessions", lambda **kwargs: set())
    monkeypatch.setattr(legacy_daemons, "live_daemon_pid", lambda *args: None)
    monkeypatch.setattr(watcher, "v1_local_socket", lambda name: f"/tmp/{name}.sock")
    monkeypatch.setattr(
        legacy_daemons, "pending_v1_legacy_restores", lambda: [("watch", "work", 123)]
    )
    assert legacy_handoff.v1_legacy_source_inventory({}) == {
        core.v1_source_key({"socket_path": "/tmp/work.sock"})
    }
    monkeypatch.setattr(
        legacy_daemons, "pending_v1_legacy_restores", lambda: [("remote", None, 456)]
    )
    assert legacy_handoff.v1_legacy_source_inventory({}) is None


def test_v1_legacy_inventory_discards_only_confirmed_dead_pid(monkeypatch, tmp_path):
    monkeypatch.setattr(legacy_events, "known_sessions", lambda **kwargs: set())
    monkeypatch.setattr(legacy_daemons, "live_daemon_pid", lambda *args: None)
    pidfile = tmp_path / "remote.pid"
    pidfile.write_text(json.dumps({"pid": 987654}))

    def dead(pid, sig):
        raise ProcessLookupError()

    monkeypatch.setattr(os, "kill", dead)
    assert legacy_handoff.v1_legacy_source_inventory({}) == set()
    assert not pidfile.exists()
    pidfile.write_text(json.dumps({"pid": 987654}))

    def denied(pid, sig):
        raise PermissionError()

    monkeypatch.setattr(os, "kill", denied)
    assert legacy_handoff.v1_legacy_source_inventory({}) is None
    assert pidfile.exists()


def test_dead_pidfile_cleanup_uses_incarnation_and_preserves_live_replacement(
    monkeypatch, tmp_path
):
    path = tmp_path / "watch-work.pid"
    old, new = [1] * 8, [2] * 8
    monkeypatch.setattr(os, "kill", lambda *args: None)
    monkeypatch.setattr(
        legacy_daemons, "v1_daemon_incarnation_exited", lambda pid, token: token == old
    )
    path.write_text(json.dumps({"pid": 123, "audit_token": old}))
    assert legacy_handoff.v1_remove_confirmed_dead_pidfile(path)
    assert not path.exists()
    path.write_text(json.dumps({"pid": 123, "audit_token": new}))
    assert not legacy_handoff.v1_remove_confirmed_dead_pidfile(path)
    assert path.exists()


def test_v1_legacy_inventory_includes_sources_without_daemon(monkeypatch, tmp_path):
    monkeypatch.setattr(legacy_events, "known_sessions", lambda **kwargs: {"work"})
    monkeypatch.setattr(watcher, "v1_local_socket", lambda name: f"/tmp/{name}.sock")
    monkeypatch.setattr(legacy_daemons, "live_daemon_pid", lambda *args: None)
    cfg = {"remotes": [{"ssh_target": "host", "session": "remote", "cmux_title": "w"}]}
    monkeypatch.setattr(core, "load_config_strict", lambda: cfg)
    assert legacy_handoff.v1_legacy_source_inventory(cfg) == {
        core.v1_source_key({"socket_path": "/tmp/work.sock"}),
        core.v1_source_key({"remote": "host", "session": "remote"}),
    }
    (tmp_path / "state.json").write_text("{broken")
    assert legacy_handoff.v1_legacy_source_inventory(cfg) is None


def test_v1_activation_marker_preserves_previous_value_on_sync_failure(
    monkeypatch, tmp_path
):
    legacy_handoff.v1_write_activation_marker("pending")
    marker = tmp_path / "v1-enabled"
    assert marker.read_text() == "pending"
    assert marker.stat().st_mode & 0o077 == 0
    fsync = os.fsync
    calls = []

    def fail_first(fd):
        calls.append(fd)
        if len(calls) == 1:
            raise OSError("sync failed")
        return fsync(fd)

    monkeypatch.setattr(os, "fsync", fail_first)
    with pytest.raises(OSError, match="sync failed"):
        legacy_handoff.v1_write_activation_marker("enabled")
    assert marker.read_text() == "pending"
    legacy_handoff.v1_write_activation_marker("enabled")
    assert marker.read_text() == "enabled"


def test_v1_activation_rollback_requires_directory_sync(monkeypatch, tmp_path):
    (tmp_path / "v1-enabled").write_text("pending")
    with legacy_handoff.v1_activation_lock():

        def fail_sync(_fd):
            raise OSError("directory sync failed")

        monkeypatch.setattr(os, "fsync", fail_sync)
        with pytest.raises(OSError, match="directory sync failed"):
            legacy_handoff.v1_remove_activation_marker()


def test_v1_recovery_syncs_absent_marker_before_restarting(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(
        legacy_daemons, "pending_v1_legacy_restores", lambda: [("watch", "a", 1)]
    )
    monkeypatch.setattr(
        watcher, "restart_legacy_daemons", lambda _plan: calls.append("restart")
    )
    sync = os.fsync

    def record_sync(fd):
        calls.append("sync")
        return sync(fd)

    monkeypatch.setattr(os, "fsync", record_sync)
    assert not legacy_handoff.v1_recover_legacy_handoff()
    assert calls == ["sync", "restart"]


def test_v1_recovery_syncs_absent_marker_without_restore_plan(monkeypatch, tmp_path):
    monkeypatch.setattr(legacy_daemons, "pending_v1_legacy_restores", lambda: [])
    sync = os.fsync
    calls = []

    def record_sync(fd):
        calls.append(True)
        return sync(fd)

    monkeypatch.setattr(os, "fsync", record_sync)
    assert legacy_handoff.v1_recover_legacy_handoff()
    assert calls == [True]


def test_v1_daemon_startup_requires_absent_marker_sync(monkeypatch, tmp_path):

    def fail_sync(_fd):
        raise OSError("sync failed")

    monkeypatch.setattr(os, "fsync", fail_sync)
    with (
        pytest.raises(OSError, match="sync failed"),
        legacy_handoff.v1_daemon_startup_lock(tmp_path / "watch-test.pid"),
    ):
        pytest.fail("daemon must not start after failed directory sync")


def test_v1_activation_rollback_does_not_modify_replaced_directory(
    monkeypatch, tmp_path
):
    state = tmp_path / "state"
    state.mkdir()
    monkeypatch.setattr(core, "state_dir", state)
    (state / "v1-enabled").write_text("pending")
    with legacy_handoff.v1_activation_lock():
        state.rename(tmp_path / "original")
        state.mkdir()
        marker = state / "v1-enabled"
        marker.write_text("enabled")
        with pytest.raises(ValueError, match="activation directory changed"):
            legacy_handoff.v1_remove_activation_marker()
        assert marker.read_text() == "enabled"


def test_v1_activation_lock_rejects_fifo(monkeypatch, tmp_path):
    os.mkfifo(tmp_path / "v1-activation.lock", 0o600)
    with pytest.raises((OSError, ValueError)), legacy_handoff.v1_activation_lock():
        pytest.fail("FIFO must not be used as an activation lock")


@pytest.mark.parametrize("backend", ["preparing-owned", "owned"])
def test_v1_legacy_recovery_does_not_restart_writers_after_backend_fence(
    monkeypatch, tmp_path, backend
):
    marker = tmp_path / "v1-notification-backend"
    marker.write_text(backend)
    marker.chmod(0o600)
    (tmp_path / "v1-enabled").write_text("pending")
    restarts = []
    monkeypatch.setattr(
        legacy_daemons, "pending_v1_legacy_restores", lambda: [("watch", "a", 123)]
    )
    monkeypatch.setattr(watcher, "restart_legacy_daemons", restarts.append)
    assert not legacy_handoff.v1_recover_legacy_handoff()
    assert (tmp_path / "v1-enabled").read_text() == "pending"
    assert not restarts
