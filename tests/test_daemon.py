import ctypes
import fcntl
import json
import os
import signal
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

import pytest

from cmux_herdr_core import (
    cmux,
    core,
    legacy_daemons,
    legacy_events,
    legacy_handoff,
    remote_bridge,
    watcher,
)


def test_remote_daemon_without_remotes_cleans_up(monkeypatch, cmux_calls, tmp_path):
    monkeypatch.setattr(core, "config_dir", tmp_path)
    core.save_remote_state(
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
    monkeypatch.setattr(cmux, "cmux_workspaces_by_title", lambda cfg: {})

    assert legacy_daemons.remote_daemon({}) == 0

    read = [c for c in cmux_calls if c[0] == "mark-notification-read"]
    assert read == []
    saved = json.loads((tmp_path / "remote-state.json").read_text())
    assert saved["remotes"] == {}


def test_remote_daemon_malformed_config_does_not_clean_up(
    monkeypatch, cmux_calls, tmp_path
):
    monkeypatch.setattr(core, "config_dir", tmp_path)
    (tmp_path / "config.toml").write_text("not = [valid")
    core.save_remote_state(
        {"remotes": {"tom": {"panes": {"w1:p1": {"status": "blocked"}}}}}
    )

    assert legacy_daemons.remote_daemon({}) == 1
    assert cmux_calls == []
    saved = json.loads((tmp_path / "remote-state.json").read_text())
    assert "tom" in saved["remotes"]


def test_read_pidfile_formats(tmp_path):
    p = tmp_path / "remote.pid"
    p.write_text("1234")  # pre-JSON format
    assert legacy_daemons.read_pidfile(p) == {"pid": 1234}
    p.write_text('{"pid": 5, "script": "x", "mtime": 1}')
    assert legacy_daemons.read_pidfile(p)["script"] == "x"
    p.write_text("garbage")
    assert legacy_daemons.read_pidfile(p) == {}


def test_legacy_ready_winner_acknowledges_only_its_session(monkeypatch, tmp_path):
    monkeypatch.setattr(
        os, "kill", lambda *args: (_ for _ in ()).throw(ProcessLookupError())
    )
    legacy_daemons.update_v1_legacy_restores(
        lambda entries: [
            ("watch", "work", 123),
            ("watch", "other", 456),
            ("remote", None, 789),
        ]
    )
    legacy_daemons.ack_v1_legacy_ready("watch", "work")
    assert legacy_daemons.pending_v1_legacy_restores() == [
        ("watch", "other", 456),
        ("remote", None, 789),
    ]


def test_legacy_ready_does_not_acknowledge_a_still_live_old_writer(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(os, "kill", lambda *args: None)
    legacy_daemons.update_v1_legacy_restores(lambda entries: [("watch", "work", 123)])
    legacy_daemons.ack_v1_legacy_ready("watch", "work")
    assert legacy_daemons.pending_v1_legacy_restores() == [("watch", "work", 123)]


def test_legacy_ready_uses_recorded_incarnation_when_pid_is_reused(
    monkeypatch, tmp_path
):
    token = [1] * 8
    legacy_daemons.update_v1_legacy_restores(
        lambda entries: [("watch", "work", 123, token)]
    )
    monkeypatch.setattr(
        legacy_daemons,
        "v1_daemon_incarnation_exited",
        lambda pid, saved: pid == 123 and saved == token,
    )
    monkeypatch.setattr(os, "kill", lambda *args: None)
    legacy_daemons.ack_v1_legacy_ready("watch", "work")
    assert legacy_daemons.pending_v1_legacy_restores() == []


def test_normal_watcher_start_acknowledges_old_restore_after_ready(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(core, "current_session", lambda: "work")
    monkeypatch.delenv("CMUX_HERDR_RESTORE_PID", raising=False)
    monkeypatch.setattr(legacy_daemons, "take_over_daemon", lambda *args: None)
    monkeypatch.setattr(signal, "signal", lambda *args: None)
    monkeypatch.setattr(
        os, "kill", lambda *args: (_ for _ in ()).throw(ProcessLookupError())
    )
    legacy_daemons.update_v1_legacy_restores(lambda entries: [("watch", "work", 123)])

    def run_watch(cfg, *, on_ready):
        assert legacy_daemons.pending_v1_legacy_restores() == [("watch", "work", 123)]
        on_ready()

    monkeypatch.setattr(watcher, "watch_loop", run_watch)
    assert watcher.watch_daemon({}) == 0
    assert legacy_daemons.pending_v1_legacy_restores() == []


def test_v1_handoff_barrier_blocks_remote_startup_cleanup(monkeypatch, tmp_path):
    monkeypatch.setattr(core, "load_config_strict", lambda: {})
    monkeypatch.setattr(
        remote_bridge,
        "sweep_remote_orphan_pills",
        lambda *args: pytest.fail("cleanup escaped barrier"),
    )
    started = threading.Event()
    finished = threading.Event()
    result = []

    def launch():
        started.set()
        result.append(legacy_daemons.remote_daemon({}))
        finished.set()

    with open(tmp_path / "v1-activation.lock", "w") as barrier:
        fcntl.flock(barrier, fcntl.LOCK_EX)
        worker = threading.Thread(target=launch)
        worker.start()
        assert started.wait(1)
        assert not finished.wait(0.1)
        (tmp_path / "v1-enabled").write_text("pending")
    assert finished.wait(2)
    worker.join()
    assert result == [0]


def test_legacy_retirement_fails_closed_when_daemon_lock_is_busy(monkeypatch, tmp_path):
    path = tmp_path / "watch-work.pid"
    path.write_text(json.dumps({"pid": 123}))
    result = []
    with open(tmp_path / "watch-work.lock", "w") as busy:
        fcntl.flock(busy, fcntl.LOCK_EX)
        worker = threading.Thread(
            target=lambda: result.append(
                legacy_daemons.stop_v1_legacy_daemon(path, "watch", [])
            )
        )
        worker.start()
        worker.join(timeout=0.5)
        assert not worker.is_alive()
        assert result == [False]


def test_legacy_retirement_does_not_assume_unknown_process_is_dead(
    monkeypatch, tmp_path
):
    path = tmp_path / "watch-work.pid"
    path.write_text(json.dumps({"pid": 123}))
    monkeypatch.setattr(legacy_daemons, "live_daemon_pid", lambda *args: None)
    monkeypatch.setattr(
        os, "kill", lambda *args: (_ for _ in ()).throw(PermissionError())
    )
    assert not legacy_daemons.stop_v1_legacy_daemon(path, "watch", [])
    assert path.exists()


@pytest.mark.skipif(sys.platform != "darwin", reason="requires macOS audit tokens")
def test_v1_audit_token_signal_rejects_wrong_process_generation():
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        token = legacy_daemons.v1_process_audit_token(child.pid)
        assert token is not None
        wrong = token.copy()
        wrong[7] ^= 1
        with pytest.raises(OSError):
            legacy_daemons.v1_signal_audit_token(wrong, signal.SIGTERM)
        assert child.poll() is None
        legacy_daemons.v1_signal_audit_token(token, signal.SIGTERM)
        assert child.wait(timeout=3) == -signal.SIGTERM
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=3)


@pytest.mark.skipif(sys.platform != "darwin", reason="requires macOS audit tokens")
def test_v1_daemon_self_info_includes_kernel_process_identity():
    info = legacy_daemons.daemon_self_info()
    assert info["audit_token"] == legacy_daemons.v1_process_audit_token(os.getpid())


def test_v1_daemon_identity_requires_matching_recorded_audit_token(
    monkeypatch, tmp_path
):
    path = tmp_path / "watch-work.pid"
    token = list(range(8))
    path.write_text(json.dumps({"pid": 123, "audit_token": token}))
    monkeypatch.setattr(
        legacy_daemons, "v1_process_audit_token", lambda pid: token.copy()
    )
    assert legacy_daemons.v1_verified_daemon_audit_token(path, 123) == token
    monkeypatch.setattr(legacy_daemons, "v1_process_audit_token", lambda pid: [0] * 8)
    assert legacy_daemons.v1_verified_daemon_audit_token(path, 123) is None
    path.write_text(json.dumps({"pid": 123}))
    assert legacy_daemons.v1_verified_daemon_audit_token(path, 123) is None


@pytest.mark.parametrize("invalid_pid", [2**32, True, 1.5, -1, 1])
def test_v1_daemon_pidfile_rejects_invalid_pid_without_syscall(
    monkeypatch, tmp_path, invalid_pid
):
    path = tmp_path / "watch-work.pid"
    path.write_text(json.dumps({"pid": invalid_pid}))
    monkeypatch.setattr(
        os, "kill", lambda *args: pytest.fail("invalid PID reached syscall")
    )
    assert legacy_daemons.live_daemon_pid(path, "watch") is None
    assert not legacy_handoff.v1_remove_confirmed_dead_pidfile(path)


@pytest.mark.skipif(sys.platform != "darwin", reason="requires macOS audit tokens")
def test_v1_audit_token_rejects_out_of_range_pid_and_signal():
    assert legacy_daemons.v1_process_audit_token(os.getpid() + 2**32) is None
    token = legacy_daemons.v1_process_audit_token(os.getpid())
    assert token is not None
    for invalid in (True, -1, 2**32 + int(signal.SIGTERM)):
        with pytest.raises(ValueError):
            legacy_daemons.v1_signal_audit_token(token, invalid)


def test_v1_audit_signal_fails_closed_on_unsupported_platform(monkeypatch):
    monkeypatch.setattr(sys, "platform", "unsupported")
    with pytest.raises(OSError):
        legacy_daemons.v1_signal_audit_token(list(range(8)), signal.SIGTERM)


def test_v1_audit_signal_fails_closed_when_symbol_is_missing(monkeypatch):
    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(ctypes, "CDLL", lambda *args, **kwargs: SimpleNamespace())
    with pytest.raises(OSError):
        legacy_daemons.v1_signal_audit_token(list(range(8)), signal.SIGTERM)


def test_retirement_refuses_to_signal_when_pid_generation_changed(
    monkeypatch, tmp_path
):
    path = tmp_path / "watch-work.pid"
    path.write_text(json.dumps({"pid": 123, "audit_token": list(range(8))}))
    monkeypatch.setattr(legacy_daemons, "live_daemon_pid", lambda *args: 123)
    monkeypatch.setattr(legacy_events, "known_sessions", lambda: {"work"})
    monkeypatch.setattr(legacy_daemons, "v1_process_audit_token", lambda pid: [1] * 8)
    monkeypatch.setattr(
        legacy_daemons, "v1_daemon_incarnation_exited", lambda *args: False
    )
    monkeypatch.setattr(
        os,
        "kill",
        lambda pid, sig: None if sig == 0 else pytest.fail("numeric PID was signaled"),
    )
    monkeypatch.setattr(
        legacy_daemons,
        "v1_signal_audit_token",
        lambda *args: pytest.fail("replacement was signaled"),
    )
    assert not legacy_daemons.stop_v1_legacy_daemon(path, "watch", [])


def test_daemon_takeover_refuses_unverified_old_process(monkeypatch, tmp_path):
    path = tmp_path / "watch-work.pid"
    path.write_text(json.dumps({"pid": 123, "script": "old", "mtime": 0}))
    monkeypatch.setattr(legacy_daemons, "live_daemon_pid", lambda *args: 123)
    monkeypatch.setattr(legacy_daemons, "v1_process_audit_token", lambda pid: None)
    monkeypatch.setattr(
        os, "kill", lambda *args: pytest.fail("numeric PID was signaled")
    )
    monkeypatch.setattr(
        legacy_daemons,
        "v1_signal_audit_token",
        lambda *args: pytest.fail("unverified PID was signaled"),
    )
    with pytest.raises(SystemExit) as failure:
        legacy_daemons.take_over_daemon(path, "watch")
    assert failure.value.code == 1


def test_daemon_takeover_uses_monotonic_shutdown_deadline(monkeypatch, tmp_path):
    path = tmp_path / "watch-work.pid"
    path.write_text(json.dumps({"pid": 123, "script": "old", "mtime": 0}))
    monkeypatch.setattr(legacy_daemons, "live_daemon_pid", lambda *args: 123)
    monkeypatch.setattr(
        legacy_daemons, "v1_verified_daemon_audit_token", lambda *args: [123] * 8
    )
    monkeypatch.setattr(legacy_daemons, "v1_signal_audit_token", lambda *args: None)
    monkeypatch.setattr(
        legacy_daemons, "v1_daemon_incarnation_exited", lambda *args: True
    )
    monkeypatch.setattr(time, "time", lambda: pytest.fail("wall clock used"))
    monkeypatch.setattr(time, "monotonic", lambda: 100)
    legacy_daemons.take_over_daemon(path, "watch")


def test_legacy_retirement_aborts_when_new_watchers_keep_arriving(
    monkeypatch, tmp_path
):
    (tmp_path / "watch-0.pid").touch()
    calls = []

    def stop(path, mode, restore_plan=None, *, deadline=None):
        if mode == "watch":
            calls.append(path)
            if len(calls) > 5:
                pytest.fail("retirement scan did not stop")
            (tmp_path / f"watch-{len(calls)}.pid").touch()
        return True

    monkeypatch.setattr(legacy_daemons, "stop_v1_legacy_daemon", stop)
    clock = iter([0, 1, 2, 31, 32, 33])
    monkeypatch.setattr(time, "monotonic", lambda: next(clock, 33))
    assert not legacy_daemons.retire_v1_legacy_daemons()
    assert calls


def test_legacy_retirement_rejects_replacement_at_seen_pidfile(monkeypatch, tmp_path):
    path = tmp_path / "watch-work.pid"
    path.write_text(json.dumps({"pid": 123}))

    def stop(target, mode, restore_plan=None, *, deadline=None):
        if mode == "watch":
            target.write_text(json.dumps({"pid": 456}))
        return True

    monkeypatch.setattr(legacy_daemons, "stop_v1_legacy_daemon", stop)
    monkeypatch.setattr(os, "kill", lambda *args: None)
    assert not legacy_daemons.retire_v1_legacy_daemons()


def test_legacy_retirement_deadline_applies_inside_batch(monkeypatch, tmp_path):
    for i in range(4):
        (tmp_path / f"watch-{i}.pid").touch()
    clock = [0]
    calls = []
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])

    def stop(path, mode, restore_plan=None, *, deadline=None):
        if mode == "watch":
            calls.append(path)
            clock[0] += 15
        return False

    monkeypatch.setattr(legacy_daemons, "stop_v1_legacy_daemon", stop)
    assert not legacy_daemons.retire_v1_legacy_daemons()
    assert len(calls) == 2
    assert clock[0] == 30


def test_legacy_retirement_does_not_signal_after_discovery_exceeds_deadline(
    monkeypatch, tmp_path
):
    path = tmp_path / "watch-work.pid"
    path.touch()
    monkeypatch.setattr(legacy_daemons, "live_daemon_pid", lambda *args: 123)
    monkeypatch.setattr(
        legacy_daemons, "v1_verified_daemon_audit_token", lambda *args: [123] * 8
    )
    clock = [0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])

    def sessions():
        clock[0] = 11
        return {"work"}

    monkeypatch.setattr(legacy_events, "known_sessions", sessions)
    monkeypatch.setattr(
        legacy_daemons,
        "v1_signal_audit_token",
        lambda *args: pytest.fail("signal after deadline"),
    )
    plan = []
    assert not legacy_daemons.stop_v1_legacy_daemon(path, "watch", plan, deadline=10)
    assert not plan


def test_legacy_retirement_cancels_new_journal_if_deadline_passes_before_signal(
    monkeypatch, tmp_path
):
    path = tmp_path / "watch-work.pid"
    path.touch()
    monkeypatch.setattr(legacy_daemons, "live_daemon_pid", lambda *args: 123)
    monkeypatch.setattr(
        legacy_daemons, "v1_verified_daemon_audit_token", lambda *args: [123] * 8
    )
    monkeypatch.setattr(legacy_events, "known_sessions", lambda: {"work"})
    clock = [0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])
    record = legacy_daemons.record_v1_legacy_restore

    def slow_record(plan, item):
        added = record(plan, item)
        clock[0] = 11
        return added

    monkeypatch.setattr(legacy_daemons, "record_v1_legacy_restore", slow_record)
    monkeypatch.setattr(
        legacy_daemons,
        "v1_signal_audit_token",
        lambda *args: pytest.fail("signal after deadline"),
    )
    plan = []
    assert not legacy_daemons.stop_v1_legacy_daemon(path, "watch", plan, deadline=10)
    assert not plan
    assert legacy_daemons.pending_v1_legacy_restores() == []


def test_legacy_retirement_treats_reused_pid_as_original_exit(monkeypatch, tmp_path):
    path = tmp_path / "watch-work.pid"
    path.touch()
    original, replacement = [1] * 7 + [10], [1] * 7 + [11]
    monkeypatch.setattr(legacy_daemons, "live_daemon_pid", lambda *args: 123)
    monkeypatch.setattr(
        legacy_daemons, "v1_verified_daemon_audit_token", lambda *args: original
    )
    monkeypatch.setattr(legacy_daemons, "v1_signal_audit_token", lambda *args: None)
    monkeypatch.setattr(os, "kill", lambda *args: None)
    monkeypatch.setattr(
        legacy_daemons, "v1_process_audit_token", lambda pid: replacement
    )
    monkeypatch.setattr(
        legacy_daemons, "v1_audit_token_pidversion", lambda token: token[7]
    )
    assert legacy_daemons.stop_v1_legacy_daemon(path, "watch")


def test_daemon_exit_is_unknown_when_audit_lookup_fails(monkeypatch):
    monkeypatch.setattr(os, "kill", lambda *args: None)
    monkeypatch.setattr(legacy_daemons, "v1_process_audit_token", lambda pid: None)
    assert not legacy_daemons.v1_daemon_incarnation_exited(123, [1] * 8)


def test_v1_remote_daemon_monitors_when_inventory_write_fails(monkeypatch, tmp_path):
    cfg = {"remotes": [{"ssh_target": "old", "session": "work", "cmux_title": "w"}]}
    monkeypatch.setattr(core, "load_config_strict", lambda: cfg)
    monkeypatch.setattr(legacy_daemons, "take_over_daemon", lambda *args: None)
    monkeypatch.setattr(remote_bridge, "sweep_remote_orphan_pills", lambda *args: None)
    monkeypatch.setattr(signal, "signal", lambda *args: None)
    monkeypatch.setattr(core, "DAEMON_SHUTDOWN", threading.Event())

    def fail(*args):
        raise OSError("disk full")

    monkeypatch.setattr(legacy_handoff, "write_v1_legacy_remote_inventory", fail)
    polled = threading.Event()
    monkeypatch.setattr(
        legacy_daemons, "poll_remote_worker", lambda *args: polled.set()
    )

    def stop(seconds):
        polled.wait(2)
        raise KeyboardInterrupt()

    monkeypatch.setattr(time, "sleep", stop)
    assert legacy_daemons.remote_daemon(cfg) == 0
    assert polled.is_set()
    assert not (tmp_path / "remote.pid").exists()


def test_pid_is_daemon_distinguishes_modes(monkeypatch):
    def fake_ps(cmd, capture_output, text, timeout):
        return SimpleNamespace(stdout="python3 /plugin/cmux_herdr.py watch\n")

    monkeypatch.setattr(subprocess, "run", fake_ps)

    assert legacy_daemons.pid_is_daemon(123, "watch") is True
    assert legacy_daemons.pid_is_daemon(123, "remote") is False


def test_v1_daemon_takeover_syncs_after_reacquiring_activation(monkeypatch, tmp_path):
    path = tmp_path / "watch-work.pid"
    path.write_text(json.dumps({"pid": 123, "script": "old", "mtime": 0}))
    monkeypatch.setattr(legacy_daemons, "live_daemon_pid", lambda *_args: 123)
    monkeypatch.setattr(
        legacy_daemons, "v1_verified_daemon_audit_token", lambda *_args: [123] * 8
    )
    monkeypatch.setattr(legacy_daemons, "v1_signal_audit_token", lambda *_args: None)
    monkeypatch.setattr(
        legacy_daemons, "v1_daemon_incarnation_exited", lambda *_args: True
    )
    calls = []
    monkeypatch.setattr(
        legacy_handoff,
        "_v1_sync_absent_activation_marker",
        lambda _lock: calls.append("sync"),
    )
    with (tmp_path / "v1-activation.lock").open("w") as activation:
        fcntl.flock(activation, fcntl.LOCK_EX)
        legacy_daemons.take_over_daemon(path, "watch", activation)
    assert calls == ["sync"]


def test_v1_migration_stops_only_verified_legacy_daemons(monkeypatch, tmp_path):
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

    monkeypatch.setattr(legacy_daemons, "live_daemon_pid", find)
    monkeypatch.setattr(
        legacy_daemons, "v1_verified_daemon_audit_token", lambda path, pid: [pid] * 8
    )
    monkeypatch.setattr(
        legacy_daemons,
        "v1_signal_audit_token",
        lambda token, sig: signals.append((token[0], sig)),
    )
    monkeypatch.setattr(os, "kill", kill)
    assert not legacy_daemons.retire_v1_legacy_daemons()
    assert set(probes) == {
        ("remote.pid", "remote"),
        ("watch-default.pid", "watch"),
        ("watch-work.pid", "watch"),
    }
    assert signals == [(123, signal.SIGTERM), (456, signal.SIGTERM)]


def test_v1_migration_discovers_watchers_created_during_scan(monkeypatch, tmp_path):
    checked = []

    def probe(path, mode):
        checked.append(path.name)
        if mode == "remote":
            (tmp_path / "watch-new.pid").touch()
        return None

    monkeypatch.setattr(legacy_daemons, "live_daemon_pid", probe)
    assert not legacy_daemons.retire_v1_legacy_daemons()
    assert checked == ["remote.pid", "watch-new.pid"]


def test_v1_restore_acknowledges_only_the_matching_obligation(monkeypatch, tmp_path):
    journal = []
    old = ("watch", "work", 456)
    newer = ("watch", "work", 789)
    legacy_daemons.record_v1_legacy_restore(journal, old)
    legacy_daemons.record_v1_legacy_restore(journal, newer)
    legacy_daemons.ack_v1_legacy_restore("watch", "work", "456")
    assert legacy_daemons.pending_v1_legacy_restores() == [newer]


def test_v1_restore_waits_for_signaled_daemon_before_restarting(monkeypatch, tmp_path):
    monkeypatch.setenv("CMUX_HERDR_RESTORE_PID", "456")
    pids = iter([456, 456, None])
    monkeypatch.setattr(
        legacy_daemons, "live_daemon_pid", lambda path, mode: next(pids)
    )
    waits = []
    monkeypatch.setattr(time, "sleep", lambda seconds: waits.append(seconds))
    legacy_daemons.wait_for_v1_legacy_restore(tmp_path / "watch-new.pid", "watch")
    assert len(waits) == 2


def test_v1_restore_does_not_wait_for_reused_pid(monkeypatch, tmp_path):
    monkeypatch.setenv("CMUX_HERDR_RESTORE_PID", "456")
    monkeypatch.setenv("CMUX_HERDR_RESTORE_TOKEN", json.dumps([1] * 8))
    monkeypatch.setattr(legacy_daemons, "live_daemon_pid", lambda *args: 456)
    monkeypatch.setattr(
        legacy_daemons, "v1_daemon_incarnation_exited", lambda pid, token: True
    )
    monkeypatch.setattr(
        time, "sleep", lambda *args: pytest.fail("waited for reused PID")
    )
    legacy_daemons.wait_for_v1_legacy_restore(tmp_path / "watch-new.pid", "watch")


def test_v1_restore_gives_up_when_another_cutover_starts(monkeypatch, tmp_path):
    monkeypatch.setenv("CMUX_HERDR_RESTORE_PID", "456")
    monkeypatch.setattr(legacy_daemons, "live_daemon_pid", lambda path, mode: 456)
    monkeypatch.setattr(
        time, "sleep", lambda seconds: (tmp_path / "v1-enabled").touch()
    )
    with pytest.raises(SystemExit):
        legacy_daemons.wait_for_v1_legacy_restore(tmp_path / "watch-new.pid", "watch")


def test_legacy_retirement_removes_reused_daemon_pidfile(monkeypatch, tmp_path):
    path = watcher.watch_pid_path("work")
    path.write_text(json.dumps({"pid": 456, "audit_token": [1] * 8}))
    monkeypatch.setattr(legacy_daemons, "live_daemon_pid", lambda *args: 456)
    monkeypatch.setattr(
        legacy_daemons, "v1_verified_daemon_audit_token", lambda *args: None
    )
    monkeypatch.setattr(os, "kill", lambda *args: None)
    monkeypatch.setattr(
        legacy_daemons, "v1_daemon_incarnation_exited", lambda *args: True
    )
    assert legacy_daemons.stop_v1_legacy_daemon(path, "watch", [])
    assert not path.exists()


def test_v1_retirement_journals_watchers_discovered_after_cutover(
    monkeypatch, tmp_path
):
    path = watcher.watch_pid_path("new")
    path.touch()
    monkeypatch.setattr(legacy_events, "known_sessions", lambda: {"new"})
    monkeypatch.setattr(legacy_daemons, "live_daemon_pid", lambda path, mode: 456)
    monkeypatch.setattr(
        legacy_daemons, "v1_verified_daemon_audit_token", lambda *args: [456] * 8
    )
    observed = []
    journal = []

    def kill(pid, sig):
        if sig == 0:
            raise ProcessLookupError()
        observed.append(list(journal))

    monkeypatch.setattr(os, "kill", kill)
    monkeypatch.setattr(
        legacy_daemons,
        "v1_signal_audit_token",
        lambda token, sig: observed.append(list(journal)),
    )
    assert legacy_daemons.stop_v1_legacy_daemon(path, "watch", journal)
    assert observed == [[("watch", "new", 456, [456] * 8)]]


def test_v1_retirement_keeps_watcher_when_its_session_is_unknown(monkeypatch, tmp_path):
    path = watcher.watch_pid_path("unknown")
    path.touch()
    monkeypatch.setattr(legacy_events, "known_sessions", lambda: None)
    monkeypatch.setattr(legacy_daemons, "live_daemon_pid", lambda path, mode: 456)
    monkeypatch.setattr(
        legacy_daemons, "v1_verified_daemon_audit_token", lambda *args: [456] * 8
    )
    monkeypatch.setattr(
        legacy_daemons,
        "v1_signal_audit_token",
        lambda *args: pytest.fail("signaled watcher"),
    )
    monkeypatch.setattr(os, "kill", lambda *args: pytest.fail("signaled watcher"))
    journal = []
    assert not legacy_daemons.stop_v1_legacy_daemon(path, "watch", journal)
    assert journal == []


def test_v1_retirement_can_restore_a_watcher_for_a_stopped_session(
    monkeypatch, tmp_path
):
    path = watcher.watch_pid_path("stopped")
    path.touch()
    monkeypatch.setattr(
        cmux,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout=json.dumps({"sessions": [{"name": "stopped", "running": False}]}),
        ),
    )
    monkeypatch.setattr(legacy_daemons, "live_daemon_pid", lambda path, mode: 456)
    monkeypatch.setattr(
        legacy_daemons, "v1_verified_daemon_audit_token", lambda *args: [456] * 8
    )
    monkeypatch.setattr(legacy_daemons, "v1_signal_audit_token", lambda *args: None)
    monkeypatch.setattr(
        os,
        "kill",
        lambda pid, sig: (
            (_ for _ in ()).throw(ProcessLookupError()) if sig == 0 else None
        ),
    )
    journal = []
    assert legacy_daemons.stop_v1_legacy_daemon(path, "watch", journal)
    assert journal == [("watch", "stopped", 456, [456] * 8)]


def test_v1_retirement_recovers_deleted_session_from_watcher_environment(
    monkeypatch, tmp_path
):
    path = watcher.watch_pid_path("deleted.session")
    path.touch()
    monkeypatch.setattr(legacy_events, "known_sessions", lambda: {"default"})
    monkeypatch.setattr(legacy_daemons, "live_daemon_pid", lambda path, mode: 456)
    monkeypatch.setattr(
        legacy_daemons, "v1_verified_daemon_audit_token", lambda *args: [456] * 8
    )
    monkeypatch.setattr(legacy_daemons, "v1_signal_audit_token", lambda *args: None)
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout="python cmux_herdr.py watch HERDR_SESSION=deleted.session",
        ),
    )
    monkeypatch.setattr(
        os,
        "kill",
        lambda pid, sig: (
            (_ for _ in ()).throw(ProcessLookupError()) if sig == 0 else None
        ),
    )
    journal = []
    assert legacy_daemons.stop_v1_legacy_daemon(path, "watch", journal)
    assert journal == [("watch", "deleted.session", 456, [456] * 8)]
