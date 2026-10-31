import json
import subprocess
import sys

import pytest

import cmux_herdr as ch


def test_legacy_retry_loop_runs_until_stopped(monkeypatch):
    stop = ch.threading.Event()
    calls = []

    def retry(cfg, *, session=None):
        calls.append(cfg)
        stop.set()

    monkeypatch.setattr(ch, "retry_legacy_notifications", retry)
    ch.legacy_retry_loop({"key": "value"}, stop)
    assert calls == [{"key": "value"}]


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


def test_v1_handoff_barrier_blocks_new_watcher_start(monkeypatch, tmp_path):
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


def test_busy_daemon_lock_does_not_block_other_startup(monkeypatch, tmp_path):
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


def test_v1_legacy_restore_does_not_inherit_another_cmux_surface(monkeypatch, tmp_path):
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


def test_v1_restore_does_not_hold_startup_lock_while_waiting(monkeypatch, tmp_path):
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
