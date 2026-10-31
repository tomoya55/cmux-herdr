import json
import os
import subprocess
import sys
import uuid
from contextlib import suppress
from pathlib import Path
from types import SimpleNamespace

import pytest
from helpers import status_event

import cmux_herdr as ch


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


def test_main_event_mode(monkeypatch, tmp_path, cfg):
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


def test_v1_supervisor_publishes_while_discovery_is_stalled(monkeypatch, tmp_path):
    import threading

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


def test_v1_pending_events_are_replayed_on_rollback(monkeypatch, tmp_path):
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


def test_v1_replay_uses_original_event_workspace(monkeypatch, tmp_path, cmux_calls):
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


def test_v1_client_entrypoint_does_not_activate_partial_handoff(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "config_dir", tmp_path)
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


def test_v1_legacy_dispatch_is_disabled_after_opt_in(monkeypatch, tmp_path):
    (tmp_path / "v1-enabled").touch()
    monkeypatch.setattr(sys, "argv", ["cmux_herdr.py", "reconcile"])
    monkeypatch.setattr(ch, "load_config", lambda: pytest.fail("legacy writer ran"))
    assert ch.main() == 0


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
