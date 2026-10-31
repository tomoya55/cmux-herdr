"""Command-line entry point."""

import json
import os
import re
import signal
import subprocess
import sys
import threading

from cmux_herdr_core import (
    cmux,
    core,
    focus,
    legacy_daemons,
    legacy_events,
    legacy_handoff,
    remote_bridge,
    supervisor,
    watcher,
)


def main():

    if len(sys.argv) < 2 or sys.argv[1] not in {
        "event",
        "reconcile",
        "detect",
        "remote",
        "watch",
        "client",
        "supervisor",
        "supervisor-source",
        "force-read",
    }:
        print(
            "usage: cmux_herdr.py "
            "<event|reconcile|detect|remote|watch|client|supervisor|force-read>",
            file=sys.stderr,
        )
        return 2
    mode = sys.argv[1]
    if mode == "force-read":
        return 0 if focus.v1_force_read_action() else 1
    if mode == "supervisor":
        if (
            not core.interactive_client_target(sys.argv[2:])
            or not os.environ.get("CMUX_WORKSPACE_ID")
            or not os.environ.get("CMUX_SURFACE_ID")
        ):
            return 0
        core.state_dir, core.config_dir = core.v1_plugin_paths(os.environ)
        core.STATE_PATH = core.state_dir / "state.json"
        stop = threading.Event()
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        cfg = None
        while not stop.is_set():
            candidate = core.load_config_strict()
            if candidate is not None and supervisor.v1_runtime_config_valid(candidate):
                cfg = candidate
                break
            stop.wait(core.LOOP_TICK_SECONDS)
        if cfg is None:
            return 0
        while not stop.is_set():
            changed_to = []

            def config_changed(current_cfg=cfg, next_config=changed_to):
                current = core.load_config_strict()
                if (
                    current is None
                    or current == current_cfg
                    or not supervisor.v1_runtime_config_valid(current)
                ):
                    return False
                next_config[:] = [current]
                return True

            if (
                supervisor.v1_run_supervisor(cfg, stop, config_changed=config_changed)
                != "config-changed"
            ):
                break
            if changed_to:
                cfg = changed_to[0]
        return 0
    if mode == "supervisor-source":
        if len(sys.argv) != 4:
            return 2
        try:
            source_id, target = sys.argv[2], json.loads(sys.argv[3])
            boot = target.get("server_boot_id")
            socket_generation = target.get("socket_generation")
            if (
                not isinstance(target, dict)
                or not (
                    isinstance(boot, str)
                    and re.fullmatch(r"[0-9a-f]{32}", boot)
                    or boot is None
                    and isinstance(socket_generation, str)
                    and re.fullmatch(r"\d+:\d+", socket_generation)
                )
                or source_id != core.v1_supervised_source_key(target)
            ):
                return 2
        except (ValueError, TypeError, AttributeError):
            return 2
        core.state_dir, core.config_dir = core.v1_plugin_paths(os.environ)
        core.STATE_PATH = core.state_dir / "state.json"
        stop = threading.Event()

        def stop_source(*_args):
            stop.set()
            raise SystemExit(0)

        signal.signal(signal.SIGTERM, stop_source)
        parent_pid = os.getppid()
        if parent_pid <= 1:
            return 1
        threading.Thread(
            target=watcher.v1_watch_parent, args=(parent_pid, stop), daemon=True
        ).start()
        try:
            watched = watcher.run_v1_supervised_source(
                source_id,
                target,
                lambda: print("ready", flush=True),
                stop,
                on_not_ready=lambda: print("not-ready", flush=True),
            )
            return 0 if watched else 1
        except (
            remote_bridge.SocketError,
            OSError,
            ValueError,
            KeyError,
            subprocess.TimeoutExpired,
        ) as error:
            core.log(f"v1 supervised source failed: {error}")
            return 1
        finally:
            stop.set()
    if mode == "client":
        core.log("the per-client observer cannot safely migrate existing sessions")
        return 1
    if mode == "reconcile" and (core.state_dir / "v1-enabled").exists():
        return 0
    cfg = core.load_config()
    try:
        if mode == "remote":
            return legacy_daemons.remote_daemon(cfg)
        if mode == "watch":
            return watcher.watch_daemon(cfg)
        with core.state_lock():
            if mode in {"event", "reconcile"}:
                try:
                    marker = (core.state_dir / "v1-enabled").read_text()
                except FileNotFoundError:
                    marker = None
                if marker is not None:
                    if mode == "event" and marker == "pending":
                        raw = os.environ.get("HERDR_PLUGIN_EVENT_JSON")
                        if raw:
                            state = legacy_handoff.v1_load_legacy_journal_state()
                            state.setdefault("v1_pending_legacy_events", []).append(
                                {
                                    "session": core.current_session(),
                                    "workspace": os.environ.get("CMUX_WORKSPACE_ID"),
                                    "herdr_socket_path": os.environ.get(
                                        "HERDR_SOCKET_PATH"
                                    ),
                                    "event": json.loads(raw),
                                }
                            )
                            core.save_state(state)
                    return 0
            state = core.load_state()
            if mode == "event":
                raw = os.environ.get("HERDR_PLUGIN_EVENT_JSON")
                if raw:
                    legacy_events.handle_event(cfg, state, json.loads(raw))
            elif mode == "reconcile":
                legacy_events.reconcile(cfg, state)
            else:
                bucket = core.session_bucket(state)
                bucket.pop("session_ref", None)
                ref = cmux.detect_session_workspace(cfg, bucket)
                print(
                    json.dumps(
                        {
                            "cmux_workspace": ref,
                            "herdr_session": os.environ.get("HERDR_SESSION"),
                            "candidate_pids": cmux.herdr_session_pids(),
                        }
                    )
                )
            core.save_state(state)
    except Exception as e:
        core.log(f"unhandled error in {mode}: {e!r}")
        return 1
    return 0
