"""Herdr subscriptions, the watch daemon and supervised source workers."""

import fcntl
import hashlib
import json
import os
import re
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid

from cmux_herdr_core import (
    cmux,
    core,
    focus,
    legacy_daemons,
    legacy_events,
    legacy_handoff,
    process_identity,
    remote_bridge,
    source_sync,
)

WATCH_BACKOFF_SECONDS = 2.0


WATCH_MAX_BACKOFF_SECONDS = 60.0


def watch_pid_path(session):
    return core.state_dir / f"watch-{cmux.sanitize_key(session or 'default')}.pid"


def watch_subscriptions(pane_ids):
    """One agent-status subscription per pane, plus the lifecycle wildcards.

    pane.agent_status_changed takes no wildcard, so the set has to be rebuilt
    whenever a pane appears or vanishes; pane.created and pane.closed are what
    tell the subscriber to do that.
    """
    subs = [
        {"type": "pane.agent_status_changed", "pane_id": pane_id}
        for pane_id in pane_ids
    ]
    subs.append({"type": "pane.created"})
    subs.append({"type": "pane.closed"})
    return subs


def v1_subscriptions(pane_ids):
    """Observe agent changes, pane lifecycle, metadata and focus changes."""
    subs = watch_subscriptions(pane_ids)
    subs.extend(
        {"type": event}
        for event in (
            "pane.agent_detected",
            "pane.updated",
            "pane.focused",
            "pane.moved",
            "tab.focused",
            "workspace.focused",
        )
    )
    return subs


def pane_is_tracked(sock_path, pane_id):
    """Whether herdr reports the pane in a status the sidebar shows.

    Answers True when the server cannot be asked: an unverified event must
    never retire a pill.
    """
    try:
        agents = remote_bridge.socket_rpc(sock_path, "agent.list", {}).get("agents", [])
    except (remote_bridge.SocketError, OSError, ValueError) as e:
        core.log(f"watch: agent list failed: {e}")
        return True
    for agent in agents:
        if agent.get("pane_id") == pane_id:
            return agent.get("agent_status") in core.TRACKED_STATUSES
    return False


def apply_watch_event(cfg, state, sock_path, data, *, session=None, tracked=None):
    """Apply a watcher transition while the caller holds the state lock."""
    ws_id, pane_id = data.get("workspace_id"), data.get("pane_id")
    if not ws_id or not pane_id:
        return
    bucket = core.session_bucket(state, session)
    if pane_id not in bucket["workspaces"].get(ws_id, {}).get("panes", {}):
        return
    pane = bucket["workspaces"][ws_id]["panes"][pane_id]
    if data.get("agent_status") == "unknown" and pane.get("status") == "blocked":
        return
    if tracked if tracked is not None else pane_is_tracked(sock_path, pane_id):
        return
    legacy_events.forget_pane(cfg, bucket, ws_id, pane_id)


def handle_watch_event(cfg, sock_path, data):
    """Apply one status transition the plugin's event hook never received."""
    if data.get("agent_status") in core.TRACKED_STATUSES:
        # Delivered to hooks correctly; they own every addition, notification
        # and log entry, so acting here would duplicate them.
        return
    with core.state_lock():
        state = core.load_state()
        marker = core.state_dir / "v1-enabled"
        if marker.exists():
            if marker.read_text() == "pending":
                state = legacy_handoff.v1_load_legacy_journal_state()
                state.setdefault("v1_pending_legacy_events", []).append(
                    {
                        "kind": "watch",
                        "session": core.current_session(),
                        "workspace": os.environ.get("CMUX_WORKSPACE_ID"),
                        "herdr_socket_path": os.environ.get("HERDR_SOCKET_PATH"),
                        "socket_path": str(sock_path),
                        "event": data,
                    }
                )
                core.save_state(state)
            return
        apply_watch_event(cfg, state, sock_path, data)
        core.save_state(state)


def session_socket_path(session=None):
    """API socket of the herdr session this process was spawned from."""
    name = core.current_session() if session is None else session
    binary = os.environ.get("HERDR_BIN_PATH", "herdr")
    try:
        proc = cmux.run([binary, "session", "list", "--json"])
    except (FileNotFoundError, subprocess.TimeoutExpired) as e:
        core.log(f"watch: session list failed: {e}")
        return None
    if proc.returncode != 0:
        core.log(f"watch: session list failed: {proc.stderr.strip()}")
        return None
    try:
        sessions = json.loads(proc.stdout).get("sessions", [])
    except ValueError as e:
        core.log(f"watch: session list unreadable: {e}")
        return None
    for session in sessions:
        if session.get("name") == name or (not name and session.get("default")):
            return session.get("socket_path")
    core.log(f"watch: no API socket for herdr session {name!r}")
    return None


def v1_local_socket_provider(deadline):
    cache = []

    def get_paths():
        if not cache:
            remaining = deadline - time.monotonic()
            cache.append(
                v1_running_local_socket_paths(timeout=min(remaining, 10))
                if remaining > 0
                else None
            )
        return cache[0]

    return get_paths


def v1_running_local_socket_paths(*, timeout=10):
    """Return the complete, unambiguous inventory of running local API sockets."""
    binary = os.environ.get("HERDR_BIN_PATH", "herdr")
    try:
        if timeout <= 0:
            return None
        proc = cmux.run([binary, "session", "list", "--json"], timeout=timeout)
        if proc.returncode:
            return None
        sessions = json.loads(proc.stdout).get("sessions")
        if not isinstance(sessions, list) or len(sessions) > 4096:
            return None
        paths = []
        for entry in sessions:
            if not isinstance(entry, dict) or type(entry.get("running")) is not bool:
                return None
            if not entry["running"]:
                continue
            path = entry.get("socket_path")
            if not isinstance(path, str) or not os.path.isabs(path) or "\x00" in path:
                return None
            paths.append(path)
        identities = [os.path.realpath(path) for path in paths]
        return paths if len(identities) == len(set(identities)) else None
    except (OSError, subprocess.TimeoutExpired, ValueError, TypeError, AttributeError):
        return None


def v1_local_socket(session):
    """Resolve a running local session; never guess a stale socket path."""
    binary = os.environ.get("HERDR_BIN_PATH", "herdr")
    try:
        proc = cmux.run([binary, "session", "list", "--json"])
        if proc.returncode:
            return None
        for item in json.loads(proc.stdout).get("sessions", []):
            if (
                item.get("name") == session if session else item.get("default")
            ) and item.get("running"):
                return item.get("socket_path")
    except (OSError, subprocess.TimeoutExpired, ValueError, TypeError):
        return None
    return None


def open_subscription(sock_path, subscriptions, *, timeout=None):
    """Connected socket streaming the subscribed events as NDJSON."""
    conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        if timeout is not None:
            conn.settimeout(timeout)
        conn.connect(str(sock_path))
        remote_bridge.v1_check_connected_herdr_peer(
            conn, remote_bridge.v1_check_herdr_source_fence(sock_path)
        )
        conn.sendall(
            json.dumps(
                {
                    "id": "watch",
                    "method": "events.subscribe",
                    "params": {"subscriptions": subscriptions},
                }
            ).encode()
            + b"\n"
        )
    except BaseException:
        conn.close()
        raise
    return conn


V1_SUBSCRIPTION_IDLE_TIMEOUT = 10


def open_v1_subscription(sock_path, subscriptions):
    return open_subscription(
        sock_path, subscriptions, timeout=V1_SUBSCRIPTION_IDLE_TIMEOUT
    )


def list_pane_ids(sock_path):
    panes = remote_bridge.socket_rpc(sock_path, "pane.list", {}).get("panes", [])
    return [p["pane_id"] for p in panes if p.get("pane_id")]


def v1_subscription_matches_source(source_id, event_boot):
    with core.state_lock():
        source = core.load_state().get("v1", {}).get("sources", {}).get(source_id, {})
        return source.get("server_boot_id") == event_boot


def v1_watch_connection(
    source_id, generation, sock_path, *, on_change=None, expected_boot=None
):
    """Observe status transitions in order and rebuild when panes change."""
    pane_ids = list_pane_ids(sock_path)
    conn = open_v1_subscription(sock_path, v1_subscriptions(pane_ids))
    try:
        stream = conn.makefile("rb")
        ack = json.loads(stream.readline())
        if ack.get("result", {}).get("type") != "subscription_started":
            raise remote_bridge.SocketError("Herdr subscription was not acknowledged")
        event_boot = ack["result"].get("server_boot_id")
        if not isinstance(event_boot, str) or not re.fullmatch(
            r"[0-9a-f]{32}", event_boot
        ):
            event_boot = None
        if expected_boot is not None and event_boot != expected_boot:
            raise remote_bridge.SocketError(
                "Herdr subscription server does not match client source"
            )
        sync_options = (
            {"expected_boot": expected_boot} if expected_boot is not None else {}
        )

        def sync_with_retry(**options):
            while True:
                try:
                    return source_sync.sync_v1_source(
                        source_id,
                        generation,
                        sock_path,
                        retry_snapshot_race=True,
                        **options,
                    )
                except source_sync.V1SnapshotRace:
                    time.sleep(0.05)

        if not sync_with_retry(reset_predictions=True, **sync_options):
            raise remote_bridge.SocketError("Herdr snapshot failed after subscription")
        if event_boot and not v1_subscription_matches_source(source_id, event_boot):
            raise remote_bridge.SocketError("Herdr subscription server changed")
        if list_pane_ids(sock_path) != pane_ids:
            return
        if on_change is not None:
            on_change()
        for line in stream:
            try:
                msg = json.loads(line)
            except ValueError:
                continue
            event = str(msg.get("event") or "").replace(".", "_")
            if event in ("pane_created", "pane_closed", "pane_moved"):
                return
            if event == "pane_agent_status_changed":
                data = msg.get("data") or {}
                updated = source_sync.persist_v1_status_event(
                    source_id, data, event_boot
                )
                if not updated or data.get("agent_status") == "done":
                    if not sync_with_retry(**sync_options):
                        raise remote_bridge.SocketError(
                            "Herdr snapshot failed after status event"
                        )
                    if event_boot and not v1_subscription_matches_source(
                        source_id, event_boot
                    ):
                        raise remote_bridge.SocketError(
                            "Herdr subscription server changed"
                        )
                if on_change is not None:
                    on_change()
            elif event == "pane_updated":
                if (
                    source_sync.persist_v1_ack_event(
                        source_id, msg.get("data") or {}, event_boot
                    )
                    and on_change
                ):
                    on_change()
            elif event == "pane_agent_detected":
                if not sync_with_retry(**sync_options):
                    raise remote_bridge.SocketError(
                        "Herdr snapshot failed after agent lifecycle event"
                    )
                if event_boot and not v1_subscription_matches_source(
                    source_id, event_boot
                ):
                    raise remote_bridge.SocketError("Herdr subscription server changed")
                if on_change is not None:
                    on_change()
    finally:
        conn.close()


def v1_watch_supervised_source(source_id, target, on_ready, *, on_not_ready=None):
    """Prepare one source without publishing to cmux before handoff."""
    name = "v1-" + hashlib.sha256(source_id.encode()).hexdigest()[:16]
    entry, procs = {}, {}
    with core.v1_source_leader(source_id) as leader:
        if not leader:
            return False
        token_stop = threading.Event()
        token_worker = None
        notified = False
        fence_token = None
        try:
            if target.get("remote"):
                sock = remote_bridge.ensure_forward(
                    name, core.v1_remote_config(target), entry, procs
                )
                generation = core.v1_remote_generation(
                    target["remote"], entry["socket_path"]
                )
            else:
                sock = target["socket_path"]
                generation = core.v1_local_generation(sock)
            if target.get("socket_generation") is not None and (
                generation != target["socket_generation"]
            ):
                return False
            fence = None
            if target.get("server_boot_id") is None:
                if (
                    target.get("remote")
                    or target.get("socket_generation") != generation
                ):
                    return False
                peer_pid = focus.v1_unix_peer_pid(str(sock))
                peer = process_identity.v1_native_process_info(peer_pid)
                if peer is None:
                    return False
                if (
                    target.get("server_pid", peer_pid) != peer_pid
                    or target.get("server_start_token", peer["start_token"])
                    != peer["start_token"]
                ):
                    return False
                fence = {
                    "path": str(sock),
                    "generation": generation,
                    "pid": peer_pid,
                    "start_token": peer["start_token"],
                }
                fence_token = core.HERDR_SOURCE_FENCE.set(fence)

            def on_change():
                nonlocal notified, token_worker
                if not notified:
                    on_ready()
                    notified = True
                    token_worker = threading.Thread(
                        target=v1_supervised_token_loop,
                        args=(
                            source_id,
                            sock,
                            target.get("server_boot_id"),
                            token_stop,
                            fence,
                        ),
                        daemon=True,
                    )
                    token_worker.start()

            v1_watch_connection(
                source_id,
                generation,
                sock,
                on_change=on_change,
                expected_boot=target.get("server_boot_id"),
            )
            return True
        finally:
            if fence_token is not None:
                core.HERDR_SOURCE_FENCE.reset(fence_token)
            if notified and on_not_ready is not None:
                on_not_ready()
            token_stop.set()
            if token_worker is not None:
                token_worker.join()
            if target.get("remote"):
                remote_bridge.drop_forward(name, entry, procs)


def v1_supervised_token_loop(source_id, socket_path, expected_boot, stop, fence=None):
    """Synchronize force-read tokens without waiting for discovery."""
    token = core.HERDR_SOURCE_FENCE.set(fence)
    try:
        while not stop.is_set():
            try:
                if (core.state_dir / "v1-enabled").read_text() == "enabled":
                    source_sync.sync_v1_episode_tokens(
                        source_id, socket_path, expected_boot=expected_boot
                    )
            except FileNotFoundError:
                pass
            except (
                OSError,
                remote_bridge.SocketError,
                ValueError,
                KeyError,
                TypeError,
            ) as error:
                core.log(f"v1 supervised token sync failed: {error}")
            stop.wait(core.LOOP_TICK_SECONDS)
    finally:
        core.HERDR_SOURCE_FENCE.reset(token)


def run_v1_supervised_source(
    source_id, target, on_ready, stop, *, on_not_ready=lambda: None
):
    """Rebuild silent or disconnected subscriptions without waiting for discovery."""
    ready = False

    def ready_once():
        nonlocal ready
        if not ready:
            on_ready()
            ready = True

    def not_ready():
        nonlocal ready
        if ready:
            ready = False
            on_not_ready()

    while not stop.is_set():
        try:
            if not v1_watch_supervised_source(
                source_id, target, ready_once, on_not_ready=not_ready
            ):
                return False
        except (
            remote_bridge.SocketError,
            OSError,
            ValueError,
            KeyError,
            subprocess.TimeoutExpired,
        ) as error:
            core.log(f"v1 supervised source reconnecting: {error}")
        finally:
            not_ready()
        stop.wait(core.LOOP_TICK_SECONDS)
    return True


def v1_watch_parent(parent_pid, stop):
    """Retire a shell-launched observer if its owning shell disappears."""
    while not stop.wait(core.LOOP_TICK_SECONDS):
        if os.getppid() != parent_pid:
            os.kill(os.getpid(), signal.SIGTERM)
            return


def v1_probe_source(target):
    """Require a live Herdr API before retiring the legacy writer."""
    entry, procs = {}, {}
    name = "v1-probe-" + uuid.uuid4().hex[:16]
    try:
        if target.get("remote"):
            rcfg = core.v1_remote_config(target)
            sock = remote_bridge.ensure_forward(name, rcfg, entry, procs)
        else:
            sock = target["socket_path"]
        result = remote_bridge.socket_rpc(sock, "session.snapshot", {})
        snapshot = result.get("snapshot")
        expected_boot = target.get("server_boot_id")
        return isinstance(snapshot, dict) and (
            expected_boot is None or snapshot.get("server_boot_id") == expected_boot
        )
    except (
        remote_bridge.SocketError,
        OSError,
        ValueError,
        KeyError,
        TypeError,
        subprocess.TimeoutExpired,
    ):
        return False
    finally:
        if target.get("remote"):
            remote_bridge.drop_forward(name, entry, procs)


def restart_legacy_daemons(plan):
    """Restore writers that were live before an interrupted first cutover."""
    for entry in plan:
        mode, session, pid = entry[:3]
        env = {
            key: value
            for key, value in os.environ.items()
            if key not in (*core.CMUX_CALLER_IDENTITY_VARS, "HERDR_SOCKET_PATH")
        }
        env["HERDR_PLUGIN_STATE_DIR"] = str(core.state_dir)
        env["HERDR_PLUGIN_CONFIG_DIR"] = str(core.config_dir)
        env["CMUX_HERDR_RESTORE_PID"] = str(pid)
        if len(entry) == 4:
            env["CMUX_HERDR_RESTORE_TOKEN"] = json.dumps(entry[3])
        else:
            env.pop("CMUX_HERDR_RESTORE_TOKEN", None)
        if mode == "watch":
            env["HERDR_SESSION"] = session or ""
        try:
            pid_path = (
                watch_pid_path(session)
                if mode == "watch"
                else core.state_dir / "remote.pid"
            )
            with (
                open(
                    core.state_dir / f"{pid_path.stem}.restore.lock", "w"
                ) as restore_lock,
                open(core.state_dir / f"{mode}.log", "a") as log_file,
            ):
                try:
                    fcntl.flock(restore_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    continue
                subprocess.Popen(
                    [sys.executable, os.path.abspath(__file__), mode],
                    stdin=subprocess.DEVNULL,
                    stdout=log_file,
                    stderr=log_file,
                    env=env,
                    start_new_session=True,
                    pass_fds=(restore_lock.fileno(),),
                )
        except OSError as e:
            core.log(f"failed to restore legacy {mode} daemon: {e}")


def watch_connection(cfg, sock_path, *, on_ready=None):
    """Subscribe to every pane and dispatch until the pane set goes stale."""
    pane_ids = list_pane_ids(sock_path)
    conn = open_subscription(sock_path, watch_subscriptions(pane_ids))
    try:
        stream = conn.makefile("rb")
        try:
            ack = json.loads(next(stream, b""))
        except ValueError as error:
            raise remote_bridge.SocketError(
                "invalid Herdr subscription acknowledgement"
            ) from error
        result = ack.get("result") if isinstance(ack, dict) else None
        if not isinstance(result, dict) or result.get("type") != "subscription_started":
            raise remote_bridge.SocketError("Herdr subscription was not acknowledged")
        # herdr 0.9 starts a subscription with live events instead of replaying
        # history, so a pane created between the listing and the subscribe would
        # be invisible. Re-listing after subscribing catches exactly that.
        if list_pane_ids(sock_path) != pane_ids:
            return
        if on_ready is not None:
            on_ready()
        for line in stream:
            try:
                msg = json.loads(line)
            except ValueError:
                continue
            # herdr spells event names with dots in some streams and
            # underscores in others.
            event = str(msg.get("event") or "").replace(".", "_")
            if event in ("pane_created", "pane_closed"):
                return  # the subscription set is stale; rebuild it
            if event == "pane_agent_status_changed":
                handle_watch_event(cfg, sock_path, msg.get("data") or {})
    finally:
        conn.close()


def watch_loop(cfg, *, on_ready=None):
    backoff = WATCH_BACKOFF_SECONDS
    while True:
        sock_path = session_socket_path()
        if sock_path:
            try:
                watch_connection(cfg, sock_path, on_ready=on_ready)
                backoff = WATCH_BACKOFF_SECONDS
                continue  # a stale pane set reconnects immediately
            except (remote_bridge.SocketError, OSError, ValueError) as e:
                core.log(f"watch: connection lost: {e}")
        time.sleep(backoff)
        backoff = min(backoff * 2, WATCH_MAX_BACKOFF_SECONDS)


def legacy_retry_loop(cfg, stop, session=None):
    while not stop.is_set():
        try:
            legacy_events.retry_legacy_notifications(cfg, session=session)
        except (OSError, ValueError, KeyError, TypeError) as error:
            core.log(f"legacy notification retry failed: {error}")
        stop.wait(core.LOOP_TICK_SECONDS)


def watch_daemon(cfg):
    session = core.current_session()
    restore_pid = os.environ.get("CMUX_HERDR_RESTORE_PID", "")
    core.state_dir.mkdir(parents=True, exist_ok=True)
    pid_path = watch_pid_path(session)
    if restore_pid:
        legacy_daemons.wait_for_v1_legacy_restore(pid_path, "watch")
        os.environ.pop("CMUX_HERDR_RESTORE_PID", None)
        os.environ.pop("CMUX_HERDR_RESTORE_TOKEN", None)
    # Serialize concurrent startup/refresh invocations: only one watcher per
    # session may take over and write the pidfile.
    with legacy_handoff.v1_daemon_startup_lock(pid_path) as activation:
        if (core.state_dir / "v1-enabled").exists():
            return 0
        legacy_daemons.take_over_daemon(pid_path, "watch", activation)
        if (core.state_dir / "v1-enabled").exists():
            return 0
        pid_path.write_text(
            json.dumps({**legacy_daemons.daemon_self_info(), "handoff_protocol": 1})
        )
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    core.log(
        f"watch daemon started (pid {os.getpid()}) for session {session or 'default'}"
    )
    retry_stop = threading.Event()
    retry_worker = threading.Thread(
        target=legacy_retry_loop, args=(cfg, retry_stop, session), daemon=True
    )
    retry_worker.start()
    try:
        watch_loop(
            cfg,
            on_ready=lambda: legacy_daemons.ack_v1_legacy_ready("watch", session or ""),
        )
    finally:
        retry_stop.set()
        retry_worker.join()
        # Only unlink our own pidfile; a replacement may already own it.
        try:
            if legacy_daemons.read_pidfile(pid_path).get("pid") == os.getpid():
                pid_path.unlink(missing_ok=True)
        except OSError:
            pass
    return 0
