"""Activation marker and handoff between the legacy daemons and v1."""

import fcntl
import json
import os
import stat
import subprocess
import time
from contextlib import contextmanager, suppress

from cmux_herdr_core import (
    core,
    discovery,
    legacy_daemons,
    legacy_events,
    publication,
    remote_bridge,
    supervisor,
    watcher,
)


def v1_load_legacy_journal_state():
    try:
        state = json.loads(core.STATE_PATH.read_text())
    except FileNotFoundError:
        state = {"sessions": {}}
    if not isinstance(state, dict):
        raise ValueError("invalid legacy state")
    return state


def v1_replay_pane_status(sock_path, pane_id):
    """Return the current state only after a validated agent-list response."""
    result = remote_bridge.socket_rpc(sock_path, "agent.list", {})
    agents = result["agents"]
    if not isinstance(agents, list) or any(
        not isinstance(agent, dict) for agent in agents
    ):
        raise ValueError("invalid agent list")
    if any(
        agent.get("pane_id") == pane_id
        and not isinstance(agent.get("agent_status"), str)
        for agent in agents
    ):
        raise ValueError("invalid agent status")
    return next(
        (
            agent.get("agent_status")
            for agent in agents
            if agent.get("pane_id") == pane_id
        ),
        None,
    )


def _v1_check_activation_directory():
    expected = core.ACTIVATION_DIRECTORY.get()
    if expected is None:
        raise ValueError("activation lock is not held")
    current = os.stat(core.state_dir, follow_symlinks=False)
    if (current.st_dev, current.st_ino) != expected:
        raise ValueError("activation directory changed")


@contextmanager
def v1_activation_lock():
    """Pin the directory containing the activation lock for its lifetime."""
    directory_fd = os.open(core.state_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        directory = os.fstat(directory_fd)
        if directory.st_uid != os.getuid() or directory.st_mode & 0o022:
            raise ValueError("invalid activation directory")
        lock_fd = os.open(
            "v1-activation.lock",
            os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK,
            0o600,
            dir_fd=directory_fd,
        )
        try:
            lock_info = os.fstat(lock_fd)
            if (
                not stat.S_ISREG(lock_info.st_mode)
                or lock_info.st_uid != os.getuid()
                or lock_info.st_mode & 0o022
            ):
                raise ValueError("invalid activation lock")
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            token = core.ACTIVATION_DIRECTORY.set((directory.st_dev, directory.st_ino))
            try:
                _v1_check_activation_directory()
                yield
            finally:
                core.ACTIVATION_DIRECTORY.reset(token)
        finally:
            os.close(lock_fd)
    finally:
        os.close(directory_fd)


def v1_remove_activation_marker():
    """Make rollback durable before allowing legacy writers to restart."""
    _v1_check_activation_directory()
    directory_fd = os.open(core.state_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        directory = os.fstat(directory_fd)
        if (directory.st_dev, directory.st_ino) != core.ACTIVATION_DIRECTORY.get():
            raise ValueError("activation directory changed")
        with suppress(FileNotFoundError):
            os.unlink("v1-enabled", dir_fd=directory_fd)
        os.fsync(directory_fd)
        _v1_check_activation_directory()
    finally:
        os.close(directory_fd)


def _v1_rollback_legacy_events_and_marker_locked():
    """Replay pending hooks while the caller holds the activation lock."""
    _v1_check_activation_directory()
    try:
        if publication.v1_notification_backend() != "ordinary":
            return False
    except (OSError, UnicodeError, ValueError):
        return False
    with core.state_lock():
        state = v1_load_legacy_journal_state()
        pending = state.get("v1_pending_legacy_events", [])
        if not isinstance(pending, list):
            raise ValueError("invalid pending legacy event journal")
        barriers = state.setdefault("v1_pending_watch_barriers", {})
        if not isinstance(barriers, dict):
            raise ValueError("invalid pending watcher barriers")
        if pending:
            cfg = core.load_config_strict()
            if cfg is None:
                return False
            while pending:
                item = pending[0]
                if (
                    not isinstance(item, dict)
                    or not isinstance(item.get("session"), str)
                    or not isinstance(item.get("event"), dict)
                ):
                    raise ValueError("invalid pending legacy event")
                token = core.LEGACY_REPLAY_CONTEXT.set(
                    {
                        "session": item["session"],
                        "workspace": item.get("workspace"),
                        "socket_path": item.get("herdr_socket_path"),
                    }
                )
                try:
                    if item.get("kind") == "watch":
                        data = item["event"]
                        key = json.dumps(
                            [
                                item["session"],
                                data.get("workspace_id"),
                                data.get("pane_id"),
                            ]
                        )
                        sock_path = item.get("socket_path")
                        if not isinstance(sock_path, str) or not sock_path:
                            return False
                        try:
                            current = v1_replay_pane_status(
                                sock_path, data.get("pane_id")
                            )
                        except (
                            remote_bridge.SocketError,
                            OSError,
                            ValueError,
                            KeyError,
                            TypeError,
                        ):
                            return False
                        barriers[key] = sock_path
                        watcher.apply_watch_event(
                            cfg,
                            state,
                            item.get("socket_path"),
                            item["event"],
                            session=item["session"],
                            tracked=current in core.TRACKED_STATUSES,
                        )
                    else:
                        data = item["event"].get("data", {})
                        key = json.dumps(
                            [
                                item["session"],
                                data.get("workspace_id"),
                                data.get("pane_id"),
                            ]
                        )
                        sock_path = barriers.get(key)
                        if (
                            sock_path
                            and data.get("type") == "pane_agent_status_changed"
                        ):
                            try:
                                current = v1_replay_pane_status(
                                    sock_path, data.get("pane_id")
                                )
                            except (
                                remote_bridge.SocketError,
                                OSError,
                                ValueError,
                                KeyError,
                                TypeError,
                            ):
                                return False
                            if current != data.get("agent_status"):
                                pending.pop(0)
                                if not pending:
                                    state.pop("v1_pending_legacy_events", None)
                                    state.pop("v1_pending_watch_barriers", None)
                                core.save_state(state)
                                continue
                        legacy_events.handle_event(
                            cfg, state, item["event"], session=item["session"]
                        )
                finally:
                    core.LEGACY_REPLAY_CONTEXT.reset(token)
                pending.pop(0)
                if not pending:
                    state.pop("v1_pending_legacy_events", None)
                    state.pop("v1_pending_watch_barriers", None)
                core.save_state(state)
        else:
            state.pop("v1_pending_watch_barriers", None)
        v1_remove_activation_marker()
        return True


def v1_write_activation_marker(value):
    """Durably replace the legacy handoff marker under the activation lock."""
    if value not in {"pending", "enabled"}:
        raise ValueError("invalid activation marker")
    if core.ACTIVATION_DIRECTORY.get() is not None:
        _v1_check_activation_directory()
    directory_fd = os.open(core.state_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        info = os.fstat(directory_fd)
        if (
            core.ACTIVATION_DIRECTORY.get() is not None
            and (info.st_dev, info.st_ino) != core.ACTIVATION_DIRECTORY.get()
        ):
            raise ValueError("activation directory changed")
        if info.st_uid != os.getuid() or info.st_mode & 0o022:
            raise ValueError("invalid activation directory")
        with core._v1_private_temporary(
            directory_fd, "activation", value.encode("ascii")
        ) as (temporary, _):
            current = os.stat(core.state_dir, follow_symlinks=False)
            if (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino):
                raise ValueError("activation directory changed")
            os.replace(
                temporary,
                "v1-enabled",
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
            )
            os.fsync(directory_fd)
            current = os.stat(core.state_dir, follow_symlinks=False)
            if (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino):
                raise ValueError("activation directory changed")
    finally:
        os.close(directory_fd)


def v1_recover_legacy_handoff():
    """Retry restoration even if a prior recovery already removed the marker."""
    core.state_dir.mkdir(parents=True, exist_ok=True)
    marker = core.state_dir / "v1-enabled"
    with v1_activation_lock():
        try:
            marker_value = marker.read_text()
        except FileNotFoundError:
            marker_value = None
        if marker_value == "enabled":
            return True
        try:
            if publication.v1_notification_backend() != "ordinary":
                return False
        except (OSError, UnicodeError, ValueError):
            return False
        if (
            marker_value is not None
            and not _v1_rollback_legacy_events_and_marker_locked()
        ):
            return False
        if marker_value is None:
            v1_remove_activation_marker()
        plan = legacy_daemons.pending_v1_legacy_restores()
        if plan:
            _v1_check_activation_directory()
            watcher.restart_legacy_daemons(plan)
            return False
        return True


def v1_try_supervisor_handoff(cfg, sources, stop, *, final_check=None):
    """Fence legacy writers and commit only after repeated complete coverage."""
    core.state_dir.mkdir(parents=True, exist_ok=True)
    marker = core.state_dir / "v1-enabled"
    with v1_activation_lock():  # noqa: SIM117 - retain lock ordering
        with open(core.state_dir / "remote-transition.lock", "w") as transition:
            fcntl.flock(transition, fcntl.LOCK_EX)
            try:
                marker_value = marker.read_text()
            except FileNotFoundError:
                marker_value = None
            if marker_value == "enabled":
                return True
            if marker_value is not None:
                if not _v1_rollback_legacy_events_and_marker_locked():
                    return False
                _v1_check_activation_directory()
                watcher.restart_legacy_daemons(
                    legacy_daemons.pending_v1_legacy_restores()
                )
                return False
            inventory = discovery.v1_discover_clients(cfg)
            if inventory is None or stop.is_set():
                return False
            desired = discovery.v1_discovered_targets(inventory)
            ready = sources.reconcile(desired, complete=False)
            legacy = v1_legacy_source_inventory(cfg)
            if not supervisor.v1_handoff_has_coverage(
                inventory["complete"], desired, ready, legacy, sources
            ) or not supervisor.v1_supervisor_bindings_match(desired):
                return False
            restore_plan = legacy_daemons.pending_v1_legacy_restores()
            committed = False
            try:
                with core.state_lock():
                    v1_write_activation_marker("pending")
                for before_retirement in (True, False):
                    if stop.is_set():
                        return False
                    current = discovery.v1_discover_clients(cfg)
                    if (
                        current is None
                        or discovery.v1_discovered_targets(current) != desired
                    ):
                        return False
                    ready = sources.reconcile(desired, complete=False)
                    if (
                        not supervisor.v1_handoff_has_coverage(
                            current["complete"], desired, ready, legacy, sources
                        )
                        or not supervisor.v1_supervisor_bindings_match(desired)
                        or not all(
                            watcher.v1_probe_source(spec["target"])
                            for spec in desired.values()
                        )
                    ):
                        return False
                    if (
                        before_retirement
                        and not legacy_daemons.retire_v1_legacy_daemons(restore_plan)
                    ):
                        return False
                if final_check is not None and not final_check():
                    return False
                with core.state_lock():
                    if v1_load_legacy_journal_state().get("v1_pending_legacy_events"):
                        return False
                    v1_write_activation_marker("enabled")
                committed = True
                legacy_daemons.update_v1_legacy_restores(lambda entries: [])
                return True
            finally:
                if not committed and _v1_rollback_legacy_events_and_marker_locked():
                    _v1_check_activation_directory()
                    watcher.restart_legacy_daemons(
                        legacy_daemons.pending_v1_legacy_restores()
                    )


def write_v1_legacy_remote_inventory(pid, epoch, identities):
    """Publish the daemon's conservative set of active remote identities."""
    path = core.state_dir / "remote-active-sources.json"
    tmp = path.with_suffix(f".{pid}.tmp")
    tmp.write_text(
        json.dumps({"pid": pid, "epoch": epoch, "sources": sorted(identities)})
    )
    tmp.replace(path)


def read_v1_legacy_remote_inventory(pid):
    try:
        pid_record = legacy_daemons.read_pidfile(core.state_dir / "remote.pid")
        epoch = pid_record.get("inventory_epoch")
        if pid_record.get("pid") != pid or not isinstance(epoch, str) or not epoch:
            return None
        data = json.loads((core.state_dir / "remote-active-sources.json").read_text())
        identities = data["sources"]
        if (
            data["pid"] != pid
            or data["epoch"] != epoch
            or not isinstance(identities, list)
        ):
            return None
        if any(
            not isinstance(item, list)
            or len(item) != 2
            or any(not isinstance(value, str) or not value for value in item)
            for item in identities
        ):
            return None
        return {(target, session) for target, session in identities}
    except (OSError, ValueError, KeyError, TypeError):
        return None


def v1_legacy_remote_next_config(current, watched, epoch):
    """Publish old and candidate sources before adopting a configuration."""
    try:
        with open(core.state_dir / "remote-transition.lock", "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            return v1_legacy_remote_next_config_locked(current, watched, epoch)
    except OSError as error:
        core.log(f"remote transition lock failed; retaining configuration: {error}")
        return current


def v1_legacy_remote_next_config_locked(current, watched, epoch):
    """Apply a source transition while holding the handoff barrier."""
    candidate = core.load_config_strict()
    if candidate is None or remote_bridge.remote_configs(candidate) is None:
        candidate = current
    remotes = remote_bridge.remote_configs(candidate)
    identities = {(target, session) for target, session, _ in watched.values()}
    identities.update((entry["ssh_target"], entry["session"]) for entry in remotes)
    try:
        write_v1_legacy_remote_inventory(os.getpid(), epoch, identities)
    except OSError as error:
        core.log(
            f"remote inventory publication failed; retaining configuration: {error}"
        )
        return current
    return candidate


def v1_remove_confirmed_dead_pidfile_locked(path):
    """Remove a conclusively dead PID while holding its daemon lock."""
    if not path.exists():
        return True
    try:
        raw = path.read_text()
    except FileNotFoundError:
        return True
    info = legacy_daemons.read_pidfile(path)
    pid = info.get("pid")
    if type(pid) is not int or not 1 < pid <= 0x7FFFFFFF:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        path.unlink(missing_ok=True)
        return True
    except OSError:
        return False
    token = info.get("audit_token")
    if token is not None and legacy_daemons.v1_daemon_incarnation_exited(pid, token):
        if path.read_text() != raw:
            return False
        path.unlink(missing_ok=True)
        return True
    return False


def v1_remove_confirmed_dead_pidfile(path):
    """Remove only a conclusively dead PID, serialized with daemon startup."""
    with open(core.state_dir / f"{path.stem}.lock", "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        return v1_remove_confirmed_dead_pidfile_locked(path)


def _v1_sync_absent_activation_marker(activation):
    directory_fd = os.open(core.state_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        directory = os.fstat(directory_fd)
        locked = os.fstat(activation.fileno())
        current_lock = os.stat(
            "v1-activation.lock", dir_fd=directory_fd, follow_symlinks=False
        )
        if (locked.st_dev, locked.st_ino) != (current_lock.st_dev, current_lock.st_ino):
            raise ValueError("activation lock changed")
        try:
            os.stat("v1-enabled", dir_fd=directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            os.fsync(directory_fd)
        current = os.stat(core.state_dir, follow_symlinks=False)
        if (current.st_dev, current.st_ino) != (directory.st_dev, directory.st_ino):
            raise ValueError("activation directory changed")
    finally:
        os.close(directory_fd)


@contextmanager
def v1_daemon_startup_lock(pid_path):
    """Do not hold the global handoff lock while a daemon lock is busy."""
    while True:
        with (
            open(core.state_dir / "v1-activation.lock", "w") as activation,
            open(core.state_dir / f"{pid_path.stem}.lock", "w") as daemon_lock,
        ):
            fcntl.flock(activation, fcntl.LOCK_EX)
            try:
                fcntl.flock(daemon_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                pass
            else:
                _v1_sync_absent_activation_marker(activation)
                yield activation
                return
        time.sleep(0.05)


def v1_legacy_source_inventory(cfg):
    """Identify live legacy writers; ambiguous state cannot authorize handoff."""
    sources = set()
    try:
        if core.load_config_strict() != cfg:
            return None
        remotes = remote_bridge.remote_configs(cfg)
        if remotes is None:
            return None
        sources.update(
            core.v1_source_key(
                {"remote": entry["ssh_target"], "session": entry["session"]}
            )
            for entry in remotes
        )
        running = legacy_events.known_sessions(running_only=True)
        if running is None:
            return None
        with core.state_lock():
            try:
                legacy_state = json.loads(core.STATE_PATH.read_text())
            except FileNotFoundError:
                legacy_state = {}
        if not isinstance(legacy_state, dict) or any(
            key in legacy_state for key in ("workspaces", "session_ref")
        ):
            return None
        buckets = legacy_state.get("sessions", {})
        if not isinstance(buckets, dict):
            return None
        if any(
            not isinstance(name, str)
            or not isinstance(bucket, dict)
            or not isinstance(bucket.get("workspaces", {}), dict)
            for name, bucket in buckets.items()
        ):
            return None
        retained = {
            name
            for name, bucket in buckets.items()
            if isinstance(bucket, dict)
            and any(
                entry.get("panes")
                for entry in bucket.get("workspaces", {}).values()
                if isinstance(entry, dict)
            )
        }
        for name in running | retained:
            socket_path = watcher.v1_local_socket("" if name == "default" else name)
            if not socket_path:
                return None
            sources.add(core.v1_source_key({"socket_path": socket_path}))
        for mode, session, *_ in legacy_daemons.pending_v1_legacy_restores():
            if mode == "remote":
                # The journal identifies a daemon but not its last active targets.
                return None
            socket_path = watcher.v1_local_socket(session or "")
            if not socket_path:
                return None
            sources.add(core.v1_source_key({"socket_path": socket_path}))
        watcher_paths = sorted(core.state_dir.glob("watch-*.pid"))
        names = legacy_events.known_sessions() if watcher_paths else set()
        if names is None:
            return None
        names = {"" if name == "default" else name for name in names}
        for path in watcher_paths:
            pid = legacy_daemons.live_daemon_pid(path, "watch")
            if pid is None:
                if v1_remove_confirmed_dead_pidfile(path):
                    continue
                return None
            info = legacy_daemons.read_pidfile(path)
            if info.get("pid") != pid or info.get("handoff_protocol") != 1:
                return None
            matches = [name for name in names if watcher.watch_pid_path(name) == path]
            if len(matches) != 1:
                recovered = legacy_daemons.legacy_watcher_session(pid, path)
                if recovered is None:
                    return None
                matches = ["" if recovered == "default" else recovered]
            socket_path = watcher.v1_local_socket(matches[0])
            if not socket_path:
                return None
            sources.add(core.v1_source_key({"socket_path": socket_path}))
        remote_path = core.state_dir / "remote.pid"
        remote_pid = legacy_daemons.live_daemon_pid(remote_path, "remote")
        if (
            remote_pid is None
            and remote_path.exists()
            and not v1_remove_confirmed_dead_pidfile(remote_path)
        ):
            return None
        if remote_pid is not None:
            active = read_v1_legacy_remote_inventory(remote_pid)
            if active is None:
                return None
            sources.update(
                core.v1_source_key({"remote": target, "session": session})
                for target, session in active
            )
        return sources
    except (OSError, ValueError, TypeError, subprocess.TimeoutExpired):
        return None
