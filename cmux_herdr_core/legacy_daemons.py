"""Legacy daemon identity, takeover, retirement and the remote daemon."""

import ctypes
import errno
import fcntl
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
import uuid

from cmux_herdr_core import core, legacy_events, legacy_handoff, remote_bridge, watcher


def v1_process_audit_token(pid):
    """Obtain a kernel-issued macOS process identity, or None if unavailable."""
    if sys.platform != "darwin" or type(pid) is not int or not 0 < pid <= 0x7FFFFFFF:
        return None
    try:
        lib = ctypes.CDLL("/usr/lib/libSystem.B.dylib", use_errno=True)
        lib.task_name_for_pid.argtypes = [
            ctypes.c_uint,
            ctypes.c_int,
            ctypes.POINTER(ctypes.c_uint),
        ]
        lib.task_name_for_pid.restype = ctypes.c_int
        lib.task_info.argtypes = [
            ctypes.c_uint,
            ctypes.c_int,
            ctypes.POINTER(ctypes.c_uint32),
            ctypes.POINTER(ctypes.c_uint),
        ]
        lib.task_info.restype = ctypes.c_int
        lib.mach_port_deallocate.argtypes = [ctypes.c_uint, ctypes.c_uint]
        lib.mach_port_deallocate.restype = ctypes.c_int
        own_task = ctypes.c_uint.in_dll(lib, "mach_task_self_").value
        task = ctypes.c_uint()
        if lib.task_name_for_pid(own_task, pid, ctypes.byref(task)) != 0:
            return None
        try:
            token = (ctypes.c_uint32 * 8)()
            count = ctypes.c_uint(8)
            if (
                lib.task_info(task, 15, token, ctypes.byref(count)) != 0
                or count.value != 8
            ):
                return None
            return list(token)
        finally:
            lib.mach_port_deallocate(own_task, task)
    except (OSError, AttributeError, ValueError):
        return None


def v1_signal_audit_token(token, signum):
    """Ask the kernel to signal exactly the process incarnation in token."""
    if sys.platform != "darwin":
        raise OSError(errno.ENOTSUP, "process audit signaling unavailable")
    if (
        not isinstance(token, list)
        or len(token) != 8
        or any(type(word) is not int or not 0 <= word <= 0xFFFFFFFF for word in token)
        or isinstance(signum, bool)
        or not isinstance(signum, int)
        or signum not in signal.valid_signals()
    ):
        raise ValueError("invalid process audit token")
    try:
        lib = ctypes.CDLL("/usr/lib/libSystem.B.dylib", use_errno=True)
        lib.proc_signal_with_audittoken.argtypes = [
            ctypes.POINTER(ctypes.c_uint32),
            ctypes.c_int,
        ]
        lib.proc_signal_with_audittoken.restype = ctypes.c_int
    except (OSError, AttributeError) as error:
        raise OSError(errno.ENOTSUP, "process audit signaling unavailable") from error
    raw = (ctypes.c_uint32 * 8)(*token)
    result = lib.proc_signal_with_audittoken(raw, signum)
    if result:
        raise OSError(result, os.strerror(result))


class V1AuditToken(ctypes.Structure):
    _fields_ = [("words", ctypes.c_uint32 * 8)]


def v1_audit_token_pidversion(token):
    """Decode the OS-defined generation without depending on token layout."""
    if sys.platform != "darwin" or not isinstance(token, list) or len(token) != 8:
        return None
    if any(type(word) is not int or not 0 <= word <= 0xFFFFFFFF for word in token):
        return None
    try:
        lib = ctypes.CDLL("/usr/lib/libbsm.0.dylib", use_errno=True)
        lib.audit_token_to_pidversion.argtypes = [V1AuditToken]
        lib.audit_token_to_pidversion.restype = ctypes.c_int
        return lib.audit_token_to_pidversion(
            V1AuditToken((ctypes.c_uint32 * 8)(*token))
        )
    except (OSError, AttributeError, ValueError):
        return None


def v1_daemon_incarnation_exited(pid, token):
    """Treat unavailable identity as unknown, never as a confirmed exit."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except OSError:
        return False
    current = v1_process_audit_token(pid)
    if current is None:
        return False
    previous_version = v1_audit_token_pidversion(token)
    current_version = v1_audit_token_pidversion(current)
    return (
        previous_version is not None
        and current_version is not None
        and previous_version != current_version
    )


def daemon_self_info():
    script = os.path.abspath(__file__)
    try:
        mtime = os.path.getmtime(script)
    except OSError:
        mtime = 0
    info = {"pid": os.getpid(), "script": script, "mtime": mtime}
    token = v1_process_audit_token(info["pid"])
    if token is not None:
        info["audit_token"] = token
    return info


def read_pidfile(pid_path):
    try:
        data = json.loads(pid_path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    if isinstance(data, int):  # pre-JSON format
        return {"pid": data}
    return data if isinstance(data, dict) else {}


def v1_verified_daemon_audit_token(pid_path, pid):
    """Require the pidfile and the live process to identify the same incarnation."""
    info = read_pidfile(pid_path)
    recorded = info.get("audit_token")
    if (
        info.get("pid") != pid
        or not isinstance(recorded, list)
        or len(recorded) != 8
        or any(
            type(word) is not int or not 0 <= word <= 0xFFFFFFFF for word in recorded
        )
    ):
        return None
    current = v1_process_audit_token(pid)
    return recorded if current is not None and current == recorded else None


def pid_is_daemon(pid, mode):
    """Guard against signaling an unrelated process after PID reuse.

    `mode` keeps the daemons apart: a live watcher must not read as a stale
    remote daemon, which would let a second one start.
    """
    try:
        proc = subprocess.run(
            ["ps", "-p", str(pid), "-o", "args="],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except subprocess.TimeoutExpired:
        return False
    parts = proc.stdout.strip().split()
    return any(p.endswith("cmux_herdr.py") for p in parts) and mode in parts


def live_daemon_pid(pid_path, mode):
    """PID of the running remote daemon, or None (stale/absent pidfile)."""
    if not pid_path.exists():
        return None
    pid = read_pidfile(pid_path).get("pid")
    if type(pid) is not int or not 1 < pid <= 0x7FFFFFFF:
        return None
    try:
        os.kill(pid, 0)
    except OSError:
        return None
    if not pid_is_daemon(pid, mode):
        core.log(f"ignoring stale pidfile: pid {pid} is not a cmux-herdr {mode} daemon")
        return None
    return pid


def wait_for_v1_legacy_restore(pid_path, mode):
    """Wait outside the startup lock for the previous writer to exit."""
    old_pid = live_daemon_pid(pid_path, mode)
    restore_pid = os.environ.get("CMUX_HERDR_RESTORE_PID", "")
    try:
        restore_token = json.loads(os.environ.get("CMUX_HERDR_RESTORE_TOKEN", "null"))
    except ValueError:
        restore_token = None
    while old_pid is not None and str(old_pid) == restore_pid:
        if (core.state_dir / "v1-enabled").exists():
            raise SystemExit(0)
        if restore_token is not None and v1_daemon_incarnation_exited(
            old_pid, restore_token
        ):
            return
        time.sleep(0.1)
        old_pid = live_daemon_pid(pid_path, mode)


def take_over_daemon(pid_path, mode, activation=None):
    """Ensure this is the only (and latest-code) daemon of `mode` running."""
    old_pid = live_daemon_pid(pid_path, mode)
    if old_pid is None:
        return
    info = read_pidfile(pid_path)
    self = daemon_self_info()
    if info.get("script") == self["script"] and info.get("mtime") == self["mtime"]:
        core.log(f"{mode} daemon already running (pid {old_pid})")
        raise SystemExit(0)
    token = v1_verified_daemon_audit_token(pid_path, old_pid)
    if token is None:
        core.log(f"cannot verify outdated {mode} daemon incarnation {old_pid}")
        raise SystemExit(1)
    # The plugin was reinstalled or edited since the old daemon started.
    core.log(f"replacing outdated {mode} daemon (pid {old_pid})")
    try:
        v1_signal_audit_token(token, signal.SIGTERM)
    except ProcessLookupError:
        return
    except OSError as error:
        core.log(f"cannot stop outdated {mode} daemon {old_pid}: {error}")
        raise SystemExit(1) from error
    if activation is not None:
        fcntl.flock(activation, fcntl.LOCK_UN)
    try:
        # Must exceed the old daemon's worst-case shutdown (worker quiesce).
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if v1_daemon_incarnation_exited(old_pid, token):
                return
            time.sleep(0.1)
    finally:
        if activation is not None:
            fcntl.flock(activation, fcntl.LOCK_EX)
            legacy_handoff._v1_sync_absent_activation_marker(activation)
    # The old daemon refuses to die; starting another one would fight over
    # forwarded sockets and state. Retry on the next startup/refresh.
    core.log(f"outdated {mode} daemon (pid {old_pid}) did not exit; aborting")
    raise SystemExit(1)


def update_v1_legacy_restores(update):
    """Serialize and atomically persist pending legacy restoration obligations."""
    core.state_dir.mkdir(parents=True, exist_ok=True)
    path = core.state_dir / "v1-legacy-restores.json"
    with open(core.state_dir / "v1-legacy-restores.lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            entries = json.loads(path.read_text())
        except FileNotFoundError:
            entries = []
        if not isinstance(entries, list) or any(
            not isinstance(item, list)
            or len(item) not in (3, 4)
            or item[0] not in ("watch", "remote")
            or not (item[1] is None or isinstance(item[1], str))
            or type(item[2]) is not int
            or not 1 < item[2] <= 0x7FFFFFFF
            or (
                len(item) == 4
                and (
                    not isinstance(item[3], list)
                    or len(item[3]) != 8
                    or any(
                        type(word) is not int or not 0 <= word <= 0xFFFFFFFF
                        for word in item[3]
                    )
                )
            )
            for item in entries
        ):
            raise ValueError("invalid legacy restoration journal")
        entries = update([tuple(item) for item in entries])
        if entries:
            tmp = path.with_suffix(f".{os.getpid()}.tmp")
            tmp.write_text(json.dumps(entries))
            tmp.replace(path)
        else:
            path.unlink(missing_ok=True)
        return entries


def pending_v1_legacy_restores():
    return update_v1_legacy_restores(lambda entries: entries)


def record_v1_legacy_restore(plan, item):
    added = False

    def add(entries):
        nonlocal added
        if item in entries:
            return entries
        added = True
        return [*entries, item]

    update_v1_legacy_restores(add)
    if item not in plan:
        plan.append(item)
    return added


def ack_v1_legacy_restore(mode, session, restore_pid):
    if not restore_pid or not restore_pid.isdecimal():
        return
    item = (mode, session, int(restore_pid))
    update_v1_legacy_restores(
        lambda entries: [entry for entry in entries if entry[:3] != item]
    )


def ack_v1_legacy_ready(mode, session):
    """A ready replacement satisfies any older restore for the same source."""

    def still_needed(entry):
        if (entry[0], entry[1]) != (mode, session):
            return True
        if len(entry) == 4:
            return not v1_daemon_incarnation_exited(entry[2], entry[3])
        try:
            os.kill(entry[2], 0)
        except ProcessLookupError:
            return False
        except OSError:
            return True
        return True

    update_v1_legacy_restores(
        lambda entries: [entry for entry in entries if still_needed(entry)]
    )


def legacy_watcher_session(pid, pid_path):
    """Recover the session of a verified watcher, including deleted sessions."""
    try:
        proc = subprocess.run(
            ["ps", "-eww", "-p", str(pid)],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if proc.returncode:
        return None
    names = re.findall(r"(?:^|\s)HERDR_SESSION=([A-Za-z0-9_.-]+)(?=\s|$)", proc.stdout)
    if len(names) != 1 or watcher.watch_pid_path(names[0]) != pid_path:
        return None
    return names[0]


def stop_v1_legacy_daemon(pid_path, mode, restore_plan=None, *, deadline=None):
    """Stop one verified writer while holding its legacy startup lock."""
    with open(core.state_dir / f"{pid_path.stem}.lock", "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        pid = live_daemon_pid(pid_path, mode)
        if pid is None:
            return legacy_handoff.v1_remove_confirmed_dead_pidfile_locked(pid_path)
        token = v1_verified_daemon_audit_token(pid_path, pid)
        if token is None:
            core.log(f"cannot verify legacy {mode} daemon incarnation {pid}")
            return legacy_handoff.v1_remove_confirmed_dead_pidfile_locked(pid_path)
        if deadline is not None and time.monotonic() >= deadline:
            return False
        if restore_plan is not None:
            session = None
            if mode == "watch":
                sessions = legacy_events.known_sessions()
                if sessions is None:
                    return False
                names = {"" if name == "default" else name for name in sessions}
                matches = [
                    name for name in names if watcher.watch_pid_path(name) == pid_path
                ]
                if len(matches) != 1:
                    recovered = legacy_watcher_session(pid, pid_path)
                    if recovered is None:
                        core.log(
                            f"cannot safely restore legacy watcher {pid_path.name}"
                        )
                        return False
                    matches = ["" if recovered == "default" else recovered]
                session = matches[0]
            if deadline is not None and time.monotonic() >= deadline:
                return False
            item = (mode, session, pid, token)
            added = record_v1_legacy_restore(restore_plan, item)
            if deadline is not None and time.monotonic() >= deadline:
                if added:
                    update_v1_legacy_restores(
                        lambda entries: [entry for entry in entries if entry != item]
                    )
                    restore_plan.remove(item)
                return False
        try:
            v1_signal_audit_token(token, signal.SIGTERM)
        except ProcessLookupError:
            return True
        except OSError as e:
            core.log(f"cannot stop legacy {mode} daemon {pid}: {e}")
            return False
        stop_deadline = time.monotonic() + 15
        if deadline is not None:
            stop_deadline = min(stop_deadline, deadline)
        while time.monotonic() < stop_deadline:
            if v1_daemon_incarnation_exited(pid, token):
                return True
            time.sleep(0.1)
        core.log(f"legacy {mode} daemon {pid} did not exit")
        return False


def retire_v1_legacy_daemons(restore_plan=None):
    """Stop verified legacy writers before enabling v1 publication."""
    core.state_dir.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + 30
    success = stop_v1_legacy_daemon(
        core.state_dir / "remote.pid", "remote", restore_plan, deadline=deadline
    )
    seen = set()
    while True:
        if time.monotonic() >= deadline or len(seen) >= 4096:
            core.log("legacy watcher inventory did not stabilize")
            return False
        unseen = sorted(set(core.state_dir.glob("watch-*.pid")) - seen)
        if not unseen:
            for path in [core.state_dir / "remote.pid", *seen]:
                if (
                    path.exists()
                    and not legacy_handoff.v1_remove_confirmed_dead_pidfile(path)
                ):
                    return False
            break
        for path in unseen:
            if time.monotonic() >= deadline or len(seen) >= 4096:
                return False
            seen.add(path)
            if not stop_v1_legacy_daemon(
                path, "watch", restore_plan, deadline=deadline
            ):
                success = False
    return success


def poll_remote_worker(rcfg, procs, results):
    """Fetch one remote's snapshot in the background. Shared state is not
    touched: the result carries everything the main thread needs to apply
    (or safely discard) it."""
    name = remote_bridge.remote_name(rcfg)
    identity = (rcfg["ssh_target"], rcfg["session"], rcfg.get("cmux_title") or "")
    fctx = {}  # forward context (socket_path cache), owned by this worker
    try:
        agents, labels = remote_bridge.fetch_remote(name, rcfg, fctx, procs)
        results[name] = {
            "ok": True,
            "identity": identity,
            "socket_path": fctx.get("socket_path"),
            "agents": agents,
            "labels": labels,
        }
    except Exception as e:
        results[name] = {"ok": False, "identity": identity, "error": e}


def tombstone_worker(cfg, rstate, names, done, cancel):
    """Retry retired remotes' pill clears off the scheduling thread.

    Reads shared state but never mutates it; the main thread applies the
    results only for remotes that are still retired. Names in cancel
    (remotes that became active again) are skipped before any cmux call.
    """
    for name in names:
        if name in cancel:
            done[name] = None
            continue
        entry = rstate["remotes"].get(name)
        if not entry:
            done[name] = None
            continue
        pending = entry.get("pending_clear")
        if isinstance(pending, str):  # scalar format from an earlier version
            pending = [pending]
        remaining = []
        for ref in pending or []:
            if name in cancel:
                break
            if not remote_bridge.clear_remote_pill_keys(cfg, name, entry, ref):
                remaining.append(ref)
        done[name] = remaining if name not in cancel else None


def remote_daemon(cfg):
    # A malformed config must not look like "no remotes" (which triggers
    # cleanup); keep the previous behavior of doing nothing instead.
    strict = core.load_config_strict()
    if strict is None:
        return 1
    cfg = strict
    remotes = remote_bridge.remote_configs(cfg)
    if remotes is None:
        # Remotes are present but unusable; refuse to start (and crucially,
        # do not run the empty-config cleanup against persisted state).
        return 1
    core.state_dir.mkdir(parents=True, exist_ok=True)
    restore_pid = os.environ.get("CMUX_HERDR_RESTORE_PID", "")
    pid_path = core.state_dir / "remote.pid"
    if restore_pid:
        wait_for_v1_legacy_restore(pid_path, "remote")
        os.environ.pop("CMUX_HERDR_RESTORE_PID", None)
        os.environ.pop("CMUX_HERDR_RESTORE_TOKEN", None)
    if not remotes:
        # No remotes configured: retire anything a previous daemon left
        # behind (pills, unread notifications, stale state), then exit.
        # A new daemon is spawned by each startup hook and refresh action.
        with legacy_handoff.v1_daemon_startup_lock(pid_path) as activation:
            if (core.state_dir / "v1-enabled").exists():
                return 0
            if live_daemon_pid(pid_path, "remote") is not None:
                # A current daemon hot-reloads the empty config and retires
                # its remotes itself (this raises SystemExit in that case);
                # an outdated one is replaced so upgrades still take effect.
                take_over_daemon(pid_path, "remote", activation)
                if (core.state_dir / "v1-enabled").exists():
                    return 0
                core.log("replaced outdated remote daemon before cleanup")
            rstate = core.load_remote_state()
            for name in list(rstate.get("remotes", {})):
                remote_bridge.retire_remote(cfg, rstate, name, {}, {})
            remote_bridge.sweep_remote_orphan_pills(cfg, [])
            core.save_remote_state(rstate)
            ack_v1_legacy_restore("remote", None, restore_pid)
        core.log("no [[remotes]] configured; remote daemon exiting")
        return 0
    # Serialize concurrent startup/refresh invocations: only one daemon
    # may take over and write the pidfile.
    inventory_epoch = uuid.uuid4().hex
    with legacy_handoff.v1_daemon_startup_lock(pid_path) as activation:
        if (core.state_dir / "v1-enabled").exists():
            return 0
        take_over_daemon(pid_path, "remote", activation)
        if (core.state_dir / "v1-enabled").exists():
            return 0
        pid_path.write_text(
            json.dumps({**daemon_self_info(), "inventory_epoch": inventory_epoch})
        )
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    rstate = core.load_remote_state()
    procs = {}
    next_due = {}
    inflight = {}  # name -> worker thread (fetch only; main thread applies)
    results = {}
    tombstone = {"thread": None, "done": {}, "cancel": set()}
    tombstone_due = {}
    # Seed watched from the startup config so a config change between
    # startup reconciliation and the first loop iteration is still caught.
    watched = {
        remote_bridge.remote_name(r): (
            r["ssh_target"],
            r["session"],
            r.get("cmux_title") or "",
        )
        for r in remotes
    }
    restore_seen = {}
    configured = {remote_bridge.remote_name(r): r for r in remotes}
    for name in list(rstate["remotes"]):
        entry = rstate["remotes"][name]
        rcfg = configured.get(name)
        if rcfg is None or entry.get("identity") != [
            rcfg["ssh_target"],
            rcfg["session"],
        ]:
            # Persisted while a previous daemon was running, or the entry
            # was retargeted (or written by a version without identity
            # tracking) while no daemon was alive; never retired.
            remote_bridge.retire_remote(cfg, rstate, name, procs, next_due)
            continue
        title = rcfg.get("cmux_title") or ""
        if entry.get("cmux_title") != title:
            # Title changed while no daemon was running; reroute now so the
            # old workspace is cleaned even if the remote is unreachable.
            remote_bridge.reroute_remote(cfg, rstate, name, entry, title)
    remote_bridge.sweep_remote_orphan_pills(cfg, remotes)
    names = [remote_bridge.remote_name(r) for r in remotes]
    core.log(f"remote daemon started (pid {os.getpid()}); watching: {', '.join(names)}")
    try:
        while True:
            cfg = legacy_handoff.v1_legacy_remote_next_config(
                cfg, watched, inventory_epoch
            )
            remotes = remote_bridge.remote_configs(cfg) or []
            active = {
                remote_bridge.remote_name(r): (
                    r["ssh_target"],
                    r["session"],
                    r.get("cmux_title") or "",
                )
                for r in remotes
            }
            # Reap finished fetch workers and apply their snapshots on this
            # thread, keeping all state and cmux mutations serialized.
            for name, t in list(inflight.items()):
                if t.is_alive():
                    continue
                del inflight[name]
                res = results.pop(name, {"ok": False, "error": "poll missing"})
                rcfg = next(
                    (r for r in remotes if remote_bridge.remote_name(r) == name), None
                )
                identity = (
                    (rcfg["ssh_target"], rcfg["session"], rcfg.get("cmux_title") or "")
                    if rcfg
                    else None
                )
                if identity is None or res.get("identity") != identity:
                    # Retired or retargeted while the fetch was in flight;
                    # discard the stale snapshot and kill any forward the
                    # orphaned worker may have (re)created.
                    remote_bridge.drop_forward(
                        name, rstate["remotes"].get(name, {}), procs
                    )
                    continue
                if res["ok"]:
                    entry = rstate["remotes"].setdefault(name, {"panes": {}})
                    if res.get("socket_path"):
                        entry["socket_path"] = res["socket_path"]
                    if remote_bridge.apply_remote_snapshot(
                        cfg, rstate, name, rcfg, res["agents"], res["labels"]
                    ):
                        core.save_remote_state(rstate)
                    restore_seen[name] = res["identity"]
                    next_due[name] = time.time() + remote_bridge.poll_interval(rcfg)
                else:
                    core.log(f"remote {name}: {res['error']}")
                    next_due[name] = time.time() + core.REMOTE_ERROR_BACKOFF_SECONDS
            for name, identity in list(watched.items()):
                if active.get(name) != identity:
                    core.log(f"remote {name}: removed or retargeted; tearing down")
                    remote_bridge.retire_remote(cfg, rstate, name, procs, next_due)
                    del watched[name]
            for name, identity in active.items():
                watched.setdefault(name, identity)
            if (core.state_dir / "v1-legacy-restores.json").exists() and all(
                restore_seen.get(name) == identity for name, identity in active.items()
            ):
                ack_v1_legacy_ready("remote", None)
            # Spawn fetches for due remotes without waiting for them, so one
            # unreachable remote cannot starve the others.
            for rcfg in remotes:
                name = remote_bridge.remote_name(rcfg)
                if name in inflight:
                    continue
                # Clamp deadlines far in the future so a decreased
                # poll_seconds applies promptly, without undoing the error
                # backoff (which is at most REMOTE_ERROR_BACKOFF_SECONDS).
                cap = time.time() + max(
                    remote_bridge.poll_interval(rcfg), core.REMOTE_ERROR_BACKOFF_SECONDS
                )
                if next_due.get(name, 0) > cap:
                    next_due[name] = cap
                if time.time() < next_due.get(name, 0):
                    continue
                t = threading.Thread(
                    target=poll_remote_worker,
                    args=(rcfg, procs, results),
                    daemon=True,
                )
                inflight[name] = t
                t.start()
            # Retry pill cleanups left behind by retired remotes, backed off
            # and off this thread (cmux calls can block for seconds).
            # Re-added remotes cancel their in-flight cleanup so a late
            # clear cannot wipe a freshly republished pill.
            tombstone["cancel"].update(active)
            finished = tombstone["thread"]
            if finished is not None and not finished.is_alive():
                for name, remaining in tombstone["done"].items():
                    if remaining is None or name in active:
                        # Cleanup was cancelled (or a clear-status was still
                        # in flight) when the remote became active again; a
                        # late clear may have removed the new pill, so force
                        # a republish on the next poll.
                        if name in active:
                            entry = rstate["remotes"].get(name)
                            if entry is not None:
                                entry["pill_published"] = False
                                core.save_remote_state(rstate)
                        continue
                    entry = rstate["remotes"].get(name)
                    if entry is None:
                        continue
                    if remaining:
                        entry["pending_clear"] = remaining
                        tombstone_due[name] = (
                            time.time() + core.REMOTE_ERROR_BACKOFF_SECONDS
                        )
                    else:
                        tombstone_due.pop(name, None)
                        rstate["remotes"].pop(name, None)
                        core.save_remote_state(rstate)
                tombstone["thread"] = None
                tombstone["done"] = {}
            if tombstone["thread"] is None:
                pending_names = [
                    name
                    for name in list(rstate["remotes"])
                    if name not in active
                    and rstate["remotes"][name].get("pending_clear")
                    and time.time() >= tombstone_due.get(name, 0)
                ]
                if pending_names:
                    # This generation must not inherit cancellations from
                    # when these names were active in the past.
                    tombstone["cancel"] -= set(pending_names)
                    tombstone["thread"] = threading.Thread(
                        target=tombstone_worker,
                        args=(
                            cfg,
                            rstate,
                            pending_names,
                            tombstone["done"],
                            tombstone["cancel"],
                        ),
                        daemon=True,
                    )
                    tombstone["thread"].start()
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        core.DAEMON_SHUTDOWN.set()
        # Quiesce fetch workers before tearing down their SSH forwards, so
        # no worker can spawn a new forward after this loop has run.
        deadline = time.time() + 10
        for t in inflight.values():
            t.join(max(0, deadline - time.time()))
        for name in list(procs):
            remote_bridge.drop_forward(name, rstate["remotes"].get(name, {}), procs)
        # Only unlink our own pidfile; a replacement daemon may have
        # already written its own.
        try:
            if read_pidfile(pid_path).get("pid") == os.getpid():
                pid_path.unlink(missing_ok=True)
        except OSError:
            pass
    return 0
