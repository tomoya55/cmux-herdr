"""Discovery of Herdr clients running inside cmux surfaces."""

import ctypes
import hashlib
import json
import os
import selectors
import signal
import subprocess
import sys
import threading
import time
from contextlib import suppress
from pathlib import Path

from cmux_herdr_core import core, process_identity, watcher


def v1_validate_discovery_tree(payload, deadline):
    """Validate the entire roster before granting any surface evidence."""
    seen, surfaces = set(), set()
    try:
        if not isinstance(payload.get("windows"), list):
            return False
        for window in payload["windows"]:
            if not isinstance(window.get("workspaces"), list):
                return False
            for workspace in window["workspaces"]:
                workspace_id = workspace["id"]
                if (
                    not isinstance(workspace_id, str)
                    or not workspace_id
                    or not isinstance(workspace.get("panes"), list)
                ):
                    return False
                for pane in workspace["panes"]:
                    if not isinstance(pane.get("surfaces"), list):
                        return False
                    for surface in pane["surfaces"]:
                        if time.monotonic() >= deadline:
                            return False
                        kind = surface.get("type")
                        if kind == "browser":
                            continue
                        surface_id = surface.get("id")
                        if (
                            kind != "terminal"
                            or not isinstance(surface_id, str)
                            or not surface_id
                        ):
                            return False
                        key = (workspace_id.casefold(), surface_id.casefold())
                        if key in surfaces or not isinstance(
                            surface.get("processes"), list
                        ):
                            return False
                        surfaces.add(key)
                        stack = list(surface["processes"])
                        while stack:
                            if time.monotonic() >= deadline:
                                return False
                            process = stack.pop()
                            if not isinstance(process, dict) or len(seen) >= 16384:
                                return False
                            pid, children = process.get("pid"), process.get("children")
                            if (
                                type(pid) is not int
                                or pid <= 0
                                or pid in seen
                                or not isinstance(children, list)
                            ):
                                return False
                            seen.add(pid)
                            stack.extend(children)
        return True
    except (KeyError, TypeError, AttributeError):
        return False


def _v1_process_named(name, path, expected):
    return name == expected or isinstance(path, str) and Path(path).name == expected


def _v1_is_verified_upstream_server(pid, upstream_sockets, deadline):
    """A Herdr server process on the surface is expected, not an unknown client."""
    paths = upstream_sockets() if upstream_sockets is not None else None
    return paths is not None and process_identity.v1_is_verified_local_server(
        pid, paths, deadline=deadline
    )


def _v1_discovered_client(pid, workspace_id, surface_id, inventory, **extra):
    return {
        "pid": pid,
        "workspace": workspace_id,
        "surface": surface_id,
        "instance": inventory["instance"],
        "sources": inventory["sources"],
        "sources_complete": inventory["sources_complete"],
        **extra,
    }


def v1_discover_clients_in_tree(
    payload, directories, *, deadline=None, upstream_sockets=None
):
    """Collect candidates from one tree; recheck them before accepting ownership."""
    clients, seen, surfaces = [], set(), set()
    complete = True
    if deadline is None:
        deadline = time.monotonic() + 20
    if time.monotonic() >= deadline:
        return {"clients": [], "complete": False, "surface_complete": {}}
    if not v1_validate_discovery_tree(payload, deadline):
        return None
    surface_complete = {}
    try:
        if not isinstance(payload.get("windows"), list):
            return None
        for window in payload["windows"]:
            if time.monotonic() >= deadline:
                return {"clients": clients, "complete": False}
            if not isinstance(window.get("workspaces"), list):
                return None
            for workspace in window["workspaces"]:
                if time.monotonic() >= deadline:
                    return {"clients": clients, "complete": False}
                workspace_id = workspace["id"]
                if not isinstance(workspace_id, str) or not workspace_id:
                    return None
                if not isinstance(workspace.get("panes"), list):
                    return None
                for pane in workspace["panes"]:
                    if time.monotonic() >= deadline:
                        return {"clients": clients, "complete": False}
                    if not isinstance(pane.get("surfaces"), list):
                        return None
                    for surface in pane["surfaces"]:
                        if time.monotonic() >= deadline:
                            return {"clients": clients, "complete": False}
                        if not isinstance(surface.get("type"), str):
                            return None
                        if surface.get("type") != "terminal":
                            if surface["type"] != "browser":
                                return None
                            continue
                        surface_id = surface["id"]
                        if not isinstance(surface_id, str) or not surface_id:
                            return None
                        key = (workspace_id.casefold(), surface_id.casefold())
                        if key in surfaces:
                            return None
                        surfaces.add(key)
                        surface_complete[key] = True
                        tty_device = process_identity.v1_tty_device(surface.get("tty"))
                        if tty_device is None:
                            complete = False
                            surface_complete[key] = False
                            continue
                        if not isinstance(surface.get("processes"), list):
                            return None
                        stack = list(surface["processes"])
                        nodes = {}
                        while stack:
                            if time.monotonic() >= deadline:
                                return {"clients": clients, "complete": False}
                            process = stack.pop()
                            if not isinstance(process, dict) or len(seen) >= 16384:
                                return None
                            pid = process.get("pid")
                            children = process.get("children")
                            if (
                                type(pid) is not int
                                or pid <= 0
                                or pid in seen
                                or not isinstance(children, list)
                            ):
                                return None
                            seen.add(pid)
                            stack.extend(children)
                            nodes[pid] = process
                        owners, transports, native_clients, native_complete = (
                            process_identity.v1_native_surface_ownership(
                                nodes, directories, tty_device, deadline
                            )
                        )
                        complete &= native_complete
                        surface_complete[key] &= native_complete
                        for pid, process in nodes.items():
                            if time.monotonic() >= deadline:
                                return {"clients": clients, "complete": False}
                            name, path = process.get("name"), process.get("path")
                            if not (isinstance(name, str) and name) and not (
                                isinstance(path, str) and path
                            ):
                                complete = False
                                surface_complete[key] = False
                                continue
                            if pid in owners or pid in transports:
                                continue
                            if _v1_process_named(name, path, "ssh"):
                                # A remote shell may hide a Herdr client.
                                complete = False
                                surface_complete[key] = False
                            if not _v1_process_named(name, path, "herdr"):
                                continue
                            if type(process.get("tty_device")) is not int:
                                if _v1_is_verified_upstream_server(
                                    pid, upstream_sockets, deadline
                                ):
                                    continue
                                complete = False
                                surface_complete[key] = False
                                continue
                            if process["tty_device"] != tty_device:
                                continue
                            if pid in native_clients:
                                inventory, ownership = native_clients[pid]
                                clients.append(
                                    _v1_discovered_client(
                                        pid,
                                        workspace_id,
                                        surface_id,
                                        inventory,
                                        native_ownership=ownership,
                                    )
                                )
                                continue
                            sockets = [
                                candidate
                                for directory in directories
                                if process_identity.v1_observer_socket_exists(
                                    candidate := directory / f"{pid}.sock"
                                )
                            ]
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                return {"clients": clients, "complete": False}
                            if len(sockets) == 1:
                                inventory = process_identity.v1_client_source_inventory(
                                    sockets[0], pid, timeout=min(2, remaining)
                                )
                            elif not sockets and upstream_sockets is not None:
                                paths = upstream_sockets()
                                inventory = (
                                    process_identity.v1_upstream_local_client_inventory(
                                        pid, tty_device, paths, deadline=deadline
                                    )
                                    if paths is not None
                                    else None
                                )
                            else:
                                inventory = None
                            if not inventory:
                                if _v1_is_verified_upstream_server(
                                    pid, upstream_sockets, deadline
                                ):
                                    continue
                                complete = False
                                surface_complete[key] = False
                                continue
                            complete &= inventory["sources_complete"]
                            surface_complete[key] &= inventory["sources_complete"]
                            clients.append(
                                _v1_discovered_client(
                                    pid, workspace_id, surface_id, inventory
                                )
                            )
        return {
            "clients": clients,
            "complete": complete and time.monotonic() < deadline,
            "surface_complete": surface_complete,
        }
    except (KeyError, TypeError, OSError, ValueError, AttributeError):
        return None


def v1_wait_child_state(pid):
    """Inspect an exited child without releasing its process-group identity."""
    options = os.WEXITED | os.WNOHANG | os.WNOWAIT
    if hasattr(os, "waitid"):
        status = os.waitid(os.P_PID, pid, options)
        return (status.si_code, status.si_status) if status else None
    if sys.platform != "darwin":
        raise OSError("non-reaping child status unavailable")

    class DarwinSiginfo(ctypes.Structure):
        _fields_ = [
            ("signo", ctypes.c_int),
            ("error", ctypes.c_int),
            ("code", ctypes.c_int),
            ("pid", ctypes.c_int),
            ("uid", ctypes.c_uint),
            ("status", ctypes.c_int),
            ("remainder", ctypes.c_byte * 128),
        ]

    libc = ctypes.CDLL(None, use_errno=True)
    waitid = libc.waitid
    waitid.argtypes = [
        ctypes.c_int,
        ctypes.c_uint,
        ctypes.POINTER(DarwinSiginfo),
        ctypes.c_int,
    ]
    waitid.restype = ctypes.c_int
    info = DarwinSiginfo()
    if waitid(os.P_PID, pid, ctypes.byref(info), options) != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    return (info.code, info.status) if info.pid else None


def v1_bounded_command_output(cmd, *, max_bytes=32 * 1024 * 1024, timeout=20):
    """Read a command's stdout with a total deadline and memory limit."""
    proc = None
    handlers, pending = {}, []
    try:
        if threading.current_thread() is threading.main_thread():
            blocked = {signal.SIGTERM, signal.SIGINT}
            previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, blocked)
            try:
                for signum in (signal.SIGTERM, signal.SIGINT):
                    handler = signal.getsignal(signum)
                    if callable(handler) or handler == signal.SIG_DFL:
                        handlers[signum] = handler
                        signal.signal(
                            signum, lambda sig, frame: pending.append((sig, frame))
                        )
            finally:
                signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
        if pending:
            return None
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        if pending:
            return None
        deadline = time.monotonic() + timeout
        data = bytearray()
        with selectors.DefaultSelector() as selector:
            selector.register(proc.stdout, selectors.EVENT_READ)
            while True:
                if pending:
                    return None
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                if not selector.select(min(remaining, 0.1)):
                    continue
                chunk = os.read(
                    proc.stdout.fileno(), min(65536, max_bytes + 1 - len(data))
                )
                if not chunk:
                    break
                data.extend(chunk)
                if len(data) > max_bytes:
                    return None
        while True:
            if pending:
                return None
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            status = v1_wait_child_state(proc.pid)
            if status is not None:
                break
            time.sleep(min(0.01, remaining))
        if status != (os.CLD_EXITED, 0):
            return None
        result = data.decode("utf-8")
        return result
    except (OSError, UnicodeDecodeError, subprocess.TimeoutExpired):
        return None
    finally:
        try:
            if proc is not None:
                with suppress(ProcessLookupError, PermissionError):
                    os.killpg(proc.pid, signal.SIGKILL)
                try:
                    proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                if proc.stdout is not None:
                    proc.stdout.close()
        finally:
            restoration_error = None
            if handlers:
                previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set(handlers))
                try:
                    while True:
                        try:
                            for signum, handler in handlers.items():
                                signal.signal(signum, handler)
                            break
                        except (KeyboardInterrupt, SystemExit) as error:
                            restoration_error = restoration_error or error
                finally:
                    try:
                        signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
                    except (KeyboardInterrupt, SystemExit) as error:
                        restoration_error = restoration_error or error
            defaults = [sig for sig, _ in pending if handlers[sig] == signal.SIG_DFL]
            if defaults:
                os.kill(os.getpid(), defaults[0])
            elif restoration_error:
                raise restoration_error
            elif pending:
                signum, frame = pending[0]
                handlers[signum](signum, frame)


def v1_discover_clients(cfg):
    """Bracket client observations with two cmux surface process inventories."""
    binary = cfg.get("cmux_bin") or core.DEFAULT_CMUX_BIN
    state_home = Path(os.environ.get("XDG_STATE_HOME") or Path.home() / ".local/state")
    directories = [
        state_home / name / "client-observers" for name in ("herdr", "herdr-dev")
    ]
    scans = []
    deadline = time.monotonic() + 20
    upstream_sockets = watcher.v1_local_socket_provider(deadline)

    try:
        for _ in range(2):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            output = v1_bounded_command_output(
                [
                    binary,
                    "--id-format",
                    "both",
                    "top",
                    "--all",
                    "--processes",
                    "--json",
                ],
                timeout=remaining,
            )
            if output is None:
                return None
            scan = v1_discover_clients_in_tree(
                json.loads(output),
                directories,
                deadline=deadline,
                upstream_sockets=upstream_sockets,
            )
            if scan is None:
                return None
            scans.append(scan)
        before, after = scans

        def identity(client):
            return (
                client["pid"],
                client["workspace"],
                client["surface"],
                client["instance"],
            )

        index = {}
        for client in before["clients"]:
            if time.monotonic() >= deadline:
                return None
            key = identity(client)
            if key in index:
                return None
            index[key] = client
        stable = []
        for client in after["clients"]:
            if time.monotonic() >= deadline:
                return None
            if index.get(identity(client)) == client:
                stable.append(client)
        complete = (
            before["complete"]
            and after["complete"]
            and len(stable) == len(before["clients"]) == len(after["clients"])
            and time.monotonic() < deadline
        )
        result = {"clients": stable, "complete": complete}
        if "surface_complete" in before and "surface_complete" in after:
            first, second = before["surface_complete"], after["surface_complete"]
            surface_complete = {}
            for key in set(first) | set(second):
                if time.monotonic() >= deadline:
                    return None
                before_group = {
                    identity(c): c
                    for c in before["clients"]
                    if (c["workspace"].casefold(), c["surface"].casefold()) == key
                }
                after_group = {
                    identity(c): c
                    for c in after["clients"]
                    if (c["workspace"].casefold(), c["surface"].casefold()) == key
                }
                surface_complete[key] = bool(
                    first.get(key) and second.get(key) and before_group == after_group
                )
            result["surface_complete"] = surface_complete
            result["complete"] = (
                complete
                and set(first) == set(second)
                and all(surface_complete.values())
            )
        return result
    except (OSError, ValueError, TypeError, RecursionError, subprocess.TimeoutExpired):
        return None


def v1_discovered_targets(inventory):
    """Build per-client source obligations from verified discovery results."""
    targets = {}
    for client in inventory["clients"]:
        for source in client["sources"]:
            identity = [
                client["pid"],
                client["instance"],
                client["workspace"],
                client["surface"],
                source["endpoint"],
            ]
            key = hashlib.sha256(
                json.dumps(identity, separators=(",", ":")).encode()
            ).hexdigest()
            if source.get("boot_id") is not None:
                target = {"server_boot_id": source["boot_id"]}
            else:
                target = {"socket_generation": source["socket_generation"]}
                for field in ("server_pid", "server_start_token"):
                    if field in source:
                        target[field] = source[field]
            if "api_socket_path" in source:
                target.update(remote=None, socket_path=source["api_socket_path"])
            else:
                target.update(remote=source["ssh_target"], session=source["session"])
            targets[key] = {
                "workspace": client["workspace"],
                "surface": client["surface"],
                "observed_client_pid": client["pid"],
                "observed_client_instance": client["instance"],
                "endpoint": source["endpoint"],
                "connection_generation": source["connection_generation"],
                "target": target,
            }
            if "native_ownership" in client:
                targets[key]["native_ownership"] = client["native_ownership"]
    return targets


def v1_foreground_observation(
    processes, directories, *, upstream_sockets=None, deadline=None
):
    """Require exactly one Herdr client with one working observation socket."""
    if not isinstance(processes, dict):
        return None
    candidates = []
    if deadline is None:
        deadline = time.monotonic() + 20
    for pid, process in processes.items():
        name, path = process.get("name"), process.get("path")
        if name == "herdr" or isinstance(path, str) and Path(path).name == "herdr":
            if upstream_sockets is not None:
                paths = upstream_sockets()
                if paths is not None and process_identity.v1_is_verified_local_server(
                    pid, paths, deadline=deadline
                ):
                    continue
            candidates.append(pid)
    if not candidates:
        return None
    try:
        existing = [
            (pid, path)
            for pid in candidates
            for directory in directories
            if process_identity.v1_observer_socket_exists(
                path := directory / f"{pid}.sock"
            )
        ]
    except (OSError, TypeError, AttributeError):
        return None
    if len(existing) != 1:
        return None
    pid, path = existing[0]
    ancestors, current = set(), pid
    while current in processes and current not in ancestors:
        ancestors.add(current)
        current = processes[current].get("ppid")
    if not set(candidates).issubset(ancestors):
        return None
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return None
    return process_identity.v1_client_observation(path, pid, timeout=min(remaining, 2))
