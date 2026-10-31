"""Process, socket-peer and native transport identity for discovered clients."""

import ctypes
import json
import os
import re
import socket
import stat
import subprocess
import sys
import time
from pathlib import Path

from cmux_herdr_core import core, discovery, focus


def v1_frontmost_app():
    """Read the macOS frontmost application without changing app focus."""
    if sys.platform != "darwin":
        return None, None
    try:
        objc = ctypes.CDLL("/usr/lib/libobjc.A.dylib")
        ctypes.CDLL("/System/Library/Frameworks/AppKit.framework/AppKit")
        objc.objc_getClass.argtypes = [ctypes.c_char_p]
        objc.objc_getClass.restype = ctypes.c_void_p
        objc.sel_registerName.argtypes = [ctypes.c_char_p]
        objc.sel_registerName.restype = ctypes.c_void_p
        send = ctypes.CFUNCTYPE(ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p)(
            ("objc_msgSend", objc)
        )
        send_string = ctypes.CFUNCTYPE(
            ctypes.c_char_p, ctypes.c_void_p, ctypes.c_void_p
        )(("objc_msgSend", objc))
        send_int = ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)(
            ("objc_msgSend", objc)
        )

        def selector(name):
            return objc.sel_registerName(name.encode())

        pool = send(objc.objc_getClass(b"NSAutoreleasePool"), selector("new"))
        try:
            workspace = send(
                objc.objc_getClass(b"NSWorkspace"), selector("sharedWorkspace")
            )
            app = send(workspace, selector("frontmostApplication"))
            bundle = send(app, selector("bundleIdentifier"))
            result = send_string(bundle, selector("UTF8String")) if bundle else None
            pid = send_int(app, selector("processIdentifier")) if app else None
            return result.decode("utf-8") if result else None, pid
        finally:
            if pool:
                send(pool, selector("drain"))
    except (OSError, AttributeError, UnicodeDecodeError, TypeError, ValueError):
        return None, None


def valid_v1_client_envelope(response, pid):
    return isinstance(response, dict) and not (
        type(response.get("version")) is not int
        or response["version"] != 1
        or type(response.get("pid")) is not int
        or response["pid"] != pid
        or response.get("status") != "ok"
        or not isinstance(response.get("instance"), str)
        or not response["instance"]
    )


def valid_v1_client_observation(response, pid):
    """Reject incomplete or incompatible focus evidence."""
    if not valid_v1_client_envelope(response, pid):
        return False
    observation = response.get("observation")
    if not isinstance(observation, dict):
        return False
    if any(
        not isinstance(observation.get(key), str) or not observation[key]
        for key in ("endpoint", "pane_id", "boot_id")
    ):
        return False
    if not re.fullmatch(r"[0-9a-f]{32}", observation["boot_id"]):
        return False
    if any(
        type(observation.get(key)) is not int or observation[key] < 0
        for key in ("snapshot_revision", "connection_generation")
    ):
        return False
    agent_sequence = observation.get("agent_state_change_seq")
    if "agent_state_change_seq" not in observation or (
        agent_sequence is not None
        and (type(agent_sequence) is not int or agent_sequence < 0)
    ):
        return False
    outer = observation.get("outer_focused")
    return "outer_focused" in observation and (outer is None or type(outer) is bool)


def valid_v1_source_inventory(response, pid):
    """Validate source identity independently of the current input target."""
    if not valid_v1_client_envelope(response, pid):
        return False
    sources = response.get("sources")
    if type(response.get("sources_complete")) is not bool or not isinstance(
        sources, list
    ):
        return False
    if len(sources) > 65:
        return False
    seen = set()
    for source in sources:
        if not isinstance(source, dict):
            return False
        endpoint, boot = source.get("endpoint"), source.get("boot_id")
        generation = source.get("socket_generation")
        identity_valid = (
            isinstance(boot, str) and re.fullmatch(r"[0-9a-f]{32}", boot)
        ) or (
            boot is None
            and endpoint == "local"
            and "api_socket_path" in source
            and isinstance(generation, str)
            and re.fullmatch(r"\d+:\d+", generation)
        )
        if (
            not isinstance(endpoint, str)
            or not re.fullmatch(r"local|ssh:[0-9a-f]{32}", endpoint)
            or endpoint in seen
            or not identity_valid
            or type(source.get("connection_generation")) is not int
            or source["connection_generation"] < 0
        ):
            return False
        seen.add(endpoint)
        if "api_socket_path" in source:
            path = source.get("api_socket_path")
            if (
                endpoint != "local"
                or not isinstance(path, str)
                or not os.path.isabs(path)
                or "\x00" in path
            ):
                return False
            if "ssh_target" in source or "session" in source:
                return False
        else:
            target, session = source.get("ssh_target"), source.get("session")
            if (
                not isinstance(target, str)
                or not target
                or target.startswith("-")
                or "\x00" in target
                or not isinstance(session, str)
                or not session
                or "\x00" in session
                or "api_socket_path" in source
            ):
                return False
    return True


def v1_read_client_response(path, expected_peer=None, *, timeout=2):
    """Read one bounded response and optionally verify its socket peer PID."""

    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate observer response key")
            result[key] = value
        return result

    try:
        deadline = time.monotonic() + timeout
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
            conn.settimeout(timeout)
            conn.connect(str(path))
            if expected_peer is not None and (
                sys.platform != "darwin" or conn.getsockopt(0, 2) != expected_peer
            ):
                return None
            data = b""
            while b"\n" not in data and len(data) <= 65536:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                conn.settimeout(remaining)
                chunk = conn.recv(min(4096, 65537 - len(data)))
                if not chunk:
                    return None
                data += chunk
            if b"\n" not in data or len(data) > 65536:
                return None
            response = json.loads(
                data.split(b"\n", 1)[0], object_pairs_hook=unique_object
            )
        return response if isinstance(response, dict) else None
    except (OSError, ValueError, TypeError, RecursionError):
        return None


def v1_client_observation(path, pid, *, timeout=2):
    response = v1_read_client_response(path, expected_peer=pid, timeout=timeout)
    return response if valid_v1_client_observation(response, pid) else None


def v1_client_source_inventory(path, pid, *, timeout=2):
    response = v1_read_client_response(path, expected_peer=pid, timeout=timeout)
    return response if valid_v1_source_inventory(response, pid) else None


def v1_parse_lsof_unix_peers(output):
    """Parse lsof's machine-readable Unix socket devices and peers."""
    if not isinstance(output, str) or len(output) > 4 * 1024 * 1024:
        raise ValueError("invalid lsof output")
    result = {}
    pid = device = None
    for field in output.splitlines():
        if field.startswith("p"):
            value = field[1:]
            if not value.isdecimal() or int(value) <= 1:
                raise ValueError("invalid lsof process")
            pid, device = int(value), None
            result.setdefault(pid, set())
        elif field.startswith("f"):
            device = None
        elif field.startswith("d"):
            value = field[1:]
            if re.fullmatch(r"0x[0-9a-fA-F]+", value):
                device = value.lower()
        elif field.startswith("n") and pid is not None and device:
            value = field[1:]
            peer = value[2:].lower() if value.startswith("->") else None
            if peer is not None and not re.fullmatch(r"0x[0-9a-f]+", peer):
                raise ValueError("invalid lsof socket peer")
            result[pid].add((device, peer, value if peer is None else None))
    return result


def v1_lsof_processes_connected(peers, client_pid, server_pid, client_path=None):
    if client_pid == server_pid:
        return False
    servers = {
        device
        for device, _, name in peers.get(server_pid, set())
        if client_path is None
        or name is not None
        and os.path.isabs(name)
        and os.path.realpath(name) == os.path.realpath(client_path)
    }
    return any(peer in servers for _, peer, _ in peers.get(client_pid, set()) if peer)


def v1_is_verified_local_server(pid, socket_paths, *, deadline):
    """Identify a Herdr API server without treating unknown processes as servers."""
    try:
        before = v1_native_process_info(pid)
        if before is None:
            return False
        for path in socket_paths:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            try:
                generation = core.v1_local_generation(path)
                if focus.v1_unix_peer_pid(path, timeout=min(remaining, 1)) != pid:
                    continue
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                verified = (
                    v1_native_process_info(pid) == before
                    and core.v1_local_generation(path) == generation
                    and focus.v1_unix_peer_pid(path, timeout=min(remaining, 1)) == pid
                )
                return verified and time.monotonic() < deadline
            except (OSError, ValueError, TypeError, KeyError):
                continue
    except (OSError, ValueError, TypeError, KeyError):
        return False
    return False


def v1_upstream_local_client_inventory(pid, tty_device, socket_paths, *, deadline=None):
    """Identify one live local Herdr connection using kernel socket peers."""
    before_client = v1_native_process_info(pid)
    if before_client is None or before_client.get("tty_device") != tty_device:
        return None
    if deadline is None:
        deadline = time.monotonic() + 10
    matches = []
    try:
        for path in socket_paths:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            try:
                generation = core.v1_local_generation(path)
            except FileNotFoundError:
                continue
            server_pid = focus.v1_unix_peer_pid(path, timeout=min(remaining, 1))
            if server_pid is None:
                try:
                    core.v1_local_generation(path)
                except FileNotFoundError:
                    continue
                return None
            before_server = v1_native_process_info(server_pid)
            if before_server is None:
                try:
                    core.v1_local_generation(path)
                except FileNotFoundError:
                    continue
                return None
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            output = discovery.v1_bounded_command_output(
                ["lsof", "-n", "-F", "pfdn", "-a", "-p", f"{pid},{server_pid}", "-U"],
                max_bytes=4 * 1024 * 1024,
                timeout=min(remaining, 3),
            )
            if output is None:
                return None
            peers = v1_parse_lsof_unix_peers(output)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            if v1_native_process_info(pid) != before_client or (
                v1_native_process_info(server_pid) != before_server
                or core.v1_local_generation(path) != generation
                or focus.v1_unix_peer_pid(path, timeout=min(remaining, 1)) != server_pid
                or time.monotonic() >= deadline
            ):
                return None
            client_path = str(Path(path).with_name(f"{Path(path).stem}-client.sock"))
            if v1_lsof_processes_connected(peers, pid, server_pid, client_path):
                own_devices = {device for device, _, _ in peers.get(pid, set())}
                server_devices = {
                    device for device, _, _ in peers.get(server_pid, set())
                }
                if any(
                    peer not in own_devices | server_devices
                    for _, peer, _ in peers.get(pid, set())
                    if peer
                ):
                    return None
                matches.append(
                    (path, generation, server_pid, before_server["start_token"])
                )
        if len(matches) != 1:
            return None
        path, generation, server_pid, server_start_token = matches[0]
        return {
            "version": 1,
            "pid": pid,
            "status": "ok",
            "observation": None,
            "instance": f"{pid}:{before_client['start_token']}",
            "sources_complete": True,
            "sources": [
                {
                    "endpoint": "local",
                    "api_socket_path": path,
                    "socket_generation": generation,
                    "server_pid": server_pid,
                    "server_start_token": server_start_token,
                    "connection_generation": before_client["start_token"],
                }
            ],
        }
    except (OSError, ValueError, TypeError, KeyError, subprocess.TimeoutExpired):
        return None


def v1_tty_device(tty):
    if not isinstance(tty, str) or not tty.strip() or tty.strip() == "not a tty":
        return None
    tty = tty.strip()
    path = tty if tty.startswith("/dev/") else f"/dev/{tty}"
    try:
        info = os.stat(path)
        return info.st_rdev if stat.S_ISCHR(info.st_mode) else None
    except OSError:
        return None


def v1_native_process_info(pid):
    """Read a live Darwin process incarnation and its kernel relationships."""
    if sys.platform != "darwin" or type(pid) is not int or not 0 < pid < 2**31:
        return None

    class ProcBsdInfo(ctypes.Structure):
        _fields_ = [
            ("flags", ctypes.c_uint32),
            ("status", ctypes.c_uint32),
            ("xstatus", ctypes.c_uint32),
            ("pid", ctypes.c_uint32),
            ("ppid", ctypes.c_uint32),
            ("uid", ctypes.c_uint32),
            ("gid", ctypes.c_uint32),
            ("ruid", ctypes.c_uint32),
            ("rgid", ctypes.c_uint32),
            ("svuid", ctypes.c_uint32),
            ("svgid", ctypes.c_uint32),
            ("reserved", ctypes.c_uint32),
            ("comm", ctypes.c_char * 16),
            ("name", ctypes.c_char * 32),
            ("nfiles", ctypes.c_uint32),
            ("pgid", ctypes.c_uint32),
            ("pjobc", ctypes.c_uint32),
            ("tty_device", ctypes.c_uint32),
            ("tty_pgid", ctypes.c_uint32),
            ("nice", ctypes.c_int32),
            ("start_sec", ctypes.c_uint64),
            ("start_usec", ctypes.c_uint64),
        ]

    try:
        lib = ctypes.CDLL("/usr/lib/libSystem.B.dylib", use_errno=True)
        proc_pidinfo = lib.proc_pidinfo
        proc_pidinfo.argtypes = [
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_uint64,
            ctypes.c_void_p,
            ctypes.c_int,
        ]
        proc_pidinfo.restype = ctypes.c_int
        info = ProcBsdInfo()
        size = ctypes.sizeof(info)
        if proc_pidinfo(pid, 3, 0, ctypes.byref(info), size) != size:
            return None
        if info.pid != pid or info.status == 5 or info.start_usec >= 1_000_000:
            return None
        start_token = info.start_sec * 1_000_000 + info.start_usec
        if not 0 < start_token < 2**64:
            return None
        proc_pidpath = lib.proc_pidpath
        proc_pidpath.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_uint32]
        proc_pidpath.restype = ctypes.c_int
        path = ctypes.create_string_buffer(4096)
        if proc_pidpath(pid, path, len(path)) <= 0:
            return None
        executable = os.fsdecode(path.value)
        if not os.path.isabs(executable):
            return None
        return {
            "pid": pid,
            "ppid": info.ppid,
            "pgid": info.pgid,
            "tty_device": info.tty_device,
            "tty_pgid": info.tty_pgid,
            "start_token": start_token,
            "path": executable,
        }
    except (OSError, AttributeError, ValueError, OverflowError):
        return None


def v1_native_transport_envelope(path, pid, *, timeout=2):
    """Read an observer envelope from the same live launcher incarnation.

    Its bridge inventory is untrusted until separately validated.
    """
    before = v1_native_process_info(pid)
    if before is None:
        return None
    response = v1_read_client_response(path, expected_peer=pid, timeout=timeout)
    after = v1_native_process_info(pid)
    if (
        before != after
        or not isinstance(response, dict)
        or type(response.get("version")) is not int
        or response["version"] != 1
        or type(response.get("owner_pid")) is not int
        or response["owner_pid"] != pid
        or type(response.get("owner_start_token")) is not int
        or response["owner_start_token"] != before["start_token"]
        or not isinstance(response.get("instance"), str)
        or not re.fullmatch(r"[0-9a-f]{32}", response["instance"])
        or type(response.get("revision")) is not int
        or not 0 <= response["revision"] < 2**64
    ):
        return None
    return response


def v1_native_transport_claims(response):
    """Validate launcher-scoped claims; callers must verify peer and topology."""

    def uint(value, *, positive=False, maximum=2**64):
        return type(value) is int and int(positive) <= value < maximum

    def identity(value):
        return (
            isinstance(value, dict)
            and uint(value.get("pid"), positive=True, maximum=2**31)
            and uint(value.get("start_token"), positive=True)
        )

    def identifier(value):
        return isinstance(value, str) and bool(value) and "\x00" not in value

    if (
        not isinstance(response, dict)
        or type(response.get("version")) is not int
        or response["version"] != 1
        or response.get("status") != "ok"
        or response.get("ownership_complete") is not True
        or not uint(response.get("owner_pid"), positive=True, maximum=2**31)
        or not uint(response.get("owner_start_token"), positive=True)
        or not isinstance(response.get("instance"), str)
        or not re.fullmatch(r"[0-9a-f]{32}", response["instance"])
        or not uint(response.get("revision"))
        or response["revision"] == 2**64 - 1
    ):
        return None
    bridges, bounds = response.get("bridges"), response.get("bound_observations")
    if (
        not isinstance(bridges, list)
        or not 0 < len(bridges) <= 64
        or not isinstance(bounds, list)
        or len(bridges) != len(bounds)
    ):
        return None
    indexed = {}
    for bound in bounds:
        if (
            not isinstance(bound, dict)
            or not identifier(bound.get("bridge_id"))
            or bound["bridge_id"] in indexed
        ):
            return None
        indexed[bound["bridge_id"]] = bound
    claims, seen_processes, seen_connections = [], {response["owner_pid"]}, set()
    for bridge in bridges:
        if not isinstance(bridge, dict) or not identifier(bridge.get("id")):
            return None
        bound = indexed.pop(bridge["id"], None)
        connection, client, source = (
            bridge.get("connection"),
            bridge.get("client"),
            bridge.get("source"),
        )
        if (
            not isinstance(bound, dict)
            or not isinstance(connection, dict)
            or not isinstance(source, dict)
            or not identifier(source.get("target"))
            or source["target"].startswith("-")
            or not identifier(source.get("session"))
            or not identity(client)
            or not identity(connection.get("consumer"))
            or client != connection["consumer"]
            or not identity(connection.get("ssh_child"))
            or not identifier(connection.get("id"))
            or type(connection.get("peer_identity_verified")) is not bool
            or connection["id"] in seen_connections
            or bound.get("connection_id") != connection["id"]
            or not uint(bound.get("generation"))
            or not isinstance(bound.get("instance"), str)
            or not re.fullmatch(r"[0-9a-f]{32}", bound["instance"])
        ):
            return None
        ssh = connection["ssh_child"]
        if (
            client["pid"] in seen_processes
            or ssh["pid"] in seen_processes
            or ssh["pid"] == client["pid"]
        ):
            return None
        seen_processes.update((client["pid"], ssh["pid"]))
        seen_connections.add(connection["id"])
        nested = bound.get("observation")
        if (
            not valid_v1_source_inventory(nested, client["pid"])
            or nested["instance"] != bound["instance"]
            or nested["sources_complete"] is not True
            or "observation" not in nested
            or not all(
                uint(item["connection_generation"]) for item in nested["sources"]
            )
        ):
            return None
        local = [item for item in nested["sources"] if item["endpoint"] == "local"]
        if (
            len(local) != 1
            or local[0].get("ssh_target") != source.get("target")
            or local[0].get("session") != source.get("session")
            or local[0]["connection_generation"] != bound["generation"]
        ):
            return None
        pane = nested["observation"]
        if pane is not None and (
            not valid_v1_client_observation(nested, client["pid"])
            or not uint(pane["snapshot_revision"])
            or (
                pane["agent_state_change_seq"] is not None
                and not uint(pane["agent_state_change_seq"])
            )
            or not any(
                pane["endpoint"] == item["endpoint"]
                and pane["boot_id"] == item["boot_id"]
                and pane["connection_generation"] == item["connection_generation"]
                for item in nested["sources"]
            )
        ):
            return None
        claims.append(
            {
                "bridge_id": bridge["id"],
                "connection_id": connection["id"],
                "client": client,
                "ssh_child": ssh,
                "source": source,
                "generation": bound["generation"],
                "observation": nested,
            }
        )
    return claims if not indexed else None


def v1_native_transport_topology(response, claims, nodes, tty_device):
    """Check validated claims against a single surface and live kernel identities."""
    try:
        owner_pid = response["owner_pid"]
        owner_node = nodes[owner_pid]
        if type(tty_device) is not int or not isinstance(claims, list) or not claims:
            return None

        def role(node, name):
            path = node.get("path")
            return (
                node.get("name") == name
                or isinstance(path, str)
                and Path(path).name == name
            )

        def process(pid, token, name, parent=None):
            node = nodes.get(pid)
            if (
                not isinstance(node, dict)
                or node.get("pid") != pid
                or not role(node, name)
                or node.get("tty_device") != tty_device
            ):
                return None
            info = v1_native_process_info(pid)
            if (
                not info
                or info.get("pid") != pid
                or info.get("start_token") != token
                or info.get("tty_device") != tty_device
                or not role(info, name)
            ):
                return None
            if node.get("ppid") != info.get("ppid") or (
                parent is not None and info.get("ppid") != parent
            ):
                return None
            return info

        owner = process(owner_pid, response["owner_start_token"], "herdr")
        if owner is None or not isinstance(owner_node.get("children"), list):
            return None
        child_nodes = owner_node["children"]
        if any(not isinstance(node, dict) for node in child_nodes):
            return None
        observations = {owner_pid: owner}
        records = []
        for claim in claims:
            client, ssh = claim["client"], claim["ssh_child"]
            for identity, name in ((client, "herdr"), (ssh, "ssh")):
                pid = identity["pid"]
                matching = [node for node in child_nodes if node.get("pid") == pid]
                if (
                    len(matching) != 1
                    or matching[0] != nodes.get(pid)
                    or pid in observations
                ):
                    return None
                info = process(pid, identity["start_token"], name, owner_pid)
                if info is None:
                    return None
                observations[pid] = info
            nested = claim["observation"]
            records.append(
                {
                    "owner_pid": owner_pid,
                    "owner_start_token": response["owner_start_token"],
                    "owner_instance": response["instance"],
                    "revision": response["revision"],
                    "bridge_id": claim["bridge_id"],
                    "connection_id": claim["connection_id"],
                    "client": client.copy(),
                    "ssh_child": ssh.copy(),
                    "source": claim["source"].copy(),
                    "generation": claim["generation"],
                    "client_instance": nested["instance"],
                    "sources": sorted(
                        nested["sources"], key=lambda item: item["endpoint"]
                    ),
                    "tty_device": tty_device,
                }
            )
        if any(
            v1_native_process_info(pid) != info for pid, info in observations.items()
        ):
            return None
        return records
    except (KeyError, TypeError, AttributeError, ValueError, OSError):
        return None


def v1_foreground_surface_processes(payload, workspace_id, surface_id):
    """Find foreground processes in one unambiguous cmux surface tree."""

    def matches(node, identity):
        return isinstance(identity, str) and any(
            isinstance(node.get(key), str)
            and node[key].casefold() == identity.casefold()
            for key in ("id", "ref")
        )

    try:
        surfaces = [
            surface
            for window in payload["windows"]
            for workspace in window["workspaces"]
            if matches(workspace, workspace_id)
            for pane in workspace["panes"]
            for surface in pane["surfaces"]
            if matches(surface, surface_id)
        ]
        if len(surfaces) != 1 or not isinstance(surfaces[0]["processes"], list):
            return None
        tty_device = v1_tty_device(surfaces[0].get("tty"))
        if tty_device is None:
            return None
        stack = list(surfaces[0]["processes"])
        seen, foreground, foreground_groups = set(), {}, set()
        while stack:
            process = stack.pop()
            if not isinstance(process, dict) or len(seen) >= 16384:
                return None
            pid = process.get("pid")
            if type(pid) is not int or pid <= 0 or pid in seen:
                return None
            seen.add(pid)
            children = process.get("children")
            if not isinstance(children, list):
                return None
            stack.extend(children)
            pgid, tpgid = process.get("pgid"), process.get("tpgid")
            if (
                type(process.get("tty_device")) is int
                and process["tty_device"] == tty_device
                and type(tpgid) is int
                and tpgid > 0
            ):
                foreground_groups.add(tpgid)
            if (
                type(pgid) is int
                and type(tpgid) is int
                and pgid > 0
                and pgid == tpgid
                and type(process.get("tty_device")) is int
                and process["tty_device"] == tty_device
            ):
                foreground[pid] = process
        return foreground if len(foreground_groups) <= 1 else None
    except (KeyError, TypeError, AttributeError):
        return None


def v1_observer_socket_exists(path):
    try:
        return stat.S_ISSOCK(path.lstat().st_mode)
    except FileNotFoundError:
        return False


def v1_native_surface_ownership(nodes, directories, tty_device, deadline):
    """Find validated native bridges without exempting unrelated processes."""
    owners, transports, clients, used = set(), set(), {}, set()
    complete = True
    for pid, node in nodes.items():
        if time.monotonic() >= deadline:
            return owners, transports, clients, False
        if type(node.get("tty_device")) is int and node["tty_device"] != tty_device:
            continue
        path = node.get("path")
        if not (
            node.get("name") == "herdr"
            or isinstance(path, str)
            and Path(path).name == "herdr"
        ):
            continue
        sockets = [
            directory.parent / "transport-observers" / f"{pid}.sock"
            for directory in directories
        ]
        sockets = [
            candidate for candidate in sockets if v1_observer_socket_exists(candidate)
        ]
        if not sockets:
            continue
        if len(sockets) != 1:
            complete = False
            continue
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return owners, transports, clients, False
        envelope = v1_native_transport_envelope(
            sockets[0], pid, timeout=min(2, remaining)
        )
        claims = v1_native_transport_claims(envelope)
        records = (
            v1_native_transport_topology(envelope, claims, nodes, tty_device)
            if claims
            else None
        )
        if not records or len(records) != len(claims):
            complete = False
            continue
        claimed = {pid}
        for claim in claims:
            claimed.update((claim["client"]["pid"], claim["ssh_child"]["pid"]))
        if len(claimed) != 1 + 2 * len(claims) or claimed & used:
            complete = False
            continue
        used.update(claimed)
        owners.add(pid)
        for claim, record in zip(claims, records, strict=True):
            client_pid = claim["client"]["pid"]
            transports.add(claim["ssh_child"]["pid"])
            clients[client_pid] = (claim["observation"], record)
    return owners, transports, clients, complete
