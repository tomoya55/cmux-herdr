"""Focus observation, focus acknowledgment and the force-read action."""

import json
import os
import re
import socket
import subprocess
import sys
import time
from pathlib import Path

from cmux_herdr_core import (
    attention_ledger,
    cmux,
    core,
    discovery,
    process_identity,
    publication,
    remote_bridge,
    watcher,
)


def v1_cmux_identity(cfg, workspace_id, surface_id):
    """Check whether the calling terminal is cmux's focused surface."""
    try:
        if not workspace_id or not surface_id:
            return None
        binary = cfg.get("cmux_bin") or core.DEFAULT_CMUX_BIN
        proc = cmux.run(
            [
                binary,
                "--json",
                "identify",
                "--workspace",
                workspace_id,
                "--surface",
                surface_id,
            ]
        )
        if proc.returncode:
            return None
        result = json.loads(proc.stdout)
        caller, focused = result.get("caller", {}), result.get("focused", {})
        keys = ("window_ref", "workspace_ref", "surface_ref")
        bundle = result.get("bundle_identifier")
        if not bundle or any(not caller.get(key) for key in keys):
            return None
        matches = all(caller.get(key) == focused.get(key) for key in keys)
        return {
            "focused": bool(matches),
            "bundle": bundle,
            "socket_path": result.get("socket_path"),
        }
    except (OSError, subprocess.TimeoutExpired, ValueError, TypeError, AttributeError):
        return None


def v1_unix_peer_pid(path, *, timeout=1):
    """Read the owner PID of a macOS Unix socket connection."""
    if (
        sys.platform != "darwin"
        or not isinstance(path, str)
        or not path
        or timeout <= 0
    ):
        return None
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
            conn.settimeout(timeout)
            conn.connect(path)
            pid = conn.getsockopt(0, 2)  # Darwin SOL_LOCAL, LOCAL_PEERPID
            return pid if pid > 0 else None
    except OSError:
        return None


def v1_cmux_terminal_focus(cfg, workspace_id, surface_id):
    """Verify the actual key window and terminal first responder."""
    try:
        binary = cfg.get("cmux_bin") or core.DEFAULT_CMUX_BIN
        proc = cmux.run([binary, "rpc", "debug.terminals", "{}"])
        if proc.returncode:
            return False
        terminals = json.loads(proc.stdout).get("terminals")
        if not isinstance(terminals, list):
            return False

        def matches(node, prefix, identity):
            return isinstance(identity, str) and any(
                isinstance(node.get(f"{prefix}_{key}"), str)
                and node[f"{prefix}_{key}"].casefold() == identity.casefold()
                for key in ("id", "ref")
            )

        candidates = [
            terminal
            for terminal in terminals
            if isinstance(terminal, dict)
            and matches(terminal, "workspace", workspace_id)
            and matches(terminal, "surface", surface_id)
        ]
        return len(candidates) == 1 and all(
            candidates[0].get(key) is True
            for key in (
                "mapped",
                "workspace_selected",
                "window_key",
                "window_visible",
                "surface_view_first_responder",
            )
        )
    except (OSError, ValueError, TypeError, AttributeError, subprocess.TimeoutExpired):
        return False


def v1_cmux_is_foreground(cfg, workspace_id, surface_id):
    identity = v1_cmux_identity(cfg, workspace_id, surface_id)
    if not identity or not identity["focused"]:
        return False
    bundle, pid = process_identity.v1_frontmost_app()
    return (
        bundle == identity["bundle"]
        and type(pid) is int
        and pid > 0
        and v1_unix_peer_pid(identity.get("socket_path")) == pid
        and v1_cmux_terminal_focus(cfg, workspace_id, surface_id)
    )


def apply_v1_focus_ack(state, source_id, evidence):
    """Acknowledge a verified episode only with unambiguous focus evidence."""
    if not all(
        evidence.get(key) is True
        for key in ("single_client", "foreground", "surface_focused")
    ):
        return False
    pane_id, episode = evidence.get("pane_id"), evidence.get("episode")
    if not isinstance(pane_id, str) or not isinstance(episode, str):
        return False
    source = state.get("v1", {}).get("sources", {}).get(source_id, {})
    return attention_ledger.force_acknowledge_attention(
        source.get("attention", {}), pane_id, episode
    )


def v1_verified_focus_episode(state, source_id, observation, snapshot):
    """Match a client's input target to a current, exact server completion."""
    if not isinstance(observation, dict) or not isinstance(snapshot, dict):
        return None
    boot = observation.get("boot_id")
    pane_id = observation.get("pane_id")
    if (
        not isinstance(boot, str)
        or not re.fullmatch(r"[0-9a-f]{32}", boot)
        or snapshot.get("server_boot_id") != boot
        or not isinstance(pane_id, str)
        or not pane_id
        or observation.get("outer_focused") is False
    ):
        return None
    agents = snapshot.get("agents")
    if not isinstance(agents, list) or any(not isinstance(a, dict) for a in agents):
        return None
    matches = [agent for agent in agents if agent.get("pane_id") == pane_id]
    if len(matches) > 1:
        return None
    entry = (
        state.get("v1", {})
        .get("sources", {})
        .get(source_id, {})
        .get("attention", {})
        .get(pane_id, {})
    )
    generation = entry.get("generation")
    terminal = entry.get("terminal_id")
    sequence = attention_ledger.v1_completion_sequence(entry)
    if (
        entry.get("status") != "done"
        or entry.get("provisional")
        or not isinstance(generation, str)
        or not generation
        or entry.get("server_boot_id") != boot
        or not isinstance(terminal, str)
        or not terminal
        or type(sequence) is not int
        or sequence < 0
    ):
        return None
    if matches:
        agent = matches[0]
        if (
            agent.get("agent_status") not in ("done", "idle")
            or agent.get("terminal_id") != terminal
            or type(agent.get("state_change_seq")) is not int
            or type(observation.get("agent_state_change_seq")) is not int
            or observation["agent_state_change_seq"] != agent["state_change_seq"]
        ):
            return None
    if not matches or matches[0]["state_change_seq"] != sequence:
        panes = snapshot.get("panes")
        presented = observation.get("pane_state")
        if not isinstance(panes, list) or any(not isinstance(p, dict) for p in panes):
            return None
        pane_matches = [pane for pane in panes if pane.get("pane_id") == pane_id]
        if len(pane_matches) != 1 or not isinstance(presented, dict):
            return None
        pane = pane_matches[0]
        release_sequence = pane.get("state_change_seq")
        if (
            (not matches and observation.get("agent_state_change_seq") is not None)
            or presented.get("agent_present") is not bool(matches)
            or pane.get("terminal_id") != terminal
            or presented.get("terminal_id") != terminal
            or type(release_sequence) is not int
            or release_sequence < sequence
            or type(presented.get("released_completion_seq")) is not int
            or presented["released_completion_seq"] != sequence
            or type(presented.get("state_change_seq")) is not int
            or presented["state_change_seq"] != release_sequence
            or (matches and matches[0]["state_change_seq"] != release_sequence)
            or (matches and pane.get("released_completion_seq") != sequence)
        ):
            return None
    episode = f"{generation}:{terminal}:{sequence}"
    return episode if entry.get("episode") == episode else None


def v1_surface_observation(cfg, workspace_id, surface_id):
    """Read the sole foreground Herdr client in the given cmux surface."""
    binary = cfg.get("cmux_bin") or core.DEFAULT_CMUX_BIN
    deadline = time.monotonic() + 20
    upstream_sockets = watcher.v1_local_socket_provider(deadline)
    try:
        proc = cmux.run(
            [binary, "--id-format", "both", "top", "--all", "--processes", "--json"],
            timeout=20,
        )
        if proc.returncode:
            return None
        tree = json.loads(proc.stdout)
        processes = process_identity.v1_foreground_surface_processes(
            tree, workspace_id, surface_id
        )
        state_home = Path(
            os.environ.get("XDG_STATE_HOME") or Path.home() / ".local/state"
        )
        directories = [
            state_home / name / "client-observers" for name in ("herdr", "herdr-dev")
        ]
        observation = discovery.v1_foreground_observation(
            processes, directories, upstream_sockets=upstream_sockets, deadline=deadline
        )
        if not observation:
            return None

        def matches(node, identity):
            return (
                isinstance(node, dict)
                and isinstance(identity, str)
                and any(
                    isinstance(node.get(key), str)
                    and node[key].casefold() == identity.casefold()
                    for key in ("id", "ref")
                )
            )

        surfaces = [
            surface
            for window in tree["windows"]
            for workspace in window["workspaces"]
            if matches(workspace, workspace_id)
            for pane in workspace["panes"]
            for surface in pane["surfaces"]
            if matches(surface, surface_id)
        ]
        if len(surfaces) != 1:
            return None
        scoped = {
            "windows": [
                {
                    "workspaces": [
                        {
                            "id": workspace_id,
                            "panes": [
                                {"surfaces": [{**surfaces[0], "id": surface_id}]}
                            ],
                        }
                    ]
                }
            ]
        }
        scan = discovery.v1_discover_clients_in_tree(
            scoped, directories, upstream_sockets=upstream_sockets, deadline=deadline
        )
        if not scan or not scan["complete"]:
            return None
        matches = [
            client
            for client in scan["clients"]
            if client["pid"] == observation.get("pid")
            and client["instance"] == observation.get("instance")
            and client["workspace"].casefold() == workspace_id.casefold()
            and client["surface"].casefold() == surface_id.casefold()
        ]
        if len(matches) != 1:
            return None
        if "native_ownership" in matches[0]:
            observation = {
                **observation,
                "native_ownership": matches[0]["native_ownership"],
            }
        return observation
    except (
        OSError,
        ValueError,
        TypeError,
        KeyError,
        AttributeError,
        RecursionError,
        subprocess.TimeoutExpired,
    ):
        return None


def v1_focus_observations_match(before, after):
    """Compare focused identity while allowing unrelated projection updates."""
    if not isinstance(before, dict) or not isinstance(after, dict):
        return False
    first, second = before.get("observation"), after.get("observation")
    if not isinstance(first, dict) or not isinstance(second, dict):
        return False
    old_revision, new_revision = (
        first.get("snapshot_revision"),
        second.get("snapshot_revision"),
    )
    if (old_revision is not None or new_revision is not None) and (
        type(old_revision) is not int
        or type(new_revision) is not int
        or not 0 <= old_revision <= new_revision < 2**64
    ):
        return False
    first = {key: value for key, value in first.items() if key != "snapshot_revision"}
    second = {key: value for key, value in second.items() if key != "snapshot_revision"}
    return {**before, "observation": first} == {**after, "observation": second}


def v1_has_acknowledgeable_completion(state, source_id):
    attention = (
        state.get("v1", {}).get("sources", {}).get(source_id, {}).get("attention", {})
    )
    return any(
        isinstance(entry, dict)
        and entry.get("status") == "done"
        and not entry.get("provisional")
        for entry in attention.values()
    )


def v1_reconcile_focus(cfg, client_id, source_id, workspace_id, surface_id, sock_path):
    """Acknowledge a completion only after two matching focus observations."""
    try:
        # Focus observation spawns several cmux processes; skip it while the
        # source has nothing focus could acknowledge.
        if not v1_has_acknowledgeable_completion(core.load_state(), source_id):
            return False
        if not v1_cmux_is_foreground(cfg, workspace_id, surface_id):
            return False
        before = v1_surface_observation(cfg, workspace_id, surface_id)
        if not before:
            return False
        snapshot = remote_bridge.socket_rpc(
            sock_path, "session.snapshot", {}, timeout=3
        ).get("snapshot")
        after = v1_surface_observation(cfg, workspace_id, surface_id)
        if not v1_focus_observations_match(before, after) or not v1_cmux_is_foreground(
            cfg, workspace_id, surface_id
        ):
            return False
        observation = after["observation"]
        with core.state_lock():
            state = core.load_state()
            publication.prune_v1_clients(state)
            client = state.get("v1", {}).get("clients", {}).get(client_id, {})
            if any(
                client.get(key) != value
                for key, value in (
                    ("source", source_id),
                    ("workspace", workspace_id),
                    ("surface", surface_id),
                )
            ):
                return False
            if (
                type(client.get("observed_client_pid")) is not int
                or not isinstance(client.get("observed_client_instance"), str)
                or not client["observed_client_instance"]
                or not isinstance(client.get("endpoint"), str)
                or not client["endpoint"]
                or type(client.get("connection_generation")) is not int
                or not isinstance(client.get("server_boot_id"), str)
                or after.get("pid") != client["observed_client_pid"]
                or after.get("instance") != client["observed_client_instance"]
                or observation.get("endpoint") != client["endpoint"]
                or observation.get("connection_generation")
                != client["connection_generation"]
                or observation.get("boot_id") != client["server_boot_id"]
                or after.get("native_ownership") != client.get("native_ownership")
            ):
                return False
            episode = v1_verified_focus_episode(state, source_id, observation, snapshot)
            if not episode:
                return False
            evidence = {
                "single_client": True,
                "foreground": True,
                "surface_focused": True,
                "pane_id": observation["pane_id"],
                "episode": episode,
            }
            if not apply_v1_focus_ack(state, source_id, evidence):
                return False
            core.save_state(state)
            return True
    except (
        remote_bridge.SocketError,
        OSError,
        ValueError,
        KeyError,
        TypeError,
        AttributeError,
    ):
        return False


def v1_force_read_action():
    """Send the invoking pane's verified completion ID through Herdr metadata."""
    fence_token = None
    try:
        context = json.loads(os.environ.get("HERDR_PLUGIN_CONTEXT_JSON", "{}"))
        pane_id = context.get("focused_pane_id")
        sock_path = os.environ.get("HERDR_SOCKET_PATH")
        if not isinstance(pane_id, str) or not pane_id or not sock_path:
            return False
        parent_pid = os.getppid()
        identity = remote_bridge.v1_herdr_process_identity(parent_pid)
        if identity is None:
            return False
        fence_token = core.HERDR_SOURCE_FENCE.set(
            {
                "path": str(sock_path),
                "generation": core.v1_local_generation(sock_path),
                "pid": parent_pid,
                "start_token": identity["start_token"],
            }
        )
        snapshot = (
            remote_bridge.socket_rpc(
                sock_path, "session.snapshot", {}, expected_peer=parent_pid
            ).get("snapshot")
            or {}
        )
        boot = snapshot.get("server_boot_id")
        if boot is not None and (
            not isinstance(boot, str) or not re.fullmatch(r"[0-9a-f]{32}", boot)
        ):
            return False
        peer = parent_pid if boot is None else None
        peer_options = {"expected_peer": peer} if peer is not None else {}
        pane = remote_bridge.socket_rpc(
            sock_path, "pane.get", {"pane_id": pane_id}, **peer_options
        ).get("pane", {})
        status = pane.get("agent_status")
        if pane.get("pane_id") != pane_id or not (
            status in ("done", "idle")
            or status == "unknown"
            and pane.get("agent") is None
        ):
            return False
        token = (pane.get("tokens") or {}).get("cmux_herdr_done")
        if not isinstance(token, str) or not token:
            return False
        params = {
            "pane_id": pane_id,
            "source": "cmux-herdr.v1",
            "tokens": {"cmux_herdr_ack": token},
            "ttl_ms": 30000,
        }
        if boot is not None:
            params["expected_server_boot_id"] = boot
        method = "pane.report_metadata_if_boot" if boot else "pane.report_metadata"
        remote_bridge.socket_rpc(sock_path, method, params, **peer_options)
        return True
    except (
        remote_bridge.SocketError,
        OSError,
        ValueError,
        TypeError,
        AttributeError,
    ) as e:
        core.log(f"v1 force read failed: {e}")
        return False
    finally:
        if fence_token is not None:
            core.HERDR_SOURCE_FENCE.reset(fence_token)
