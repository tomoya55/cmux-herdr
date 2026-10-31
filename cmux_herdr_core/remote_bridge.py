"""Herdr socket RPC and the legacy SSH remote bridge."""

import hashlib
import json
import math
import os
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

from cmux_herdr_core import cmux, core, process_identity


class SocketError(Exception):
    pass


def v1_herdr_process_identity(pid):
    if sys.platform == "darwin":
        return process_identity.v1_native_process_info(pid)
    if sys.platform.startswith("linux") and type(pid) is int and pid > 1:
        try:
            directory = Path("/proc") / str(pid)
            if directory.stat().st_uid != os.getuid():
                return None
            fields = (directory / "stat").read_text().rsplit(")", 1)[1].split()
            return {"start_token": int(fields[19])}
        except (OSError, ValueError, IndexError):
            return None
    return None


def v1_check_herdr_source_fence(sock_path):
    fence = core.HERDR_SOURCE_FENCE.get()
    if fence is None:
        return None
    if (
        str(sock_path) != fence["path"]
        or core.v1_local_generation(sock_path) != fence["generation"]
    ):
        raise SocketError("Herdr source socket changed")
    identity = v1_herdr_process_identity(fence["pid"])
    if identity is None or identity.get("start_token") != fence["start_token"]:
        raise SocketError("Herdr source process changed")
    return fence["pid"]


def v1_check_connected_herdr_peer(sock, expected_peer):
    if expected_peer is None:
        return
    if sys.platform == "darwin":
        peer_pid = core.darwin_peer_pid(sock)
    elif sys.platform.startswith("linux"):
        credentials = sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
        peer_pid, _uid, _gid = struct.unpack("3i", credentials)
    else:
        raise SocketError("cannot verify Herdr socket peer")
    if peer_pid != expected_peer:
        raise SocketError("Herdr server incarnation changed")


def connect_herdr_socket(sock_path, timeout=None, *, expected_peer=None):
    """Connect to a Herdr API socket and verify the server process behind it."""
    conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        if timeout is not None:
            conn.settimeout(timeout)
        conn.connect(str(sock_path))
        fenced_peer = v1_check_herdr_source_fence(sock_path)
        if (
            expected_peer is not None
            and fenced_peer is not None
            and expected_peer != fenced_peer
        ):
            raise SocketError("Herdr server identity conflict")
        v1_check_connected_herdr_peer(conn, expected_peer or fenced_peer)
    except BaseException:
        conn.close()
        raise
    return conn


def send_herdr_request(conn, request_id, method, params):
    request = {"id": request_id, "method": method, "params": params}
    conn.sendall(json.dumps(request).encode() + b"\n")


def socket_rpc(sock_path, method, params, timeout=10, *, expected_peer=None):
    """Single NDJSON request/response against a herdr API socket."""
    s = connect_herdr_socket(sock_path, timeout, expected_peer=expected_peer)
    try:
        send_herdr_request(s, "1", method, params)
        buf = b""
        while b"\n" not in buf:
            chunk = s.recv(1 << 16)
            if not chunk:
                raise SocketError(f"{method}: connection closed")
            buf += chunk
        msg = json.loads(buf.split(b"\n", 1)[0])
    finally:
        s.close()
    if "error" in msg:
        raise SocketError(f"{method}: {msg['error'].get('message')}")
    return msg.get("result") or {}


def remote_name(rcfg):
    if rcfg.get("name"):
        return rcfg["name"]
    target, session = rcfg["ssh_target"], rcfg["session"]
    if ":" in target or ":" in session:
        # IPv6 / ssh:// forms would make "target:session" ambiguous.
        digest = hashlib.sha1(f"{target}\0{session}".encode()).hexdigest()[:8]
        return f"remote-{digest}"
    return f"{target}:{session}"


def remote_key(name):
    return f"{core.REMOTE_KEY_PREFIX}{name}"


def remote_sock_path(name):
    # AF_UNIX paths are limited to ~104 bytes on macOS, so keep this short
    # and independent of the (unbounded) configured remote name.
    digest = hashlib.sha1(name.encode()).hexdigest()[:8]
    return Path(tempfile.gettempdir()) / f"cmux-herdr-{os.getuid()}-{digest}.sock"


def clear_remote_pill_keys(cfg, name, entry, ref):
    """Clear the aggregate pill and any per-pane pills; True when cmux
    accepted every clear."""
    base = remote_key(name)
    keys = {base}
    keys |= {cmux.pane_key(base, pid) for pid in entry.get("panes", {})}
    # Also catch per-pane pills left behind by older state (e.g. written by
    # a version where per-pane pills were opt-in) so teardown never
    # strands them.
    for k in cmux.cmux_herdr_status_keys(cfg, ref):
        if k == base or k.startswith(base + "."):
            keys.add(k)
    ok = True
    for k in sorted(keys):
        if not cmux.cmux_cli(cfg, ["clear-status", k, "--workspace", ref]):
            ok = False
    return ok


def clear_remote_pill(cfg, name, entry, ref):
    """Clear a pill, remembering it for retry when cmux rejects the call."""
    if clear_remote_pill_keys(cfg, name, entry, ref):
        return
    pending = entry.get("pending_clear")
    if isinstance(pending, str):  # scalar format from an earlier version
        pending = [pending]
    elif not isinstance(pending, list):
        pending = []
    if ref not in pending:
        pending.append(ref)
    entry["pending_clear"] = pending


def retry_pending_clears(cfg, name, entry):
    pending = entry.get("pending_clear")
    if isinstance(pending, str):  # scalar format from an earlier version
        pending = [pending]
    remaining = []
    for ref in pending or []:
        if not clear_remote_pill_keys(cfg, name, entry, ref):
            remaining.append(ref)
    if remaining:
        entry["pending_clear"] = remaining
    else:
        entry.pop("pending_clear", None)


def remote_ref(cfg, rstate, name, entry):
    """Resolve the cmux workspace ref for a remote via its configured title."""
    if entry.get("pending_clear"):
        retry_pending_clears(cfg, name, entry)
    title = entry.get("cmux_title") or ""
    cached = entry.get("ref")
    if cached and time.time() - entry.get("ref_at", 0) < core.REMOTE_REF_TTL_SECONDS:
        return cached
    ref = None
    if title:
        ref = cmux.cmux_workspaces_by_title(cfg).get(title.strip().lower())
    if ref:
        if cached and ref != cached:
            # The title now resolves elsewhere; don't leave anything behind.
            clear_remote_pill(cfg, name, entry, cached)
            remote_mark_read_ref(cfg, rstate, name, cached)
        entry["ref"] = ref
        entry["ref_at"] = time.time()
        return ref
    if cached:
        age = time.time() - entry.get("ref_at", 0)
        if age < core.REMOTE_STALE_REF_TTL_SECONDS:
            return cached  # tolerate transient cmux lookup failures
        # Unresolvable for too long: drop the pill from the stale workspace.
        clear_remote_pill(cfg, name, entry, cached)
        entry.pop("ref", None)
        entry.pop("ref_at", None)
        entry["pill_published"] = False
        return None
    core.log(f"no cmux workspace titled {title!r} for remote {name}; skipping")
    return None


def remote_notify(cfg, rstate, name, entry, title, body):
    ref = remote_ref(cfg, rstate, name, entry)
    if not ref:
        return False
    if not cmux.legacy_notification_preflight(cfg, ref):
        return False
    return cmux.cmux_cli(
        cfg,
        [
            "notify",
            "--title",
            title,
            "--subtitle",
            core.NOTIFICATION_SUBTITLE,
            "--body",
            body,
            "--workspace",
            ref,
        ],
    )


def remote_mark_read_ref(cfg, rstate, name, ref):
    """Legacy remote notifications lack IDs; leave unrelated notices untouched."""
    return


def remote_mark_read(cfg, rstate, name, entry):
    ref = remote_ref(cfg, rstate, name, entry)
    if ref:
        remote_mark_read_ref(cfg, rstate, name, ref)


def update_remote_pill(cfg, rstate, name, entry):
    ref = remote_ref(cfg, rstate, name, entry)
    entry["pill_at"] = time.time()
    if not ref:
        entry["pill_published"] = False
        return
    cmux.push_pill(cfg, ref, remote_key(name), entry.get("panes", {}))
    entry["pill_published"] = True


def reroute_remote(cfg, rstate, name, entry, cmux_title):
    """Move a remote to a new cmux workspace title, clearing the old one."""
    old_ref = entry.pop("ref", None)
    entry.pop("ref_at", None)
    if old_ref:
        clear_remote_pill(cfg, name, entry, old_ref)
        remote_mark_read_ref(cfg, rstate, name, old_ref)
    entry["cmux_title"] = cmux_title
    entry["pill_published"] = False


def apply_remote_snapshot(cfg, rstate, name, rcfg, agents, labels):
    """Diff one remote agent.list snapshot against state and drive cmux.

    Returns True when the tracked pane set changed.
    """
    entry = rstate["remotes"].setdefault(name, {"panes": {}})
    changed = False
    identity = [rcfg["ssh_target"], rcfg["session"]]
    same_identity = entry.get("identity") == identity
    if not same_identity:
        # Retargeted: the cached socket path belongs to the previous target.
        entry["identity"] = identity
        entry.pop("socket_path", None)
        entry.pop("pending_notifications", None)
        changed = True
    cmux_title = rcfg.get("cmux_title") or ""
    if entry.get("cmux_title") != cmux_title:
        reroute_remote(cfg, rstate, name, entry, cmux_title)
        changed = True

    old = entry["panes"]
    old_statuses = {pid: p["status"] for pid, p in old.items()}
    new = {}
    for a in agents:
        status = a.get("agent_status")
        if (
            same_identity
            and status == "unknown"
            and old.get(a.get("pane_id"), {}).get("status") == "blocked"
        ):
            new[a["pane_id"]] = old[a["pane_id"]]
            continue
        if status not in core.TRACKED_STATUSES:
            continue
        new[a["pane_id"]] = {
            "status": status,
            "agent": a.get("display_agent") or a.get("agent") or "agent",
            "title": a.get("terminal_title_stripped") or a.get("terminal_title") or "",
            "workspace_id": a.get("workspace_id") or "",
        }
    if same_identity:
        present = {a.get("pane_id") for a in agents}
        for pane_id, pane in old.items():
            if pane_id not in present and pane.get("status") == "blocked":
                new[pane_id] = pane
    pending = entry.setdefault("pending_notifications", {})
    for pane_id in list(pending):
        if new.get(pane_id, {}).get("status") != pending[pane_id]:
            del pending[pane_id]
            changed = True

    label = cmux_title or name
    observed_statuses = {a.get("pane_id"): a.get("agent_status") for a in agents}
    for pid, p in new.items():
        if observed_statuses.get(pid) != p["status"]:
            continue
        prev = old_statuses.get(pid)
        if prev == p["status"] and pending.get(pid) != p["status"]:
            continue
        what = p["title"] or labels.get(p["workspace_id"]) or pid
        if p["status"] in core.ATTENTION_STATUSES:
            body = f"{label} · {what}"
            title = (
                f"{p['agent']}: waiting for input"
                if p["status"] == "blocked"
                else f"{p['agent']}: finished"
            )
            sent = remote_notify(cfg, rstate, name, entry, title, body)
            if sent:
                if pid in pending:
                    del pending[pid]
                    changed = True
            elif pending.get(pid) != p["status"]:
                pending[pid] = p["status"]
                changed = True

    if new == old:
        # Republish when the pill was never pushed (e.g. the cmux workspace
        # did not exist yet, or the title just changed) or may have been lost
        # (e.g. a cmux restart).
        stale = time.time() - entry.get("pill_at", 0) > core.REMOTE_REF_TTL_SECONDS
        if stale or not entry.get("pill_published"):
            update_remote_pill(cfg, rstate, name, entry)
        return changed

    removed = [pid for pid in old if pid not in new]
    if removed:
        ref = remote_ref(cfg, rstate, name, entry)
        if ref:
            for pid in removed:
                cmux.cmux_cli(
                    cfg,
                    [
                        "clear-status",
                        cmux.pane_key(remote_key(name), pid),
                        "--workspace",
                        ref,
                    ],
                )

    was_attention = cmux.has_attention_panes(old)
    entry["panes"] = new
    changed = True
    if was_attention and not cmux.has_attention_panes(new):
        # The prompt was answered or dismissed on the remote side.
        remote_mark_read(cfg, rstate, name, entry)
    update_remote_pill(cfg, rstate, name, entry)
    return changed


def resolve_remote_socket(rcfg):
    ssh_target = rcfg["ssh_target"]
    session = rcfg["session"]
    # Note: non-interactive ssh gets a minimal PATH on the remote, so
    # herdr_bin may need to be an absolute path in the config.
    herdr_bin = rcfg.get("herdr_bin") or "herdr"
    proc = cmux.run(
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            ssh_target,
            herdr_bin,
            "session",
            "list",
            "--json",
        ],
        timeout=20,
    )
    if proc.returncode != 0:
        raise SocketError(
            f"ssh {ssh_target} {herdr_bin} session list failed: {proc.stderr.strip()}"
        )
    for s in json.loads(proc.stdout).get("sessions", []):
        if s.get("name") == session:
            path = s.get("socket_path")
            if path:
                return path
    raise SocketError(f"session {session!r} not found on {ssh_target}")


def drop_forward(name, entry, procs):
    proc = procs.pop(name, None)
    if proc is not None and proc.poll() is None:
        proc.terminate()
    entry.pop("socket_path", None)
    remote_sock_path(name).unlink(missing_ok=True)


def ensure_forward(name, rcfg, entry, procs):
    """Return the local end of an ssh unix-socket forward, (re)spawning it."""
    if core.DAEMON_SHUTDOWN.is_set():
        raise SocketError("daemon is shutting down")
    local_sock = remote_sock_path(name)
    proc = procs.get(name)
    if proc is not None and proc.poll() is None and local_sock.exists():
        return local_sock
    if proc is not None:
        drop_forward(name, entry, procs)
    remote_path = entry.get("socket_path")
    if not remote_path:
        remote_path = resolve_remote_socket(rcfg)
        entry["socket_path"] = remote_path
    if core.DAEMON_SHUTDOWN.is_set():
        raise SocketError("daemon is shutting down")
    local_sock.unlink(missing_ok=True)
    previous_handler = None
    pending = []
    if threading.current_thread() is threading.main_thread():
        previous_handler = signal.getsignal(signal.SIGTERM)
        if callable(previous_handler):
            signal.signal(
                signal.SIGTERM, lambda sig, frame: pending.append((sig, frame))
            )
    try:
        procs[name] = subprocess.Popen(
            [
                "ssh",
                "-N",
                "-T",
                "-o",
                "BatchMode=yes",
                "-o",
                "ExitOnForwardFailure=yes",
                "-o",
                "ServerAliveInterval=15",
                "-o",
                "ServerAliveCountMax=2",
                "-L",
                f"{local_sock}:{remote_path}",
                rcfg["ssh_target"],
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    finally:
        if callable(previous_handler):
            signal.signal(signal.SIGTERM, previous_handler)
            if pending:
                previous_handler(*pending[-1])
    deadline = time.time() + 10
    while time.time() < deadline:
        if procs[name].poll() is not None:
            drop_forward(name, entry, procs)
            raise SocketError(
                f"ssh forward to {rcfg['ssh_target']} failed to establish; "
                "non-interactive (key-based) SSH auth is required"
            )
        if local_sock.exists():
            return local_sock
        time.sleep(0.2)
    drop_forward(name, entry, procs)
    raise SocketError(f"timed out waiting for ssh forward to {rcfg['ssh_target']}")


def poll_interval(rcfg):
    try:
        value = float(rcfg.get("poll_seconds") or core.REMOTE_DEFAULT_POLL_SECONDS)
    except (TypeError, ValueError):
        return core.REMOTE_DEFAULT_POLL_SECONDS
    if not math.isfinite(value) or value <= 0:
        return core.REMOTE_DEFAULT_POLL_SECONDS
    return max(value, 0.5)


def fetch_remote(name, rcfg, entry, procs):
    """Network-only part of a poll; safe to run in a worker thread."""
    sock = ensure_forward(name, rcfg, entry, procs)
    try:
        agents = socket_rpc(sock, "agent.list", {}).get("agents", [])
        workspaces = socket_rpc(sock, "workspace.list", {}).get("workspaces", [])
    except (SocketError, OSError, json.JSONDecodeError):
        # Remote server restarted or tunnel went stale; rebuild it next time.
        drop_forward(name, entry, procs)
        raise
    labels = {}
    for w in workspaces:
        ws_id = w.get("workspace_id") or w.get("id")
        if ws_id:
            labels[ws_id] = w.get("label") or ""
    return agents, labels


def sweep_remote_orphan_pills(cfg, remotes):
    """Clear herdr.remote.* pills for remotes no longer in the config."""
    active = {remote_key(remote_name(r)) for r in remotes}
    # Per-pane pills of active remotes belong to them; everything else with
    # the remote prefix is orphaned.
    prefixes = tuple(k + "." for k in active)
    for ref in cmux.cmux_workspaces_by_title(cfg).values():
        for key in cmux.cmux_herdr_status_keys(cfg, ref):
            if (
                key.startswith(core.REMOTE_KEY_PREFIX)
                and key not in active
                and not key.startswith(prefixes)
            ):
                core.log(f"clearing orphaned remote status {key} on {ref}")
                cmux.cmux_cli(cfg, ["clear-status", key, "--workspace", ref])


def remote_configs(cfg):
    """Validated [[remotes]] entries. Returns None when any entry is
    unusable, so callers can keep the previous config instead of tearing
    remotes down over a typo."""
    raw = cfg.get("remotes", [])
    if not isinstance(raw, list):
        core.log("[[remotes]] must be an array of tables")
        return None
    remotes = []
    seen = set()
    for r in raw:
        if not isinstance(r, dict):
            core.log(f"invalid [[remotes]] entry {r!r}; expected a table")
            return None
        if not (
            isinstance(r.get("ssh_target"), str)
            and isinstance(r.get("session"), str)
            and r["ssh_target"]
            and r["session"]
        ):
            core.log(f"invalid [[remotes]] entry {r!r}; ssh_target/session required")
            return None
        if r.get("name") is not None and not isinstance(r["name"], str):
            core.log(f"remote {r!r}: name must be a string")
            return None
        if r.get("herdr_bin") is not None and not isinstance(r["herdr_bin"], str):
            core.log(f"remote {r!r}: herdr_bin must be a string")
            return None
        if not isinstance(r.get("cmux_title"), str) or not r.get("cmux_title"):
            core.log(f"remote {remote_name(r)!r}: cmux_title is required")
            return None
        name = remote_name(r)
        if name in seen:
            core.log(
                f"duplicate remote name {name!r}; set a unique `name` in [[remotes]]"
            )
            return None
        seen.add(name)
        remotes.append(r)
    return remotes


def retire_remote(cfg, rstate, name, procs, next_due):
    """Tear down a remote that was removed from the config or retargeted.

    Keeps a minimal entry behind when cmux rejects a pill clear, so a later
    daemon start retries the cleanup instead of stranding the pill.
    """
    entry = rstate["remotes"].get(name, {})
    drop_forward(name, entry, procs)
    next_due.pop(name, None)
    retry_pending_clears(cfg, name, entry)
    ref = entry.get("ref")
    if not ref:
        title = (entry.get("cmux_title") or "").strip().lower()
        if title:
            ref = cmux.cmux_workspaces_by_title(cfg).get(title)
    if ref:
        clear_remote_pill(cfg, name, entry, ref)
        remote_mark_read_ref(cfg, rstate, name, ref)
    if entry.get("pending_clear"):
        entry["panes"] = {}
        entry.pop("ref", None)
        core.log(f"remote {name}: retired, but pill cleanup is still pending")
    else:
        rstate["remotes"].pop(name, None)
    core.save_remote_state(rstate)
