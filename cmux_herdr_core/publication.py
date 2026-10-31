"""Publishing attention to cmux sidebars and notifications."""

import copy
import hashlib
import json
import os
import subprocess

from cmux_herdr_core import attention_ledger, cmux, core, legacy_daemons, remote_bridge


def sidebar_panes(panes, attention):
    """Combine live pane state with pending attention."""
    result = {}
    for pane_id, pane in panes.items():
        status = pane.get("status")
        if status == "done" and not attention_ledger.has_pending_attention(
            attention, pane_id
        ):
            continue
        if status not in core.TRACKED_STATUSES:
            pending_status = attention.get(pane_id, {}).get("status")
            if pending_status not in core.ATTENTION_STATUSES:
                continue
            status = pending_status
        result[pane_id] = {**pane, "status": status}
    return result


def v1_attention_title(attention):
    pending = [
        entry
        for entry in attention.values()
        if entry.get("status") in core.ATTENTION_STATUSES
    ]
    if not pending:
        return None
    waiting = sum(entry["status"] == "blocked" for entry in pending)
    done = len(pending) - waiting
    parts = [
        f"{count} {label}"
        for count, label in ((waiting, "waiting"), (done, "finished"))
        if count
    ]
    return "herdr: " + " · ".join(parts)


def reconcile_aggregate_notification(
    cfg, workspace_id, surface_id, attention, notifications, notification_id=None
):
    """Reconcile one surface without knowingly replacing another source's unread."""
    pending = [
        entry
        for entry in attention.values()
        if entry.get("status") in core.ATTENTION_STATUSES
    ]
    relevant = [
        item
        for item in notifications
        if str(item.get("workspace_id", "")).casefold() == workspace_id.casefold()
        and str(item.get("surface_id", "")).casefold() == surface_id.casefold()
    ]
    own = next(
        (
            item
            for item in relevant
            if item.get("id") == notification_id
            and item.get("subtitle") == core.NOTIFICATION_SUBTITLE
        ),
        None,
    )
    if not pending:
        if own and not own.get("is_read"):
            cmux.cmux_cli(cfg, ["mark-notification-read", "--id", notification_id])
        return
    if any(not item.get("is_read") and item is not own for item in relevant):
        return
    title = v1_attention_title(attention)
    if own and not own.get("is_read") and own.get("title") == title:
        return notification_id
    payload = {
        "workspace_id": workspace_id,
        "surface_id": surface_id,
        "title": title,
        "subtitle": core.NOTIFICATION_SUBTITLE,
        "effects": {
            "markUnread": True,
            "desktop": False,
            "sound": False,
            "command": False,
        },
    }
    binary = cfg.get("cmux_bin") or core.DEFAULT_CMUX_BIN
    try:
        proc = cmux.run(
            [binary, "rpc", "notification.create_for_target", json.dumps(payload)]
        )
        if proc.returncode:
            core.log(
                f"cmux notification.create_for_target failed: {proc.stderr.strip()}"
            )
            return None
        return json.loads(proc.stdout).get("id")
    except (OSError, subprocess.TimeoutExpired, ValueError) as e:
        core.log(f"cmux notification.create_for_target failed: {e}")
        return None


def v1_destination_key(workspace_id, surface_id):
    return json.dumps([workspace_id, surface_id], separators=(",", ":"))


def reconcile_v1_supervised_bindings(
    state, desired, ready, *, complete, supervisor_pid, certified_surfaces=None
):
    """Apply prepared bindings without retiring unknown or unready sources.

    The caller holds each prepared client's lease and serializes this update
    under the state lock. ``ready`` maps IDs to the exact prepared specification.
    """
    clients = state.setdefault("v1", {}).setdefault("clients", {})
    changed = False
    all_ready = True
    for client_id, spec in desired.items():
        existing = clients.get(client_id)
        if ready.get(client_id) != spec or existing and not existing.get("supervised"):
            all_ready = False
            continue
        record = {
            "source": core.v1_supervised_source_key(spec["target"]),
            "workspace": spec["workspace"],
            "surface": spec["surface"],
            "pid": supervisor_pid,
            "supervised": True,
            "server_boot_id": spec["target"].get("server_boot_id"),
            "connection_generation": spec["connection_generation"],
            "observed_client_pid": spec.get("observed_client_pid"),
            "observed_client_instance": spec.get("observed_client_instance"),
            "endpoint": spec.get("endpoint"),
        }
        if "native_ownership" in spec:
            record["native_ownership"] = spec["native_ownership"]
        target = spec["target"]
        if target.get("remote") and target.get("session"):
            record["legacy_target"] = {
                "remote": target["remote"],
                "session": target["session"],
            }
        elif spec.get("legacy_target"):
            record["legacy_target"] = spec["legacy_target"].copy()
        if existing != record:
            clients[client_id] = record
            changed = True
    if complete or certified_surfaces:
        groups = {}
        for key, spec in desired.items():
            groups.setdefault(
                (spec["workspace"].casefold(), spec["surface"].casefold()), {}
            )[key] = spec
        for client_id, client in list(clients.items()):
            target = (client.get("workspace"), client.get("surface"))
            normalized = (
                tuple(value.casefold() for value in target)
                if all(isinstance(value, str) for value in target)
                else None
            )
            group = groups.get(normalized, {})
            target_ready = all(
                ready.get(key) == spec and clients.get(key, {}).get("supervised")
                for key, spec in group.items()
            )
            certified = (
                bool(
                    certified_surfaces
                    and certified_surfaces.get(
                        (target[0].casefold(), target[1].casefold()), False
                    )
                )
                if all(isinstance(value, str) for value in target)
                else False
            )
            if (
                client.get("supervised")
                and client_id not in desired
                and (
                    (complete and (all_ready or (bool(group) and target_ready)))
                    or (certified and target_ready)
                )
            ):
                del clients[client_id]
                changed = True
    return changed


def prune_v1_clients(state):
    """Remove registrations whose client process no longer holds its lease."""
    clients = state.get("v1", {}).get("clients", {})
    for client_id in list(clients):
        with core.v1_source_leader("client:" + client_id) as stale:
            if stale:
                clients.pop(client_id, None)


def find_v1_legacy_notification(state, source_ids, workspace, surface, notifications):
    """Recognize old notifications only when old and live agent states agree."""
    words = {"blocked": "waiting for input", "done": "finished"}
    v1 = state.get("v1", {})
    clients = v1.get("clients", {}).values()
    sources = v1.get("sources", {})
    remote_state = core.load_remote_state().get("remotes", {})
    legacy_counts = {}
    for _, _, workspace_entry in core.all_workspaces(state):
        for pane in workspace_entry.get("panes", {}).values():
            key = (pane.get("agent"), pane.get("status"))
            legacy_counts[key] = legacy_counts.get(key, 0) + 1
    for remote_entry in remote_state.values():
        for pane in remote_entry.get("panes", {}).values():
            key = (pane.get("agent"), pane.get("status"))
            legacy_counts[key] = legacy_counts.get(key, 0) + 1
    expected = set()
    for client in clients:
        source_id = client.get("source")
        target = client.get("legacy_target") or {}
        if (
            source_id not in source_ids
            or str(client.get("workspace", "")).casefold() != workspace.casefold()
            or str(client.get("surface", "")).casefold() != surface.casefold()
            or "session" not in target
        ):
            continue
        session = target["session"] or "default"
        old_panes = {}
        if target.get("remote"):
            identity = [target["remote"], session]
            for entry in remote_state.values():
                if entry.get("identity") == identity:
                    old_panes.update(entry.get("panes", {}))
        else:
            names = {session} if session != "default" else {"", "default"}
            for name in names:
                bucket = state.get("sessions", {}).get(name, {})
                for entry in bucket.get("workspaces", {}).values():
                    old_panes.update(entry.get("panes", {}))
        source = sources.get(source_id, {})
        for pane_id, attention in source.get("attention", {}).items():
            status = attention.get("status")
            agent = source.get("panes", {}).get(pane_id, {}).get("agent")
            old = old_panes.get(pane_id, {})
            if (
                status in words
                and agent
                and old.get("status") == status
                and old.get("agent") == agent
                and legacy_counts.get((agent, status)) == 1
            ):
                expected.add(f"{agent}: {words[status]}")
    matches = [
        item.get("id")
        for item in notifications
        if str(item.get("workspace_id", "")).casefold() == workspace.casefold()
        and str(item.get("surface_id", "")).casefold() == surface.casefold()
        and item.get("subtitle") == core.NOTIFICATION_SUBTITLE
        and item.get("title") in expected
        and not item.get("is_read")
        and item.get("id")
    ]
    return matches[0] if len(matches) == 1 else None


def reconcile_v1_destination(
    cfg, state, workspace_id, surface_id, source_ids, *, is_current=lambda: True
):
    """Combine attention for a destination and track the aggregate's ID."""
    notifications = cmux.cmux_notifications_checked(cfg)
    if notifications is None or not is_current():
        return False
    v1 = state.setdefault("v1", {})
    sources = v1.setdefault("sources", {})
    attention = {}
    for source_id in source_ids:
        for pane_id, entry in sources.get(source_id, {}).get("attention", {}).items():
            attention[json.dumps([source_id, pane_id])] = entry
    destinations = v1.setdefault("destinations", {})
    matching_keys = []
    for key in destinations:
        try:
            w, s = json.loads(key)
            if (
                w.casefold() == workspace_id.casefold()
                and s.casefold() == surface_id.casefold()
            ):
                matching_keys.append(key)
        except (TypeError, ValueError, AttributeError):
            continue
    if len(matching_keys) > 1:
        matching_keys.sort()
        canonical = matching_keys[0]
        owned_ids = {
            destinations[key].get("notification_id") for key in matching_keys
        } - {None, ""}
        visible = [
            item
            for item in notifications
            if item.get("id") in owned_ids
            and item.get("subtitle") == core.NOTIFICATION_SUBTITLE
            and str(item.get("workspace_id", "")).casefold() == workspace_id.casefold()
            and str(item.get("surface_id", "")).casefold() == surface_id.casefold()
        ]
        chosen = next(
            (item.get("id") for item in visible if not item.get("is_read")), None
        )
        chosen = (
            chosen
            or next((item.get("id") for item in visible), None)
            or next(iter(sorted(owned_ids)), None)
        )
        for item in visible:
            if item.get("id") != chosen and not item.get("is_read"):
                if not is_current() or not cmux.cmux_cli(
                    cfg, ["mark-notification-read", "--id", item["id"]]
                ):
                    return False
                item["is_read"] = True
        if not is_current():
            return False
        merged = destinations[canonical].copy()
        if chosen:
            merged["notification_id"] = chosen
        for key in matching_keys:
            destinations.pop(key)
        destinations[canonical] = merged
        matching_keys = [canonical]
    destination = destinations.setdefault(
        matching_keys[0]
        if matching_keys
        else v1_destination_key(workspace_id, surface_id),
        {},
    )
    notification_id = destination.get("notification_id")
    if not notification_id:
        notification_id = find_v1_legacy_notification(
            state, source_ids, workspace_id, surface_id, notifications
        )
        if notification_id:
            destination["notification_id"] = notification_id
    created_id = reconcile_aggregate_notification(
        cfg,
        workspace_id,
        surface_id,
        attention,
        notifications,
        notification_id,
    )
    if created_id:
        destination["notification_id"] = created_id
    return True


def retire_v1_destination(cfg, state, key, *, is_current=lambda: True):
    destinations = state.get("v1", {}).get("destinations", {})
    destination = destinations.get(key)
    if destination is None:
        return True
    try:
        workspace, surface = json.loads(key)
    except (TypeError, ValueError):
        return False
    notifications = cmux.cmux_notifications_checked(cfg)
    if notifications is None or not is_current():
        return False
    owned = next(
        (
            item
            for item in notifications
            if item.get("id") == destination.get("notification_id")
            and item.get("subtitle") == core.NOTIFICATION_SUBTITLE
            and str(item.get("workspace_id", "")).casefold() == workspace.casefold()
            and str(item.get("surface_id", "")).casefold() == surface.casefold()
        ),
        None,
    )
    if (
        owned
        and not owned.get("is_read")
        and not cmux.cmux_cli(cfg, ["mark-notification-read", "--id", owned["id"]])
    ):
        return False
    destinations.pop(key, None)
    return True


def reconcile_v1_legacy_sidebar(cfg, state, remote_state, *, is_current=lambda: True):
    """Retire known legacy status keys, retrying failed cmux requests."""
    targets = set()
    clients = state.get("v1", {}).get("clients", {}).values()
    active_refs = {
        client.get("workspace") for client in clients if client.get("workspace")
    }
    active_refs |= cmux.cmux_workspace_refs(cfg)
    for bucket in state.get("sessions", {}).values():
        cached = (bucket.get("session_ref") or {}).get("ref")
        for ws_id, entry in bucket.get("workspaces", {}).items():
            label = entry.get("label") or ""
            refs = active_refs | {
                cfg.get("workspaces", {}).get(ws_id),
                cfg.get("labels", {}).get(label),
                cached,
            }
            base = cmux.status_key(ws_id)
            for ref in refs - {None, ""}:
                targets.add((ref, base))
                targets.update(
                    (ref, cmux.pane_key(base, pid)) for pid in entry.get("panes", {})
                )
    for name, entry in remote_state.get("remotes", {}).items():
        pending = entry.get("pending_clear") or []
        if isinstance(pending, str):
            pending = [pending]
        refs = active_refs | {ref for ref in [entry.get("ref"), *pending] if ref}
        base = remote_bridge.remote_key(name)
        for ref in refs:
            targets.add((ref, base))
            targets.update(
                (ref, cmux.pane_key(base, pid)) for pid in entry.get("panes", {})
            )
    refs = {ref for ref, _ in targets}
    for ref in sorted(refs):
        visible = cmux.cmux_herdr_status_keys_checked(cfg, ref)
        if visible is None:
            continue
        for key in sorted(
            set(visible) & {key for target, key in targets if target == ref}
        ):
            if is_current():
                cmux.cmux_cli(cfg, ["clear-status", key, "--workspace", ref])


def reconcile_v1_sidebar(cfg, state, workspaces, *, allowed=None, is_current=None):
    """Maintain only sidebar keys owned by the active v1 clients."""
    v1 = state.setdefault("v1", {})
    sources = v1.setdefault("sources", {})
    tracked = v1.setdefault("sidebar_keys", {})
    aliases = {}
    for workspace in set(workspaces) | set(tracked):
        aliases.setdefault(workspace.casefold(), set()).add(workspace)
    for names in aliases.values():
        workspace = sorted(names & set(tracked) or names)[0]
        if allowed is not None and workspace.casefold() not in {
            value.casefold() for value in allowed
        }:
            continue
        if is_current is not None and not is_current(workspace):
            continue
        desired = set()
        previous = {key for name in names for key in tracked.get(name, [])}
        interrupted = False
        workspace_sources = {
            source for name in names for source in workspaces.get(name, ())
        }
        for source_id in sorted(workspace_sources):
            source = sources.get(source_id, {})
            visible = sidebar_panes(
                source.get("panes", {}), source.get("attention", {})
            )
            groups = {}
            for pane_id, pane in visible.items():
                group = pane.get("workspace_id", "")
                digest = hashlib.sha256(pane_id.encode()).hexdigest()
                groups.setdefault(group, {})[digest] = pane
            for group, panes in groups.items():
                identity = json.dumps([source_id, group], separators=(",", ":"))
                base = "herdr.v1." + hashlib.sha256(identity.encode()).hexdigest()
                if is_current is None:
                    cmux.push_pill(cfg, workspace, base, panes)
                elif not cmux.push_pill(
                    cfg,
                    workspace,
                    base,
                    panes,
                    is_current=lambda workspace=workspace: is_current(workspace),
                ):
                    interrupted = True
                desired.add(base)
                desired.update(cmux.pane_key(base, pane_id) for pane_id in panes)
                if interrupted:
                    break
            if interrupted:
                break
        if interrupted:
            desired.update(previous)
        for key in previous - desired:
            if (
                is_current is not None and not is_current(workspace)
            ) or not cmux.cmux_cli(
                cfg, ["clear-status", key, "--workspace", workspace]
            ):
                desired.add(key)
        if desired:
            tracked[workspace] = sorted(desired)
        else:
            tracked.pop(workspace, None)
        for name in names - {workspace}:
            tracked.pop(name, None)


def v1_destination_inputs_current(snapshot, workspace, surface):
    """Reject a stale destination plan before sending its cmux notification."""

    def relevant(state):
        v1 = state.get("v1", {})
        clients = {
            key: value
            for key, value in v1.get("clients", {}).items()
            if str(value.get("workspace", "")).casefold() == workspace.casefold()
            and str(value.get("surface", "")).casefold() == surface.casefold()
        }
        sources = {}
        for client in clients.values():
            source_id = client.get("source")
            source = v1.get("sources", {}).get(source_id)
            sources[source_id] = (
                {
                    key: value
                    for key, value in source.items()
                    if key != "published_tokens"
                }
                if isinstance(source, dict)
                else source
            )
        destinations = {}
        for key, value in v1.get("destinations", {}).items():
            try:
                w, s = json.loads(key)
                if (
                    w.casefold() == workspace.casefold()
                    and s.casefold() == surface.casefold()
                ):
                    destinations[key] = value
            except (TypeError, ValueError, AttributeError):
                continue
        return clients, sources, destinations

    with core.state_lock():
        return relevant(snapshot) == relevant(core.load_state())


def v1_notification_backend():
    """Read the backend marker without following links or blocking on a FIFO."""
    try:
        fd = os.open(
            core.state_dir / "v1-notification-backend",
            os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
        )
    except FileNotFoundError:
        return "ordinary"
    try:
        core._v1_check_private_file(
            os.fstat(fd), "invalid notification backend file", max_size=32
        )
        return os.read(fd, 33).decode("utf-8")
    finally:
        os.close(fd)


def publish_v1_clients(
    cfg,
    *,
    wait=False,
    eligible_destinations=None,
    eligibility_check=None,
    workspace_check=None,
    notifications=True,
):
    """Reconcile each registered surface once using all of its sources."""
    with core.v1_source_leader("publication", blocking=wait) as leader:
        if not leader or not legacy_daemons.retire_v1_legacy_daemons():
            return
        try:
            backend = v1_notification_backend()
        except (OSError, UnicodeError, ValueError) as error:
            core.log(f"notification backend unavailable: {error}")
            backend = "unavailable"
        if backend not in {"ordinary", "preparing-owned", "owned"}:
            core.log("invalid notification backend; ordinary notifications disabled")
        notifications = notifications and backend == "ordinary"
        for _ in range(3):
            if not publish_v1_clients_once(
                cfg,
                eligible_destinations=eligible_destinations,
                eligibility_check=eligibility_check,
                workspace_check=workspace_check,
                notifications=notifications,
            ):
                break


def v1_publication_inputs(state):
    """Capture the client and source data used to produce notifications."""
    v1 = state.get("v1", {})
    sources = copy.deepcopy(v1.get("sources", {}))
    for source in sources.values():
        if isinstance(source, dict):
            source.pop("published_tokens", None)
    return copy.deepcopy(v1.get("clients", {})), sources


def v1_workspace_inputs_current(snapshot, workspace):
    def relevant(state):
        clients, sources = v1_publication_inputs(state)
        clients = {
            key: value
            for key, value in clients.items()
            if str(value.get("workspace", "")).casefold() == workspace.casefold()
        }
        return clients, {
            client.get("source"): sources.get(client.get("source"))
            for client in clients.values()
        }

    with core.state_lock():
        return relevant(snapshot) == relevant(core.load_state())


def publish_v1_clients_once(
    cfg,
    *,
    eligible_destinations=None,
    eligibility_check=None,
    workspace_check=None,
    notifications=True,
):
    with core.state_lock():
        state = core.load_state()
        prune_v1_clients(state)
        core.save_state(state)
    baseline = copy.deepcopy(state.get("v1", {}))
    inputs = v1_publication_inputs(state)
    destinations = {}
    target_names = {}
    for client in state.get("v1", {}).get("clients", {}).values():
        workspace = client.get("workspace")
        surface = client.get("surface")
        source = client.get("source")
        if workspace and surface and source:
            normalized = (workspace.casefold(), surface.casefold())
            target = target_names.setdefault(normalized, (workspace, surface))
            destinations.setdefault(target, set()).add(source)
    workspaces = {}
    for (workspace, _surface), sources in destinations.items():
        workspaces.setdefault(workspace, set()).update(sources)

    def includes(scope, target):
        return any(
            w.casefold() == target[0].casefold()
            and s.casefold() == target[1].casefold()
            for w, s in scope
        )

    def permitted(target):
        if eligibility_check is None:
            return True
        current_scope = eligibility_check()
        return current_scope is None or includes(current_scope, target)

    def workspace_permitted(workspace):
        if workspace_check is not None and not workspace_check(workspace):
            return False
        targets = [
            target
            for target in destinations
            if target[0].casefold() == workspace.casefold()
        ]
        if (
            not targets
            and eligibility_check is not None
            and eligibility_check() is not None
            and workspace_check is None
        ):
            return False
        return all(
            permitted(target) for target in targets
        ) and v1_workspace_inputs_current(state, workspace)

    if eligible_destinations is None:
        if eligibility_check is None:
            reconcile_v1_legacy_sidebar(cfg, state, core.load_remote_state())
            reconcile_v1_sidebar(cfg, state, workspaces)
        else:
            if eligibility_check() is None:
                reconcile_v1_legacy_sidebar(
                    cfg,
                    state,
                    core.load_remote_state(),
                    is_current=lambda: eligibility_check() is None,
                )
            reconcile_v1_sidebar(cfg, state, workspaces, is_current=workspace_permitted)
    else:
        for workspace, _surface in eligible_destinations:
            if not any(w.casefold() == workspace.casefold() for w in workspaces):
                workspaces[workspace] = set()
        allowed = {
            workspace
            for workspace in workspaces
            if all(
                includes(eligible_destinations, target)
                for target in destinations
                if target[0].casefold() == workspace.casefold()
            )
        }
        reconcile_v1_sidebar(
            cfg,
            state,
            {w: workspaces[w] for w in allowed},
            allowed=allowed,
            **({"is_current": workspace_permitted} if eligibility_check else {}),
        )
    notice_targets = destinations.items() if notifications else ()
    for (workspace, surface), sources in notice_targets:
        if eligible_destinations is not None and not includes(
            eligible_destinations, (workspace, surface)
        ):
            continue
        reconcile_v1_destination(
            cfg,
            state,
            workspace,
            surface,
            sorted(sources),
            is_current=lambda workspace=workspace, surface=surface: (
                permitted((workspace, surface))
                and v1_destination_inputs_current(state, workspace, surface)
            ),
        )
    active = {
        (workspace.casefold(), surface.casefold())
        for workspace, surface in destinations
    }
    retired_targets = list(state.get("v1", {}).get("destinations", {}))
    for key in retired_targets if notifications else ():
        try:
            workspace, surface = json.loads(key)
            if (workspace.casefold(), surface.casefold()) in active:
                continue
        except (TypeError, ValueError, AttributeError):
            continue
        if eligible_destinations is not None and not includes(
            eligible_destinations, (workspace, surface)
        ):
            continue
        retire_v1_destination(
            cfg,
            state,
            key,
            is_current=lambda workspace=workspace, surface=surface: (
                permitted((workspace, surface))
                and v1_destination_inputs_current(state, workspace, surface)
            ),
        )
    with core.state_lock():
        current = core.load_state()
        live_v1 = current.setdefault("v1", {})
        planned_v1 = state.get("v1", {})
        missing = object()
        fields = ("sidebar_keys",)
        if notifications:
            fields += ("destinations",)
        for field in fields:
            before = baseline.get(field, {})
            after = planned_v1.get(field, {})
            live = live_v1.setdefault(field, {})
            for key in set(before) | set(after):
                if live.get(key, missing) != before.get(key, missing):
                    continue
                if key in after:
                    live[key] = after[key]
                else:
                    live.pop(key, None)
        core.save_state(current)
        return inputs != v1_publication_inputs(current)
