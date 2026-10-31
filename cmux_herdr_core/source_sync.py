"""Refreshing and persisting supervised Herdr source state."""

import copy
import json
import re
import uuid

from cmux_herdr_core import attention_ledger, core, remote_bridge


class V1SnapshotRace(ValueError):
    """A transient release raced with snapshot collection."""


def v1_preserve_stable_snapshot_releases(
    source, generation, boot, before, panes, after
):
    """Preserve release provenance only when all snapshots agree on the same pane."""
    same_source = (
        source.get("cursor_generation") == generation
        and source.get("server_boot_id") == boot
    )
    collections = (before.get("panes"), panes, after.get("panes"))
    indexed = []
    for items in collections:
        if not isinstance(items, list):
            return {}
        mapping = {}
        for pane in items:
            if not isinstance(pane, dict) or not isinstance(pane.get("pane_id"), str):
                return {}
            if pane["pane_id"] in mapping:
                return {}
            mapping[pane["pane_id"]] = pane
        indexed.append(mapping)
    for pane_id in set().union(*(mapping.keys() for mapping in indexed)):
        records = [mapping.get(pane_id) for mapping in indexed]
        if any(
            isinstance(record, dict)
            and record.get("released_completion_seq") is not None
            for record in records
        ):
            identity = ("terminal_id", "state_change_seq", "released_completion_seq")
            if any(not isinstance(record, dict) for record in records) or any(
                any(record.get(key) != records[0].get(key) for key in identity)
                for record in records[1:]
            ):
                raise V1SnapshotRace("Herdr release changed during snapshot")
    releases = {}
    for pane_id, pane in indexed[1].items():
        records = [mapping.get(pane_id) for mapping in indexed]
        if any(not isinstance(record, dict) for record in records):
            continue
        terminal = pane.get("terminal_id")
        sequence = pane.get("state_change_seq")
        released = pane.get("released_completion_seq")
        if (
            not isinstance(terminal, str)
            or not terminal
            or type(sequence) is not int
            or type(released) is not int
            or not 0 <= released <= sequence <= 0xFFFFFFFFFFFFFFFF
        ):
            continue
        if any(
            record.get("terminal_id") != terminal
            or record.get("state_change_seq") != sequence
            or record.get("released_completion_seq") != released
            or record.get("agent_status") not in ("done", "idle", "unknown")
            for record in records
        ):
            continue
        preserved = same_source and attention_ledger.v1_preserve_released_completion(
            source, pane_id, terminal, sequence, released, boot
        )
        releases[pane_id] = (terminal, sequence, released, preserved)
    return releases


def refresh_v1_source(
    state,
    source_id,
    generation,
    socket_path,
    *,
    reconnect=False,
    expected_boot=None,
    retry_snapshot_race=False,
):
    """Refresh only after both Herdr snapshots have been obtained successfully."""
    try:
        before = (
            remote_bridge.socket_rpc(socket_path, "session.snapshot", {}).get(
                "snapshot"
            )
            or {}
        )
        agents = remote_bridge.socket_rpc(socket_path, "agent.list", {}).get("agents")
        panes = remote_bridge.socket_rpc(socket_path, "pane.list", {}).get("panes")
        after = (
            remote_bridge.socket_rpc(socket_path, "session.snapshot", {}).get(
                "snapshot"
            )
            or {}
        )
        before_boot, after_boot = (
            before.get("server_boot_id"),
            after.get("server_boot_id"),
        )
        if before_boot and after_boot and before_boot != after_boot:
            raise ValueError("Herdr server changed during snapshot")
        if not isinstance(agents, list) or not isinstance(panes, list):
            raise ValueError("invalid Herdr snapshot")
        if any(not isinstance(agent, dict) for agent in agents) or any(
            not isinstance(pane, dict) or not isinstance(pane.get("pane_id"), str)
            for pane in panes
        ):
            raise ValueError("invalid Herdr snapshot entry")
        pane_ids = [pane["pane_id"] for pane in panes]
        candidate = copy.deepcopy(state)
        verified_boot = (
            isinstance(before_boot, str)
            and re.fullmatch(r"[0-9a-f]{32}", before_boot)
            and before_boot == after_boot
        )
        if expected_boot is not None and (
            not verified_boot or before_boot != expected_boot
        ):
            raise ValueError("Herdr snapshot does not match expected server")
        previous_source = state.get("v1", {}).get("sources", {}).get(source_id, {})
        if (
            previous_source.get("server_boot_id")
            and previous_source.get("socket_generation") == generation
            and not verified_boot
        ):
            raise ValueError("Herdr server identity unavailable")
        episode_generation = (
            json.dumps([generation, before_boot], separators=(",", ":"))
            if verified_boot
            else generation
        )
        if verified_boot:
            source = candidate.get("v1", {}).get("sources", {}).get(source_id, {})
            releases = v1_preserve_stable_snapshot_releases(
                source, episode_generation, before_boot, before, panes, after
            )
            adjusted = []
            for agent in agents:
                release = releases.get(agent.get("pane_id"))
                if (
                    release
                    and not release[3]
                    and agent.get("terminal_id") == release[0]
                    and agent.get("state_change_seq") == release[1]
                    and agent.get("agent_status") == "done"
                    and agent.get("completion_seq") != release[1]
                ):
                    entry = source.get("attention", {}).get(agent["pane_id"], {})
                    if (
                        entry.get("generation") == episode_generation
                        and entry.get("terminal_id") == release[0]
                        and type(entry.get("sequence")) is int
                        and entry["sequence"] <= release[1]
                    ):
                        source["attention"].pop(agent["pane_id"], None)
                    agent = {**agent, "agent_status": "idle"}
                adjusted.append(agent)
            agents = adjusted
        attention_ledger.apply_v1_snapshot(
            candidate,
            source_id,
            episode_generation,
            agents,
            pane_ids,
            panes,
            reconnect=reconnect,
            boot_id=before_boot if verified_boot else None,
        )
        if verified_boot:
            attention_ledger.v1_reconcile_absent_agents(
                candidate, source_id, episode_generation, before, agents, panes, after
            )
            source = candidate["v1"]["sources"][source_id]
            for pane in panes:
                pane_id = pane["pane_id"]
                entry = source.get("attention", {}).get(pane_id, {})
                if (
                    "released_completion_seq" not in entry
                    and "release_provenance_seq" not in entry
                    or pane_id in releases
                ):
                    continue
                records = [
                    [
                        item
                        for item in snapshot.get("panes", [])
                        if isinstance(item, dict) and item.get("pane_id") == pane_id
                    ]
                    for snapshot in (before, after)
                ]
                sequence = pane.get("state_change_seq")
                terminal = pane.get("terminal_id")
                if (
                    entry.get("generation") == episode_generation
                    and entry.get("terminal_id") == terminal
                    and type(entry.get("sequence")) is int
                    and type(sequence) is int
                    and entry["sequence"] <= sequence <= 0xFFFFFFFFFFFFFFFF
                    and pane.get("released_completion_seq") is None
                    and all(
                        len(matches) == 1
                        and matches[0].get("terminal_id") == terminal
                        and matches[0].get("state_change_seq") == sequence
                        and matches[0].get("released_completion_seq") is None
                        for matches in records
                    )
                ):
                    entry.pop("released_completion_seq", None)
                    entry.pop("release_provenance_seq", None)
                    entry["sequence"] = sequence
                    if (
                        entry.get("status") == "done"
                        and type(attention_ledger.v1_completion_sequence(entry)) is int
                        and attention_ledger.v1_completion_sequence(entry) < sequence
                    ):
                        source["attention"][pane_id] = {
                            "sequence": sequence,
                            "generation": episode_generation,
                            "terminal_id": terminal,
                        }
            for pane_id, (terminal, sequence, released, preserved) in releases.items():
                entry = source.get("attention", {}).get(pane_id, {})
                tracked = source.get("panes", {}).get(pane_id, {})
                if (
                    not preserved
                    and not entry
                    and tracked.get("terminal_id") == terminal
                ):
                    entry = source.setdefault("attention", {}).setdefault(
                        pane_id,
                        {
                            "generation": episode_generation,
                            "terminal_id": terminal,
                            "sequence": sequence,
                        },
                    )
                if (
                    not preserved
                    and entry.get("status") == "blocked"
                    and entry.get("generation") == episode_generation
                    and entry.get("terminal_id") == terminal
                    and type(entry.get("sequence")) is int
                    and entry["sequence"] <= sequence
                ):
                    entry = {
                        "generation": episode_generation,
                        "terminal_id": terminal,
                        "sequence": sequence,
                    }
                    source["attention"][pane_id] = entry
                if (
                    not preserved
                    and entry.get("generation") == episode_generation
                    and entry.get("terminal_id") == terminal
                    and type(entry.get("sequence")) is int
                    and entry["sequence"] <= sequence
                    and entry.get("status") not in core.ATTENTION_STATUSES
                ):
                    entry["sequence"] = sequence
                    entry["released_completion_seq"] = released
                    entry["release_provenance_seq"] = released
                    pane = next((p for p in panes if p.get("pane_id") == pane_id), {})
                    attention_ledger.apply_v1_ack_event(
                        candidate, source_id, {"pane": pane}
                    )
        if verified_boot:
            candidate["v1"]["sources"][source_id]["server_boot_id"] = before_boot
        else:
            candidate["v1"]["sources"][source_id].pop("server_boot_id", None)
        candidate["v1"]["sources"][source_id]["socket_generation"] = generation
        if verified_boot and isinstance(after.get("agents"), list):
            source = candidate["v1"]["sources"][source_id]
            snapshot_agents = after["agents"]
            for agent in agents:
                pane_id = agent.get("pane_id")
                entry = source.get("attention", {}).get(pane_id, {})
                matches = [
                    current
                    for current in snapshot_agents
                    if isinstance(current, dict) and current.get("pane_id") == pane_id
                ]
                if (
                    len(matches) == 1
                    and (
                        agent.get("agent_status") == "done"
                        and (
                            matches[0].get("agent_status") == "done"
                            or matches[0].get("agent_status") == "idle"
                            and matches[0].get("completion_seq")
                            == matches[0].get("state_change_seq")
                        )
                        or agent.get("agent_status") == "idle"
                        and matches[0].get("agent_status") == "idle"
                        and (
                            entry.get("server_boot_id") == before_boot
                            or agent.get("completion_seq")
                            == agent.get("state_change_seq")
                            and matches[0].get("completion_seq")
                            == matches[0].get("state_change_seq")
                        )
                    )
                    and agent.get("terminal_id") == matches[0].get("terminal_id")
                    and agent.get("state_change_seq")
                    == matches[0].get("state_change_seq")
                    and entry.get("status") == "done"
                    and entry.get("terminal_id") == agent.get("terminal_id")
                    and entry.get("sequence") == agent.get("state_change_seq")
                    and not entry.get("provisional")
                ):
                    entry["server_boot_id"] = before_boot
        state.clear()
        state.update(candidate)
        return True
    except V1SnapshotRace:
        if retry_snapshot_race:
            raise
        return False
    except (
        remote_bridge.SocketError,
        OSError,
        ValueError,
        KeyError,
        TypeError,
        AttributeError,
    ) as e:
        core.log(f"v1 source refresh failed: {e}")
        return False


def sync_v1_source(
    source_id,
    generation,
    socket_path,
    *,
    reset_predictions=False,
    expected_boot=None,
    retry_snapshot_race=False,
):
    """Commit a remote snapshot only if its source state has not changed."""
    with core.state_lock():
        state = core.load_state()
        baseline = copy.deepcopy(state.get("v1", {}).get("sources", {}).get(source_id))
    if reset_predictions:
        source = state.get("v1", {}).get("sources", {}).get(source_id, {})
        for entry in source.get("attention", {}).values():
            entry.pop("provisional", None)
            entry.pop("predicted_sequence", None)
            entry.pop("observed_changes", None)
    if not refresh_v1_source(
        state,
        source_id,
        generation,
        socket_path,
        reconnect=reset_predictions,
        expected_boot=expected_boot,
        retry_snapshot_race=retry_snapshot_race,
    ):
        return False
    updated = state.get("v1", {}).get("sources", {}).get(source_id)
    if updated is None:
        return False
    with core.state_lock():
        current = core.load_state()
        current_source = current.get("v1", {}).get("sources", {}).get(source_id)

        def snapshot_state(source):
            if not isinstance(source, dict):
                return source
            return {
                key: value for key, value in source.items() if key != "published_tokens"
            }

        if snapshot_state(current_source) != snapshot_state(baseline):
            if retry_snapshot_race:
                raise V1SnapshotRace("source changed during remote snapshot")
            return False
        if (
            isinstance(baseline, dict)
            and isinstance(current_source, dict)
            and all(
                updated.get(key) == baseline.get(key)
                for key in ("cursor_generation", "socket_generation", "server_boot_id")
            )
        ):
            if "published_tokens" in current_source:
                updated["published_tokens"] = current_source["published_tokens"]
            else:
                updated.pop("published_tokens", None)
        current.setdefault("v1", {}).setdefault("sources", {})[source_id] = updated
        core.save_state(current)
    return True


def persist_v1_status_event(source_id, data, event_boot=None):
    """Persist one ordered status event without overwriting another source."""
    with core.state_lock():
        state = core.load_state()
        if not attention_ledger.apply_v1_status_event(
            state, source_id, data, event_boot
        ):
            return False
        core.save_state(state)
        return True


def persist_v1_ack_event(source_id, data, event_boot=None):
    with core.state_lock():
        state = core.load_state()
        source = state.get("v1", {}).get("sources", {}).get(source_id, {})
        if source.get("server_boot_id") != event_boot and (
            source.get("server_boot_id") is not None or event_boot is not None
        ):
            return False
        if not attention_ledger.apply_v1_ack_event(state, source_id, data):
            return False
        core.save_state(state)
        return True


def sync_v1_episode_tokens(source_id, socket_path, *, expected_boot=None):
    """Expose only verified pending completions to a Herdr action."""

    def desired_token(source, pane_id):
        entry = source.get("attention", {}).get(pane_id, {})
        return (
            entry.get("episode")
            if entry.get("status") == "done" and not entry.get("provisional")
            else None
        )

    with core.v1_source_leader("episode-token:" + source_id) as leader:
        if not leader:
            return
        with core.state_lock():
            state = core.load_state()
            source = state.get("v1", {}).get("sources", {}).get(source_id)
            if (
                not source
                or expected_boot is not None
                and source.get("server_boot_id") != expected_boot
            ):
                return
            if expected_boot is None:
                expected_boot = source.get("server_boot_id")
            pane_ids = sorted(
                set(source.get("attention", {}))
                | set(source.get("published_tokens", {}))
            )
        for pane_id in pane_ids:
            with core.state_lock():
                state = core.load_state()
                source = state.get("v1", {}).get("sources", {}).get(source_id)
                if not source or source.get("server_boot_id") != expected_boot:
                    return
                published = source.setdefault("published_tokens", {})
                if pane_id not in source.get("panes", {}):
                    if pane_id in published:
                        published.pop(pane_id)
                        core.save_state(state)
                    continue
                desired = desired_token(source, pane_id)
                if published.get(pane_id) == desired:
                    continue
                generation = source.get("socket_generation")
                pending = {"pending": True, "id": uuid.uuid4().hex}
                published[pane_id] = pending
                core.save_state(state)
            try:
                params = {
                    "pane_id": pane_id,
                    "source": "cmux-herdr.v1",
                    "tokens": {"cmux_herdr_done": desired},
                }
                if expected_boot is not None:
                    params["expected_server_boot_id"] = expected_boot
                remote_bridge.socket_rpc(
                    socket_path,
                    "pane.report_metadata_if_boot"
                    if expected_boot is not None
                    else "pane.report_metadata",
                    params,
                )
            except (remote_bridge.SocketError, OSError, ValueError) as e:
                core.log(f"v1 episode token update failed: {e}")
                continue
            with core.state_lock():
                state = core.load_state()
                source = state.get("v1", {}).get("sources", {}).get(source_id)
                if (
                    not source
                    or source.get("server_boot_id") != expected_boot
                    or source.get("socket_generation") != generation
                ):
                    continue
                published = source.get("published_tokens", {})
                if (
                    published.get(pane_id) != pending
                    or pane_id not in source.get("panes", {})
                    or desired_token(source, pane_id) != desired
                ):
                    continue
                if desired is None:
                    published.pop(pane_id, None)
                else:
                    published[pane_id] = desired
                core.save_state(state)
