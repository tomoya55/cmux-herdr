"""Per-agent attention state: snapshots, status events and acknowledgment."""

import re
import uuid

from cmux_herdr_core import core


def v1_completion_sequence(entry):
    """Recover the exact completed episode without treating its watermark as its ID."""
    generation, terminal = entry.get("generation"), entry.get("terminal_id")
    episode, watermark = entry.get("episode"), entry.get("sequence")
    if not all(
        isinstance(value, str) and value for value in (generation, terminal, episode)
    ):
        return None
    prefix = f"{generation}:{terminal}:"
    if not episode.startswith(prefix):
        return None
    suffix = episode[len(prefix) :]
    if not re.fullmatch(r"0|[1-9][0-9]{0,19}", suffix):
        return None
    sequence = int(suffix)
    if (
        sequence > 0xFFFFFFFFFFFFFFFF
        or type(watermark) is not int
        or sequence > watermark
    ):
        return None
    saved = entry.get("completion_sequence", sequence)
    return sequence if type(saved) is int and saved == sequence else None


def apply_attention_status(attention, pane_id, status, episode, *, sequence=None):
    """Update a pane's attention without treating a seen result as resolved."""
    current = attention.get(pane_id, {})
    previous_sequence = current.get("sequence")
    if (
        sequence is not None
        and previous_sequence is not None
        and sequence <= previous_sequence
    ):
        return
    if status in core.ATTENTION_STATUSES:
        if current.get("acknowledged") == episode:
            return
        if current.get("status") != status or current.get("episode") != episode:
            attention[pane_id] = {"status": status, "episode": episode}
    elif status == "working" or (
        status == "idle" and current.get("status") == "blocked"
    ):
        attention.pop(pane_id, None)
    elif status == "unknown" and current.get("status") == "done":
        completion_sequence = v1_completion_sequence(current)
        if completion_sequence is not None:
            current.setdefault("completion_sequence", completion_sequence)
    watermark = sequence if sequence is not None else previous_sequence
    if watermark is not None:
        attention.setdefault(pane_id, {})["sequence"] = watermark
    for key in ("generation", "terminal_id", "observed_changes"):
        if key in current:
            attention.setdefault(pane_id, {})[key] = current[key]


def force_acknowledge_attention(attention, pane_id, episode):
    """Acknowledge only the specific completed episode named by the caller."""
    current = attention.get(pane_id, {})
    if (
        current.get("status") != "done"
        or current.get("episode") != episode
        or current.get("provisional")
    ):
        return False
    attention[pane_id] = {"acknowledged": episode}
    for key in (
        "sequence",
        "generation",
        "terminal_id",
        "provisional",
        "observed_changes",
        "predicted_sequence",
        "release_provenance_seq",
        "released_completion_seq",
    ):
        if key in current:
            attention[pane_id][key] = current[key]
    return True


def has_pending_attention(attention, pane_id):
    return attention.get(pane_id, {}).get("status") in core.ATTENTION_STATUSES


def reconcile_agent_snapshot(attention, agents, generation):
    """Apply a successful agent snapshot without inferring missing panes closed."""
    for agent in agents:
        pane_id = agent.get("pane_id")
        terminal_id = agent.get("terminal_id")
        sequence = agent.get("state_change_seq")
        if not pane_id or not terminal_id or not isinstance(sequence, int):
            continue
        current = attention.get(pane_id, {})
        if current.get("terminal_id") not in (None, terminal_id):
            attention.pop(pane_id, None)
            current = {}
        if current.get("generation") not in (None, generation):
            if current.get("status") == "blocked":
                attention.pop(pane_id, None)
                current = {}
            else:
                current.pop("sequence", None)
        episode = f"{generation}:{terminal_id}:{sequence}"
        apply_attention_status(
            attention, pane_id, agent.get("agent_status"), episode, sequence=sequence
        )
        attention.setdefault(pane_id, {})["generation"] = generation
        attention[pane_id]["terminal_id"] = terminal_id


def apply_v1_snapshot(
    state,
    source_id,
    generation,
    agents,
    pane_ids,
    pane_records=None,
    *,
    reconnect=False,
    boot_id=None,
):
    """Persist observed state, retiring panes only with an authoritative list."""
    if agents is None:
        return
    source = (
        state.setdefault("v1", {})
        .setdefault("sources", {})
        .setdefault(source_id, {"attention": {}, "panes": {}})
    )
    attention = source.setdefault("attention", {})
    panes = source.setdefault("panes", {})
    if source.get("cursor_generation") not in (None, generation):
        panes.clear()
        for pane_id, entry in list(attention.items()):
            if entry.get("status") == "blocked":
                attention.pop(pane_id, None)
    for pane in pane_records or []:
        pane_id = pane["pane_id"]
        panes.setdefault(
            pane_id,
            {
                "workspace_id": pane.get("workspace_id", ""),
                "terminal_id": pane.get("terminal_id", ""),
                "status": pane.get("agent_status", "unknown"),
                "agent": pane.get("display_agent") or pane.get("agent") or "agent",
                "title": pane.get("terminal_title_stripped") or pane.get("title") or "",
            },
        )
    accepted = []
    for agent in agents:
        current = attention.get(agent.get("pane_id"), {})
        sequence = agent.get("state_change_seq")
        if (
            source.get("server_boot_id")
            and agent.get("agent_status") == "idle"
            and current.get("status") == "done"
            and current.get("generation") == generation
            and current.get("terminal_id") == agent.get("terminal_id")
            and type(sequence) is int
            and type(current.get("sequence")) is int
            and sequence > current["sequence"]
        ):
            if agent.get("completion_seq") == sequence:
                agent = {**agent, "agent_status": "done"}
            elif reconnect:
                attention.pop(agent.get("pane_id"), None)
                current = {}
            else:
                continue
        if (
            current.get("generation") == generation
            and current.get("terminal_id") == agent.get("terminal_id")
            and isinstance(sequence, int)
            and isinstance(current.get("sequence"), int)
            and sequence < current["sequence"]
        ):
            continue
        accepted.append(agent)
    snapshot_agents = []
    for agent in accepted:
        pane_id = agent.get("pane_id")
        current = attention.get(pane_id, {})
        pane = panes.get(pane_id, {})
        status = agent.get("agent_status")
        if (
            current.get("provisional")
            and current.get("generation") == generation
            and current.get("terminal_id") == agent.get("terminal_id")
            and (
                pane.get("status") == status
                and status in core.ATTENTION_STATUSES
                or pane.get("status") in ("done", "idle")
                and status == "idle"
                and current.get("status") == "done"
            )
            and isinstance(agent.get("state_change_seq"), int)
        ):
            current["episode"] = (
                f"{generation}:{agent['terminal_id']}:{agent['state_change_seq']}"
            )
            current["sequence"] = agent["state_change_seq"]
            current.pop("provisional", None)
            current.pop("predicted_sequence", None)
        else:
            if (
                (
                    boot_id
                    or source.get("server_boot_id")
                    and source.get("cursor_generation") == generation
                )
                and status == "idle"
                and type(agent.get("state_change_seq")) is int
                and agent.get("completion_seq") == agent["state_change_seq"]
            ):
                snapshot_agents.append({**agent, "agent_status": "done"})
            else:
                snapshot_agents.append(agent)
    reconcile_agent_snapshot(attention, snapshot_agents, generation)
    for agent in accepted:
        attention.get(agent.get("pane_id"), {}).pop("observed_changes", None)
    sequences = [
        agent["state_change_seq"]
        for agent in accepted
        if isinstance(agent.get("state_change_seq"), int)
    ]
    if source.get("cursor_generation") != generation:
        source.pop("published_tokens", None)
        source["event_cursor"] = max(sequences, default=0)
        source["cursor_generation"] = generation
    elif sequences:
        source["event_cursor"] = max(source.get("event_cursor", 0), *sequences)
    for agent in accepted:
        pane_id = agent.get("pane_id")
        if not pane_id:
            continue
        panes[pane_id] = {
            "workspace_id": agent.get("workspace_id", ""),
            "terminal_id": agent.get("terminal_id", ""),
            "status": agent.get("agent_status"),
            "completion_seq": agent.get("completion_seq"),
            "agent": agent.get("display_agent") or agent.get("agent") or "agent",
            "title": agent.get("terminal_title_stripped") or agent.get("title") or "",
        }
    for pane in pane_records or []:
        apply_v1_ack_event(state, source_id, {"pane": pane})
    if pane_ids is not None:
        live = set(pane_ids)
        for pane_id in set(attention) | set(panes):
            if pane_id not in live:
                attention.pop(pane_id, None)
                panes.pop(pane_id, None)
                source.get("retired_blocked", {}).pop(pane_id, None)


def new_v1_episode_id():
    return uuid.uuid4().hex


def v1_reconcile_absent_agents(
    state, source_id, generation, before, agents, panes, after
):
    """Retire blocked attention only with a stable, sequenced absence proof."""
    source = state.get("v1", {}).get("sources", {}).get(source_id, {})
    attention, tracked = source.get("attention", {}), source.get("panes", {})
    if any(
        not isinstance(snapshot.get("agents"), list)
        or not isinstance(snapshot.get("panes"), list)
        for snapshot in (before, after)
    ):
        return
    if any(
        not isinstance(item, dict) or not isinstance(item.get("pane_id"), str)
        for collection in (
            before["agents"],
            after["agents"],
            before["panes"],
            after["panes"],
            agents,
            panes,
        )
        for item in collection
    ):
        return

    def unique_panes(items):
        result = {}
        for item in items:
            pane_id = item["pane_id"]
            if pane_id in result:
                return None
            result[pane_id] = item
        return result

    snapshots = [
        unique_panes(items) for items in (before["panes"], panes, after["panes"])
    ]
    if any(snapshot is None for snapshot in snapshots):
        return
    active = {
        item["pane_id"]
        for collection in (before["agents"], agents, after["agents"])
        for item in collection
    }
    retired = source.setdefault("retired_blocked", {})
    for pane_id in snapshots[-1]:
        if pane_id in active:
            continue
        observations = [snapshot.get(pane_id) for snapshot in snapshots]
        if any(not isinstance(pane, dict) for pane in observations):
            continue
        terminal = observations[0].get("terminal_id")
        sequence = observations[0].get("state_change_seq")
        if (
            not isinstance(terminal, str)
            or not terminal
            or type(sequence) is not int
            or sequence < 0
            or any(
                pane.get("terminal_id") != terminal
                or pane.get("state_change_seq") != sequence
                or pane.get("agent") is not None
                or pane.get("agent_status") not in ("idle", "unknown")
                for pane in observations
            )
        ):
            continue
        prior = retired.get(pane_id, {})
        if (
            prior.get("generation") == generation
            and prior.get("terminal_id") == terminal
            and type(prior.get("sequence")) is int
            and prior["sequence"] > sequence
        ):
            continue
        retired[pane_id] = {
            "generation": generation,
            "terminal_id": terminal,
            "sequence": sequence,
        }
        current = attention.get(pane_id, {})
        observed_unknown_at_absence = (
            current.get("status") == "blocked"
            and tracked.get(pane_id, {}).get("status") == "unknown"
            and current.get("sequence") == sequence
        )
        if (
            current.get("generation") == generation
            and current.get("terminal_id") == terminal
            and type(current.get("sequence")) is int
            and current["sequence"] >= sequence
            and not observed_unknown_at_absence
        ):
            continue
        if current.get("terminal_id") not in (None, terminal):
            attention.pop(pane_id, None)
        elif (
            current.get("status") == "blocked"
            and current.get("generation") == generation
            and current.get("terminal_id") == terminal
            and type(current.get("sequence")) is int
            and (current["sequence"] < sequence or observed_unknown_at_absence)
        ):
            attention[pane_id] = {
                "sequence": current["sequence"],
                "generation": generation,
                "terminal_id": terminal,
            }
        tracked[pane_id] = {
            "workspace_id": observations[-1].get("workspace_id", ""),
            "terminal_id": terminal,
            "status": observations[-1]["agent_status"],
            "completion_seq": None,
            "agent": tracked.get(pane_id, {}).get("agent", "agent")
            if current.get("status") == "done"
            else "agent",
            "title": tracked.get(pane_id, {}).get("title", "")
            if current.get("status") == "done"
            else observations[-1].get("terminal_title_stripped")
            or observations[-1].get("title")
            or "",
        }


def v1_preserve_released_completion(
    source, pane_id, terminal, sequence, released, boot
):
    """Advance a watermark only for an already known, exact released completion."""
    generation = source.get("cursor_generation")
    entry = source.get("attention", {}).get(pane_id, {})
    if (
        not boot
        or source.get("server_boot_id") != boot
        or not isinstance(generation, str)
        or not generation
        or not isinstance(terminal, str)
        or not terminal
        or type(sequence) is not int
        or not 0 <= sequence <= 0xFFFFFFFFFFFFFFFF
        or type(released) is not int
        or not 0 <= released <= sequence
        or entry.get("generation") != generation
        or entry.get("terminal_id") != terminal
        or type(entry.get("sequence")) is not int
        or entry["sequence"] > sequence
    ):
        return False
    episode = f"{generation}:{terminal}:{released}"
    pending = (
        entry.get("status") == "done" and v1_completion_sequence(entry) == released
    )
    acknowledged = entry.get("acknowledged") == episode
    if not pending and not acknowledged:
        return False
    if pending:
        entry["completion_sequence"] = released
    entry["release_provenance_seq"] = released
    entry["sequence"] = sequence
    source["event_cursor"] = max(source.get("event_cursor", 0), sequence)
    return True


def apply_v1_status_event(state, source_id, data, event_boot=None):
    """Apply a status event in stream order without collapsing quick transitions."""
    source = state.get("v1", {}).get("sources", {}).get(source_id)
    pane_id = data.get("pane_id")
    status = data.get("agent_status")
    if not source or pane_id not in source.get("panes", {}):
        return False
    if source.get("server_boot_id") != event_boot and (
        source.get("server_boot_id") is not None or event_boot is not None
    ):
        return False
    if status not in core.TRACKED_STATUSES | {"idle", "unknown"}:
        return False
    pane = source["panes"][pane_id]
    previous = pane.get("status")
    attention = source.setdefault("attention", {})
    current = attention.get(pane_id, {})
    sequence, terminal = data.get("state_change_seq"), data.get("terminal_id")
    if event_boot:
        if (
            type(sequence) is not int
            or sequence < 0
            or not isinstance(terminal, str)
            or not terminal
            or terminal != pane.get("terminal_id")
            or not source.get("cursor_generation")
        ):
            return False
        released = data.get("released_completion_seq")
        if released is not None and status in ("done", "idle", "unknown"):
            if v1_preserve_released_completion(
                source, pane_id, terminal, sequence, released, event_boot
            ):
                pane["status"] = status
                return True
            if data.get("completion_seq") != sequence or released != sequence:
                return False
        retired = source.get("retired_blocked", {}).get(pane_id, {})
        if (
            status == "blocked"
            and retired.get("generation") == source["cursor_generation"]
            and retired.get("terminal_id") == terminal
            and type(retired.get("sequence")) is int
            and sequence <= retired["sequence"]
        ):
            return True
        watermark = current.get("sequence")
        if type(watermark) is int and sequence <= watermark:
            generation = source["cursor_generation"]
            episode = f"{generation}:{terminal}:{sequence}"
            if sequence == watermark and released is None:
                current.pop("released_completion_seq", None)
                if (
                    current.get("status") == "done"
                    and "release_provenance_seq" in current
                    and type(v1_completion_sequence(current)) is int
                    and v1_completion_sequence(current) < watermark
                ):
                    attention[pane_id] = current = {
                        "sequence": watermark,
                        "generation": generation,
                        "terminal_id": terminal,
                    }
                current.pop("release_provenance_seq", None)
            if (
                sequence < watermark
                and status in ("done", "idle")
                and data.get("completion_seq") == sequence
                and current.get("released_completion_seq") == sequence
                and current.get("generation") == generation
                and current.get("terminal_id") == terminal
                and current.get("status") not in core.ATTENTION_STATUSES
                and current.get("acknowledged") != episode
            ):
                attention[pane_id] = {
                    "status": "done",
                    "episode": episode,
                    "sequence": watermark,
                    "completion_sequence": sequence,
                    "release_provenance_seq": sequence,
                    "generation": generation,
                    "terminal_id": terminal,
                    "server_boot_id": event_boot,
                }
                return True
            if (
                sequence == watermark
                and status in ("idle", "done")
                and previous == "idle"
                and type(data.get("completion_seq")) is int
                and data["completion_seq"] == sequence
                and pane.get("completion_seq") == sequence
                and current.get("status") not in core.ATTENTION_STATUSES
                and current.get("acknowledged") != episode
            ):
                attention[pane_id] = {
                    "status": "done",
                    "episode": episode,
                    "sequence": sequence,
                    "generation": generation,
                    "terminal_id": terminal,
                    "server_boot_id": event_boot,
                }
            if (
                sequence == watermark
                and previous in ("done", "idle")
                and status in ("done", "idle")
            ):
                pane["status"] = status
            return True
        current.pop("released_completion_seq", None)
        if (
            released is None
            and current.get("status") == "done"
            and "release_provenance_seq" in current
            and type(v1_completion_sequence(current)) is int
            and type(watermark) is int
            and v1_completion_sequence(current) < watermark
        ):
            attention[pane_id] = {
                "sequence": watermark,
                "generation": source["cursor_generation"],
                "terminal_id": terminal,
            }
        elif released is None:
            current.pop("release_provenance_seq", None)
        generation = source["cursor_generation"]
        episode = f"{generation}:{terminal}:{sequence}"
        effective_status = (
            "done"
            if status == "idle"
            and type(data.get("completion_seq")) is int
            and data["completion_seq"] == sequence
            else status
        )
        if status == "idle" and effective_status == "idle":
            attention.pop(pane_id, None)
        apply_attention_status(
            attention, pane_id, effective_status, episode, sequence=sequence
        )
        entry = attention.setdefault(pane_id, {})
        entry.update(generation=generation, terminal_id=terminal)
        entry.pop("provisional", None)
        entry.pop("predicted_sequence", None)
        if effective_status == "done":
            entry["server_boot_id"] = event_boot
        source["event_cursor"] = max(source.get("event_cursor", 0), sequence)
        pane["status"] = status
        if data.get("display_agent") or data.get("agent"):
            pane["agent"] = data.get("display_agent") or data.get("agent")
        if data.get("title"):
            pane["title"] = data["title"]
        return True
    episode = ""
    if status in core.ATTENTION_STATUSES:
        same_episode = status == previous or (
            status == "done" and previous == "idle" and bool(current)
        )
        episode = (
            current.get("episode") or current.get("acknowledged")
            if same_episode
            else None
        ) or new_v1_episode_id()
    apply_attention_status(attention, pane_id, status, episode)
    entry = attention.setdefault(pane_id, {})
    if source.get("cursor_generation") is not None:
        entry.setdefault("generation", source["cursor_generation"])
    if pane.get("terminal_id"):
        entry.setdefault("terminal_id", pane["terminal_id"])
    effective_previous = "idle" if previous in ("idle", "done") else previous
    effective_status = "idle" if status in ("idle", "done") else status
    if effective_previous is not None and effective_previous != effective_status:
        entry = attention.setdefault(pane_id, {})
        entry["observed_changes"] = entry.get("observed_changes", 0) + 1
        source["event_cursor"] = source.get("event_cursor", 0) + 1
        if status in core.ATTENTION_STATUSES:
            entry["predicted_sequence"] = source["event_cursor"]
    if status in core.ATTENTION_STATUSES and not same_episode:
        attention[pane_id]["provisional"] = True
    if status == "done" and event_boot and source.get("server_boot_id") == event_boot:
        attention[pane_id]["server_boot_id"] = event_boot
    pane["status"] = status
    if data.get("display_agent") or data.get("agent"):
        pane["agent"] = data.get("display_agent") or data.get("agent")
    if data.get("title"):
        pane["title"] = data["title"]
    return True


def apply_v1_ack_event(state, source_id, data):
    """Apply a pane metadata acknowledgment only to its matching completion."""
    source = state.get("v1", {}).get("sources", {}).get(source_id)
    pane = data.get("pane") or {}
    token = (pane.get("tokens") or {}).get("cmux_herdr_ack")
    if not source or not isinstance(token, str) or not token:
        return False
    entry = source.get("attention", {}).get(pane.get("pane_id"), {})
    released = entry.get("released_completion_seq")
    if (
        type(released) is int
        and entry.get("generation") == source.get("cursor_generation")
        and entry.get("terminal_id") == pane.get("terminal_id")
        and token == f"{entry['generation']}:{entry['terminal_id']}:{released}"
        and entry.get("status") not in core.ATTENTION_STATUSES
    ):
        entry["acknowledged"] = token
        return True
    return force_acknowledge_attention(
        source.get("attention", {}), pane.get("pane_id"), token
    )
