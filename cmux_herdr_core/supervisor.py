"""The v1 supervisor: leases, source workers and handoff coverage."""

import copy
import hashlib
import json
import os
import select
import signal
import subprocess
import sys
import threading
import time
from contextlib import suppress

from cmux_herdr_core import (
    cmux,
    core,
    discovery,
    focus,
    legacy_events,
    legacy_handoff,
    publication,
    remote_bridge,
)


class V1SupervisedLeases:
    """Hold client leases while committing verified supervisor bindings."""

    def __init__(self):
        self._leases = {}
        self._leader = None

    def acquire_leader(self):
        if self._leader is None:
            leader = core.v1_source_leader("supervisor")
            if not leader.__enter__():
                return False
            self._leader = leader
        return True

    def reconcile(self, desired, ready, *, complete, certified_surfaces=None):
        if not self.acquire_leader():
            return False
        newly_acquired = set()
        available = {}
        committed = False
        try:
            for client_id, spec in desired.items():
                if ready.get(client_id) != spec:
                    continue
                if client_id not in self._leases:
                    lease = core.v1_source_leader("client:" + client_id)
                    if not lease.__enter__():
                        continue
                    self._leases[client_id] = lease
                    newly_acquired.add(client_id)
                available[client_id] = spec
            with core.state_lock():
                state = core.load_state()
                changed = False
                blocked = False
                for client_id, record in state.get("v1", {}).get("clients", {}).items():
                    if not record.get("supervised"):
                        continue
                    if client_id not in self._leases:
                        lease = core.v1_source_leader("client:" + client_id)
                        if not lease.__enter__():
                            blocked = True
                            continue
                        self._leases[client_id] = lease
                        newly_acquired.add(client_id)
                    if record.get("pid") != os.getpid():
                        record["pid"] = os.getpid()
                        changed = True
                changed |= publication.reconcile_v1_supervised_bindings(
                    state,
                    desired,
                    available,
                    complete=complete and not blocked,
                    supervisor_pid=os.getpid(),
                    certified_surfaces=certified_surfaces if not blocked else None,
                )
                if changed:
                    core.save_state(state)
                retained = {
                    key
                    for key, value in state.get("v1", {}).get("clients", {}).items()
                    if value.get("supervised") and value.get("pid") == os.getpid()
                }
                committed = True
            for client_id in set(self._leases) - retained:
                self._leases.pop(client_id).__exit__(None, None, None)
            return changed
        finally:
            if not committed:
                for client_id in newly_acquired:
                    self._leases.pop(client_id).__exit__(None, None, None)

    def close(self):
        for lease in self._leases.values():
            lease.__exit__(None, None, None)
        self._leases.clear()
        if self._leader is not None:
            self._leader.__exit__(None, None, None)
            self._leader = None


class V1SupervisedSourceProcess:
    """Own one source worker and its bounded readiness channel."""

    def __init__(self, source_id, target):
        env = os.environ.copy()
        env["HERDR_PLUGIN_STATE_DIR"] = str(core.state_dir)
        env["HERDR_PLUGIN_CONFIG_DIR"] = str(core.config_dir)
        self._process = subprocess.Popen(
            [
                sys.executable,
                os.path.abspath(__file__),
                "supervisor-source",
                source_id,
                json.dumps(target),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=env,
            start_new_session=True,
        )
        self._deadline = time.monotonic() + 20
        self._buffer = b""
        self._ready = False
        self._closed = False
        self._protocol_failed = False

    @property
    def alive(self):
        if self._closed:
            return False
        ready = self.ready
        if getattr(self, "_protocol_failed", False):
            return False
        if not ready and time.monotonic() >= self._deadline:
            return False
        try:
            return discovery.v1_wait_child_state(self._process.pid) is None
        except OSError:
            return False

    @property
    def ready(self):
        if self._closed or getattr(self, "_protocol_failed", False):
            return False
        stream = self._process.stdout
        try:
            for _ in range(64):
                if not select.select([stream], [], [], 0)[0]:
                    break
                data = os.read(stream.fileno(), 1024)
                if not data:
                    self._protocol_failed = True
                    return False
                self._buffer += data
                while b"\n" in self._buffer:
                    frame, self._buffer = self._buffer.split(b"\n", 1)
                    if frame == b"ready":
                        self._ready = True
                    elif frame == b"not-ready":
                        if self._ready:
                            self._deadline = time.monotonic() + 20
                        self._ready = False
                    else:
                        self._protocol_failed = True
                        return False
                if len(self._buffer) > 16:
                    self._protocol_failed = True
                    return False
            else:
                self._protocol_failed = True
                return False
            return self._ready
        except OSError:
            self._protocol_failed = True
            return False

    def stop(self):
        if self._closed:
            return
        with suppress(ProcessLookupError, PermissionError):
            os.killpg(self._process.pid, signal.SIGTERM)
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            try:
                if discovery.v1_wait_child_state(self._process.pid) is not None:
                    break
            except OSError:
                break
            time.sleep(0.01)
        with suppress(ProcessLookupError, PermissionError):
            os.killpg(self._process.pid, signal.SIGKILL)
        self._process.wait(timeout=2)
        self._process.stdout.close()
        self._closed = True


class V1SupervisedSources:
    """Prepare one live subscription for each distinct discovered source."""

    def __init__(self, start):
        self._start = start
        self._workers = {}
        self._lock = threading.RLock()

    def reconcile(self, desired, *, complete):
        with self._lock:
            return self._reconcile(desired, complete=complete)

    def _reconcile(self, desired, *, complete):
        grouped = {}
        for client_id, spec in desired.items():
            target = spec["target"]
            source_id = core.v1_supervised_source_key(target)
            grouped.setdefault(source_id, []).append((client_id, spec))

        ready = {}
        all_ready = True
        for source_id, bindings in grouped.items():
            target = bindings[0][1]["target"].copy()
            existing = self._workers.get(source_id)
            if existing and (
                not existing["worker"].alive or existing["target"] != target
            ):
                existing["worker"].stop()
                self._workers.pop(source_id)
                existing = None
            if existing is None:
                try:
                    worker = self._start(source_id, target)
                except OSError as error:
                    core.log(f"v1 source worker failed to start: {error}")
                    worker = None
                if worker is None:
                    all_ready = False
                    continue
                existing = {"target": target.copy(), "worker": worker}
                self._workers[source_id] = existing
            if not existing["worker"].alive or not existing["worker"].ready:
                all_ready = False
                continue
            ready.update(bindings)
        if complete and all_ready:
            self.retire_obsolete(set(grouped))
        return ready

    def retire_obsolete(self, active):
        with self._lock:
            for source_id in set(self._workers) - active:
                self._workers[source_id]["worker"].stop()
                self._workers.pop(source_id)

    def prepared_target(self, target):
        """Return the immutable endpoint prepared for this live incarnation."""
        with self._lock:
            return self._prepared_target(target)

    def _prepared_target(self, target):
        entry = self._workers.get(core.v1_supervised_source_key(target))
        if not entry or not entry["worker"].alive or not entry["worker"].ready:
            return None
        prepared = entry["target"]
        comparison = prepared.copy()
        candidate = target.copy()
        if not candidate.get("remote"):
            candidate["socket_path"] = os.path.realpath(candidate["socket_path"])
            comparison["socket_path"] = os.path.realpath(comparison["socket_path"])
        return prepared.copy() if comparison == candidate else None

    def current_ready(self, desired):
        """Read worker readiness independently of the discovery loop."""
        with self._lock:
            return {
                key: spec
                for key, spec in desired.items()
                if self._prepared_target(spec["target"]) is not None
            }

    def close(self):
        with self._lock:
            for entry in self._workers.values():
                entry["worker"].stop()
            self._workers.clear()


def v1_supervisor_bindings_match(desired, *, exact=False):
    """Verify all destinations belong to this supervisor and prepared source."""
    with core.state_lock():
        clients = core.load_state().get("v1", {}).get("clients", {})
        return v1_bindings_match_clients(clients, desired, exact=exact)


def v1_bindings_match_clients(clients, desired, *, exact=False):
    if exact and {
        key for key, client in clients.items() if client.get("supervised")
    } != set(desired):
        return False
    return all(
        clients.get(key, {}).get("supervised")
        and clients[key].get("pid") == os.getpid()
        and clients[key].get("workspace") == spec["workspace"]
        and clients[key].get("surface") == spec["surface"]
        and clients[key].get("source") == core.v1_supervised_source_key(spec["target"])
        and clients[key].get("server_boot_id") == spec["target"].get("server_boot_id")
        and clients[key].get("connection_generation") == spec["connection_generation"]
        and clients[key].get("observed_client_pid") == spec.get("observed_client_pid")
        and clients[key].get("observed_client_instance")
        == spec.get("observed_client_instance")
        and clients[key].get("endpoint") == spec.get("endpoint")
        and clients[key].get("native_ownership") == spec.get("native_ownership")
        for key, spec in desired.items()
    )


def v1_verified_publication_targets(desired, ready, certified=None):
    """Identify surfaces whose complete client membership is prepared."""
    groups = {}
    for client_id, spec in desired.items():
        groups.setdefault(
            (spec["workspace"].casefold(), spec["surface"].casefold()), {}
        )[client_id] = spec
    if certified is not None:
        existing_keys = {(w.casefold(), s.casefold()) for w, s in groups}
        for target, complete in certified.items():
            if complete and target not in existing_keys:
                groups[target] = {}
    with core.state_lock():
        clients = core.load_state().get("v1", {}).get("clients", {})
    result = set()
    for target, group in groups.items():
        if certified is not None and not certified.get(
            (target[0].casefold(), target[1].casefold()), False
        ):
            continue
        existing = {
            key
            for key, client in clients.items()
            if (
                str(client.get("workspace", "")).casefold(),
                str(client.get("surface", "")).casefold(),
            )
            == (target[0].casefold(), target[1].casefold())
        }
        if (
            existing == set(group)
            and all(ready.get(key) == spec for key, spec in group.items())
            and v1_bindings_match_clients(clients, group)
        ):
            result.add(target)
    return result


class V1PublicationVerification:
    """Hold process-local discovery evidence and check live source readiness."""

    def __init__(self, sources):
        self._sources = sources
        self._lock = threading.RLock()
        self._desired = {}
        self._complete = False
        self._surfaces = None

    def update(self, desired, *, complete, surface_complete=None):
        with self._lock:
            self._desired = (
                copy.deepcopy(desired)
                if complete or surface_complete is not None
                else {}
            )
            self._complete = complete
            self._surfaces = copy.deepcopy(surface_complete)

    def eligible(self):
        with self._lock:
            desired = self._desired
            ready = self._sources.current_ready(desired)
            if self._surfaces is None:
                return v1_verified_publication_targets(desired, ready)
            return v1_verified_publication_targets(desired, ready, self._surfaces)

    def scope(self):
        with self._lock:
            if not self._complete and self._surfaces is None:
                return set()
            ready = self._sources.current_ready(self._desired)
            with core.state_lock():
                clients = core.load_state().get("v1", {}).get("clients", {})
                if (
                    self._complete
                    and ready == self._desired
                    and set(clients) == set(self._desired)
                    and v1_bindings_match_clients(clients, self._desired, exact=True)
                ):
                    return None
            if self._surfaces is None:
                return v1_verified_publication_targets(self._desired, ready)
            return v1_verified_publication_targets(self._desired, ready, self._surfaces)

    def workspace_allowed(self, workspace):
        with self._lock:
            if not self._complete and self._surfaces is None:
                return False
            targets = {
                (spec["workspace"].casefold(), spec["surface"].casefold())
                for spec in self._desired.values()
                if spec["workspace"].casefold() == workspace.casefold()
            }
            if self._surfaces is not None:
                targets.update(
                    key for key in self._surfaces if key[0] == workspace.casefold()
                )
            scope = self.scope()
            if self._surfaces is not None and any(
                key[0] == workspace.casefold() and not complete
                for key, complete in self._surfaces.items()
            ):
                return False
            return (
                scope is None
                or bool(targets)
                and targets.issubset({(w.casefold(), s.casefold()) for w, s in scope})
            )


def v1_reconcile_supervisor_inventory(inventory, sources, leases):
    """Prepare sources before committing bindings; retire old sources afterward."""
    if inventory is None or not leases.acquire_leader():
        return False
    desired = discovery.v1_discovered_targets(inventory)
    ready = sources.reconcile(desired, complete=False)
    options = {"complete": inventory["complete"]}
    if "surface_complete" in inventory:
        options["certified_surfaces"] = inventory["surface_complete"]
    changed = leases.reconcile(desired, ready, **options)
    if (
        inventory["complete"]
        and len(ready) == len(desired)
        and v1_supervisor_bindings_match(desired)
    ):
        sources.retire_obsolete(
            {core.v1_supervised_source_key(spec["target"]) for spec in desired.values()}
        )
    return changed


def v1_handoff_has_coverage(
    discovery_complete, desired, ready, legacy_sources, sources
):
    """Require every observed legacy source to have a prepared replacement."""
    if (
        not discovery_complete
        or not desired
        or any(ready.get(key) != spec for key, spec in desired.items())
    ):
        return False
    prepared_sources = set()
    for spec in desired.values():
        target = sources.prepared_target(spec["target"])
        if target is None:
            return False
        prepared_sources.add(core.v1_source_key(target))
    return legacy_sources is not None and set(legacy_sources).issubset(prepared_sources)


def v1_supervisor_focus_tick(cfg, desired, ready):
    """Apply focus evidence only to currently prepared client sources."""
    for client_id, spec in desired.items():
        if ready.get(client_id) != spec:
            continue
        target = spec["target"]
        source_id = core.v1_supervised_source_key(target)
        if target.get("remote"):
            name = "v1-" + hashlib.sha256(source_id.encode()).hexdigest()[:16]
            sock_path = remote_bridge.remote_sock_path(name)
        else:
            sock_path = target["socket_path"]
        focus.v1_reconcile_focus(
            cfg, client_id, source_id, spec["workspace"], spec["surface"], sock_path
        )


def v1_supervised_publish_loop(cfg, stop, verified):
    """Publish committed state independently of slow discovery and focus RPCs."""
    while not stop.is_set():
        try:
            if (core.state_dir / "v1-enabled").read_text() == "enabled":
                scope = verified.scope()
                if scope is None:
                    publication.publish_v1_clients(
                        cfg,
                        eligibility_check=verified.scope,
                        workspace_check=verified.workspace_allowed,
                    )
                elif scope:
                    publication.publish_v1_clients(
                        cfg,
                        eligible_destinations=scope,
                        eligibility_check=verified.scope,
                        workspace_check=verified.workspace_allowed,
                    )
        except FileNotFoundError:
            pass
        except (
            OSError,
            ValueError,
            KeyError,
            TypeError,
            subprocess.TimeoutExpired,
        ) as error:
            core.log(f"v1 supervisor publication failed: {error}")
        stop.wait(core.LOOP_TICK_SECONDS)


def v1_maybe_supervisor_handoff(cfg, sources, stop, verified, environment):
    """Attempt handoff once existing cmux notices can be safely preserved."""
    if (
        verified.scope() is not None
        or publication.v1_notification_backend() != "ordinary"
    ):
        return False

    def ready_for_handoff():
        visible = cmux.cmux_notifications_checked(cfg)
        if visible is None:
            return False
        return all(
            isinstance(item, dict)
            and isinstance(item.get("id"), str)
            and type(item.get("is_read")) is bool
            and (item["is_read"] or not legacy_events.is_our_notification(item))
            for item in visible
        )

    if not ready_for_handoff():
        return False
    return legacy_handoff.v1_try_supervisor_handoff(
        cfg, sources, stop, final_check=ready_for_handoff
    )


def v1_runtime_config_valid(cfg):
    if not isinstance(cfg, dict):
        return False
    if "cmux_bin" in cfg and not isinstance(cfg["cmux_bin"], str):
        return False
    if "match_by_label" in cfg and type(cfg["match_by_label"]) is not bool:
        return False
    for name in ("workspaces", "labels"):
        value = cfg.get(name, {})
        if not isinstance(value, dict) or any(
            not isinstance(key, str) or not isinstance(ref, str)
            for key, ref in value.items()
        ):
            return False
    remotes = remote_bridge.remote_configs(cfg)
    if remotes is None:
        return False
    binaries = {}
    for entry in remotes:
        binary = entry.get("herdr_bin")
        if binary:
            binaries.setdefault((entry["ssh_target"], entry["session"]), set()).add(
                binary
            )
    return all(len(paths) == 1 for paths in binaries.values())


def v1_run_supervisor(cfg, stop, *, config_changed=lambda: False):
    """Maintain discovered bindings; publish only after migration commits."""
    leases = V1SupervisedLeases()
    if not leases.acquire_leader():
        return False

    def start_source(source_id, target):
        resolved = core.v1_target_with_remote_config(cfg, target)
        return V1SupervisedSourceProcess(source_id, resolved) if resolved else None

    sources = V1SupervisedSources(start_source)
    publisher_stop = threading.Event()
    publisher_verified = V1PublicationVerification(sources)
    environment = os.environ.copy()
    publisher = None
    try:
        leases.reconcile({}, {}, complete=False)
        publisher = threading.Thread(
            target=v1_supervised_publish_loop,
            args=(cfg, publisher_stop, publisher_verified),
            daemon=True,
        )
        publisher.start()
        next_recovery = 0
        recovery_ready = False
        while not stop.is_set():
            try:
                if config_changed():
                    return "config-changed"
                if time.monotonic() >= next_recovery:
                    recovery_ready = legacy_handoff.v1_recover_legacy_handoff()
                    next_recovery = time.monotonic() + 5
                inventory = discovery.v1_discover_clients(cfg)
                desired = ready = None
                if inventory is not None:
                    publisher_verified.update({}, complete=False)
                    v1_reconcile_supervisor_inventory(inventory, sources, leases)
                    desired = discovery.v1_discovered_targets(inventory)
                    ready = sources.reconcile(desired, complete=False)
                    publisher_verified.update(
                        desired,
                        complete=inventory["complete"],
                        surface_complete=inventory.get("surface_complete"),
                    )
                else:
                    publisher_verified.update({}, complete=False)
                try:
                    enabled = (core.state_dir / "v1-enabled").read_text() == "enabled"
                except FileNotFoundError:
                    enabled = False
                if not enabled and desired and recovery_ready:
                    enabled = v1_maybe_supervisor_handoff(
                        cfg, sources, stop, publisher_verified, environment
                    )
                    if enabled:
                        publisher_verified.update({}, complete=False)
                        continue
                if enabled and desired is not None:
                    v1_supervisor_focus_tick(cfg, desired, ready)
            except (
                OSError,
                ValueError,
                KeyError,
                TypeError,
                subprocess.TimeoutExpired,
            ) as error:
                core.log(f"v1 supervisor reconciliation failed: {error}")
            stop.wait(core.LOOP_TICK_SECONDS)
        return True
    finally:
        publisher_stop.set()
        if publisher is not None:
            publisher.join()
        try:
            sources.close()
        finally:
            leases.close()
