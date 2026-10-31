import os
import signal
import subprocess
from types import SimpleNamespace

import pytest

from cmux_herdr_core import core, discovery, publication, supervisor


def test_v1_supervisor_leases_hold_prepared_bindings_and_retire_only_on_complete_scan(
    monkeypatch, tmp_path
):
    spec = {
        "workspace": "w",
        "surface": "s",
        "target": {
            "remote": None,
            "socket_path": "/tmp/source.sock",
            "server_boot_id": "a" * 32,
        },
        "connection_generation": 1,
    }
    leases = supervisor.V1SupervisedLeases()
    try:
        assert leases.reconcile({"c": spec}, {"c": spec}, complete=True)
        with core.state_lock():
            state = core.load_state()
            publication.prune_v1_clients(state)
            assert "c" in state["v1"]["clients"]
        assert not leases.reconcile({}, {}, complete=False)
        assert "c" in core.load_state()["v1"]["clients"]
        assert leases.reconcile({}, {}, complete=True)
        assert "c" not in core.load_state()["v1"]["clients"]
    finally:
        leases.close()


def test_v1_supervisor_lease_conflict_keeps_existing_binding(monkeypatch, tmp_path):
    spec = {
        "workspace": "w",
        "surface": "s",
        "target": {
            "remote": None,
            "socket_path": "/tmp/source.sock",
            "server_boot_id": "a" * 32,
        },
        "connection_generation": 1,
    }
    first, second = supervisor.V1SupervisedLeases(), supervisor.V1SupervisedLeases()
    try:
        assert first.reconcile({"c": spec}, {"c": spec}, complete=True)
        assert not second.reconcile({"c": spec}, {"c": spec}, complete=True)
        assert "c" in core.load_state()["v1"]["clients"]
    finally:
        second.close()
        first.close()


def test_v1_supervisor_leader_fences_other_supervisors(monkeypatch, tmp_path):
    spec = {
        "workspace": "w",
        "surface": "s",
        "target": {
            "remote": None,
            "socket_path": "/tmp/source.sock",
            "server_boot_id": "a" * 32,
        },
        "connection_generation": 1,
    }
    first, second = supervisor.V1SupervisedLeases(), supervisor.V1SupervisedLeases()
    try:
        assert first.reconcile({"c": spec}, {"c": spec}, complete=True)
        assert not second.reconcile({}, {}, complete=True)
        assert "c" in core.load_state()["v1"]["clients"]
    finally:
        second.close()
        first.close()


def test_v1_supervisor_reclaims_previous_leases_before_incomplete_scan(
    monkeypatch, tmp_path
):
    spec = {
        "workspace": "w",
        "surface": "s",
        "target": {
            "remote": None,
            "socket_path": "/tmp/source.sock",
            "server_boot_id": "a" * 32,
        },
        "connection_generation": 1,
    }
    first = supervisor.V1SupervisedLeases()
    assert first.reconcile({"c": spec}, {"c": spec}, complete=True)
    first.close()
    second = supervisor.V1SupervisedLeases()
    try:
        second.reconcile({}, {}, complete=False)
        with core.state_lock():
            state = core.load_state()
            publication.prune_v1_clients(state)
            assert "c" in state["v1"]["clients"]
    finally:
        second.close()


def test_v1_supervisor_releases_new_lease_if_state_write_fails(monkeypatch, tmp_path):
    spec = {
        "workspace": "w",
        "surface": "s",
        "target": {
            "remote": None,
            "socket_path": "/tmp/source.sock",
            "server_boot_id": "a" * 32,
        },
        "connection_generation": 1,
    }
    leases = supervisor.V1SupervisedLeases()
    monkeypatch.setattr(
        core, "save_state", lambda state: (_ for _ in ()).throw(OSError("disk"))
    )
    with pytest.raises(OSError, match="disk"):
        leases.reconcile({"c": spec}, {"c": spec}, complete=True)
    with core.v1_source_leader("client:c") as acquired:
        assert acquired
    leases.close()


def test_v1_supervisor_sources_prepare_once_and_keep_old_on_incomplete_discovery():
    class Worker:
        def __init__(self):
            self.ready = True
            self.alive = True

        def stop(self):
            self.alive = False

    started = []

    def start(source_id, target):
        worker = Worker()
        started.append((source_id, worker))
        return worker

    manager = supervisor.V1SupervisedSources(start)
    spec = {
        "workspace": "w",
        "surface": "s",
        "target": {
            "remote": None,
            "socket_path": "/tmp/source.sock",
            "server_boot_id": "a" * 32,
        },
        "connection_generation": 1,
    }
    try:
        assert manager.reconcile({"c1": spec, "c2": spec}, complete=True) == {
            "c1": spec,
            "c2": spec,
        }
        assert len(started) == 1
        assert manager.reconcile({}, complete=False) == {}
        assert started[0][1].alive
        manager.reconcile({}, complete=True)
        assert not started[0][1].alive
    finally:
        manager.close()


def test_v1_supervisor_sources_retries_dead_worker_without_early_ready():
    class Worker:
        def __init__(self, ready):
            self.ready = ready
            self.alive = True

        def stop(self):
            self.alive = False

    started = []

    def start(source_id, target):
        worker = Worker(ready=len(started) > 0)
        started.append(worker)
        return worker

    manager = supervisor.V1SupervisedSources(start)
    spec = {
        "workspace": "w",
        "surface": "s",
        "target": {
            "remote": None,
            "socket_path": "/tmp/source.sock",
            "server_boot_id": "a" * 32,
        },
        "connection_generation": 1,
    }
    try:
        assert manager.reconcile({"c": spec}, complete=True) == {}
        started[0].alive = False
        assert manager.reconcile({"c": spec}, complete=True) == {"c": spec}
        assert len(started) == 2
    finally:
        manager.close()


def test_v1_supervisor_prepares_new_boot_without_stopping_old_worker():
    class Worker:
        ready = True
        alive = True

        def stop(self):
            self.alive = False

    started = []

    def start(*args):
        worker = Worker()
        started.append(worker)
        return worker

    manager = supervisor.V1SupervisedSources(start)
    spec = {
        "workspace": "w",
        "surface": "s",
        "target": {
            "remote": None,
            "socket_path": "/tmp/source.sock",
            "server_boot_id": "a" * 32,
        },
        "connection_generation": 1,
    }
    replacement = {
        **spec,
        "target": {**spec["target"], "server_boot_id": "b" * 32},
        "connection_generation": 2,
    }
    try:
        manager.reconcile({"c": spec}, complete=True)
        assert manager.reconcile({"c": replacement}, complete=False) == {
            "c": replacement
        }
        assert len(started) == 2 and all(worker.alive for worker in started)
    finally:
        manager.close()


def test_v1_supervisor_groups_symlink_aliases_for_same_source(tmp_path):
    class Worker:
        ready = True
        alive = True

        def stop(self):
            self.alive = False

    path = tmp_path / "source.sock"
    path.touch()
    alias = tmp_path / "alias.sock"
    alias.symlink_to(path)
    started = []

    def start(*args):
        worker = Worker()
        started.append(worker)
        return worker

    manager = supervisor.V1SupervisedSources(start)
    spec = {
        "workspace": "w",
        "surface": "s",
        "target": {
            "remote": None,
            "socket_path": str(path),
            "server_boot_id": "a" * 32,
        },
        "connection_generation": 1,
    }
    other = {**spec, "target": {**spec["target"], "socket_path": str(alias)}}
    try:
        assert manager.reconcile({"c1": spec, "c2": other}, complete=True) == {
            "c1": spec,
            "c2": other,
        }
        assert len(started) == 1
    finally:
        manager.close()


def test_v1_supervisor_preserves_discovered_socket_path(tmp_path):
    class Worker:
        ready = True
        alive = True

        def stop(self):
            self.alive = False

    path = tmp_path / "source.sock"
    path.touch()
    alias = tmp_path / "alias.sock"
    alias.symlink_to(path)
    started = []
    manager = supervisor.V1SupervisedSources(
        lambda key, target: started.append(target) or Worker()
    )
    spec = {
        "workspace": "w",
        "surface": "s",
        "target": {
            "remote": None,
            "socket_path": str(alias),
            "server_boot_id": "a" * 32,
        },
        "connection_generation": 1,
    }
    try:
        manager.reconcile({"c": spec}, complete=True)
        assert started[0]["socket_path"] == str(alias)
        assert manager.current_ready({"c": spec}) == {"c": spec}
    finally:
        manager.close()


def test_v1_publication_accepts_certified_surface_from_partial_discovery(monkeypatch):
    a = {"workspace": "wa", "surface": "sa", "target": {"socket_path": "/a"}}

    class Sources:
        def current_ready(self, desired):
            return desired

    monkeypatch.setattr(
        supervisor,
        "v1_verified_publication_targets",
        lambda desired, ready, certified: (
            {("wa", "sa")} if certified[("wa", "sa")] else set()
        ),
    )
    verification = supervisor.V1PublicationVerification(Sources())
    verification.update(
        {"a": a},
        complete=False,
        surface_complete={("wa", "sa"): True, ("wb", "sb"): False},
    )
    assert verification.scope() == {("wa", "sa")}
    assert verification.workspace_allowed("wa")
    assert not verification.workspace_allowed("wb")


def test_v1_publication_workspace_requires_unregistered_discovered_surface(monkeypatch):
    a = {"workspace": "w", "surface": "a", "target": {"socket_path": "/a"}}
    b = {"workspace": "w", "surface": "b", "target": {"socket_path": "/b"}}

    class Sources:
        def current_ready(self, desired):
            return {"a": a}

    monkeypatch.setattr(
        supervisor,
        "v1_verified_publication_targets",
        lambda desired, ready: {("w", "a")},
    )
    verification = supervisor.V1PublicationVerification(Sources())
    verification.update({"a": a, "b": b}, complete=True)
    assert not verification.workspace_allowed("w")


def test_v1_publication_full_scope_rejects_unsupervised_client(monkeypatch, tmp_path):
    spec = {"workspace": "w", "surface": "s", "target": {"socket_path": "/socket"}}
    core.save_state(
        {"v1": {"clients": {"foreign": {"workspace": "w", "surface": "s"}}}}
    )

    class Sources:
        def current_ready(self, desired):
            return desired

    monkeypatch.setattr(
        supervisor, "v1_bindings_match_clients", lambda *args, **kwargs: True
    )
    verification = supervisor.V1PublicationVerification(Sources())
    verification.update({"c": spec}, complete=True)
    assert verification.scope() == set()


def test_v1_supervisor_sources_checks_readiness_without_discovery():
    class Worker:
        ready = True
        alive = True

        def stop(self):
            self.alive = False

    worker = Worker()
    manager = supervisor.V1SupervisedSources(lambda *args: worker)
    spec = {
        "workspace": "w",
        "surface": "s",
        "connection_generation": 1,
        "target": {
            "remote": None,
            "socket_path": "/tmp/source.sock",
            "server_boot_id": "a" * 32,
        },
    }
    try:
        assert manager.reconcile({"c": spec}, complete=False) == {"c": spec}
        assert manager.current_ready({"c": spec}) == {"c": spec}
        worker.ready = False
        assert manager.current_ready({"c": spec}) == {}
        worker.ready = True
        assert manager.current_ready({"c": spec}) == {"c": spec}
    finally:
        manager.close()


def test_v1_supervisor_sources_retains_handle_if_stop_fails():
    class Worker:
        ready = True
        alive = True
        fail = True

        def stop(self):
            if self.fail:
                raise OSError("stop failed")
            self.alive = False

    worker = Worker()
    manager = supervisor.V1SupervisedSources(lambda *args: worker)
    spec = {
        "workspace": "w",
        "surface": "s",
        "target": {
            "remote": None,
            "socket_path": "/tmp/source.sock",
            "server_boot_id": "a" * 32,
        },
        "connection_generation": 1,
    }
    assert manager.reconcile({"c": spec}, complete=True) == {"c": spec}
    with pytest.raises(OSError, match="stop failed"):
        manager.reconcile({}, complete=True)
    worker.fail = False
    manager.close()
    assert not worker.alive


def test_v1_supervisor_reconcile_keeps_old_source_until_binding_commit_succeeds(
    monkeypatch, tmp_path
):

    class Worker:
        ready = True
        alive = True

        def stop(self):
            self.alive = False

    started = []

    def start(*args):
        worker = Worker()
        started.append(worker)
        return worker

    sources, leases = (
        supervisor.V1SupervisedSources(start),
        supervisor.V1SupervisedLeases(),
    )
    boot = "a" * 32

    def inventory(socket):
        return {
            "complete": True,
            "clients": [
                {
                    "pid": 1,
                    "instance": "i",
                    "workspace": "w",
                    "surface": "s",
                    "sources_complete": True,
                    "sources": [
                        {
                            "endpoint": "local",
                            "api_socket_path": socket,
                            "boot_id": boot,
                            "connection_generation": 1,
                        }
                    ],
                }
            ],
        }

    try:
        assert supervisor.v1_reconcile_supervisor_inventory(
            inventory("/old.sock"), sources, leases
        )
        with monkeypatch.context() as context:
            context.setattr(
                core, "save_state", lambda state: (_ for _ in ()).throw(OSError("disk"))
            )
            with pytest.raises(OSError):
                supervisor.v1_reconcile_supervisor_inventory(
                    inventory("/new.sock"), sources, leases
                )
        assert started[0].alive
        assert supervisor.v1_reconcile_supervisor_inventory(
            inventory("/new.sock"), sources, leases
        )
        assert not started[0].alive and started[1].alive
    finally:
        sources.close()
        leases.close()


def test_v1_supervisor_preserves_but_does_not_publish_unverified_bindings(
    monkeypatch, tmp_path
):
    spec = {
        "workspace": "w",
        "surface": "s",
        "target": {
            "remote": None,
            "socket_path": "/tmp/source.sock",
            "server_boot_id": "a" * 32,
        },
        "connection_generation": 1,
    }
    previous = supervisor.V1SupervisedLeases()
    assert previous.reconcile({"c": spec}, {"c": spec}, complete=True)
    previous.close()
    (tmp_path / "v1-enabled").write_text("enabled")

    class Stop:
        done = False

        def is_set(self):
            return self.done

        def wait(self, seconds):
            self.done = True

    monkeypatch.setattr(discovery, "v1_discover_clients", lambda cfg: None)
    seen = []

    def publish(cfg):
        with core.state_lock():
            state = core.load_state()
            publication.prune_v1_clients(state)
            seen.append("c" in state["v1"]["clients"])

    monkeypatch.setattr(publication, "publish_v1_clients", publish)
    supervisor.v1_run_supervisor({}, Stop())
    assert seen == []
    assert "c" in core.load_state()["v1"]["clients"]


def test_v1_handoff_coverage_requires_every_legacy_source_and_ready_binding():
    target = {
        "remote": None,
        "socket_path": "/tmp/source.sock",
        "server_boot_id": "a" * 32,
    }
    spec = {
        "workspace": "w",
        "surface": "s",
        "target": target,
        "connection_generation": 1,
    }
    desired = {"c": spec}
    legacy = {
        core.v1_source_key(target),
        core.v1_source_key({"remote": "host", "session": "work"}),
    }

    class Worker:
        alive = True
        ready = True

        def stop(self):
            pass

    sources = supervisor.V1SupervisedSources(lambda *_: Worker())
    sources.reconcile(desired, complete=False)
    assert not supervisor.v1_handoff_has_coverage(
        True, desired, desired, legacy, sources
    )
    assert not supervisor.v1_handoff_has_coverage(
        False, desired, desired, {core.v1_source_key(target)}, sources
    )
    assert not supervisor.v1_handoff_has_coverage(
        True, desired, {}, {core.v1_source_key(target)}, sources
    )
    assert supervisor.v1_handoff_has_coverage(
        True, desired, desired, {core.v1_source_key(target)}, sources
    )
    sources.close()


def test_v1_handoff_does_not_credit_moved_socket_alias_or_dead_worker(tmp_path):
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.touch()
    second.touch()
    alias = tmp_path / "alias"
    alias.symlink_to(first)

    class Worker:
        alive = True
        ready = True

        def stop(self):
            self.alive = False

    sources = supervisor.V1SupervisedSources(lambda *_: Worker())
    spec = {
        "workspace": "w",
        "surface": "s",
        "connection_generation": 1,
        "target": {"socket_path": str(alias), "server_boot_id": "a" * 32},
    }
    desired = {"c": spec}
    ready = sources.reconcile(desired, complete=False)
    assert supervisor.v1_handoff_has_coverage(
        True, desired, ready, {core.v1_source_key(spec["target"])}, sources
    )
    alias.unlink()
    alias.symlink_to(second)
    assert not supervisor.v1_handoff_has_coverage(
        True, desired, ready, {core.v1_source_key(spec["target"])}, sources
    )
    alias.unlink()
    alias.symlink_to(first)
    next(iter(sources._workers.values()))["worker"].alive = False
    assert not supervisor.v1_handoff_has_coverage(
        True, desired, ready, {core.v1_source_key(spec["target"])}, sources
    )
    sources.close()


def test_v1_supervisor_bindings_match_requires_exact_owned_surface(
    monkeypatch, tmp_path
):
    spec = {
        "workspace": "w",
        "surface": "s",
        "connection_generation": 1,
        "target": {"socket_path": "/tmp/source.sock", "server_boot_id": "a" * 32},
    }
    desired = {"c": spec}
    state = {}
    publication.reconcile_v1_supervised_bindings(
        state, desired, desired, complete=True, supervisor_pid=os.getpid()
    )
    core.save_state(state)
    assert supervisor.v1_supervisor_bindings_match(desired)
    state["v1"]["clients"]["c"]["surface"] = "other"
    core.save_state(state)
    assert not supervisor.v1_supervisor_bindings_match(desired)


def test_v1_supervisor_source_process_requires_ready_and_stops_group(
    monkeypatch, tmp_path
):
    read_fd, write_fd = os.pipe()
    stream = os.fdopen(read_fd, "rb", buffering=0)
    calls = []
    dead = [False]
    process = SimpleNamespace(
        pid=12345, stdout=stream, wait=lambda **kwargs: calls.append("reap")
    )
    monkeypatch.setattr(subprocess, "Popen", lambda *args, **kwargs: process)
    monkeypatch.setattr(
        discovery, "v1_wait_child_state", lambda pid: (1, 0) if dead[0] else None
    )

    def killpg(pid, sig):
        calls.append(sig)
        dead[0] = True

    monkeypatch.setattr(os, "killpg", killpg)
    monkeypatch.setattr(core, "config_dir", tmp_path)
    source = supervisor.V1SupervisedSourceProcess("source", {"remote": None})
    try:
        assert source.alive and not source.ready
        os.write(write_fd, b"ready\n")
        assert source.ready
        os.write(write_fd, b"not-ready\n")
        assert not source.ready
        os.write(write_fd, b"ready\n")
        assert source.ready
        source.stop()
        assert not source.alive
        assert calls == [signal.SIGTERM, signal.SIGKILL, "reap"]
    finally:
        os.close(write_fd)
        source.stop()


def test_v1_verified_publication_targets_require_complete_surface(
    monkeypatch, tmp_path
):
    spec_a = {"workspace": "wa", "surface": "sa"}
    spec_b = {"workspace": "wb", "surface": "sb"}
    spec_c = {"workspace": "wc", "surface": "sc"}
    desired = {"a": spec_a, "b": spec_b, "c": spec_c, "d": spec_c}
    core.save_state(
        {
            "v1": {
                "clients": {
                    key: {"workspace": spec["workspace"], "surface": spec["surface"]}
                    for key, spec in desired.items()
                }
            }
        }
    )
    monkeypatch.setattr(
        supervisor, "v1_bindings_match_clients", lambda *args, **kwargs: True
    )
    assert supervisor.v1_verified_publication_targets(
        desired, {"a": spec_a, "c": spec_c}
    ) == {("wa", "sa")}
    state = core.load_state()
    state["v1"]["clients"]["extra"] = {"workspace": "wa", "surface": "sa"}
    core.save_state(state)
    assert (
        supervisor.v1_verified_publication_targets(desired, {"a": spec_a, "c": spec_c})
        == set()
    )
