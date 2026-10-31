import json
from types import SimpleNamespace

import pytest

import cmux_herdr as ch


def test_v1_focus_episode_requires_matching_server_and_current_completion():
    boot = "a" * 32
    observation = {
        "boot_id": boot,
        "pane_id": "p",
        "outer_focused": True,
        "agent_state_change_seq": 7,
    }
    snapshot = {
        "server_boot_id": boot,
        "agents": [
            {
                "pane_id": "p",
                "terminal_id": "t",
                "agent_status": "idle",
                "state_change_seq": 7,
            }
        ],
    }
    state = {
        "v1": {
            "sources": {
                "source": {
                    "attention": {
                        "p": {
                            "status": "done",
                            "episode": "gen:t:7",
                            "generation": "gen",
                            "terminal_id": "t",
                            "sequence": 7,
                            "server_boot_id": boot,
                        }
                    }
                }
            }
        }
    }
    assert (
        ch.v1_verified_focus_episode(state, "source", observation, snapshot)
        == "gen:t:7"
    )
    assert (
        ch.v1_verified_focus_episode(
            state, "source", {**observation, "boot_id": "b" * 32}, snapshot
        )
        is None
    )
    assert (
        ch.v1_verified_focus_episode(
            state, "source", {**observation, "outer_focused": False}, snapshot
        )
        is None
    )
    stale = {**snapshot, "agents": [{**snapshot["agents"][0], "state_change_seq": 8}]}
    assert ch.v1_verified_focus_episode(state, "source", observation, stale) is None
    assert (
        ch.v1_verified_focus_episode(
            state, "source", {**observation, "agent_state_change_seq": 6}, snapshot
        )
        is None
    )
    blocked = {
        **snapshot,
        "agents": [{**snapshot["agents"][0], "agent_status": "blocked"}],
    }
    assert ch.v1_verified_focus_episode(state, "source", observation, blocked) is None
    entry = state["v1"]["sources"]["source"]["attention"]["p"]
    entry["server_boot_id"] = "b" * 32
    assert ch.v1_verified_focus_episode(state, "source", observation, snapshot) is None


def test_v1_focus_episode_after_agent_release_requires_presented_release():
    boot = "a" * 32
    entry = {
        "status": "done",
        "episode": "gen:t:7",
        "generation": "gen",
        "terminal_id": "t",
        "sequence": 7,
        "server_boot_id": boot,
    }
    state = {"v1": {"sources": {"source": {"attention": {"p": entry}}}}}
    observation = {
        "boot_id": boot,
        "pane_id": "p",
        "outer_focused": True,
        "agent_state_change_seq": None,
        "pane_state": {
            "terminal_id": "t",
            "state_change_seq": 8,
            "agent_present": False,
            "released_completion_seq": 7,
        },
    }
    snapshot = {
        "server_boot_id": boot,
        "agents": [],
        "panes": [{"pane_id": "p", "terminal_id": "t", "state_change_seq": 8}],
    }
    assert (
        ch.v1_verified_focus_episode(state, "source", observation, snapshot)
        == "gen:t:7"
    )
    simultaneous = {
        **observation,
        "pane_state": {**observation["pane_state"], "state_change_seq": 7},
    }
    simultaneous_snapshot = {
        **snapshot,
        "panes": [{**snapshot["panes"][0], "state_change_seq": 7}],
    }
    assert (
        ch.v1_verified_focus_episode(
            state, "source", simultaneous, simultaneous_snapshot
        )
        == "gen:t:7"
    )
    missing_provenance = {
        **simultaneous,
        "pane_state": {**simultaneous["pane_state"], "released_completion_seq": None},
    }
    assert (
        ch.v1_verified_focus_episode(
            state, "source", missing_provenance, simultaneous_snapshot
        )
        is None
    )
    for pane_state in (
        None,
        {**observation["pane_state"], "agent_present": True},
        {**observation["pane_state"], "terminal_id": "other"},
        {**observation["pane_state"], "state_change_seq": 7},
        {**observation["pane_state"], "state_change_seq": 9},
    ):
        assert (
            ch.v1_verified_focus_episode(
                state, "source", {**observation, "pane_state": pane_state}, snapshot
            )
            is None
        )
    assert (
        ch.v1_verified_focus_episode(
            state, "source", observation, {**snapshot, "panes": snapshot["panes"] * 2}
        )
        is None
    )
    assert (
        ch.v1_verified_focus_episode(
            state, "source", observation, {**snapshot, "agents": [{"pane_id": "p"}]}
        )
        is None
    )
    later = {
        **observation,
        "pane_state": {
            **observation["pane_state"],
            "state_change_seq": 10,
            "released_completion_seq": 9,
        },
    }
    later_snapshot = {
        **snapshot,
        "panes": [{**snapshot["panes"][0], "state_change_seq": 10}],
    }
    assert ch.v1_verified_focus_episode(state, "source", later, later_snapshot) is None
    later["pane_state"]["released_completion_seq"] = 7
    assert (
        ch.v1_verified_focus_episode(state, "source", later, later_snapshot)
        == "gen:t:7"
    )


def test_v1_saved_unknown_state_recovers_exact_completion_identity():
    boot = "a" * 32
    entry = {
        "status": "done",
        "episode": "g:t:7",
        "sequence": 8,
        "generation": "g",
        "terminal_id": "t",
        "server_boot_id": boot,
    }
    state = {
        "v1": {
            "sources": {
                "source": {
                    "attention": {"p": entry},
                    "panes": {"p": {"terminal_id": "t", "status": "unknown"}},
                    "server_boot_id": boot,
                    "cursor_generation": "g",
                }
            }
        }
    }
    observation = {
        "boot_id": boot,
        "pane_id": "p",
        "outer_focused": True,
        "agent_state_change_seq": None,
        "pane_state": {
            "terminal_id": "t",
            "state_change_seq": 10,
            "agent_present": False,
            "released_completion_seq": 7,
        },
    }
    snapshot = {
        "server_boot_id": boot,
        "agents": [],
        "panes": [{"pane_id": "p", "terminal_id": "t", "state_change_seq": 10}],
    }
    assert (
        ch.v1_verified_focus_episode(state, "source", observation, snapshot) == "g:t:7"
    )
    assert ch.apply_v1_status_event(
        state,
        "source",
        {
            "pane_id": "p",
            "terminal_id": "t",
            "state_change_seq": 9,
            "agent_status": "unknown",
        },
        event_boot=boot,
    )
    assert (
        ch.v1_verified_focus_episode(state, "source", observation, snapshot) == "g:t:7"
    )
    entry["episode"] = "other:t:7"
    assert ch.v1_verified_focus_episode(state, "source", observation, snapshot) is None


def test_v1_focus_acknowledges_only_verified_focused_done():
    state = {
        "v1": {
            "sources": {
                "source": {
                    "attention": {
                        "p": {"status": "done", "episode": "done-1"},
                        "q": {"status": "blocked", "episode": "blocked-1"},
                        "r": {"status": "done", "episode": "done-2"},
                    }
                }
            }
        }
    }
    evidence = {
        "single_client": True,
        "foreground": True,
        "surface_focused": True,
        "pane_id": "p",
        "episode": "done-1",
    }
    for condition in ("single_client", "foreground", "surface_focused"):
        assert not ch.apply_v1_focus_ack(
            state, "source", {**evidence, condition: False}
        )
    assert not ch.apply_v1_focus_ack(state, "source", {**evidence, "episode": "stale"})
    assert not ch.apply_v1_focus_ack(
        state, "source", {**evidence, "pane_id": "q", "episode": "blocked-1"}
    )
    assert ch.apply_v1_focus_ack(state, "source", evidence)
    attention = state["v1"]["sources"]["source"]["attention"]
    assert attention["p"]["acknowledged"] == "done-1"
    assert attention["q"]["status"] == "blocked"
    assert attention["r"]["status"] == "done"


@pytest.mark.parametrize("release_sequence", [7, 11])
def test_v1_focus_reconciliation_acknowledges_presented_release(
    monkeypatch, tmp_path, release_sequence
):
    boot = "a" * 32
    episode = "generation:t:7"
    state = {
        "v1": {
            "clients": {
                "c": {
                    "source": "src",
                    "workspace": "w",
                    "surface": "s",
                    "observed_client_pid": 42,
                    "observed_client_instance": "instance",
                    "endpoint": "local",
                    "connection_generation": 4,
                    "server_boot_id": boot,
                }
            },
            "sources": {
                "src": {
                    "attention": {
                        "p": {
                            "status": "done",
                            "episode": episode,
                            "generation": "generation",
                            "server_boot_id": boot,
                            "terminal_id": "t",
                            "sequence": 7,
                        },
                        "q": {"status": "blocked", "episode": "blocked"},
                    }
                }
            },
        }
    }
    ch.save_state(state)
    monkeypatch.setattr(ch, "prune_v1_clients", lambda state: None)
    monkeypatch.setattr(ch, "v1_cmux_is_foreground", lambda *args: True)
    observation = {
        "pid": 42,
        "instance": "instance",
        "observation": {
            "boot_id": boot,
            "endpoint": "local",
            "connection_generation": 4,
            "pane_id": "p",
            "outer_focused": True,
            "agent_state_change_seq": None,
            "pane_state": {
                "terminal_id": "t",
                "state_change_seq": release_sequence,
                "released_completion_seq": 7,
                "agent_present": False,
            },
        },
    }
    snapshot = {
        "server_boot_id": boot,
        "agents": [],
        "panes": [
            {"pane_id": "p", "terminal_id": "t", "state_change_seq": release_sequence}
        ],
    }
    monkeypatch.setattr(
        ch, "socket_rpc", lambda *args, **kwargs: {"snapshot": snapshot}
    )
    monkeypatch.setattr(ch, "v1_surface_observation", lambda *args: observation)

    observation["observation"]["pane_state"]["released_completion_seq"] = 9
    assert not ch.v1_reconcile_focus({}, "c", "src", "w", "s", "/socket")
    assert (
        "acknowledged" not in ch.load_state()["v1"]["sources"]["src"]["attention"]["p"]
    )

    observation["observation"]["pane_state"]["released_completion_seq"] = 7
    assert ch.v1_reconcile_focus({}, "c", "src", "w", "s", "/socket")
    attention = ch.load_state()["v1"]["sources"]["src"]["attention"]
    assert attention["p"]["acknowledged"] == episode
    assert attention["q"]["status"] == "blocked"
    assert "acknowledged" not in attention["q"]


def test_v1_focus_reconciliation_acknowledges_only_stable_current_target(
    monkeypatch, tmp_path
):
    boot = "a" * 32
    episode = "generation:t:7"
    state = {
        "v1": {
            "clients": {
                "c": {
                    "source": "src",
                    "workspace": "w",
                    "surface": "s",
                    "observed_client_pid": 42,
                    "observed_client_instance": "instance",
                    "endpoint": "local",
                    "connection_generation": 4,
                    "server_boot_id": boot,
                }
            },
            "sources": {
                "src": {
                    "attention": {
                        "p": {
                            "status": "done",
                            "episode": episode,
                            "generation": "generation",
                            "server_boot_id": boot,
                            "terminal_id": "t",
                            "sequence": 7,
                        },
                        "q": {"status": "blocked", "episode": "blocked"},
                    }
                }
            },
        }
    }
    ch.save_state(state)
    monkeypatch.setattr(ch, "prune_v1_clients", lambda state: None)
    monkeypatch.setattr(ch, "v1_cmux_is_foreground", lambda *args: True)
    observation = {
        "pid": 42,
        "instance": "instance",
        "observation": {
            "boot_id": boot,
            "endpoint": "local",
            "connection_generation": 4,
            "snapshot_revision": 1,
            "pane_id": "p",
            "outer_focused": True,
            "agent_state_change_seq": 7,
        },
    }
    observed = []
    monkeypatch.setattr(ch, "v1_surface_observation", lambda *args: observed.pop(0))
    monkeypatch.setattr(
        ch,
        "socket_rpc",
        lambda *args, **kwargs: {
            "snapshot": {
                "server_boot_id": boot,
                "agents": [
                    {
                        "pane_id": "p",
                        "terminal_id": "t",
                        "agent_status": "done",
                        "state_change_seq": 7,
                    }
                ],
            }
        },
    )
    for wrong in (
        {**observation, "pid": 43},
        {**observation, "instance": "other"},
        {
            **observation,
            "observation": {
                **observation["observation"],
                "endpoint": "ssh:" + "b" * 32,
            },
        },
        {
            **observation,
            "observation": {**observation["observation"], "connection_generation": 5},
        },
    ):
        observed[:] = [wrong, wrong]
        assert not ch.v1_reconcile_focus({}, "c", "src", "w", "s", "/socket")
    ownership = {"connection_id": "original"}
    state["v1"]["clients"]["c"]["native_ownership"] = ownership
    ch.save_state(state)
    observed[:] = [observation, observation]
    assert not ch.v1_reconcile_focus({}, "c", "src", "w", "s", "/socket")
    wrong = {**observation, "native_ownership": {"connection_id": "replacement"}}
    observed[:] = [wrong, wrong]
    assert not ch.v1_reconcile_focus({}, "c", "src", "w", "s", "/socket")
    observation["native_ownership"] = ownership
    observed[:] = [
        {
            **observation,
            "observation": {**observation["observation"], "snapshot_revision": 2},
        },
        observation,
    ]
    assert not ch.v1_reconcile_focus({}, "c", "src", "w", "s", "/socket")
    observed[:] = [
        observation,
        {
            **observation,
            "observation": {**observation["observation"], "snapshot_revision": 2},
        },
    ]
    assert ch.v1_reconcile_focus({}, "c", "src", "w", "s", "/socket")
    attention = ch.load_state()["v1"]["sources"]["src"]["attention"]
    assert attention["p"]["acknowledged"] == episode
    assert attention["q"]["status"] == "blocked"
    ch.save_state(state)
    observed[:] = [observation, {**observation, "instance": "other"}]
    assert not ch.v1_reconcile_focus({}, "c", "src", "w", "s", "/socket")
    assert ch.load_state()["v1"] == state["v1"]


def test_v1_focus_reconciliation_combines_real_cmux_and_client_checks(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ch, "prune_v1_clients", lambda state: None)
    boot = "a" * 32
    state = {
        "v1": {
            "clients": {
                "c": {
                    "source": "src",
                    "workspace": "w",
                    "surface": "s",
                    "observed_client_pid": 20,
                    "observed_client_instance": "i",
                    "endpoint": "local",
                    "connection_generation": 4,
                    "server_boot_id": boot,
                }
            },
            "sources": {
                "src": {
                    "attention": {
                        "p": {
                            "status": "done",
                            "episode": "g:t:7",
                            "generation": "g",
                            "server_boot_id": boot,
                            "terminal_id": "t",
                            "sequence": 7,
                        }
                    }
                }
            },
        }
    }
    ch.save_state(state)
    caller = {
        "window_ref": "window:1",
        "workspace_ref": "workspace:1",
        "surface_ref": "surface:1",
    }
    terminal = {
        "workspace_id": "w",
        "surface_id": "s",
        "mapped": True,
        "workspace_selected": True,
        "window_key": True,
        "window_visible": True,
        "surface_view_first_responder": True,
    }
    surface = {
        "id": "s",
        "type": "terminal",
        "tty": "/dev/ttys001",
        "processes": [
            {
                "pid": 20,
                "ppid": 10,
                "name": "herdr",
                "path": "/bin/herdr",
                "tty_device": 100,
                "pgid": 20,
                "tpgid": 20,
                "children": [],
            }
        ],
    }
    tree = {
        "windows": [{"workspaces": [{"id": "w", "panes": [{"surfaces": [surface]}]}]}]
    }

    def fake_run(command, **kwargs):
        if "identify" in command:
            result = {
                "caller": caller,
                "focused": caller,
                "bundle_identifier": "com.cmuxterm.app",
                "socket_path": "/cmux.sock",
            }
        elif "debug.terminals" in command:
            result = {"terminals": [terminal]}
        else:
            result = tree
        return SimpleNamespace(returncode=0, stdout=json.dumps(result))

    monkeypatch.setattr(ch, "run", fake_run)
    monkeypatch.setattr(ch, "v1_frontmost_app", lambda: ("com.cmuxterm.app", 12))
    monkeypatch.setattr(ch, "v1_unix_peer_pid", lambda path: 12)
    monkeypatch.setattr(ch, "v1_tty_device", lambda tty: 100)
    monkeypatch.setattr(
        ch,
        "v1_observer_socket_exists",
        lambda path: (
            path.parent.name == "client-observers"
            and path.parent.parent.name == "herdr"
        ),
    )
    monkeypatch.setattr(
        ch,
        "v1_client_source_inventory",
        lambda *args, **kwargs: {
            "pid": 20,
            "instance": "i",
            "sources_complete": True,
            "sources": [],
        },
    )
    observation = {
        "pid": 20,
        "instance": "i",
        "observation": {
            "boot_id": boot,
            "endpoint": "local",
            "connection_generation": 4,
            "pane_id": "p",
            "outer_focused": None,
            "agent_state_change_seq": 7,
        },
    }
    monkeypatch.setattr(
        ch, "v1_client_observation", lambda path, pid, **kwargs: observation
    )
    monkeypatch.setattr(
        ch,
        "socket_rpc",
        lambda *args, **kwargs: {
            "snapshot": {
                "server_boot_id": boot,
                "agents": [
                    {
                        "pane_id": "p",
                        "terminal_id": "t",
                        "agent_status": "done",
                        "state_change_seq": 7,
                    }
                ],
            }
        },
    )
    terminal["surface_view_first_responder"] = False
    assert not ch.v1_reconcile_focus({}, "c", "src", "w", "s", "/socket")
    terminal["surface_view_first_responder"] = True
    assert ch.v1_reconcile_focus({}, "c", "src", "w", "s", "/socket")
    assert (
        ch.load_state()["v1"]["sources"]["src"]["attention"]["p"]["acknowledged"]
        == "g:t:7"
    )
