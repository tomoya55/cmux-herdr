from types import SimpleNamespace

import pytest

from cmux_herdr_core import cmux, core, remote_bridge


@pytest.fixture
def state():
    return {"sessions": {}}


@pytest.fixture
def bucket(state):
    return core.session_bucket(state)


@pytest.fixture
def workspaces(bucket):
    return bucket["workspaces"]


@pytest.fixture(autouse=True)
def no_session_env(monkeypatch):
    # Tests must not inherit the session/workspace of a surrounding herdr or
    # cmux pane; state is namespaced per herdr session.
    monkeypatch.delenv("HERDR_SESSION", raising=False)
    monkeypatch.delenv("HERDR_SOCKET_PATH", raising=False)
    monkeypatch.delenv("CMUX_WORKSPACE_ID", raising=False)


@pytest.fixture(autouse=True)
def fast_loop_tick(monkeypatch):
    monkeypatch.setattr(core, "LOOP_TICK_SECONDS", 0.01)


@pytest.fixture(autouse=True)
def isolated_state_dir(monkeypatch, tmp_path):
    monkeypatch.setattr(core, "state_dir", tmp_path)
    monkeypatch.setattr(core, "STATE_PATH", tmp_path / "state.json")


@pytest.fixture
def cfg():
    return {"workspaces": {"w1": "workspace:1", "w2": "workspace:2"}}


@pytest.fixture
def cmux_calls(monkeypatch):
    calls = []
    monkeypatch.setattr(cmux, "cmux_notifications_checked", lambda cfg: [])

    def fake_run(cmd, timeout=10):
        calls.append(cmd[1:])
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(cmux, "run", fake_run)
    return calls


@pytest.fixture(autouse=True)
def no_label_lookup(monkeypatch):
    monkeypatch.setattr(cmux, "workspace_label", lambda ws_id: "")


@pytest.fixture
def remote_title_map(monkeypatch):
    monkeypatch.setattr(
        cmux, "cmux_workspaces_by_title", lambda cfg: {"maguro:hd:tom": "workspace:9"}
    )


# --- Watch subscriber --------------------------------------------------------


@pytest.fixture
def watch_state(tmp_path):
    """State on disk, since the subscriber loads and saves it under the lock."""
    return tmp_path / "state.json"


@pytest.fixture
def force_action_identity(monkeypatch):
    monkeypatch.setattr(
        remote_bridge, "v1_herdr_process_identity", lambda pid: {"start_token": 1}
    )
    monkeypatch.setattr(core, "v1_local_generation", lambda path: "1:2")
