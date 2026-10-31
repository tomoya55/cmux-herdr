from types import SimpleNamespace

import pytest

import cmux_herdr as ch


@pytest.fixture
def state():
    return {"sessions": {}}


@pytest.fixture
def bucket(state):
    return ch.session_bucket(state)


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
def isolated_state_dir(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "state_dir", tmp_path)
    monkeypatch.setattr(ch, "STATE_PATH", tmp_path / "state.json")


@pytest.fixture(autouse=True)
def isolated_publisher_lease_home(monkeypatch, tmp_path):
    monkeypatch.setattr(ch, "_v1_publisher_lease_home", lambda: tmp_path)


@pytest.fixture
def cfg():
    return {"workspaces": {"w1": "workspace:1", "w2": "workspace:2"}}


@pytest.fixture
def cmux_calls(monkeypatch):
    calls = []
    monkeypatch.setattr(ch, "cmux_notifications_checked", lambda cfg: [])

    def fake_run(cmd, timeout=10):
        calls.append(cmd[1:])
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(ch, "run", fake_run)
    return calls


@pytest.fixture(autouse=True)
def no_label_lookup(monkeypatch):
    monkeypatch.setattr(ch, "workspace_label", lambda ws_id: "")


@pytest.fixture
def remote_title_map(monkeypatch):
    monkeypatch.setattr(
        ch, "cmux_workspaces_by_title", lambda cfg: {"maguro:hd:tom": "workspace:9"}
    )


# --- Watch subscriber --------------------------------------------------------


@pytest.fixture
def watch_state(tmp_path):
    """State on disk, since the subscriber loads and saves it under the lock."""
    return tmp_path / "state.json"


@pytest.fixture
def force_action_identity(monkeypatch):
    monkeypatch.setattr(ch, "v1_herdr_process_identity", lambda pid: {"start_token": 1})
    monkeypatch.setattr(ch, "v1_local_generation", lambda path: "1:2")
