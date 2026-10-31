import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import cmux_herdr as ch


def test_concurrent_save_state_does_not_race_tmp(monkeypatch, tmp_path):
    import threading

    # simulate separate hook processes sharing one state file
    monkeypatch.setattr(ch.os, "getpid", threading.get_native_id)
    errors = []

    def save(i):
        try:
            for _ in range(30):
                ch.save_state({"sessions": {str(i): {"workspaces": {}}}})
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=save, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert json.loads((tmp_path / "state.json").read_text())["sessions"]


def test_state_roundtrip(monkeypatch, tmp_path):
    assert ch.load_state() == {"sessions": {}}
    bucket = {"workspaces": {"w1": {"label": "x", "panes": {}}}}
    ch.save_state({"sessions": {"tom": bucket}})
    assert ch.load_state()["sessions"]["tom"]["workspaces"]["w1"]["label"] == "x"


def test_save_state_skips_rewriting_identical_content(tmp_path):
    path = tmp_path / "state.json"
    ch.save_state({"sessions": {"tom": {}}})
    written = path.stat().st_ino

    ch.save_state({"sessions": {"tom": {}}})
    assert path.stat().st_ino == written

    ch.save_state({"sessions": {}})
    assert path.stat().st_ino != written
    assert ch.load_state() == {"sessions": {}}


def test_load_state_drops_legacy_flat_format(monkeypatch, tmp_path):
    (tmp_path / "state.json").write_text(
        json.dumps(
            {
                "workspaces": {"w1": {"label": "x", "panes": {}}},
                "session_ref": {"ref": "workspace:1", "at": 0},
            }
        )
    )
    assert ch.load_state() == {"sessions": {}}


def test_load_config(monkeypatch, tmp_path):
    (tmp_path / "config.toml").write_text('[workspaces]\nw1 = "workspace:1"\n')
    monkeypatch.setattr(ch, "config_dir", tmp_path)
    assert ch.load_config()["workspaces"]["w1"] == "workspace:1"

    (tmp_path / "config.toml").write_text("not = [valid")
    assert ch.load_config() == {}


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        ([], {"session": "", "remote": None}),
        (["--session", "work"], {"session": "work", "remote": None}),
        (["session", "attach", "work"], {"session": "work", "remote": None}),
        (
            ["--remote", "tom@host", "--session", "work"],
            {"session": "work", "remote": "tom@host"},
        ),
        (["agent", "list"], None),
        (["--session", "work", "pane", "list"], None),
        (["--help"], None),
    ],
)
def test_interactive_client_arguments(args, expected):
    assert ch.interactive_client_target(args) == expected


@pytest.mark.parametrize(
    "args", [["--session="], ["--remote="], ["--session"], ["--remote"], ["--version"]]
)
def test_interactive_client_rejects_empty_or_noninteractive_args(args):
    assert ch.interactive_client_target(args) is None


def test_v1_plugin_paths_follow_herdr_xdg_directories():
    state_dir, config_dir = ch.v1_plugin_paths(
        {"HOME": "/home/test", "XDG_STATE_HOME": "/state", "XDG_CONFIG_HOME": "/config"}
    )
    assert state_dir == Path("/state/herdr/plugins/cmux-herdr")
    assert config_dir == Path("/config/herdr/plugins/config/cmux-herdr")


def test_v1_plugin_paths_use_explicit_plugin_environment():
    state_dir, config_dir = ch.v1_plugin_paths(
        {
            "HOME": "/home/test",
            "HERDR_PLUGIN_STATE_DIR": "/explicit/state",
            "HERDR_PLUGIN_CONFIG_DIR": "/explicit/config",
        }
    )
    assert state_dir == Path("/explicit/state")
    assert config_dir == Path("/explicit/config")


def test_v1_source_key_distinguishes_remote_and_default_session():
    assert ch.v1_source_key({"session": "", "remote": None}) == ch.v1_source_key(
        {"session": "default", "remote": None}
    )
    assert ch.v1_source_key({"session": "default", "remote": None}) != ch.v1_source_key(
        {"session": "default", "remote": "tom@host"}
    )
    assert ch.v1_source_key({"session": "a:b", "remote": "c"}) != ch.v1_source_key(
        {"session": "b", "remote": "c:a"}
    )


def test_v1_local_generation_changes_when_socket_identity_changes(tmp_path):
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.touch()
    second.touch()
    assert ch.v1_local_generation(first) != ch.v1_local_generation(second)


def test_v1_remote_generation_quotes_socket_path(monkeypatch):
    calls = []

    def fake_run(cmd, timeout=10):
        calls.append(cmd)
        return SimpleNamespace(returncode=0, stdout="12:34\n", stderr="")

    monkeypatch.setattr(ch, "run", fake_run)
    assert ch.v1_remote_generation("tom@host", "/tmp/socket with spaces") == "12:34"
    assert calls[0][-2] == "tom@host"
    assert "'/tmp/socket with spaces'" in calls[0][-1]


def test_v1_interactive_client_respects_herdr_session_environment(monkeypatch):
    monkeypatch.setenv("HERDR_SESSION", "work")
    monkeypatch.delenv("HERDR_SOCKET_PATH", raising=False)
    assert ch.interactive_client_target([]) == {"session": "work", "remote": None}
    assert ch.interactive_client_target(["--session", "other"]) == {
        "session": "other",
        "remote": None,
    }


def test_v1_interactive_client_tracks_local_socket_override(monkeypatch):
    monkeypatch.setenv("HERDR_SESSION", "work")
    monkeypatch.setenv("HERDR_SOCKET_PATH", "/tmp/custom-herdr.sock")
    target = ch.interactive_client_target([])
    assert target == {
        "session": "work",
        "remote": None,
        "socket_path": "/tmp/custom-herdr.sock",
    }
    assert ch.v1_source_key(target) != ch.v1_source_key(
        {"session": "work", "remote": None}
    )
    assert ch.interactive_client_target(["--session", "other"]) == {
        "session": "other",
        "remote": None,
    }


def test_v1_socket_override_source_identity_ignores_inherited_session():
    one = {"remote": None, "session": "one", "socket_path": "/tmp/custom-herdr.sock"}
    two = {"remote": None, "session": "two", "socket_path": "/tmp/custom-herdr.sock"}
    assert ch.v1_source_key(one) == ch.v1_source_key(two)


def test_v1_remote_source_rejects_conflicting_configured_binaries():
    target = {"remote": "tom@host", "session": "work"}
    config = {
        "remotes": [
            {
                "name": name,
                "ssh_target": "tom@host",
                "session": "work",
                "cmux_title": name,
                "herdr_bin": binary,
            }
            for name, binary in (("a", "/one/herdr"), ("b", "/two/herdr"))
        ]
    }
    assert ch.v1_target_with_remote_config(config, target) is None


@pytest.mark.parametrize(
    "extra",
    [
        {"ssh_target": "other", "session": "other"},
        {
            "ssh_target": "tom@host",
            "session": "work",
            "cmux_title": "other",
            "herdr_bin": "/another/herdr",
        },
    ],
)
def test_v1_remote_source_rejects_invalid_remote_config(extra):
    target = {"remote": "tom@host", "session": "work"}
    config = {
        "remotes": [
            {
                "ssh_target": "tom@host",
                "session": "work",
                "cmux_title": "work",
                "herdr_bin": "/opt/bin/herdr",
            },
            extra,
        ]
    }
    assert ch.v1_target_with_remote_config(config, target) is None


def test_v1_source_has_single_leader_and_recovers_after_exit(monkeypatch, tmp_path):
    with ch.v1_source_leader("remote:server:session") as first:
        assert first is True
        with ch.v1_source_leader("remote:server:session") as second:
            assert second is False
        with ch.v1_source_leader("remote:other:session") as other:
            assert other is True
    with ch.v1_source_leader("remote:server:session") as replacement:
        assert replacement is True
