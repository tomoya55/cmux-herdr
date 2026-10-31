import json
import subprocess
import time
from types import SimpleNamespace

from cmux_herdr_core import cmux, core


def test_legacy_workspace_aliases_require_unique_identity(monkeypatch):
    tree = {"windows": [{"workspaces": [{"id": "TARGET", "ref": "workspace:1"}]}]}
    monkeypatch.setattr(
        cmux,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=json.dumps(tree)),
    )
    assert cmux.legacy_workspace_aliases({}, "workspace:1") == {"target", "workspace:1"}
    tree["windows"][0]["workspaces"].append({"id": "OTHER", "ref": "workspace:1"})
    assert cmux.legacy_workspace_aliases({}, "workspace:1") is None


def test_config_mapping_wins_over_env(monkeypatch, state, bucket, cmux_calls):
    monkeypatch.setenv("CMUX_WORKSPACE_ID", "workspace:99")
    cfg = {"workspaces": {"w1": "workspace:1"}}
    ref, _ = cmux.resolve_cmux_workspace(cfg, bucket, "w1")
    assert ref == "workspace:1"


def test_env_fallback(monkeypatch, state, bucket, cmux_calls):
    monkeypatch.setenv("CMUX_WORKSPACE_ID", "workspace:99")
    ref, _ = cmux.resolve_cmux_workspace({}, bucket, "w1")
    assert ref == "workspace:99"


def test_cached_session_ref_used_without_detection(
    monkeypatch, state, bucket, cmux_calls
):
    monkeypatch.delenv("CMUX_WORKSPACE_ID", raising=False)
    bucket["session_ref"] = {"ref": "workspace:7", "at": time.time()}

    def fail_ps(*args, **kwargs):
        raise AssertionError("ps should not run for a fresh cache")

    monkeypatch.setattr(subprocess, "run", fail_ps)
    ref, _ = cmux.resolve_cmux_workspace({}, bucket, "w1")
    assert ref == "workspace:7"


def test_label_match_fallback(monkeypatch, state, bucket, cmux_calls):
    monkeypatch.delenv("CMUX_WORKSPACE_ID", raising=False)
    monkeypatch.setattr(cmux, "herdr_session_pids", lambda: [])
    monkeypatch.setattr(cmux, "workspace_label", lambda ws_id: "My Project")
    monkeypatch.setattr(
        cmux, "cmux_workspaces_by_title", lambda cfg: {"my project": "workspace:5"}
    )
    ref, label = cmux.resolve_cmux_workspace({}, bucket, "w1")
    assert (ref, label) == ("workspace:5", "My Project")


def test_detect_via_cmux_top(monkeypatch, state, bucket):
    monkeypatch.delenv("CMUX_WORKSPACE_ID", raising=False)
    monkeypatch.setattr(cmux, "herdr_session_pids", lambda: [123])
    top = {
        "windows": [
            {
                "workspaces": [
                    {
                        "type": "workspace",
                        "ref": "workspace:3",
                        "surfaces": [{"pid": 100, "children": [{"pid": 123}]}],
                    }
                ],
            }
        ],
    }

    def fake_run(cmd, timeout=10):
        return SimpleNamespace(returncode=0, stdout=json.dumps(top), stderr="")

    monkeypatch.setattr(cmux, "run", fake_run)
    ref = cmux.detect_session_workspace({}, bucket)
    assert ref == "workspace:3"
    assert bucket["session_ref"]["ref"] == "workspace:3"


def test_stale_cache_survives_failed_detection(monkeypatch, state, bucket):
    monkeypatch.delenv("CMUX_WORKSPACE_ID", raising=False)
    bucket["session_ref"] = {"ref": "workspace:7", "at": 0}
    monkeypatch.setattr(cmux, "herdr_session_pids", lambda: [])
    assert cmux.detect_session_workspace({}, bucket) == "workspace:7"


def test_herdr_session_pids_filters(monkeypatch):
    monkeypatch.setenv("HERDR_SESSION", "tom")
    ps_output = "\n".join(
        [
            "  100 /usr/local/bin/herdr server",
            "  200 /usr/local/bin/herdr --session tom",
            "  300 /usr/local/bin/herdr --session gutenberg",
            "  400 /usr/local/bin/herdr session attach tom",
            "  500 /usr/local/bin/herdr --session tom remote-client-bridge",
            "  600 /usr/local/bin/other",
            "  700 /usr/local/bin/herdr",
        ]
    )

    def fake_ps(cmd, **kwargs):
        return SimpleNamespace(returncode=0, stdout=ps_output, stderr="")

    monkeypatch.setattr(subprocess, "run", fake_ps)
    assert sorted(cmux.herdr_session_pids()) == [200, 400, 700]


def test_find_pid_workspace_nested():
    tree = {"rows": [{"workspace_id": "workspace:9", "processes": [{"pid": 42}]}]}
    assert cmux.find_pid_workspace(tree, {42}) == "workspace:9"
    assert cmux.find_pid_workspace(tree, {1}) is None


def test_cmux_workspaces_by_title_parsing(monkeypatch):
    def fake_run(cmd, timeout=10):
        return SimpleNamespace(
            returncode=0,
            stdout="workspace:1  My Project\nworkspace:2  other\n",
            stderr="",
        )

    monkeypatch.setattr(cmux, "run", fake_run)
    assert cmux.cmux_workspaces_by_title({}) == {
        "my project": "workspace:1",
        "other": "workspace:2",
    }


def test_cmux_workspaces_by_title_parses_selected_row(monkeypatch):
    def fake_run(cmd, timeout=10):
        return SimpleNamespace(
            returncode=0,
            stdout=(
                "  workspace:2  hd:tom\n"
                "* workspace:7  maguro:hd:gutenberg  [selected]\n"
            ),
            stderr="",
        )

    monkeypatch.setattr(cmux, "run", fake_run)
    assert cmux.cmux_workspaces_by_title({}) == {
        "hd:tom": "workspace:2",
        "maguro:hd:gutenberg": "workspace:7",
    }


def test_run_scrubs_cmux_caller_identity_env(monkeypatch):
    captured = {}

    def fake_subprocess_run(cmd, **kwargs):
        captured["env"] = kwargs.get("env")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_subprocess_run)
    monkeypatch.setenv("CMUX_PANEL_ID", "panel-uuid")
    monkeypatch.setenv("CMUX_SURFACE_ID", "surface-uuid")
    monkeypatch.setenv("CMUX_TAB_ID", "tab-uuid")
    monkeypatch.setenv("CMUX_WORKSPACE_ID", "ws-uuid")
    monkeypatch.setenv("CMUX_TERMINAL_LIFECYCLE_ID", "lc-uuid")
    monkeypatch.setenv("CMUX_SOCKET_CAPABILITY", "cap-token")

    cmux.run(["cmux", "notify"])

    env = captured["env"]
    assert env is not None
    for var in core.CMUX_CALLER_IDENTITY_VARS:
        assert var not in env
    assert env["CMUX_SOCKET_CAPABILITY"] == "cap-token"


def test_cmux_cli_tolerates_missing_binary(monkeypatch):
    def raise_not_found(cmd, timeout=10):
        raise FileNotFoundError

    monkeypatch.setattr(cmux, "run", raise_not_found)
    assert cmux.cmux_cli({}, ["ping"]) is False


def test_cmux_cli_tolerates_failure(monkeypatch):
    monkeypatch.setattr(
        cmux,
        "run",
        lambda cmd, timeout=10: SimpleNamespace(returncode=1, stdout="", stderr="boom"),
    )
    assert cmux.cmux_cli({}, ["ping"]) is False


def test_cmux_cli_tolerates_timeout(monkeypatch):
    def slow(cmd, timeout=10):
        raise subprocess.TimeoutExpired(cmd, timeout)

    monkeypatch.setattr(cmux, "run", slow)
    assert cmux.cmux_cli({}, ["ping"]) is False


def test_v1_legacy_workspace_scan_keeps_duplicate_titles(monkeypatch):
    monkeypatch.setattr(
        cmux,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0, stdout="* workspace:1 same [selected]\n  workspace:2 same\n"
        ),
    )
    assert cmux.cmux_workspace_refs({}) == {"workspace:1", "workspace:2"}
