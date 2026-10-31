import os
import tempfile
from pathlib import Path


def status_event(pane_id, ws_id, status, agent="claude", title="task"):
    return {
        "event": "pane_agent_status_changed",
        "data": {
            "type": "pane_agent_status_changed",
            "pane_id": pane_id,
            "workspace_id": ws_id,
            "agent": agent,
            "title": title,
            "agent_status": status,
            "state_labels": {},
        },
    }


def remote_agent(pane_id, status, agent="claude", title="task", ws_id="w1"):
    return {
        "pane_id": pane_id,
        "workspace_id": ws_id,
        "agent": agent,
        "agent_status": status,
        "terminal_title_stripped": title,
    }


def short_sock_path(name):
    # AF_UNIX paths are limited to ~104 chars on macOS; tmp_path is too long.
    path = Path(tempfile.gettempdir()) / f"ch-{os.getpid()}-{name}.sock"
    path.unlink(missing_ok=True)
    return path
