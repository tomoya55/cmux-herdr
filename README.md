# cmux-herdr

A [herdr](https://herdr.dev) plugin that propagates agent status (working / waiting for input / finished) to the [cmux](https://github.com/manaflow-ai/cmux) sidebar.

When you run a herdr session inside a cmux workspace, cmux has no way to know what the agents inside herdr are doing. This plugin bridges that gap:

- **Sidebar notifications** when an agent needs attention:
  - `blocked` → `<agent>: waiting for input`
  - `done` → `<agent>: finished`
- **A status pill** on the cmux workspace showing the live aggregate, e.g. `1 waiting · 2 working` (orange while any agent waits for input, blue while agents are working, cleared when everything is idle)
- Notifications are marked read when you focus the herdr workspace, and also as soon as the agent leaves `blocked`/`done` — answering or dismissing the prompt means the notification has been actioned
- **Self-healing reconcile** on herdr server start: rebuilds pills from `herdr agent list`, clears pills/notifications left behind by sessions that ended while the server was down, and sweeps orphaned `herdr.*` sidebar entries that plugin state no longer knows about

## Manual refresh

If the cmux sidebar ever looks stale (stuck `waiting` pills or unread badges), invoke the plugin action:

```bash
herdr plugin action invoke cmux-herdr.refresh
```

This re-runs the full reconcile: it re-pushes the current aggregate, clears orphaned `herdr.*` pills across all cmux workspaces, and marks leftover herdr notifications as read (as long as no agent is currently waiting for input). You can also bind it to a key:

```toml
[[keys.command]]
key = "prefix+r"
type = "plugin_action"
command = "cmux-herdr.refresh"
description = "refresh cmux sidebar"
```

## Requirements

- macOS
- herdr >= 0.8.0
- cmux.app (the plugin talks to it via the bundled CLI at `/Applications/cmux.app/Contents/Resources/bin/cmux`)
- Python 3.11+ (for `tomllib`)

The plugin must be linked into the herdr **server** that runs on the same machine as cmux. Hooks are executed by the herdr server, so a herdr session attached via `--remote` from the cmux machine will not work.

## Install

```bash
herdr plugin install tomoya55/cmux-herdr
```

Once installed, the plugin stays enabled across herdr server restarts. A `startup` hook reconciles state from `herdr agent list` on server start, and all updates after that are event-driven.

To update to the latest main, reinstall; to pin a release, pass `--ref <tag>`.

For development, link a local checkout instead (hooks spawn the script fresh on every event, so edits take effect without reinstalling):

```bash
git clone https://github.com/tomoya55/cmux-herdr.git
herdr plugin link /path/to/cmux-herdr
```

To remove it:

```bash
herdr plugin uninstall cmux-herdr
```

## How cmux workspaces are resolved

Each herdr event carries a herdr workspace id; the plugin maps it to a cmux workspace in this order:

1. **Explicit config** — `$HERDR_PLUGIN_CONFIG_DIR/config.toml` (print the path with `herdr plugin config-dir cmux-herdr`; see [config.example.toml](config.example.toml))
2. **`CMUX_WORKSPACE_ID` env** — when the herdr server itself runs inside a cmux surface
3. **Auto-detection** — the plugin finds this herdr session's TUI process (`herdr --session <name>` / `herdr session attach <name>`) and locates it in the per-surface process trees from `cmux top --all --processes --json`. The result is cached in the plugin state directory with a stale fallback, so brief TUI disconnects do not break the mapping
4. **Label match** — the herdr workspace label is matched against `cmux list-workspaces` titles

Note that auto-detection maps the whole herdr *session* to the cmux workspace that hosts its TUI, which is the natural topology when one cmux workspace runs one herdr session.

If no mapping is found, the plugin logs a skip message and does nothing — herdr without cmux keeps working undisturbed.

## Verifying the setup

With the herdr TUI open inside a cmux workspace:

```bash
HERDR_PLUGIN_STATE_DIR=~/.local/state/herdr/plugins/cmux-herdr \
HERDR_PLUGIN_CONFIG_DIR=~/.config/herdr/plugins/config/cmux-herdr \
python3 /path/to/cmux-herdr/cmux_herdr.py detect
```

This prints the detected cmux workspace and the candidate herdr TUI PIDs. Then run any agent and watch the cmux sidebar, or check `cmux list-notifications`.

Hook executions (stdout/stderr/exit code) are recorded by herdr:

```bash
herdr plugin log list --plugin cmux-herdr
```

## Configuration

All settings are optional. Copy [config.example.toml](config.example.toml) to `$(herdr plugin config-dir cmux-herdr)/config.toml`:

```toml
# Path to the cmux CLI (default: /Applications/cmux.app/Contents/Resources/bin/cmux)
cmux_bin = "/Applications/cmux.app/Contents/Resources/bin/cmux"

# Match herdr workspace labels against cmux workspace titles (default: true)
match_by_label = true

# Explicit mapping: herdr workspace id -> cmux workspace ref
[workspaces]
# w4 = "workspace:2"

# Explicit mapping: herdr workspace label -> cmux workspace ref
[labels]
# "my-project" = "workspace:3"
```

## How it works

`herdr-plugin.toml` declares event hooks for `pane.agent_status_changed`, `pane.closed`, `workspace.closed`, `workspace.focused`, and `workspace.renamed`, plus a `startup` hook and a `refresh` action (both run the reconcile path). herdr spawns `cmux_herdr.py` for each event, passing the payload in `HERDR_PLUGIN_EVENT_JSON`. The script keeps per-pane status in `$HERDR_PLUGIN_STATE_DIR/state.json`, aggregates it per herdr workspace, and drives the cmux CLI (`notify`, `set-status` / `clear-status`, `mark-notification-read`). All cmux failures are logged and tolerated, so a closed cmux app never breaks herdr.

## Development

Requires [uv](https://docs.astral.sh/uv/):

```bash
uv sync                 # create .venv with dev dependencies
uv run pytest           # run tests
uv run ruff check .     # lint
uv run ruff format .    # format
```

CI runs lint, format check, and tests on Python 3.11 and 3.13. The plugin itself is executed by herdr with the system `python3` (3.11+), so keep it free of third-party runtime dependencies.

## License

[MIT](LICENSE)
