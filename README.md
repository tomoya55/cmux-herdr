# cmux-herdr

A [herdr](https://herdr.dev) plugin that propagates agent status (working / waiting for input / finished) to the [cmux](https://github.com/manaflow-ai/cmux) sidebar.

When you run a herdr session inside a cmux workspace, cmux has no way to know what the agents inside herdr are doing. This plugin bridges that gap — the sidebar shows *current state* (a signal that prompts your next action), never an append-only log:

- **Sidebar notifications** when an agent needs attention:
  - `blocked` → `<agent>: waiting for input`
  - `done` → `<agent>: finished`
- **A status pill** on the cmux workspace showing the live aggregate, e.g. `1 waiting · 2 finished · 1 working` (orange with an hourglass icon while any agent waits for input, green with a checkmark while finished results are unread, blue with a bolt icon while agents are working, cleared when everything is idle)
- **A per-agent status entry** next to the aggregate (e.g. `claude: waiting`, `codex: finished`), so you can see *which* agent needs you. Entries are keyed sidebar state: they update in place while a status holds and disappear the moment the agent leaves it
- **Unread semantics for `finished`**: herdr's `done` means "finished since you last looked". Viewing the pane flips it to `idle`, which clears the entry and marks the notification read — the sidebar only shows what still needs your attention
- Notifications are also marked read when you focus the herdr workspace, and as soon as an agent leaves `blocked` — answering or dismissing the prompt means the notification has been actioned
- **Self-healing reconcile** on herdr server start: rebuilds pills from `herdr agent list`, clears pills/notifications left behind by sessions that ended while the server was down, and sweeps orphaned `herdr.*` sidebar entries that plugin state no longer knows about

## Manual refresh

If the cmux sidebar ever looks stale (stuck `waiting` pills or unread badges), invoke the plugin action:

```bash
herdr plugin action invoke cmux-herdr.refresh
```

This re-runs the full reconcile: it re-pushes the current state entries, clears orphaned `herdr.*` pills across all cmux workspaces, and marks leftover herdr notifications as read (as long as no agent is currently waiting for input). The first reconcile after upgrading from 0.4.x also clears the accumulated sidebar log once. You can also bind it to a key:

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

The plugin must be linked into the herdr **server** that runs on the same machine as cmux. Hooks are executed by the herdr server, so a herdr session attached via `--remote` from the cmux machine will not trigger them — use the remote daemon below for those sessions.

## Remote sessions

Sessions attached via `herdr --remote <host>` run on a remote herdr server, so the local plugin never sees their events. To bridge them, list the remote sessions in `config.toml`:

```toml
[[remotes]]
ssh_target = "maguro.example.ts.net"   # anything ssh(1) accepts
session = "tom"                        # remote herdr session name
cmux_title = "maguro:hd:tom"           # title of the cmux workspace to update
# name = "maguro-tom"                  # optional; defaults to "<ssh_target>:<session>"
                                       # (hashed "remote-<digest>" for IPv6/URI targets)
# poll_seconds = 2                     # optional
# herdr_bin = "/home/tom/.local/bin/herdr"  # optional; set an absolute path when
                                       # herdr is not on the remote's non-interactive PATH
```

The plugin's startup hook spawns a local daemon (`cmux_herdr.py remote`, logs to `$HERDR_PLUGIN_STATE_DIR/remote.log`) that for each `[[remotes]]` entry:

1. Discovers the remote session's API socket via `ssh <target> herdr session list --json`
2. Forwards it to a local unix socket with `ssh -L <local>:<remote> -N` (stock OpenSSH, nothing to install on the remote)
3. Polls `agent.list` / `workspace.list` over the herdr socket protocol and pushes the aggregate to the cmux workspace whose title matches `cmux_title`

Remote agents then get the same treatment as local ones — notifications on `blocked`/`done` and the same state pill plus per-agent entries (status keys `herdr.remote.<name>[.<pane>]`) — aggregated across the whole remote session. Notifications are marked read as soon as no remote agent needs attention. The tunnel reconnects automatically after network or remote server restarts, and the daemon picks up `config.toml` edits (including removing or retargeting remotes) within a few seconds. A newer daemon spawned after a plugin upgrade or local checkout edit replaces an outdated one automatically.

If the daemon was not running when you add your first `[[remotes]]` entry (it exits immediately when no remotes are configured), start it via the refresh action (`herdr plugin action invoke cmux-herdr.refresh`) or by restarting the herdr server.

**SSH auth must be non-interactive**: the daemon runs `ssh` with `BatchMode=yes`, so the remote host must accept a local key. If interactive login needs a password or Touch ID, authorize a key once:

```bash
ssh <target> 'mkdir -p ~/.ssh && cat >> ~/.ssh/authorized_keys' < ~/.ssh/id_ed25519.pub
```

Unlike local sessions, remote workspaces cannot be auto-detected (the TUI runs locally but the agents run remotely), so `cmux_title` is required, and `done`/`idle` transitions caused by focusing the remote TUI are only observed at the next poll.

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

Per-agent entries use status keys like `herdr.<workspace>.<pane>` (local) and `herdr.remote.<name>.<pane>` (remotes). They are cleared when the pane leaves the status (viewing a finished pane flips it to `idle`), when the pane/workspace closes, on retire/reroute for remotes, and by the orphan sweeps.

Upgrading from 0.4.x or older: those versions appended every transition to the cmux sidebar log, where entries piled up forever. On the first reconcile after the upgrade the plugin runs `cmux clear-log` once on every workspace it ever wrote to (status keys, state labels, and remote targets). Log entries cannot be filtered by source, so this also drops unrelated entries in those workspaces' logs.

## How it works

`herdr-plugin.toml` declares event hooks for `pane.agent_status_changed`, `pane.closed`, `workspace.closed`, `workspace.focused`, and `workspace.renamed`, plus a `startup` hook and a `refresh` action (both run the reconcile path). herdr spawns `cmux_herdr.py` for each event, passing the payload in `HERDR_PLUGIN_EVENT_JSON`. The script keeps per-pane status in `$HERDR_PLUGIN_STATE_DIR/state.json`, aggregates it per herdr workspace, and drives the cmux CLI (`notify`, `set-status` / `clear-status`, `mark-notification-read`). All cmux failures are logged and tolerated, so a closed cmux app never breaks herdr.

The state directory is shared by every herdr session on the machine, but workspace ids are only unique within a session, so `state.json` is namespaced per herdr session (`HERDR_SESSION`). Reconcile (startup/refresh) rebuilds only the invoking session's bucket — `herdr agent list` is session-scoped — and never touches other sessions' pills; the orphan sweep and notification sweep decide from the union of all sessions. Buckets of sessions that have ended without restarting are pruned (via `herdr session list`) so their pills do not linger.

Remote sessions are handled separately by a long-lived `cmux_herdr.py remote` daemon (spawned by the startup hook, single instance guarded by a pidfile). It keeps its own `remote-state.json`, so it never races the event-hook writes to `state.json`, and its `herdr.remote.*` pills are excluded from the local orphan sweep.

### Why there is also a `watch` daemon

herdr does not run plugin hooks for the `done` -> `idle` transition — the one that fires when you open a finished pane and that is supposed to retire its `finished` entry. Without help, the green entry would survive until the agent was next used. The transition *is* published on the herdr API socket, so `cmux_herdr.py watch` (one per session, spawned by the startup hook and the refresh action, single instance guarded by a `watch-<session>.pid` pidfile) subscribes there and retires the pane the hook never heard about. Measured against herdr 0.9.0; `working` -> `idle` is dispatched normally, only `done` -> `idle` is dropped.

The subscriber only ever *removes* panes, and only after re-reading the pane's current status from herdr while holding the state lock. Hooks stay the sole writer for everything herdr does deliver, so the two cannot race and no notification is ever raised twice.

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
