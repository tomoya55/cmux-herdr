# cmux-herdr

A [Herdr](https://herdr.dev) plugin that shows agent activity and attention in the [cmux](https://github.com/manaflow-ai/cmux) workspace hosting the Herdr client. It uses released Herdr and cmux APIs; neither application requires a custom build.

When an agent needs input, the workspace shows a waiting notification and status. The waiting state remains until Herdr reports that it has ended. With v1 active, a completed result remains unread until the plugin can verify focus on that exact agent pane or the result is explicitly acknowledged.

## Requirements

- macOS and cmux.app
- Herdr 0.9.1 or newer
- Python 3.11 or newer
- zsh for the optional shell integration below

The integration uses the cmux CLI at `/Applications/cmux.app/Contents/Resources/bin/cmux` by default. Set `cmux_bin` in the plugin configuration if it is installed elsewhere.

## Install

```bash
herdr plugin install tomoya55/cmux-herdr
```

For v1 notifications, source the shell integration in `~/.zshrc` and start a new shell inside cmux:

```zsh
source "${XDG_CONFIG_HOME:-$HOME/.config}/herdr/plugins/github/cmux-herdr-8009d818f543/cmux_herdr.zsh"
```

The path above is the managed checkout used by Herdr 0.9.1. If the plugin is linked from a local checkout instead, source that checkout's `cmux_herdr.zsh`. The integration starts a background supervisor when an interactive `herdr` client is launched in cmux; it preserves the normal `herdr` command and its exit status. It discovers other eligible local clients as well.

The supervisor moves existing notification handling to v1 only when it can verify source and workspace coverage. If discovery is incomplete, a configured remote is not covered, or an older Herdr notification cannot be safely identified, it retains the legacy handling and retries the transition. Existing legacy notifications may need to be marked read in cmux before migration can proceed. Restart a Herdr client in cmux after installing the shell integration.

When updating an existing installation, first stop the previous supervisor as described under Troubleshooting, then run `herdr plugin install tomoya55/cmux-herdr` again and start a Herdr client in cmux. An already-running supervisor retains its original code until it exits. For a linked checkout, update that checkout instead of reinstalling.

For development, link a checkout instead of installing it:

```bash
git clone https://github.com/tomoya55/cmux-herdr.git
herdr plugin link /path/to/cmux-herdr
```

## Acknowledge a completion

Invoke the Herdr plugin action **Mark focused agent result as read** for the agent pane. It acknowledges the current completed result only; it does not clear an agent that is waiting for input. To invoke it from the Herdr UI, add a key binding to `~/.config/herdr/config.toml` (or the corresponding file under `XDG_CONFIG_HOME`):

```toml
[[keys.command]]
key = "prefix+r"
type = "plugin_action"
command = "cmux-herdr.mark-agent-read"
description = "mark focused agent result as read"
```

Run `herdr server reload-config` or restart the Herdr server after editing the configuration. Focus the intended agent pane and press the configured key.

Herdr 0.9.1 does not expose enough information to prove which agent pane a particular client is presenting in every situation. In those cases, merely displaying or focusing a pane does not automatically acknowledge the plugin's completion notification; use the action. The action is effective when v1 is active and a completion token is available for that pane.

The plugin only marks its own identified v1 notification read. It defers a notification while an unrelated notification in the same cmux workspace is unread, and retries while attention remains. The existing cmux API cannot make the check and creation atomic: a notification arriving between those operations may still be replaced. An interrupted creation can occasionally be repeated.

## Remote sessions

The legacy remote bridge can poll a configured Herdr session over an SSH Unix-socket forward. It requires non-interactive SSH authentication and an installed remote Herdr executable. Add an entry to the plugin's `config.toml`:

```toml
[[remotes]]
ssh_target = "host.example"
session = "work"
cmux_title = "host:work"
# poll_seconds = 2
# herdr_bin = "/home/user/.local/bin/herdr"
```

The remote bridge uses `cmux_title` to choose a workspace. With upstream Herdr 0.9.1, the plugin cannot always prove which cmux surface owns a native SSH client. Such clients and configured remote sources can prevent the v1 handoff; legacy remote status reporting remains available, but the v1 per-agent acknowledgment guarantees and action are not available for those sources until they are verified. A missing remote agent or an `unknown` state does not by itself prove that a previously blocked agent resumed.

The startup hook starts the remote bridge. After changing its configuration, invoke **Refresh cmux sidebar** in Herdr or restart the local Herdr server. The refresh action is also available through the CLI:

```bash
herdr plugin action invoke cmux-herdr.refresh
```

See [config.example.toml](config.example.toml) for additional options. The configuration directory is printed by `herdr plugin config-dir cmux-herdr`.

## Troubleshooting

- If a completion remains unread after viewing it, focus the intended agent pane and invoke **Mark focused agent result as read**.
- If another notification is unread in the same cmux workspace, read it first so the plugin can retry the deferred notification.
- If v1 does not activate, check that the shell integration is sourced, the Herdr client was started in cmux, and the source and destination can be identified. Ambiguous clients, unassociated running sessions, configured remotes, or unread legacy notifications can delay migration.
- Plugin hook and action logs are available with `herdr plugin log list --plugin cmux-herdr`. The legacy daemons log in the plugin state directory.
- To stop a supervisor or a legacy `remote` or `watch` daemon, find this plugin's processes with `ps -axo pid=,command= | grep '[c]mux_herdr.py'`, verify their checkout path and mode, then run `kill <pid>` for the intended processes. The supervisor stops its source workers when it exits. A supervisor does not automatically exit when the plugin is uninstalled.

To remove the plugin, stop its running supervisor and legacy daemons as described above, remove the shell integration line, and run:

```bash
herdr plugin uninstall cmux-herdr
```

## Development

Requires [uv](https://docs.astral.sh/uv/):

```bash
uv sync
uv run pytest
uv run ruff check .
uv run ruff format --check .
```

CI tests Python 3.11 and 3.13. The plugin has no third-party runtime dependencies.

## License

[MIT](LICENSE)
