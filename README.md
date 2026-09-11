# ChatGPT Matrix Bridge

A Matrix bridge that discovers saved ChatGPT conversations and gives each one its
own room. It uses the signed-in local collector from
[alaq/codex-chatgpt-web](https://github.com/alaq/codex-chatgpt-web/tree/feat/saved-history-collector)
and the [mautrix-go bridgev2 framework](https://github.com/mautrix/go).

**Current milestone: read-only text mirroring.** The bridge compiles, reads the live
collector feed, and passes framework integration tests for room creation, continued
conversations, renaming, failed-send retry, and restart deduplication. No live
Matrix/Beeper deployment has been validated yet. Sending from Matrix into a saved
ChatGPT thread is the next major capability; this version rejects outbound sends
explicitly. Work and Codex are planned source adapters.

## Behavior

- A new saved conversation discovered after activation creates a room automatically.
- An old conversation updated after activation also enters the room list. Further
  updates and title changes reuse that room.
- Conversation and message IDs are scoped to source/account identity. Equal titles
  do not merge rooms. Vault project matching is independent of room creation.
- The first login stores an activation timestamp. It survives normal restarts and
  relogin; set `network.since` before first login to include an older window.
- Each poll runs the existing bounded incremental collector, then reads a snapshot.
  Default polling is 60 seconds plus collection/delivery time, not a latency SLA.
- Visible user and assistant text is mirrored. Attachment counts link the reader
  back to the original conversation. Hidden reasoning, raw nodes, and attachment
  credentials are not exported.

## Build and test

Requires Go 1.26.2+, a C compiler for SQLite, and Python 3 for the shared collector.
The `goolm` tag includes the framework's Go encryption implementation.

```sh
go test -race -tags goolm ./...
go build -tags goolm -o bin/chatgpt-matrix-bridge ./cmd/chatgpt-matrix-bridge
go build -o bin/source-check ./cmd/source-check
```

Check a local archive without any Matrix connection or writes:

```sh
bin/source-check \
  --backend /absolute/path/to/codex-chatgpt-web \
  --archive /absolute/path/to/private/archive \
  --since 2026-09-11T00:00:00Z
```

Add `--refresh` to run incremental collection first, and `--descriptor` if the
backend uses a non-default isolated DEV descriptor. The tool prints counts and
coverage only. It does not print conversation text, titles, or account credentials.
The backend must include `scripts/history/bridge_feed.py` and the `feed` CLI command.

## Configure a dedicated bridge

Generate a full example without starting anything:

```sh
bin/chatgpt-matrix-bridge -e -c config.yaml
```

For Beeper, [Beeper Bridge Manager](https://github.com/beeper/bridge-manager) supports
third-party bridgev2 services. After its account setup, the documented configuration
command is:

```sh
bbctl config --type bridgev2 -o config.yaml sh-chatgpt
```

That command registers/configures a real bridge: use a dedicated name and config,
with separate database and credentials from existing bridges. Add the `network`
section from the generated example, using absolute local paths:

```yaml
network:
  enabled: true
  python: python3
  backend_dir: /absolute/path/to/codex-chatgpt-web
  archive_dir: /absolute/path/to/private/archive
  descriptor: /absolute/path/to/launcher-browser.json
  poll_seconds: 60
  since: ""
```

Before the first start, configure only your own Matrix user in `bridge.permissions`,
set `bridge.split_portals: true`, and use `bridge.personal_filtering_spaces: false`
for the initial Beeper test. Set `encryption.allow`, `encryption.default`, and
`encryption.require` to `true`. Beeper credentials and the Matrix connection must be
verified in the live pilot; an encrypted build alone is not proof of encryption.
Keep configuration and databases outside the repository, with owner-only access.

Start the bridge with that configuration, then initiate its `local-collector` login
flow through the bridge's management interface. The flow reuses the configured
collector account and never requests ChatGPT passwords or cookies in Matrix.
The collector's isolated DEV browser must already be signed in and have history
reading enabled. The exact deployment sequence has not yet been tested in Beeper.

```sh
bin/chatgpt-matrix-bridge -c /absolute/private/path/config.yaml
```

## Boundaries and remaining work

- This pilot is single-operator. Treat configuration and the local backend checkout
  as trusted executable inputs. Restrict Matrix login permission to the operator.
- Sending into saved ChatGPT threads is unimplemented; Temporary Chats cannot serve
  as a substitute for the original saved conversation.
- Existing-message text edits are detected and pause that conversation rather than
  generating duplicate messages. Branch switches/deletions are not reconciled with
  previously mirrored history. That fidelity is required before general use.
- The framework's database deduplicates acknowledged messages and normal restarts.
  A process crash after a homeserver accepts a send but before the database records
  it remains an ambiguous-delivery window. Exactly-once delivery is not claimed.
  See [the validation plan](docs/validation.md).
- Collection covers the regular saved-history endpoint. Project-only/archived-only
  discovery, exhaustive history, attachment binaries, and long-running reliability
  remain unverified.
- Browser/session code stays in the backend fork. This repository owns Matrix
  rooms, source adapters, and delivery behavior. It does not file vault notes,
  approve project matches, or run an LLM for routing.

See [architecture](docs/architecture.md) and [validation](docs/validation.md).
