# ChatGPT Matrix Bridge

A Matrix bridge that discovers saved ChatGPT conversations and gives each one its
own room. It uses the signed-in local collector from
[alaq/codex-chatgpt-web](https://github.com/alaq/codex-chatgpt-web/tree/feat/saved-history-collector)
and the [mautrix-go bridgev2 framework](https://github.com/mautrix/go).

**Current milestone: a live encrypted pilot with saved-thread sending.** A dedicated
Beeper bridge has discovered a synthetic saved conversation, mirrored its history
and a fresh ChatGPT-side turn, and survived restart without duplicate rooms or
messages. The saved sender was verified against the original ChatGPT conversation.
A message sent from the operator's Beeper account continued that same saved thread,
and its answer returned without an outbound echo. A separately created ordinary
ChatGPT conversation also appeared as a new room without pairing. Work and Codex
remain planned source adapters.

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
  allow_conversations: []
  send_enabled: false
  auto_login_user: ""
```

Before the first start, configure only your own Matrix user in `bridge.permissions`,
set `bridge.split_portals: true`, and use `bridge.personal_filtering_spaces: false`
for the initial Beeper test. Set `encryption.allow`, `encryption.default`, and
`encryption.require` to `true`. Beeper credentials and the Matrix connection must be
verified in the live pilot; an encrypted build alone is not proof of encryption.
Keep configuration and databases outside the repository, with owner-only access.

Start the bridge with that configuration. Set `network.auto_login_user` to the
operator Matrix ID to bootstrap `local-collector` automatically, or initiate that
login flow through the bridge management interface. The flow reuses the configured
collector account and never requests ChatGPT passwords or cookies in Matrix.
The collector's isolated DEV browser must already be signed in and have history
reading enabled. A bounded Beeper pilot using `bbctl` Desktop login and an appservice websocket
has been verified. Websocket mode does not also start a local HTTP provisioning
listener. Keep `provisioning.allow_matrix_auth` enabled for Beeper client capability
requests; the bridge permission map still restricts access to the operator.

```sh
bin/chatgpt-matrix-bridge -c /absolute/private/path/config.yaml
```

## Saved-thread replies

The default remains `network.send_enabled: false`. Enable it only with the shared
backend's dedicated DEV saved-send capability. Start that launcher with:

```sh
CODEX_WEB_GPT_HISTORY_ENABLED=1
CODEX_WEB_GPT_SAVED_SEND_ENABLED=1
CODEX_WEB_GPT_SAVED_SEND_DIR=/absolute/private/path/saved-send
```

These are environment settings for the existing isolated DEV launcher, not commands
to switch a production Codex provider. The sender uses the ChatGPT UI at the exact
saved conversation URL. It preserves the selected web model; there is no separate
API model or LLM router in this service. The existing Temporary Chat contract is
unchanged.

Only the operator's plain text messages in verified source rooms are accepted.
Replies, edits, attachments and relayed senders are rejected in this pilot. Text is
limited to 12,000 UTF-8 bytes. A private durable outbox records the original Matrix
event before calling the backend. The backend writes a submission journal before
clicking Send, verifies the accepted source message and reconciles repeated calls
with the same transaction ID. An unresolved attempt pauses the room; sending a new
message is not a safe retry. Keep both the bridge database and backend journal.

Set `network.allow_conversations` to explicit source UUIDs for a bounded pilot.
This is an optional test filter, not a room-pairing mechanism. An empty list selects
all conversations eligible under the persisted activation boundary. A historical
`since` used during a pilot stays persisted: review that boundary before removing
the test filter.

## Boundaries and remaining work

- This pilot is single-operator. Treat configuration and the local backend checkout
  as trusted executable inputs. Restrict Matrix login permission to the operator.
- Both directions have passed a live encrypted Beeper test. The ordinary saved
  history endpoint defines discovery coverage; this is still an early implementation.
- Existing-message text edits are detected and pause that conversation rather than
  generating duplicate messages. Branch switches/deletions are not reconciled with
  previously mirrored history. That fidelity is required before general use.
- The framework's database deduplicates acknowledged messages and normal restarts.
  Source messages now use deterministic Matrix transactions on both ghost and
  custom-user connections, including encrypted sends and Beeper URL prefixes.
  This closes the tested message accept-before-commit replay gap while the same
  Matrix transaction scope is retained. Token/device resets and uncertain room
  creation remain separate recovery cases; exactly-once delivery is not claimed.
  See [the validation plan](docs/validation.md).
- Collection covers the regular saved-history endpoint. Project-only/archived-only
  discovery, exhaustive history, attachment binaries, and long-running reliability
  remain unverified.
- Browser/session code stays in the backend fork. This repository owns Matrix
  rooms, source adapters, and delivery behavior. It does not file vault notes,
  approve project matches, or run an LLM for routing.

See [architecture](docs/architecture.md) and [validation](docs/validation.md).
