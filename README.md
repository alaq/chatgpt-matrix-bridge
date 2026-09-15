# ChatGPT Matrix Bridge

A Matrix bridge that discovers saved ChatGPT conversations and gives each one its
own room. It uses the signed-in local collector from
[alaq/codex-chatgpt-web](https://github.com/alaq/codex-chatgpt-web/tree/feat/saved-history-collector)
and the [mautrix-go bridgev2 framework](https://github.com/mautrix/go).

**Current milestone: an encrypted bridge with recovery, media, source edits and
conversation creation.** Local Work/Codex discovery is an opt-in pilot. Cloud Work
conversations use their existing saved ChatGPT identity when present in regular
history. Project matching and vault project links are outside this bridge.

## Behavior

- A new saved conversation discovered after activation creates a room automatically.
- An old conversation updated after activation also enters the room list. Further
  updates and title changes reuse that room.
- Conversation and message IDs are scoped to source/account identity. Equal titles
  do not merge rooms. Vault project matching is independent of room creation.
- The first login stores an activation timestamp. It survives normal restarts and
  relogin; set `network.since` before first login to include an older window.
- Each poll runs the existing bounded incremental collector, then reads a snapshot.
  Only the ten most recently updated eligible conversations are active by default,
  across ChatGPT, Work and Codex combined. Older room mappings and history remain;
  updating an older source conversation brings its existing room back into the
  active set without creating a duplicate.
- After a conversation is completely delivered, the bridge persists a versioned
  delivery fingerprint. Later polls use cheap source metadata to return an unchanged
  marker instead of reopening/decoding that transcript or redispatching its events.
  Failed or partial delivery never advances the fingerprint; outbox recovery and
  running-task typing refresh still happen on every applicable poll.
  Default polling is 60 seconds plus collection/delivery time, not a latency SLA.
  Failed polls back off from at least one minute to 15 minutes (or a longer configured
  interval). Last captured, account-bound history remains available for local delivery
  and presentation updates while refresh is unavailable; status stays disconnected.
  A transient refresh failure does not invalidate the configured login or discard
  follow-ups. Actual sending still verifies the fresh source account before submission.
- Recovered sends retain the original Matrix event and client transaction IDs in
  their completion receipts, so clients can match them to the original outgoing
  message. Legacy outboxes resolve the client transaction from the verified event.
- Visible user and assistant text is mirrored. With `sync_media`, visible images,
  attachments and linked sandbox downloads up to 20 MiB are fetched through the
  authenticated backend and uploaded with Matrix encryption. Unavailable files keep
  their source links and do not stop later text. Hidden reasoning, raw nodes and
  signed download URLs are not exported.
- Oversized messages are split into ordered parts below Matrix's encrypted event
  limit. Rich text and code remain readable; a failed part resumes without
  duplicating accepted parts or blocking the rest of the conversation forever.
  Ordinary messages keep their existing presentation and event identities.
- The ChatGPT participant, rooms, bridge bot and network metadata use a bundled
  icon, uploaded once per homeserver/database and reused across restarts.
- Assistant Markdown is sent as Matrix HTML: bold, lists, links, tables and fenced
  code blocks. ChatGPT `:::writing` containers become labelled quoted drafts;
  internal IDs/attributes are hidden and literal code examples are preserved.
  Source HTML is escaped, unsafe Markdown URLs are disabled and no
  mention notifications are generated. User messages retain their original text.
- Web citation markers become source links when the feed includes `citation_groups`.
  Unresolved citations link to the original ChatGPT conversation. Existing assistant
  messages receive an idempotent presentation edit; their event IDs and source hashes
  stay unchanged. With `sync_edits`, source edits update existing events, including
  growth/shrink of multipart answers. Superseded parts receive a short label. Branch
  switches receive one notice; earlier branches remain as history. Source deletions
  do not erase Matrix messages.

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
  max_active_conversations: 10
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

Only the operator's normal messages in verified source rooms are accepted.
Text is limited to 12,000 UTF-8 bytes. A single image/file/audio/video attachment
up to 20 MiB can include a caption; its bytes and returned source receipt are checked.
Quoted replies, edits, threads and relayed senders are rejected. Multiline text is supported. Successful sends request
an immediate refresh after their original Matrix event is saved, so a slower idle
polling interval does not delay fetching the answer. A private durable outbox records the original Matrix
event before calling the backend. The backend writes a submission journal before
clicking Send, verifies the accepted source message and reconciles repeated calls
with the same transaction ID. An unresolved attempt pauses the room; sending a new
message is not a safe retry. Keep both the bridge database and backend journal.

`network.max_active_conversations` defaults to 10 and is bounded from 1 to 100.
Each source exports only its newest candidates, then the bridge selects the newest
configured count across sources. This bounds transcript parsing and Matrix replay;
it does not delete old Matrix rooms or source history. A dormant room becomes
active again when its source conversation is updated. Set
`network.allow_conversations` to explicit source UUIDs for a narrower pilot; the
active cap still applies. A historical `since` used during a pilot stays persisted:
review that boundary before removing the test filter.

## Commands and progress

In the bridge management room, send `status`, `retry`, or `new <first message>`.
In a conversation room, prefix commands with the configured bridge command prefix
(default `!chatgpt`; check `bridge.command_prefix` in your configuration).

`new` journals the Matrix command before creating a saved ChatGPT conversation.
A lost result is reconciled against the original creation attempt. Use `retry`
while that result is uncertain, rather than issuing a second `new` command.
The new room appears through ordinary discovery. `retry` also recovers pending
replies using their original Matrix/client transaction IDs.

Accepted sends return their receipt before generation finishes. Typing is shown
only after generation is observed. A timeout clears typing without claiming an
answer completed. Unfinished source turns expose only their completed visible
messages to the bridge; the personal archive's completed checkpoint stays behind
them. `health_path` writes private timestamps/counts, including pending attachments.

## Local Work/Codex pilot

Configure `codex_home`, `codex_adapter` (this repository's `scripts/codex_source.py`),
`codex_journal` and a separate RFC3339 `codex_since` activation date. The adapter
selects the newest configured task rows before opening rollout files, excluding
reasoning, tool calls, subagents and archived tasks from the exported messages. New or continued tasks appear as `Codex · …` rooms;
completed visible progress and answers are mirrored. Source IDs are `codex:<UUID>`,
so they cannot alias ChatGPT IDs. Shared cloud Work conversations retain their
ChatGPT UUID and use `ChatGPT Work · …` titles when the source marks them `tpp`.

Local task replies default to read-only. The experimental `codex_send_enabled`
option requires a verified owner-routing endpoint in the running desktop app. It
never launches a replacement agent or changes the task's model/permissions.
On macOS, an unavailable task owner triggers one automatic open of the exact
original task in the running Codex app, followed by up to 12 seconds of owner
reconnection. Submission then uses the original message transaction. The app may
select that task in its window. If recovery fails, the reply remains explicitly
not sent; continue in the app or retry the original message after restoring it.
Remote-host tasks, cloud Codex-only jobs, tool traces and local artifacts are not
part of this first discovery pilot.

## Automatic startup and recovery

`scripts/supervise.py --config /absolute/private/services.json` owns only the
configured browser and bridge children. It restarts them independently with
bounded backoff and stops both on SIGTERM. Run it as a macOS login LaunchAgent;
login/keychain/session verification can still require user interaction after reboot.
See [operations](docs/operations.md) for the private configuration and rollout rules.

## Boundaries and remaining work

- This pilot is single-operator. Treat configuration and the local backend checkout
  as trusted executable inputs. Restrict Matrix login permission to the operator.
- Both directions have passed a live encrypted Beeper test. The ordinary saved
  history endpoint defines discovery coverage; this is still an early implementation.
- The framework's database deduplicates acknowledged messages and normal restarts.
  Source messages now use deterministic Matrix transactions on both ghost and
  custom-user connections, including encrypted sends and Beeper URL prefixes.
  This closes the tested message accept-before-commit replay gap while the same
  Matrix transaction scope is retained. Token/device resets and uncertain room
  creation remain separate recovery cases; exactly-once delivery is not claimed.
  See [the validation plan](docs/validation.md).
- Collection covers the regular saved-history endpoint. Project-only/archived-only
  discovery and exhaustive history remain unverified. Expired source assets cannot
  always be recovered, and full Work/Codex artifacts/tool traces are outside this pilot.
- Browser/session code stays in the backend fork. This repository owns Matrix
  rooms, source adapters, and delivery behavior. It does not file vault notes,
  approve project matches, or run an LLM for routing.

See [architecture](docs/architecture.md) and [validation](docs/validation.md).
