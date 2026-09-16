# Operations

Keep runtime configuration, the Matrix database, the shared archive, and both send
journals outside Git with owner-only permissions. Back them up before upgrades.
Never reset the database to solve a sync failure: it owns room and message identity.

## Process supervision

Run `scripts/supervise.py --config /absolute/private/services.json` under a macOS
LaunchAgent with `RunAtLoad` and `KeepAlive`. The config must be mode 0600:

```json
{
  "browser": {
    "argv": ["/absolute/path/to/Electron", "/absolute/path/to/launcher", "--dev-profile"],
    "cwd": "/absolute/path/to/codex-chatgpt-web",
    "environment": {
      "CODEX_WEB_GPT_DEV_HOME": "/absolute/private/existing-dev-profile",
      "CODEX_WEB_GPT_HISTORY_ENABLED": "1",
      "CODEX_WEB_GPT_SAVED_SEND_ENABLED": "1",
      "CODEX_WEB_GPT_SAVED_SEND_DIR": "/absolute/private/saved-send"
    }
  },
  "bridge": {
    "argv": ["/absolute/path/to/chatgpt-matrix-bridge", "-c", "/absolute/private/config.yaml"],
    "cwd": "/absolute/private/runtime"
  }
}
```

Optional `metadata_path` and `metadata` fields update existing process inventories
when a child restarts. `supervisor-status.json` records child PIDs, process identities
and failure counts. After a supervisor crash, only its still-matching recorded
children are stopped before replacements start. Set the LaunchAgent `ExitTimeOut`
to 45 seconds to allow graceful shutdown and the bounded child termination wait.
Children restart independently after 2–120 seconds. A single file lock prevents two
supervisors from owning the same runtime. Stop the supervisor through launchd when
upgrading; killing a child alone deliberately restarts it. Before enabling it,
verify and stop the previously unmanaged processes so only one owner remains.

This starts after the user's login, including after reboot. It does not unlock a
logged-out desktop, solve a browser challenge, or renew an expired ChatGPT session.

## Health and recovery

Set `network.health_path` to a private JSON file. `status` in the management room
shows last successful source refresh, queued sends and unavailable attachments.
Source failure and Matrix delivery failure are separate fields. Check timestamps,
not merely whether a process exists. Failures back off; the last captured archive
remains usable. Normal polling is configured with `poll_seconds`.

Enable `matrix.message_error_notices` alongside `matrix.message_status_events`.
The connector requests a visible notice replying to the failed original message;
some clients can still show their transport-level "Sent" label when the source
rejected the message. A successful Matrix upload is not source acceptance. These
notices include whether submission is known not to have happened or is uncertain.

Use `retry` to reconcile the original pending attempts. Never erase sender journals
or create another message merely because acceptance is uncertain. Accepted user
messages are acknowledged before the generating page closes; a timeout is not
reported as completed generation. Authentication and account checks still run at
the source before a submission.

`sync_edits` enables source correction and explicit branch-change notices. Existing
Matrix event IDs remain stable. Removed source branches/deletions retain prior
Matrix history. `sync_media` enables encrypted source media; `media_since` can keep
older attachment history from being appended when enabling the feature. Unsupported
or expired files keep the original-conversation fallback.

## Local tasks

The catalog connection opens the existing database with `mode=rw`, then enables
and verifies `PRAGMA query_only=ON` before any catalog query. SQLite can initialize
its WAL/SHM companion files after the desktop's last connection closes; SQL changes
to task data and schema remain blocked. Do not switch this live catalog to
`immutable=1`, disable locking, or manually remove its WAL/SHM files: that would
lose a fresh, coherent view. See SQLite's [WAL lifecycle](https://www.sqlite.org/wal.html)
and [query-only setting](https://www.sqlite.org/pragma.html#pragma_query_only).

The active polling limit does not limit where the owner can reply. Existing dormant
rooms resolve their original source URL from the persisted portal, verified against
its account-bound portal ID and current allowlist. A changed title or topic cannot
route a message to a different conversation. Routing does not read old transcripts
or enlarge the active cache; successful source activity lets the room reenter the
normal bounded polling window. Codex replies also verify the persisted local-store
binding before submission, including before the first successful poll after restart.

Local Work/Codex discovery has its own activation date. Existing tasks updated after
that date enter with their visible history; new tasks need no manual pairing.
The default is read-only. Enable owner-routed replies only after verifying the
installed app's IPC behavior. App releases can change it, and the original task's
owner must be available. Never fall back to an unrelated CLI agent or widen the
task's permissions. Native approvals remain in the desktop app.

Before enabling `codex_send_enabled`, check the intended task without submitting
or resuming it:

```sh
python3 scripts/codex_source.py --codex-home "$HOME/.codex" --conversation-id <task-uuid> probe
```

`available: true` means the running desktop app answered the task-owner lookup.
It is a readiness check, not proof of a completed live send. `probe` remains
read-only and does not activate a task. The official
app-server continuation methods require a connection to the original server;
starting another server over the same task store does not establish that connection.

An idle task may have no registered owner even while the app is running. On macOS,
the sender automatically opens that original task once through
`/usr/bin/open -g -b com.openai.codex codex://threads/<UUID>`, then reconnects to
the desktop socket and waits up to 12 seconds for its owner. Individual connection
and discovery attempts use at most three seconds; the OS open call uses at most
five. The initial trusted socket must already connect. If the app is closed,
unsupported or still cannot expose the owner, the existing not-sent notice remains.

Activation contains only a validated existing task UUID, never message text,
model/permission overrides or a new-task request. `-g` requests background opening,
but Codex can change its selected task. The catalog and unfinished-turn state are
read again before submission. The existing task lock, original Matrix event,
client transaction, exact text and uncertain-send reconciliation remain in force.
An uncertain or already accepted submission never triggers activation or resubmission.

For an explicit operational readiness check that may open the existing task but
does not submit a message:

```sh
python3 scripts/codex_source.py --codex-home "$HOME/.codex" --conversation-id <task-uuid> reconnect
```

Reconnection is part of each new send and original-transaction retry; it does not
automatically replay previously rejected events that have already left the outbox.

The sender starts an idle task through its existing owner and steers an unfinished
turn through that same owner. It supplies no model, effort, permission or workspace
overrides. Long periods without rollout activity only stop the typing indicator;
they do not make an unfinished turn eligible for a new start. If an active turn
ends during dispatch, an uncertain steering request is retained for reconciliation
and is never replaced with a fresh start. A task-level process lock serializes
duplicate requests, and an accepted reply must match both its client message ID
and exact text in the original rollout.

## Verification

Local reader errors in the bridge log include the operation, process exit or
timeout, and a bounded static error category, adapter line number and errno when
available. Exception messages, paths and transcript content are excluded. Source
health includes both the saved ChatGPT reader and the local task reader; a local
read failure no longer advances the successful source timestamp or marks Matrix
delivery itself unavailable. Preserve these diagnostics before restarting a worker.

Run Go tests with `-race -tags goolm`, Go vet/build, and Python tests in `scripts`.
The backend has Node sender/media/creation tests and Python archive/feed tests.
After rollout, verify fresh source identity, health timestamps, existing Matrix
message/event identities, no pending outbox, and unchanged archived/read state of
previously repaired rooms. Test process restart independently of message submission.
Live message tests must respect the operator's individual-message approval rules.
