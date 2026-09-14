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
It is a readiness check, not proof of a completed live send. A missing owner keeps
the room read-only until the installed app's connection is verified. The official
app-server continuation methods require a connection to the original server;
starting another server over the same task store does not establish that connection.

The sender starts an idle task through its existing owner and steers an unfinished
turn through that same owner. It supplies no model, effort, permission or workspace
overrides. Long periods without rollout activity only stop the typing indicator;
they do not make an unfinished turn eligible for a new start. If an active turn
ends during dispatch, an uncertain steering request is retained for reconciliation
and is never replaced with a fresh start. A task-level process lock serializes
duplicate requests, and an accepted reply must match both its client message ID
and exact text in the original rollout.

## Verification

Run Go tests with `-race -tags goolm`, Go vet/build, and Python tests in `scripts`.
The backend has Node sender/media/creation tests and Python archive/feed tests.
After rollout, verify fresh source identity, health timestamps, existing Matrix
message/event identities, no pending outbox, and unchanged archived/read state of
previously repaired rooms. Test process restart independently of message submission.
Live message tests must respect the operator's individual-message approval rules.
