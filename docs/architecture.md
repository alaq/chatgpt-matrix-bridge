# Architecture

```text
ChatGPT saved history
    │ signed-in isolated browser session
    ▼
codex-chatgpt-web collector ──► private archive
                                  │ version 1 visible-only JSON feed (read-only)
                                  ▼
                          ChatGPT source adapter
                                  │ account + conversation + message identity
                                  ▼
                         mautrix-go bridgev2
                                  │ private rooms, message store, Matrix transport
                                  ▼
                              Matrix / Beeper
```

The collector alone owns archive schema, discovery cursors, and parsing. The feed
is a snapshot of the latest visible branch, not a raw transcript export or a list
of commands to execute. The bridge never consumes project-routing approvals.

The bridge stores the activation boundary in login metadata. Source account
identity must match the persisted login on every poll. Portal IDs and message IDs
are hashes of structured source/account/conversation/message tuples; titles and
content do not affect identity. A ChatResync event creates or updates the room before
its visible message events, including when no assistant answer exists yet.

All selected conversations are replayed against the framework's database, so a
collector checkpoint cannot accidentally become a Matrix delivery checkpoint.
Synchronous portal processing preserves order and stops a conversation at the first
failed message; other conversations can continue. A canceled poll cancels its
backend subprocess and in-flight delivery. Relogin preserves the activation time.

The first implementation uses `RemoteEventMessageUpsert` to detect changed text on
an existing source message ID. Unsupported edits surface an error for that room.
The framework owns successful-message records. Room creation, normal restarts, and
known pre-send failures are tested against its actual SQLite store. Its default
Matrix sender does not close the accept-before-database-write ambiguity window;
that remains a deployment gate for robust unattended use.

The future outbound adapter must target the real saved ChatGPT conversation ID,
serialize turns, verify the accepted source message, and reconcile uncertain sends
before retrying. User messages mirrored back from the source must match the stored
outbound mapping rather than echoing a second copy.

Work and Codex can implement their own discovery/history/continuation adapters.
Their APIs and local/cloud scope must be verified independently. Do not label a
saved ChatGPT ID, a Work task ID, and a Codex local thread ID as interchangeable.
