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

Source messages use `RemoteEventMessageUpsert` to detect changed text on an existing
source ID. Unsupported edits surface an error for that room. The framework owns
successful-message records. A narrowly scoped HTTP transport assigns stable
transaction IDs to each single-part source timeline message, after mautrix handles
encryption. Both appservice ghost clients and separate custom-user clients are
wrapped. State changes, receipts, reactions and ordinary bot messages retain their
normal behavior. The transaction key depends on room and source message identity,
never message text or encryption randomness.

Outbound sends enter the original saved ChatGPT thread through a separate DEV UI
adapter. A durable private bridge outbox records the Matrix event and original text
before the backend call. The backend journals a transaction and source head before
clicking Send; it reconciles exactly one matching user child of that head after an
uncertain response. A retry cannot blindly click again. The selected web model
handles the prompt; the bridge adds no system instructions or project routing.

The source message's pre-handler runs inside the serialized portal queue. It
recovers outstanding sends with their original transaction, restores the original
Matrix-event association, and then permits mirroring. This prevents an outbound
prompt from echoing back after acceptance followed by a process crash. If recovery
is uncertain, that room pauses. The private bridge outbox can contain message text;
the backend journal stores hashes and IDs, not prompts or session credentials.

Work and Codex can implement their own discovery/history/continuation adapters.
Their APIs and local/cloud scope must be verified independently. Do not label a
saved ChatGPT ID, a Work task ID, and a Codex local thread ID as interchangeable.
