# Validation

## Automated evidence

`go test -race -tags goolm ./...` passes 18 tests covering:

- version/account/identity validation, hidden-role rejection, old updated conversations,
  activation cutoff, title-independent identity and namespace separation;
- bounded subprocess output, private-error suppression and cancellation;
- automatic room creation, including empty conversations, and user/assistant attribution;
- the real bridgev2 SQLite database across discovery, replay, restart, continuation,
  renaming and equal titles belonging to separate conversations;
- pre-send failure, ordered retry and detection of unsupported source edits;
- deterministic Matrix transactions after remote acceptance and loss of the response,
  including encrypted event paths and the Beeper URL prefix;
- a durable outbound record before submission, restoration after a simulated process
  crash, mapping back to the original Matrix event, and suppression of its echo;
- wrong-sender rejection and pausing a room when outbound delivery remains uncertain;
- automatic bootstrap reusing an already-loaded login without racing the collector
  or changing the persisted activation boundary.

The shared backend separately passes 31 Python collector/feed tests and 118 selected
Node launcher/history/sender tests. Seven sender tests cover request validation,
receipt reuse after restart, lost browser responses, uncertain-send blocking,
source account/head changes, ambiguous source candidates and private journal files.
The original Temporary Chat browser ownership tests still pass. Go vet and builds pass.

These automated tests use fixtures for Matrix and browser side effects. The live
checks below are separate evidence, not inferred from mock results.

## Live evidence — September 11, 2026

- Registered a dedicated Beeper bridge using bbctl's existing Desktop login.
- Connected via the appservice websocket and bootstrapped only the configured operator.
- Automatically created an invite-only room for an existing synthetic saved ChatGPT
  conversation. Its six existing visible messages were readable in Beeper.
- Submitted a fresh prompt through the saved UI adapter, verified its source message
  ID and completed answer, and observed both in the same Beeper room.
- Replayed the exact transaction and received the existing source receipt without
  another UI submission.
- Observed a Matrix-originated message from the operator's Beeper account, matched
  its original Matrix event to the backend transaction and saved ChatGPT user-message
  ID, and verified the answer returned to the same room without an echoed user copy.
- Continued the source again after a restart. Both new events were `m.room.encrypted`
  on the homeserver, readable in Beeper, and carried stable `chatgpt_` transactions.
  This includes the separate connection used to mirror messages as the operator.
- Removed the pilot conversation filter after setting the persisted update cutoff
  to actual first live activation. A different, newly created ordinary ChatGPT
  conversation appeared in a second encrypted room automatically, without pairing.

Private source IDs, room IDs, credentials, snapshots and runtime files are kept
outside this repository. The personal project note holds the private evidence chain.
No new vault project was created for the ordinary conversation.

## Remaining coverage

- Measure normal discovery latency over several updates and test mobile explicitly.
  The current interval is configurable; collection/delivery time adds to it.
- Test sustained operation, browser session expiry/recovery and simultaneous activity.
- The transaction fix covers message replay in the same Matrix sender/token/device
  scope. Token/device resets and accept-before-local-commit room creation are distinct
  recovery cases; exactly-once delivery is not claimed.
- Existing message edits pause their room. Branch/deletion reconciliation, attachment
  binaries, project-only/archived-only discovery and exhaustive history need more work.
- The current local processes run continuously, but no login/startup supervisor has
  been installed. Keep the browser session, bridge database and sender journal together.
- Work and Codex discovery/continuation remain stretch sources.
