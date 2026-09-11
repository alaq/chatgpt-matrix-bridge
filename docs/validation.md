# Validation and next live proof

## Automated evidence

`go test -race -tags goolm ./...` exercises:

- version/account/identity validation and hidden-role rejection;
- old conversations updated after activation and historical cutoff behavior;
- title-independent room identity and account/message namespace separation;
- bounded backend subprocess output, private-error suppression, and cancellation;
- automatic room creation, including an empty conversation;
- correct user/assistant attribution and explicit unsupported outbound messages;
- the real bridgev2 SQLite database across first discovery, identical replay, restart,
  continued conversation, title change, and a different conversation with the same title;
- injected pre-send failure, retry without advancing to later messages, and detection
  of unsupported source edits.

The Matrix network in integration tests is a fixture. These tests make no live
homeserver writes. Source-check separately reads the real local collector through
its public feed contract and prints counts only.

## Live proof still required

1. Configure a separate private, encrypted test bridge; confirm the intended
   homeserver, operator account, and room membership.
2. Use an explicitly chosen synthetic ChatGPT test conversation. Confirm automatic
   discovery creates exactly one room visible in Beeper, with source URL and text.
3. Continue from ChatGPT web/mobile and verify the same room receives the new turn.
4. Restart the service and verify no new rooms or repeated acknowledged messages.
5. Inject a connection failure and an accept-before-local-commit interruption.
   Resolve uncertain Matrix delivery before claiming reliable unattended sync.
6. Implement saved-thread sending and test one Matrix-originated turn, its answer,
   reload in ChatGPT, concurrent web activity, and outbound echo suppression.

Live room creation/delivery, encryption interoperability, saved-thread outbound,
branch/edit/deletion reconciliation, attachments, and Work/Codex support are not
implied by successful local tests. Broad automatic mirroring should follow the
bounded live proof; source access and Matrix delivery have independent state.
