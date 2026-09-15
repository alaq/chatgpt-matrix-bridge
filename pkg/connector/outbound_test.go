package connector

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/alaq/chatgpt-matrix-bridge/internal/source"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

func TestDormantRoomReplySurvivesEvictionAndRestart(t *testing.T) {
	for _, kind := range []string{"chatgpt", "codex"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "bridge.db")
			mx := &matrixFixture{names: map[id.RoomID]string{}}
			br, c := startFixture(t, path, mx)
			chat := testChat()
			if kind == "codex" {
				chat.Kind, chat.ID, chat.URL = "codex", "codex:"+chat.ID, "codex://threads/"+chat.ID
			}
			// No delivery fingerprint: this also covers pre-optimization rooms.
			feedFixture(t, br, c, chat)
			account := c.login.Metadata.(*LoginMetadata).AccountKey
			c.replaceActiveCache(account, nil)
			msg := outgoingFixture(t, br, c)
			if route, ok := c.conversationForPortal(msg.Portal); !ok || route.ID != chat.ID {
				t.Fatal("eviction removed the durable route")
			}
			br.Stop()
			br, c = startFixture(t, path, mx)
			defer br.Stop()
			msg = outgoingFixture(t, br, c)
			if len(c.chats) != 0 {
				t.Fatal("restart unexpectedly loaded a transcript cache")
			}
			calls := 0
			c.sendFunc = func(_ context.Context, req source.SendRequest) (*source.SendResult, error) {
				calls++
				if req.ConversationID != chat.ID || req.AccountKey != account || req.Text != msg.Content.Body || req.TransactionID != source.StableID("matrix-send", account, string(msg.Portal.MXID), string(msg.Event.ID)) {
					t.Fatal("dormant route changed original send identity")
				}
				return &source.SendResult{Version: 1, Status: "accepted", UserMessageID: "33333333-3333-3333-3333-333333333333"}, nil
			}
			response, err := c.HandleMatrixMessage(ctx, msg)
			if err != nil || response == nil || response.DB.MXID != msg.Event.ID || calls != 1 {
				t.Fatalf("dormant send: response=%v calls=%d err=%v", response, calls, err)
			}
			if err = br.DB.Message.Insert(ctx, response.DB); err != nil {
				t.Fatal(err)
			}
			response.PostSave(ctx, response.DB)
			if len(c.chats) != 0 {
				t.Fatal("outbound routing expanded the active transcript cache")
			}
			chat.UpdatedAt += 100
			chat.Messages = append(chat.Messages,
				source.Message{ID: "33333333-3333-3333-3333-333333333333", Role: "user", Text: msg.Content.Body},
				source.Message{ID: "returned-answer", Role: "assistant", Text: "Received"})
			feedFixture(t, br, c, chat)
			if mx.rooms != 1 || mx.messages != 3 || calls != 1 {
				t.Fatalf("reactivation duplicated room/message: rooms=%d messages=%d calls=%d", mx.rooms, mx.messages, calls)
			}
		})
	}
}

func TestDormantRoomRejectsUnboundRoutesAndDisallowedConversations(t *testing.T) {
	ctx := context.Background()
	mx := &matrixFixture{names: map[id.RoomID]string{}}
	br, c := startFixture(t, filepath.Join(t.TempDir(), "bridge.db"), mx)
	defer br.Stop()
	chat := testChat()
	feedFixture(t, br, c, chat)
	c.replaceActiveCache(c.login.Metadata.(*LoginMetadata).AccountKey, nil)
	msg := outgoingFixture(t, br, c)
	c.sendFunc = func(context.Context, source.SendRequest) (*source.SendResult, error) {
		t.Fatal("unbound route submitted")
		return nil, nil
	}
	for _, topic := range []string{"", "https://evil.invalid/c/" + chat.ID, "https://chatgpt.com/c/22222222-2222-2222-2222-222222222222", chat.URL + "?thread=other", "codex://threads/" + chat.ID} {
		msg.Portal.Topic = topic
		if _, err := c.HandleMatrixMessage(ctx, msg); err == nil {
			t.Fatalf("accepted unbound route %q", topic)
		}
	}
	msg.Portal.Topic = chat.URL
	c.connector.Config.AllowConversations = []string{"22222222-2222-2222-2222-222222222222"}
	if _, err := c.HandleMatrixMessage(ctx, msg); err == nil {
		t.Fatal("allowlist bypassed")
	}
	c.connector.Config.AllowConversations = nil
	msg.Portal.Receiver = networkid.UserLoginID("another-login")
	if _, err := c.HandleMatrixMessage(ctx, msg); err == nil {
		t.Fatal("receiver binding bypassed")
	}
	if pending, err := c.getOutbound(ctx, msg.Portal); err != nil || pending != nil {
		t.Fatal("rejected route created an outbox")
	}
}

func TestDormantCodexCapabilitiesAndStoreBinding(t *testing.T) {
	ctx := context.Background()
	mx := &matrixFixture{names: map[id.RoomID]string{}}
	br, c := startFixture(t, filepath.Join(t.TempDir(), "bridge.db"), mx)
	defer br.Stop()
	chat := testChat()
	chat.Kind, chat.ID, chat.URL = "codex", "codex:"+chat.ID, "codex://threads/"+chat.ID
	feedFixture(t, br, c, chat)
	account := c.login.Metadata.(*LoginMetadata).AccountKey
	c.replaceActiveCache(account, nil)
	msg := outgoingFixture(t, br, c)
	if got := c.GetCapabilities(ctx, msg.Portal); got.ID != "codex-readonly-v1" {
		t.Fatalf("dormant room lost read-only capabilities: %#v", got)
	}
	c.connector.Config.CodexSendEnabled = true
	if got := c.GetCapabilities(ctx, msg.Portal); len(got.File) != 0 {
		t.Fatal("dormant Codex room advertised browser attachments")
	}
	for _, saved := range []string{"", source.StableID("local-task-store-v1", "/other/store")} {
		c.connector.Config.CodexHome = "/configured/store"
		if saved != "" {
			_, err := br.DB.Exec(ctx, "INSERT INTO kv_store (bridge_id,key,value) VALUES ($1,$2,$3)", br.ID, "chatgpt_local_source_"+account, saved)
			if err != nil {
				t.Fatal(err)
			}
		}
		result, err := c.send(ctx, source.SendRequest{Version: 1, AccountKey: account, ConversationID: chat.ID, TransactionID: strings.Repeat("b", 64), Text: "same task"})
		if err != nil || result.Status != "not_sent" || result.Error != "codex_source_mismatch" {
			t.Fatalf("store binding bypassed: %#v %v", result, err)
		}
	}
}

func outgoingFixture(t *testing.T, br *bridgev2.Bridge, c *Client) *bridgev2.MatrixMessage {
	t.Helper()
	portal, err := br.GetPortalByMXID(context.Background(), "!room1:test.invalid")
	if err != nil || portal == nil {
		t.Fatalf("portal: %v", err)
	}
	c.connector.Config.SendEnabled = true
	return &bridgev2.MatrixMessage{MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{
		Portal: portal, Event: &event.Event{ID: "$original-matrix", Sender: c.login.UserMXID, Timestamp: time.Now().UnixMilli(), Unsigned: event.Unsigned{TransactionID: "~beeper-original-client-transaction"}},
		Content: &event.MessageEventContent{MsgType: event.MsgText, Body: "Continue from Matrix"},
	}}
}
func TestOutboundRestartRecoversOriginalEventAndSuppressesEcho(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "bridge.db")
	mx := &matrixFixture{names: map[id.RoomID]string{}}
	br, c := startFixture(t, dbPath, mx)
	chat := testChat()
	feedFixture(t, br, c, chat)
	msg := outgoingFixture(t, br, c)
	var submitted source.SendRequest
	c.sendFunc = func(ctx context.Context, r source.SendRequest) (*source.SendResult, error) {
		out, err := c.getOutbound(ctx, msg.Portal)
		if err != nil || out == nil || out.Request.TransactionID != r.TransactionID {
			t.Fatal("submission preceded durable outbox")
		}
		if out.MatrixTransactionID != msg.Event.Unsigned.TransactionID {
			t.Fatal("client transaction missing from durable outbox")
		}
		submitted = r
		return &source.SendResult{Version: 1, Status: "accepted", UserMessageID: "33333333-3333-3333-3333-333333333333"}, nil
	}
	response, err := c.HandleMatrixMessage(ctx, msg)
	if err != nil || response.DB.MXID != msg.Event.ID {
		t.Fatalf("send: %v", err)
	}
	// Simulate a process crash after source acceptance, before framework DB insert.
	br.Stop()
	br, c = startFixture(t, dbPath, mx)
	defer br.Stop()
	c.connector.Config.SendEnabled = true
	recovered := 0
	c.sendFunc = func(_ context.Context, r source.SendRequest) (*source.SendResult, error) {
		if !reflect.DeepEqual(r, submitted) {
			t.Fatal("recovery changed transaction or text")
		}
		recovered++
		return &source.SendResult{Version: 1, Status: "accepted", UserMessageID: "33333333-3333-3333-3333-333333333333"}, nil
	}
	chat.Messages = append(chat.Messages, source.Message{ID: "33333333-3333-3333-3333-333333333333", Role: "user", Text: submitted.Text}, source.Message{ID: "new-answer", Role: "assistant", Text: "Received"})
	feedFixture(t, br, c, chat)
	if recovered != 1 || mx.messages != 3 || mx.rooms != 1 {
		t.Fatalf("recovery=%d messages=%d rooms=%d", recovered, mx.messages, mx.rooms)
	}
	if len(mx.statusEvents) != 1 || mx.statusEvents[0].TargetTxnID != msg.Event.Unsigned.TransactionID || mx.statusEvents[0].RelatesTo.EventID != msg.Event.ID || mx.statusEvents[0].Status != event.MessageStatusSuccess {
		t.Fatal("recovery receipt did not settle the original event and client transaction")
	}
	stored, err := br.DB.Message.GetPartByMXID(ctx, msg.Event.ID)
	if err != nil || stored == nil || stored.ID != response.DB.ID {
		t.Fatalf("association missing: %v", err)
	}
	feedFixture(t, br, c, chat)
	if recovered != 1 || mx.messages != 3 || len(mx.statusEvents) != 1 {
		t.Fatal("replay duplicated or recovered twice")
	}
}

func TestLegacyRecoveryReceiptVerifiesEventBeforeClearingOutbox(t *testing.T) {
	ctx := context.Background()
	mx := &matrixFixture{names: map[id.RoomID]string{}, getEvents: map[id.EventID]*event.Event{}}
	br, c := startFixture(t, filepath.Join(t.TempDir(), "bridge.db"), mx)
	defer br.Stop()
	feedFixture(t, br, c, testChat())
	msg := outgoingFixture(t, br, c)
	c.sendFunc = func(context.Context, source.SendRequest) (*source.SendResult, error) {
		return &source.SendResult{Version: 1, Status: "accepted", UserMessageID: "33333333-3333-3333-3333-333333333333"}, nil
	}
	response, err := c.HandleMatrixMessage(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	if err = br.DB.Message.Insert(ctx, response.DB); err != nil {
		t.Fatal(err)
	}
	out, err := c.getOutbound(ctx, msg.Portal)
	if err != nil {
		t.Fatal(err)
	}
	out.MatrixTransactionID = "" // Database written by the previous release.
	if err = c.setOutbound(ctx, msg.Portal, out); err != nil {
		t.Fatal(err)
	}
	c.sendFunc = func(context.Context, source.SendRequest) (*source.SendResult, error) {
		t.Fatal("resent an accepted message")
		return nil, nil
	}
	for _, original := range []*event.Event{nil, {ID: msg.Event.ID, Sender: "@wrong:test.invalid"}, {ID: msg.Event.ID, Sender: msg.Event.Sender, RoomID: "!wrong:test.invalid"}} {
		mx.getEvents[msg.Event.ID] = original
		if err = c.recoverOutbound(ctx, msg.Portal); err == nil {
			t.Fatal("unverified receipt target accepted")
		}
		pending, _ := c.getOutbound(ctx, msg.Portal)
		if pending == nil || len(mx.statusEvents) != 0 {
			t.Fatal("lost pending receipt or emitted unverified success")
		}
	}
	mx.getEvents[msg.Event.ID] = msg.Event
	if err = c.recoverOutbound(ctx, msg.Portal); err != nil {
		t.Fatal(err)
	}
	if len(mx.statusEvents) != 1 || mx.statusEvents[0].TargetTxnID != msg.Event.Unsigned.TransactionID {
		t.Fatal("legacy receipt lost client transaction")
	}
	pending, err := c.getOutbound(ctx, msg.Portal)
	if err != nil || pending != nil {
		t.Fatal("resolved recovery still pending")
	}
}
func TestUncertainOutboundPausesRoomAndRejectsWrongSender(t *testing.T) {
	ctx := context.Background()
	mx := &matrixFixture{names: map[id.RoomID]string{}}
	br, c := startFixture(t, filepath.Join(t.TempDir(), "bridge.db"), mx)
	defer br.Stop()
	chat := testChat()
	feedFixture(t, br, c, chat)
	msg := outgoingFixture(t, br, c)
	calls := 0
	c.sendFunc = func(context.Context, source.SendRequest) (*source.SendResult, error) {
		calls++
		return nil, errors.New("lost transport")
	}
	msg.Event.Sender = "@other:test.invalid"
	if _, err := c.HandleMatrixMessage(ctx, msg); err == nil || calls != 0 {
		t.Fatal("other sender accepted")
	}
	msg.Event.Sender = c.login.UserMXID
	if _, err := c.HandleMatrixMessage(ctx, msg); err == nil {
		t.Fatal("uncertain send reported success")
	}
	chat.Messages = append(chat.Messages, source.Message{ID: "new", Role: "user", Text: msg.Content.Body})
	if err := c.dispatch(ctx, chat); err == nil {
		t.Fatal("unresolved outbox did not pause echo")
	}
	if mx.messages != 2 {
		t.Fatal("uncertain send mirrored a duplicate")
	}
}

func TestUnchangedConversationRecoversPendingOutbound(t *testing.T) {
	ctx := context.Background()
	mx := &matrixFixture{names: map[id.RoomID]string{}}
	br, c := startFixture(t, filepath.Join(t.TempDir(), "bridge.db"), mx)
	defer br.Stop()
	chat := testChat()
	feedFixture(t, br, c, chat)
	msg := outgoingFixture(t, br, c)
	calls := 0
	c.sendFunc = func(context.Context, source.SendRequest) (*source.SendResult, error) {
		calls++
		return nil, errors.New("lost transport")
	}
	if _, err := c.HandleMatrixMessage(ctx, msg); err == nil {
		t.Fatal("uncertain send reported success")
	}
	c.sendFunc = func(context.Context, source.SendRequest) (*source.SendResult, error) {
		calls++
		return &source.SendResult{Version: 1, Status: "accepted", UserMessageID: "33333333-3333-3333-3333-333333333333"}, nil
	}
	beforeRooms, beforeMessages := mx.rooms, mx.messages
	chat.Unchanged = true
	if err := c.dispatchConversation(ctx, chat); err != nil {
		t.Fatal(err)
	}
	pending, err := c.getOutbound(ctx, msg.Portal)
	if err != nil || pending != nil {
		t.Fatalf("unchanged recovery remains pending: %v", err)
	}
	if calls != 2 || mx.rooms != beforeRooms || mx.messages != beforeMessages {
		t.Fatalf("calls=%d rooms=%d messages=%d", calls, mx.rooms, mx.messages)
	}
}

func TestOversizedCodexRolloutProducesActionableFailure(t *testing.T) {
	got := rejectedSendMessage("codex_rollout_unavailable")
	want := "This Codex task is too large to bridge safely. Nothing was submitted; open the original task in the desktop app."
	if got != want {
		t.Fatalf("oversized rollout notice is not actionable: %q", got)
	}
}

func TestUnavailableTaskOwnerProducesVisibleOriginalMessageFailure(t *testing.T) {
	ctx := context.Background()
	mx := &matrixFixture{names: map[id.RoomID]string{}}
	br, c := startFixture(t, filepath.Join(t.TempDir(), "bridge.db"), mx)
	defer br.Stop()
	feedFixture(t, br, c, testChat())
	msg := outgoingFixture(t, br, c)
	c.sendFunc = func(context.Context, source.SendRequest) (*source.SendResult, error) {
		return &source.SendResult{Version: 1, Status: "not_sent", Error: "codex_owner_unavailable"}, nil
	}
	response, err := c.HandleMatrixMessage(ctx, msg)
	var failure bridgev2.MessageStatus
	if response != nil || !errors.As(err, &failure) || !failure.SendNotice || !failure.IsCertain {
		t.Fatalf("missing visible pre-submission failure: %#v, %v", response, err)
	}
	info := bridgev2.StatusEventInfoFromEvent(msg.Event)
	info.MessageType = event.MsgText
	notice := failure.ToNoticeEvent(info)
	if notice.MsgType != event.MsgNotice || notice.RelatesTo.GetReplyTo() != msg.Event.ID {
		t.Fatalf("notice lost the original reply target: %#v", notice)
	}
	want := "⚠️ Your message was not bridged: " + rejectedSendMessage("codex_owner_unavailable")
	if notice.Body != want {
		t.Fatalf("notice does not explain safe retry: %q", notice.Body)
	}
	if pending, err := c.getOutbound(ctx, msg.Portal); err != nil || pending != nil {
		t.Fatalf("explicitly rejected message should remain available for user retry: %v", err)
	}
}

func TestAutomaticBootstrapPreservesAlreadyLoadedLogin(t *testing.T) {
	mx := &matrixFixture{names: map[id.RoomID]string{}}
	br, c := startFixture(t, filepath.Join(t.TempDir(), "bridge.db"), mx)
	defer br.Stop()
	c.connector.Config.AutoLoginUser = string(c.login.UserMXID)
	c.connector.Config.Since = "2030-01-01T00:00:00Z"
	// Fixture backend directories contain no collector. Success proves bootstrap
	// reused framework-owned state instead of starting a competing source refresh.
	if err := c.connector.AutoLogin(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.login.Metadata.(*LoginMetadata).Since != 150 {
		t.Fatal("activation boundary changed")
	}
}
