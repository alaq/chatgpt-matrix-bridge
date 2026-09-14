package connector

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/alaq/chatgpt-matrix-bridge/internal/source"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

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
