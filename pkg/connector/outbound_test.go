package connector

import (
	"context"
	"errors"
	"path/filepath"
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
		Portal: portal, Event: &event.Event{ID: "$original-matrix", Sender: c.login.UserMXID, Timestamp: time.Now().UnixMilli()},
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
		if r != submitted {
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
	stored, err := br.DB.Message.GetPartByMXID(ctx, msg.Event.ID)
	if err != nil || stored == nil || stored.ID != response.DB.ID {
		t.Fatalf("association missing: %v", err)
	}
	feedFixture(t, br, c, chat)
	if recovered != 1 || mx.messages != 3 {
		t.Fatal("replay duplicated or recovered twice")
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
