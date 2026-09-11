package connector

import (
	"context"
	"github.com/alaq/chatgpt-matrix-bridge/internal/source"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"strings"
	"testing"
)

func testClient() *Client {
	key := strings.Repeat("a", 64)
	return &Client{login: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: networkid.UserLoginID("chatgpt_" + key), Metadata: &LoginMetadata{AccountKey: key, Since: 150}}}}
}
func testChat() source.Conversation {
	return source.Conversation{ID: "11111111-1111-1111-1111-111111111111", Title: "Question", URL: "https://chatgpt.com/c/11111111-1111-1111-1111-111111111111", CreatedAt: 100, UpdatedAt: 200, Messages: []source.Message{{ID: "u1", Role: "user", Text: "Hello"}, {ID: "a1", Role: "assistant", Text: "Hi"}}}
}
func TestDiscoveryCreatesRoomBeforeMessagesIncludingEmptyChat(t *testing.T) {
	c := testClient()
	chat := testChat()
	evts := c.events(chat)
	room, ok := evts[0].(*simplevent.ChatResync)
	if !ok || !room.ShouldCreatePortal() {
		t.Fatal("discovery did not request a room")
	}
	if len(evts) != 3 {
		t.Fatal("wrong visible event count")
	}
	chat.Messages = nil
	if len(c.events(chat)) != 1 {
		t.Fatal("empty conversation was lost")
	}
}
func TestContinuationAndRenameReuseRoomAndMessageIDs(t *testing.T) {
	c := testClient()
	chat := testChat()
	before := c.events(chat)
	chat.Title = "Renamed"
	chat.Messages = append(chat.Messages, source.Message{ID: "u2", Role: "user", Text: "Continue"})
	after := c.events(chat)
	if before[0].GetPortalKey() != after[0].GetPortalKey() {
		t.Fatal("rename created another room")
	}
	for i := 1; i < len(before); i++ {
		if before[i].(bridgev2.RemoteMessage).GetID() != after[i].(bridgev2.RemoteMessage).GetID() {
			t.Fatal("existing message identity changed")
		}
	}
	chat.ID = "22222222-2222-2222-2222-222222222222"
	if c.events(chat)[0].GetPortalKey() == after[0].GetPortalKey() {
		t.Fatal("equal titles merged conversations")
	}
}
func TestRepeatIsNoopAndSourceEditsAreReported(t *testing.T) {
	m := testClient().events(testChat())[1].(*simplevent.PreConvertedMessage)
	db := []*database.Message{{Metadata: m.Data.Parts[0].DBMetadata}}
	r, err := m.HandleExisting(context.Background(), nil, nil, db)
	if err != nil || r.ContinueMessageHandling || len(r.SubEvents) > 0 {
		t.Fatal("repeat would resend")
	}
	db[0].Metadata = &MessageMetadata{Hash: "different"}
	if _, err = m.HandleExisting(context.Background(), nil, nil, db); err == nil {
		t.Fatal("edit silently dropped")
	}
}
func TestRoleAttributionAndUnsupportedOutbound(t *testing.T) {
	c := testClient()
	events := c.events(testChat())
	if !events[1].(*simplevent.PreConvertedMessage).Sender.IsFromMe || events[2].(*simplevent.PreConvertedMessage).Sender.IsFromMe {
		t.Fatal("source roles mixed")
	}
	if _, err := c.HandleMatrixMessage(context.Background(), nil); err == nil {
		t.Fatal("unimplemented send reported success")
	}
}
