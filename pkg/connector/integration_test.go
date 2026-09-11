package connector

// Exercise the real bridgev2 room/message store across a restart. Only the Matrix
// network is replaced; this test sends nothing to a homeserver or Beeper account.
import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/alaq/chatgpt-matrix-bridge/internal/delivery"
	"github.com/alaq/chatgpt-matrix-bridge/internal/source"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

type matrixFixture struct {
	bridgev2.MatrixConnector
	mu       sync.Mutex
	rooms    int
	messages int
	uploads  int
	contents []*event.MessageEventContent
	paths    []string
	accepted map[string]id.EventID
	names    map[id.RoomID]string
	failNext bool
}
type intentFixture struct {
	bridgev2.MatrixAPI
	mx   *matrixFixture
	user id.UserID
}

func (m *matrixFixture) Init(*bridgev2.Bridge)       {}
func (m *matrixFixture) Start(context.Context) error { return nil }
func (m *matrixFixture) PreStop()                    {}
func (m *matrixFixture) Stop()                       {}
func (m *matrixFixture) ServerName() string          { return "test.invalid" }
func (m *matrixFixture) GetCapabilities() *bridgev2.MatrixCapabilities {
	return &bridgev2.MatrixCapabilities{AutoJoinInvites: true}
}
func (m *matrixFixture) BotIntent() bridgev2.MatrixAPI {
	return &intentFixture{mx: m, user: "@bot:test.invalid"}
}
func (m *matrixFixture) FormatGhostMXID(u networkid.UserID) id.UserID {
	return id.UserID("@" + u + ":test.invalid")
}
func (m *matrixFixture) ParseGhostMXID(id.UserID) (networkid.UserID, bool) { return "", false }
func (m *matrixFixture) GhostIntent(u networkid.UserID) bridgev2.MatrixAPI {
	return &intentFixture{mx: m, user: m.FormatGhostMXID(u)}
}
func (m *matrixFixture) NewUserIntent(context.Context, id.UserID, string) (bridgev2.MatrixAPI, string, error) {
	return nil, "", nil
}
func (m *matrixFixture) SendBridgeStatus(context.Context, *status.BridgeState) error { return nil }
func (m *matrixFixture) GetPowerLevels(context.Context, id.RoomID) (*event.PowerLevelsEventContent, error) {
	return &event.PowerLevelsEventContent{}, nil
}
func (m *matrixFixture) GetMembers(context.Context, id.RoomID) (map[id.UserID]*event.MemberEventContent, error) {
	return map[id.UserID]*event.MemberEventContent{}, nil
}
func (m *matrixFixture) GetMemberInfo(context.Context, id.RoomID, id.UserID) (*event.MemberEventContent, error) {
	return &event.MemberEventContent{Membership: event.MembershipJoin}, nil
}
func (m *matrixFixture) GenerateDeterministicRoomID(networkid.PortalKey) id.RoomID { return "" }
func (i *intentFixture) GetMXID() id.UserID                                        { return i.user }
func (i *intentFixture) IsDoublePuppet() bool                                      { return false }
func (i *intentFixture) SetDisplayName(context.Context, string) error              { return nil }
func (i *intentFixture) SetAvatarURL(context.Context, id.ContentURIString) error   { return nil }
func (i *intentFixture) SetExtraProfileMeta(context.Context, any) error            { return nil }
func (i *intentFixture) SetProfile(context.Context, any) error                     { return nil }
func (i *intentFixture) EnsureJoined(context.Context, id.RoomID, ...bridgev2.EnsureJoinedParams) error {
	return nil
}
func (i *intentFixture) EnsureInvited(context.Context, id.RoomID, id.UserID) error        { return nil }
func (i *intentFixture) MarkRead(context.Context, id.RoomID, id.EventID, time.Time) error { return nil }
func (i *intentFixture) MarkTyping(context.Context, id.RoomID, bridgev2.TypingType, time.Duration) error {
	return nil
}
func (i *intentFixture) TagRoom(context.Context, id.RoomID, event.RoomTag, bool) error { return nil }
func (i *intentFixture) MuteRoom(context.Context, id.RoomID, time.Time) error          { return nil }
func (i *intentFixture) CreateRoom(_ context.Context, req *mautrix.ReqCreateRoom) (id.RoomID, error) {
	i.mx.mu.Lock()
	defer i.mx.mu.Unlock()
	i.mx.rooms++
	room := id.RoomID(fmt.Sprintf("!room%d:test.invalid", i.mx.rooms))
	i.mx.names[room] = req.Name
	return room, nil
}
func (i *intentFixture) SendState(_ context.Context, room id.RoomID, typ event.Type, _ string, content *event.Content, _ time.Time) (*mautrix.RespSendEvent, error) {
	i.mx.mu.Lock()
	defer i.mx.mu.Unlock()
	if typ == event.StateRoomName {
		i.mx.names[room] = content.Parsed.(*event.RoomNameEventContent).Name
	}
	return &mautrix.RespSendEvent{EventID: "$state"}, nil
}
func (i *intentFixture) SendMessage(ctx context.Context, room id.RoomID, _ event.Type, content *event.Content, _ *bridgev2.MatrixSendExtra) (*mautrix.RespSendEvent, error) {
	i.mx.mu.Lock()
	defer i.mx.mu.Unlock()
	if i.mx.failNext {
		i.mx.failNext = false
		return nil, fmt.Errorf("injected pre-send failure")
	}
	capture := &presentationTransport{}
	req, _ := http.NewRequestWithContext(ctx, "PUT", "https://test.invalid/_hungryserv/owner/_matrix/client/v3/rooms/"+string(room)+"/send/m.room.encrypted/random", nil)
	resp, err := (delivery.Transport{Base: capture}).RoundTrip(req)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()
	i.mx.paths = append(i.mx.paths, capture.path)
	if i.mx.accepted == nil {
		i.mx.accepted = map[string]id.EventID{}
	}
	if existing, ok := i.mx.accepted[capture.path]; ok {
		return &mautrix.RespSendEvent{EventID: existing}, nil
	}
	i.mx.messages++
	i.mx.accepted[capture.path] = id.EventID(fmt.Sprintf("$message%d", i.mx.messages))
	if parsed, ok := content.Parsed.(*event.MessageEventContent); ok {
		copyContent := *parsed
		i.mx.contents = append(i.mx.contents, &copyContent)
	}
	return &mautrix.RespSendEvent{EventID: id.EventID(fmt.Sprintf("$message%d", i.mx.messages))}, nil
}

func (i *intentFixture) UploadMedia(context.Context, id.RoomID, []byte, string, string) (id.ContentURIString, *event.EncryptedFileInfo, error) {
	i.mx.mu.Lock()
	defer i.mx.mu.Unlock()
	i.mx.uploads++
	return "mxc://test.invalid/chatgpt", nil, nil
}

func startFixture(t *testing.T, path string, mx *matrixFixture) (*bridgev2.Bridge, *Client) {
	t.Helper()
	ctx := context.Background()
	raw, err := sql.Open("sqlite3", path+"?_foreign_keys=on")
	if err != nil {
		t.Fatal(err)
	}
	db, err := dbutil.NewWithDB(raw, "sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	c := &Connector{Config: Config{Enabled: true, Python: "python3", BackendDir: t.TempDir(), ArchiveDir: t.TempDir(), PollSeconds: 60}}
	cfg := &bridgeconfig.BridgeConfig{SplitPortals: true, Permissions: bridgeconfig.PermissionConfig{"@owner:test.invalid": &bridgeconfig.PermissionLevelAdmin}}
	br := bridgev2.NewBridge("test", db, zerolog.Nop(), cfg, mx, c, func(*bridgev2.Bridge) bridgev2.CommandProcessor { return nil })
	br.Background = true
	if err = br.StartConnectors(ctx); err != nil {
		t.Fatal(err)
	}
	user, err := br.GetUserByMXID(ctx, "@owner:test.invalid")
	if err != nil {
		t.Fatal(err)
	}
	meta := testClient().login.UserLogin
	login, err := user.NewLogin(ctx, meta, nil)
	if err != nil {
		t.Fatal(err)
	}
	return br, login.Client.(*Client)
}
func feedFixture(t *testing.T, br *bridgev2.Bridge, c *Client, chats ...source.Conversation) {
	t.Helper()
	ctx := context.Background()
	for _, chat := range chats {
		c.chats[source.PortalID(c.login.Metadata.(*LoginMetadata).AccountKey, chat.ID)] = chat
		if err := c.dispatch(ctx, chat); err != nil {
			t.Fatal(err)
		}
	}
}
func TestFrameworkCreatesRoomsAndDeduplicatesAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bridge.db")
	mx := &matrixFixture{names: map[id.RoomID]string{}}
	br, c := startFixture(t, path, mx)
	chat := testChat()
	feedFixture(t, br, c, chat)
	if mx.rooms != 1 || mx.messages != 2 {
		t.Fatalf("first discovery: rooms=%d messages=%d", mx.rooms, mx.messages)
	}
	feedFixture(t, br, c, chat)
	if mx.rooms != 1 || mx.messages != 2 {
		t.Fatal("repeat duplicated room/messages")
	}
	br.Stop()
	br, c = startFixture(t, path, mx)
	defer br.Stop()
	chat.Title = "Renamed"
	chat.UpdatedAt = 300
	chat.Messages = append(chat.Messages, source.Message{ID: "u2", Role: "user", Text: "Continue"})
	feedFixture(t, br, c, chat)
	if mx.rooms != 1 || mx.messages != 3 || mx.names["!room1:test.invalid"] != "ChatGPT · Renamed" {
		t.Fatalf("restart/continuation failed: %+v", mx)
	}
	other := chat
	other.ID = "22222222-2222-2222-2222-222222222222"
	other.Messages = nil
	feedFixture(t, br, c, other)
	if mx.rooms != 2 {
		t.Fatal("new empty conversation did not get its own room")
	}
}

var _ bridgev2.MatrixConnector = (*matrixFixture)(nil)
var _ bridgev2.MatrixAPI = (*intentFixture)(nil)

func TestFrameworkRetriesFailedDeliveryWithoutAdvancingPastIt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bridge.db")
	mx := &matrixFixture{names: map[id.RoomID]string{}, failNext: true}
	br, c := startFixture(t, path, mx)
	defer br.Stop()
	chat := testChat()
	c.chats[source.PortalID(c.login.Metadata.(*LoginMetadata).AccountKey, chat.ID)] = chat
	if err := c.dispatch(context.Background(), chat); err == nil {
		t.Fatal("failure was not surfaced")
	}
	if mx.messages != 0 {
		t.Fatal("sent later message after failure")
	}
	feedFixture(t, br, c, chat)
	if mx.rooms != 1 || mx.messages != 2 {
		t.Fatalf("retry lost or duplicated delivery: rooms=%d messages=%d", mx.rooms, mx.messages)
	}
	chat.Messages[0].Text = "Edited source question"
	if err := c.dispatch(context.Background(), chat); err == nil {
		t.Fatal("source edit silently ignored")
	}
	if mx.messages != 2 {
		t.Fatal("unsupported edit generated duplicate message")
	}
}
