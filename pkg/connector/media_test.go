package connector

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"github.com/alaq/chatgpt-matrix-bridge/internal/source"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
)

func mediaFixture(t *testing.T, c *Client, fail bool) {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	root := t.TempDir()
	dir := filepath.Join(root, "scripts", "history")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	bytes := []byte("synthetic file")
	sum := sha256.Sum256(bytes)
	result, _ := json.Marshal(source.Media{Version: 1, ID: "file_123456789", Name: "note.txt", MimeType: "text/plain", Size: len(bytes), SHA256: hex.EncodeToString(sum[:]), Data: base64.StdEncoding.EncodeToString(bytes)})
	code := "print(" + strconv.Quote(string(result)) + ")\n"
	if fail {
		code = "raise SystemExit(1)\n"
	}
	if err := os.WriteFile(filepath.Join(dir, "media.py"), []byte(code), 0600); err != nil {
		t.Fatal(err)
	}
	c.backend = source.Backend{Python: python, Directory: root, Descriptor: filepath.Join(root, "descriptor.json")}
	c.connector.Config.SyncMedia = true
	c.connector.Config.SyncEdits = true
}

func TestMediaDeliveryDeduplicatesAcrossRestart(t *testing.T) {
	mx := &matrixFixture{names: map[id.RoomID]string{}}
	path := filepath.Join(t.TempDir(), "bridge.db")
	br, c := startFixture(t, path, mx)
	mediaFixture(t, c, false)
	chat := testChat()
	chat.Messages[0].AttachmentCount = 1
	chat.Messages[0].Attachments = []source.Attachment{{ID: "file_123456789", Name: "note.txt", MimeType: "text/plain", Size: 14}}
	feedFixture(t, br, c, chat)
	before := mx.messages
	uploads := mx.uploads
	if before != 3 {
		t.Fatalf("expected text, file, answer; got %d", before)
	}
	found := false
	for _, content := range mx.contents {
		if content.MsgType == event.MsgFile && content.FileName == "note.txt" {
			found = true
		}
	}
	if !found {
		t.Fatal("file content missing")
	}
	br.Stop()
	br, c = startFixture(t, path, mx)
	defer br.Stop()
	mediaFixture(t, c, false)
	feedFixture(t, br, c, chat)
	if mx.messages != before || mx.uploads != uploads {
		t.Fatal("restart repeated media delivery")
	}
}

func TestUnavailableAttachmentDoesNotBlockFollowingAnswer(t *testing.T) {
	mx := &matrixFixture{names: map[id.RoomID]string{}}
	br, c := startFixture(t, filepath.Join(t.TempDir(), "bridge.db"), mx)
	defer br.Stop()
	mediaFixture(t, c, true)
	chat := testChat()
	chat.Messages[0].Attachments = []source.Attachment{{ID: "file_123456789"}}
	if err := c.dispatch(context.Background(), chat); err != nil {
		t.Fatal(err)
	}
	if mx.messages != 2 {
		t.Fatal("missing file blocked following text")
	}
	if c.health.PendingAttachments != 1 {
		t.Fatal("missing file was not reported")
	}
	// A later successful download must use an unconsumed event transaction.
	mediaFixture(t, c, false)
	feedFixture(t, br, c, chat)
	if mx.messages != 3 {
		t.Fatal("file recovery collided with an earlier error notice")
	}
}

func TestAcceptedOutboundAttachmentDoesNotEcho(t *testing.T) {
	mx := &matrixFixture{names: map[id.RoomID]string{}}
	br, c := startFixture(t, filepath.Join(t.TempDir(), "bridge.db"), mx)
	defer br.Stop()
	mediaFixture(t, c, false)
	chat := testChat()
	feedFixture(t, br, c, chat)
	account := c.login.Metadata.(*LoginMetadata).AccountKey
	part, err := br.DB.Message.GetPartByID(context.Background(), c.login.ID, networkid.MessageID(source.MessageID(account, chat.ID, chat.Messages[0].ID)), "")
	if err != nil {
		t.Fatal(err)
	}
	part.Metadata.(*MessageMetadata).AttachmentIDs = []string{"file_123456789"}
	if err := br.DB.Message.Update(context.Background(), part); err != nil {
		t.Fatal(err)
	}
	before, uploads := mx.messages, mx.uploads
	chat.Messages[0].Attachments = []source.Attachment{{ID: "file_123456789"}}
	for i := 0; i < 3; i++ {
		feedFixture(t, br, c, chat)
	}
	if mx.messages != before || mx.uploads != uploads {
		t.Fatal("outgoing file echoed or uploaded again")
	}
}
