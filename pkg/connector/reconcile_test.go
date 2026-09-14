package connector

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"maunium.net/go/mautrix/id"
)

func TestSourceEditGrowthShrinkAndRestartAreIdempotent(t *testing.T) {
	mx := &matrixFixture{names: map[id.RoomID]string{}}
	path := filepath.Join(t.TempDir(), "bridge.db")
	br, c := startFixture(t, path, mx)
	c.connector.Config.SyncEdits = true
	chat := testChat()
	feedFixture(t, br, c, chat)
	before := mx.messages
	chat.Messages[1].Text = strings.Repeat("An expanded answer with useful details. ", 2500)
	feedFixture(t, br, c, chat)
	if mx.messages <= before {
		t.Fatal("source growth was ignored")
	}
	afterGrowth := mx.messages
	feedFixture(t, br, c, chat)
	if mx.messages != afterGrowth {
		t.Fatal("source growth repeated")
	}
	chat.Messages[1].Text = "A corrected short answer."
	feedFixture(t, br, c, chat)
	afterShrink := mx.messages
	if afterShrink <= afterGrowth {
		t.Fatal("source shrink was ignored")
	}
	br.Stop()
	br, c = startFixture(t, path, mx)
	defer br.Stop()
	c.connector.Config.SyncEdits = true
	feedFixture(t, br, c, chat)
	if mx.messages != afterShrink {
		t.Fatal("restart repeated source edits")
	}
}

func TestBranchChangePreservesHistoryAndEmitsOneNotice(t *testing.T) {
	mx := &matrixFixture{names: map[id.RoomID]string{}}
	br, c := startFixture(t, filepath.Join(t.TempDir(), "bridge.db"), mx)
	defer br.Stop()
	c.connector.Config.SyncEdits = true
	chat := testChat()
	feedFixture(t, br, c, chat)
	before := mx.messages
	chat.Messages[1].ID = "regenerated-answer"
	chat.Messages[1].Text = "A different answer."
	feedFixture(t, br, c, chat)
	if mx.messages != before+2 {
		t.Fatalf("expected notice and new answer: %d", mx.messages-before)
	}
	feedFixture(t, br, c, chat)
	if mx.messages != before+2 {
		t.Fatal("branch repeated")
	}
	var count int
	if err := br.DB.QueryRow(context.Background(), "SELECT COUNT(*) FROM message").Scan(&count); err != nil || count != before+2 {
		t.Fatal("earlier branch history lost", err, count)
	}
}

func TestSourceEditCyclesCreateFreshEdits(t *testing.T) {
	mx := &matrixFixture{names: map[id.RoomID]string{}}
	br, c := startFixture(t, filepath.Join(t.TempDir(), "bridge.db"), mx)
	defer br.Stop()
	c.connector.Config.SyncEdits = true
	chat := testChat()
	original := chat.Messages[1].Text
	feedFixture(t, br, c, chat)
	before := mx.messages
	for _, text := range []string{"Updated answer", original, "Updated answer"} {
		chat.Messages[1].Text = text
		feedFixture(t, br, c, chat)
	}
	if mx.messages != before+3 {
		t.Fatalf("edit cycles lost a new edit: got %d, want %d", mx.messages, before+3)
	}
	feedFixture(t, br, c, chat)
	if mx.messages != before+3 {
		t.Fatal("unchanged edit replayed")
	}
}
