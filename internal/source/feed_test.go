package source

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func sample() *Snapshot {
	return &Snapshot{Version: 1, Source: "chatgpt", AccountKey: strings.Repeat("a", 64), Conversations: []Conversation{
		{ID: "11111111-1111-1111-1111-111111111111", Revision: strings.Repeat("b", 64), Title: "Question", URL: "https://chatgpt.com/c/11111111-1111-1111-1111-111111111111", CreatedAt: 100, UpdatedAt: 200, Messages: []Message{{ID: "m1", Role: "user", Text: "Question"}}},
	}}
}

func TestOldConversationUpdatedAfterActivationIsSelected(t *testing.T) {
	s := sample()
	chats, err := Select(s, s.AccountKey, 150)
	if err != nil || len(chats) != 1 {
		t.Fatalf("updated old conversation missed: %v", err)
	}
	chats, err = Select(s, s.AccountKey, 201)
	if err != nil || len(chats) != 0 {
		t.Fatal("unchanged historical conversation should not create a room")
	}
}
func TestAccountChangeAndUnknownSourcesFailClosed(t *testing.T) {
	s := sample()
	if _, err := Select(s, strings.Repeat("c", 64), 150); err == nil {
		t.Fatal("account change accepted")
	}
	s.Source = "work"
	if err := s.Validate(); err == nil {
		t.Fatal("unimplemented source accepted")
	}
}
func TestIdentityDoesNotDependOnTitleOrRevision(t *testing.T) {
	s := sample()
	c := s.Conversations[0]
	room := PortalID(s.AccountKey, c.ID)
	message := MessageID(s.AccountKey, c.ID, c.Messages[0].ID)
	c.Title = "Renamed"
	c.Revision = strings.Repeat("d", 64)
	if room != PortalID(s.AccountKey, c.ID) || message != MessageID(s.AccountKey, c.ID, c.Messages[0].ID) {
		t.Fatal("identity changed")
	}
	if room == PortalID(strings.Repeat("c", 64), c.ID) {
		t.Fatal("accounts collided")
	}
	if StableID("ab", "c") == StableID("a", "bc") {
		t.Fatal("ambiguous concatenation")
	}
}
func TestMalformedAndHiddenMessagesAreRejected(t *testing.T) {
	for _, mutate := range []func(*Snapshot){
		func(s *Snapshot) { s.Conversations = append(s.Conversations, s.Conversations[0]) },
		func(s *Snapshot) {
			s.Conversations[0].Messages = append(s.Conversations[0].Messages, s.Conversations[0].Messages[0])
		},
		func(s *Snapshot) { s.Conversations[0].Messages[0].Role = "system" },
		func(s *Snapshot) { s.Conversations[0].URL = "https://other.example/private" },
		func(s *Snapshot) { s.Version = 2 },
	} {
		s := sample()
		mutate(s)
		raw, _ := json.Marshal(s)
		if _, err := Decode(raw); err == nil {
			t.Fatal("invalid feed accepted")
		}
	}
}
func TestBackendUsesArgumentsNotShellAndSuppressesSensitiveErrors(t *testing.T) {
	dir := t.TempDir()
	scripts := filepath.Join(dir, "scripts", "history")
	os.MkdirAll(scripts, 0700)
	path := filepath.Join(scripts, "cli.py")
	secret := "private-token-do-not-log"
	os.WriteFile(path, []byte("import sys\nsys.stderr.write('"+secret+"')\nsys.exit(1)\n"), 0600)
	b := Backend{Python: "python3", Directory: dir, Archive: filepath.Join(dir, "archive; not-a-command")}
	_, err := b.Read(context.Background())
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatal("sensitive backend error surfaced")
	}
}
func TestBackendContractRoundTripAndCancellation(t *testing.T) {
	dir := t.TempDir()
	scripts := filepath.Join(dir, "scripts", "history")
	os.MkdirAll(scripts, 0700)
	raw, _ := json.Marshal(sample())
	path := filepath.Join(scripts, "cli.py")
	os.WriteFile(path, []byte("print("+string(mustJSON(string(raw)))+")\n"), 0600)
	b := Backend{Python: "python3", Directory: dir, Archive: dir}
	s, err := b.Read(context.Background())
	if err != nil || len(s.Conversations) != 1 {
		t.Fatalf("contract: %v", err)
	}
	os.WriteFile(path, []byte("import time\ntime.sleep(30)\n"), 0600)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err = b.Read(ctx); err == nil {
		t.Fatal("cancellation ignored")
	}
}
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
