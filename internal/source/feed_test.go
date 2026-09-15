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
	_, err := b.Read(context.Background(), nil)
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
	os.WriteFile(path, []byte("import sys\nassert sys.argv[-6:] == ['--max-conversations', '10', '--allow-conversation', 'allowed', '--known-conversation', '11111111-1111-1111-1111-111111111111="+strings.Repeat("e", 64)+"']\nprint("+string(mustJSON(string(raw)))+")\n"), 0600)
	b := Backend{Python: "python3", Directory: dir, Archive: dir, MaxConversations: 10, AllowConversations: []string{"allowed"}}
	s, err := b.Read(context.Background(), map[string]Conversation{"11111111-1111-1111-1111-111111111111": {DeliveryFingerprint: strings.Repeat("e", 64)}})
	if err != nil || len(s.Conversations) != 1 {
		t.Fatalf("contract: %v", err)
	}
	os.WriteFile(path, []byte("import time\ntime.sleep(30)\n"), 0600)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err = b.Read(ctx, nil); err == nil {
		t.Fatal("cancellation ignored")
	}
}

func TestUnchangedConversationRequiresAndHydratesVerifiedDeliveryState(t *testing.T) {
	id := "11111111-1111-1111-1111-111111111111"
	fingerprint := strings.Repeat("e", 64)
	s := &Snapshot{Version: 1, Source: "chatgpt", AccountKey: strings.Repeat("a", 64), Conversations: []Conversation{{ID: id, Unchanged: true, DeliveryFingerprint: fingerprint}}}
	known := map[string]Conversation{id: {ID: id, Revision: strings.Repeat("b", 64), Title: "Question", URL: "https://chatgpt.com/c/" + id, CreatedAt: 100, UpdatedAt: 200, Messages: []Message{{ID: "m1", Role: "user", Text: "Question"}}, DeliveryFingerprint: fingerprint}}
	if err := HydrateUnchanged(s, known); err != nil || !s.Conversations[0].Unchanged || s.Conversations[0].Title != "Question" || len(s.Conversations[0].Messages) != 1 {
		t.Fatalf("unchanged hydration failed: %+v %v", s.Conversations[0], err)
	}
	s.Conversations[0] = Conversation{ID: id, Unchanged: true, DeliveryFingerprint: strings.Repeat("f", 64)}
	if err := HydrateUnchanged(s, known); err == nil {
		t.Fatal("mismatched fingerprint accepted")
	}
}
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
