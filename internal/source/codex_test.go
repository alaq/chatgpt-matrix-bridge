package source

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLocalTaskNamespaceCannotAliasChatGPT(t *testing.T) {
	uuid := "11111111-1111-1111-1111-111111111111"
	account := strings.Repeat("a", 64)
	c := Conversation{ID: "codex:" + uuid, Kind: "codex", URL: "codex://threads/" + uuid, Revision: account, CreatedAt: 1, UpdatedAt: 2}
	s := Snapshot{Version: 1, Source: "chatgpt", AccountKey: account, Conversations: []Conversation{c}}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	if PortalID(account, c.ID) == PortalID(account, uuid) {
		t.Fatal("source identity collision")
	}
	s.Conversations[0].URL = "https://chatgpt.com/c/" + uuid
	if s.Validate() == nil {
		t.Fatal("accepted mismatched source URL")
	}
	s.Conversations[0] = c
	s.Conversations[0].Kind = "chatgpt"
	if s.Validate() == nil {
		t.Fatal("accepted mismatched source kind")
	}
}

func TestLocalTasksPassesActiveConversationLimitToAdapter(t *testing.T) {
	dir := t.TempDir()
	adapter := filepath.Join(dir, "adapter.py")
	script := `import sys
args=sys.argv[1:]
assert args[args.index('--max-conversations')+1] == '10'
assert args[args.index('--allow-conversation')+1] == 'codex:allowed'
assert args[args.index('--known-conversation')+1] == 'codex:11111111-1111-1111-1111-111111111111=` + strings.Repeat("e", 64) + `'
print('[]')
`
	if err := os.WriteFile(adapter, []byte(script), 0600); err != nil {
		t.Fatal(err)
	}
	b := LocalTasks{Python: "python3", Adapter: adapter, Home: dir, Journal: filepath.Join(dir, "journal"), Since: 1, AllowConversations: []string{"codex:allowed"}}
	known := map[string]Conversation{"codex:11111111-1111-1111-1111-111111111111": {DeliveryFingerprint: strings.Repeat("e", 64)}}
	if _, err := b.Read(context.Background(), strings.Repeat("a", 64), known); err != nil {
		t.Fatal(err)
	}
}
