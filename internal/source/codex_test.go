package source

import (
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
