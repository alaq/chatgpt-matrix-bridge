package connector

import (
	"fmt"
	"testing"

	"github.com/alaq/chatgpt-matrix-bridge/internal/source"
)

func TestMaxActiveConversationsDefaultsToTenAndStaysBounded(t *testing.T) {
	if got := (Config{}).activeLimit(); got != 10 {
		t.Fatalf("default active limit = %d, want 10", got)
	}
	if got := (Config{MaxActiveConversations: 7}).activeLimit(); got != 7 {
		t.Fatalf("configured active limit = %d, want 7", got)
	}
	for _, value := range []int{-1, 101} {
		if got := (Config{MaxActiveConversations: value}).activeLimit(); got != 0 {
			t.Fatalf("invalid active limit %d became %d", value, got)
		}
	}
}

func TestSelectActiveConversationsKeepsNewestTenAcrossSources(t *testing.T) {
	chats := make([]source.Conversation, 0, 12)
	for i := range 12 {
		kind := "chatgpt"
		id := fmt.Sprintf("00000000-0000-4000-8000-%012d", i)
		if i%2 == 1 {
			kind = "codex"
			id = "codex:" + id
		}
		chats = append(chats, source.Conversation{ID: id, Kind: kind, UpdatedAt: float64(i)})
	}
	selected := (Config{}).selectActive(chats)
	if len(selected) != 10 {
		t.Fatalf("selected %d conversations, want 10", len(selected))
	}
	for i, chat := range selected {
		if chat.UpdatedAt != float64(i+2) {
			t.Fatalf("selected[%d] updated_at=%v, want %d", i, chat.UpdatedAt, i+2)
		}
	}
}

func TestSelectActiveConversationsAlwaysIncludesRunningConversation(t *testing.T) {
	chats := []source.Conversation{{ID: "running", UpdatedAt: 1, Running: true}}
	for i := range 10 {
		chats = append(chats, source.Conversation{ID: fmt.Sprintf("recent-%d", i), UpdatedAt: float64(100 + i)})
	}
	selected := (Config{}).selectActive(chats)
	if len(selected) != 10 {
		t.Fatalf("selected %d conversations, want 10", len(selected))
	}
	for _, chat := range selected {
		if chat.ID == "running" {
			return
		}
	}
	t.Fatal("running conversation was displaced by completed conversations")
}

func TestReplaceActiveCacheDropsDormantConversations(t *testing.T) {
	account := "account"
	client := &Client{chats: map[string]source.Conversation{
		source.PortalID(account, "old"): {ID: "old"},
	}}
	client.replaceActiveCache(account, []source.Conversation{{ID: "new"}})
	if len(client.chats) != 1 || client.chats[source.PortalID(account, "new")].ID != "new" {
		t.Fatalf("active cache was not replaced: %#v", client.chats)
	}
}
