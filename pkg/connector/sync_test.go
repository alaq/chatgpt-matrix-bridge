package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alaq/chatgpt-matrix-bridge/internal/source"
	"maunium.net/go/mautrix/id"
)

func awaitSync(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("synchronization condition timed out")
}

func writeSyncFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path+".tmp", data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		t.Fatal(err)
	}
}

func TestIndependentLoopsDeliverDuringRemoteStallAndIsolateFailures(t *testing.T) {
	root := t.TempDir()
	mx := &matrixFixture{names: map[id.RoomID]string{}}
	br, c := startFixture(t, filepath.Join(root, "bridge.db"), mx)
	defer br.Stop()
	defer c.Disconnect()
	history := filepath.Join(root, "scripts", "history")
	if err := os.MkdirAll(history, 0700); err != nil {
		t.Fatal(err)
	}
	remote := testChat()
	remote.Revision = strings.Repeat("b", 64)
	local := remote
	local.Kind, local.ID, local.URL = "codex", "codex:"+local.ID, "codex://threads/"+local.ID
	local.DeliveryFingerprint = strings.Repeat("c", 64)
	account := c.login.Metadata.(*LoginMetadata).AccountKey
	data, _ := json.Marshal(source.Snapshot{Version: 1, Source: "chatgpt", AccountKey: account, Conversations: []source.Conversation{remote}})
	writeSyncFile(t, filepath.Join(history, "feed.json"), data)
	data, _ = json.Marshal([]source.Conversation{local})
	writeSyncFile(t, filepath.Join(root, "local.json"), data)
	writeSyncFile(t, filepath.Join(history, "cli.py"), []byte(`import sys, time
from pathlib import Path
p = Path(__file__).parent
if 'sync' in sys.argv:
    with (p/'calls').open('a') as f: f.write('sync\n')
    while not (p/'release').exists(): time.sleep(.01)
    sys.exit(0 if (p/'release').read_text() == 'success' else 1)
if 'feed' in sys.argv: print((p/'feed.json').read_text())
else: sys.exit(2)
`))
	adapter := filepath.Join(root, "local.py")
	writeSyncFile(t, adapter, []byte(`from pathlib import Path
print(Path(__file__).with_name('local.json').read_text())
`))
	c.backend = source.Backend{Python: "python3", Directory: root, Archive: root}
	c.connector.Config.CodexHome, c.connector.Config.CodexAdapter = root, adapter
	c.connector.Config.CodexJournal, c.connector.Config.CodexSince = root, "1970-01-01T00:00:01Z"
	c.connector.Config.CodexPollSeconds, c.connector.Config.PollSeconds = 1, 120
	c.Connect(context.Background())
	awaitSync(t, func() bool {
		c.health.mu.Lock()
		defer c.health.mu.Unlock()
		_, err := os.Stat(filepath.Join(history, "calls"))
		return err == nil && c.health.Codex != nil && c.health.Codex.SourceAvailable
	})
	c.health.mu.Lock()
	firstLocal := c.health.Codex.LastAttempt
	remoteUnfinished := c.health.ChatGPT.LastAttempt.IsZero() && !c.health.SourceAvailable
	c.health.mu.Unlock()
	if !remoteUnfinished {
		t.Fatal("local success hid unfinished remote refresh")
	}
	// A natural local timer keeps running while the remote subprocess is blocked.
	awaitSync(t, func() bool {
		c.health.mu.Lock()
		defer c.health.mu.Unlock()
		return c.health.Codex.LastAttempt.After(firstLocal)
	})
	mx.mu.Lock()
	messages := mx.messages
	mx.mu.Unlock()
	if messages != len(local.Messages) {
		t.Fatalf("local delivery stalled: %d messages", messages)
	}
	calls, _ := os.ReadFile(filepath.Join(history, "calls"))
	if string(calls) != "sync\n" {
		t.Fatal("local tick triggered remote refresh")
	}
	writeSyncFile(t, filepath.Join(history, "release"), []byte("failure"))
	awaitSync(t, func() bool {
		c.health.mu.Lock()
		defer c.health.mu.Unlock()
		return !c.health.ChatGPT.LastAttempt.IsZero()
	})
	c.health.mu.Lock()
	failedAt := c.health.ChatGPT.LastAttempt
	remoteOutageVisible := !c.health.SourceAvailable && !c.health.ChatGPT.SourceAvailable && c.health.Codex.SourceAvailable
	c.health.mu.Unlock()
	if !remoteOutageVisible {
		t.Fatal("remote outage was masked")
	}
	c.requestConversationRefresh(local.ID)
	awaitSync(t, func() bool {
		c.health.mu.Lock()
		defer c.health.mu.Unlock()
		return c.health.Codex.LastAttempt.After(failedAt)
	})
	calls, _ = os.ReadFile(filepath.Join(history, "calls"))
	if string(calls) != "sync\n" {
		t.Fatal("local wake bypassed remote backoff")
	}
	// Recovery wakes the remote lane. A failing local reader cannot stop it.
	writeSyncFile(t, filepath.Join(root, "local.json"), []byte("invalid"))
	c.requestConversationRefresh(local.ID)
	awaitSync(t, func() bool {
		c.health.mu.Lock()
		defer c.health.mu.Unlock()
		return !c.health.Codex.SourceAvailable
	})
	writeSyncFile(t, filepath.Join(history, "release"), []byte("success"))
	c.requestRefresh()
	awaitSync(t, func() bool {
		c.health.mu.Lock()
		defer c.health.mu.Unlock()
		return c.health.ChatGPT.SourceAvailable
	})
	c.health.mu.Lock()
	localOutageVisible := !c.health.SourceAvailable && !c.health.Codex.SourceAvailable
	c.health.mu.Unlock()
	if !localOutageVisible {
		t.Fatal("remote success hid local failure")
	}
	// Restart both workers; persisted message mappings prevent duplicates.
	c.Disconnect()
	writeSyncFile(t, filepath.Join(root, "local.json"), data)
	c.Connect(context.Background())
	awaitSync(t, func() bool { return c.connected.Load() })
	c.Disconnect()
	if mx.rooms != 2 || mx.messages != len(local.Messages)+len(remote.Messages) {
		t.Fatalf("duplicate or missing delivery: rooms=%d messages=%d", mx.rooms, mx.messages)
	}
	if nextPollDelay(time.Duration(c.connector.Config.PollSeconds)*time.Second, 0) != 2*time.Minute {
		t.Fatal("normal ChatGPT interval changed")
	}
}

func TestSourceWakeupsCoalesceWithoutCrossingSources(t *testing.T) {
	c := &Client{refreshRequested: make(chan struct{}, 1), localRefreshRequested: make(chan struct{}, 1)}
	for range 20 {
		c.requestConversationRefresh("codex:task")
	}
	c.observeGeneration(nil, "codex:task")
	if len(c.refreshRequested) != 0 || len(c.localRefreshRequested) != 1 {
		t.Fatal("local activity woke ChatGPT or did not coalesce")
	}
	<-c.localRefreshRequested
	for range 20 {
		c.requestConversationRefresh("remote")
	}
	if len(c.refreshRequested) != 1 || len(c.localRefreshRequested) != 0 {
		t.Fatal("remote activity crossed sources")
	}
}

func TestIndependentCachesShareCapAndNeverDispatchOtherSource(t *testing.T) {
	c := &Client{connector: &Connector{Config: Config{MaxActiveConversations: 3}}}
	remote := []source.Conversation{{ID: "remote-old", UpdatedAt: 1}, {ID: "remote-new", UpdatedAt: 9}}
	local := []source.Conversation{{ID: "codex:old", UpdatedAt: 2}, {ID: "codex:new", UpdatedAt: 10}}
	c.updateSourceCache("account", remoteSource, remote)
	selected := c.updateSourceCache("account", localSource, local)
	if len(c.chats) != 3 || len(selected) != 2 {
		t.Fatal("combined cap wrong")
	}
	for _, chat := range selected {
		if conversationSource(chat.ID) != localSource {
			t.Fatal("local tick selected remote delivery")
		}
	}
	selected = c.updateSourceCache("account", remoteSource, remote)
	if len(selected) != 1 || selected[0].ID != "remote-new" {
		t.Fatal("remote selection ignored local candidates")
	}
	c.updateSourceCache("account", localSource, nil)
	if len(c.chats) != 2 {
		t.Fatal("empty local snapshot removed remote cache")
	}
	for i := range 10 {
		local = append(local, source.Conversation{ID: fmt.Sprintf("codex:%d", i), UpdatedAt: float64(i + 20)})
	}
	c.updateSourceCache("account", localSource, local)
	if len(c.chats) != 3 || len(c.candidates[localSource]) != 3 {
		t.Fatal("unbounded active metadata")
	}
}

func TestHealthRetainsRemoteFailureAndAttachmentCountsAcrossLocalPoll(t *testing.T) {
	mx := &matrixFixture{names: map[id.RoomID]string{}}
	br, c := startFixture(t, filepath.Join(t.TempDir(), "bridge.db"), mx)
	defer br.Stop()
	c.connector.Config.CodexHome = t.TempDir()
	c.recordSourceHealth(context.Background(), remoteSource, errors.New("offline"), nil)
	c.addPendingAttachments(remoteSource, 2)
	c.resetPendingAttachments(localSource)
	c.recordSourceHealth(context.Background(), localSource, nil, nil)
	if c.health.SourceAvailable || c.health.PendingAttachments != 2 || !c.health.LastSourceSuccess.IsZero() {
		t.Fatal("local health cleared remote failure")
	}
	if !strings.Contains(c.healthSummary(), "ChatGPT: source unavailable\nCodex: connected") {
		t.Fatal("source-specific status missing")
	}
	c.recordSourceHealth(context.Background(), remoteSource, nil, nil)
	remoteSuccess := c.health.ChatGPT.LastSourceSuccess
	c.recordSourceHealth(context.Background(), localSource, nil, nil)
	if !c.health.LastSourceSuccess.Equal(remoteSuccess) {
		t.Fatal("local tick advanced remote success timestamp")
	}
	c.health.ChatGPT.LastAttempt = time.Now().Add(-time.Hour)
	if !strings.Contains(c.healthSummary(), "ChatGPT: sync delayed") {
		t.Fatal("fast local polling masked stalled remote worker")
	}
}
