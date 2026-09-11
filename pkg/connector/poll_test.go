package connector

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alaq/chatgpt-matrix-bridge/internal/source"
	"maunium.net/go/mautrix/id"
)

func TestRefreshFailureStillDeliversVerifiedArchiveAndReportsDisconnected(t *testing.T) {
	mx := &matrixFixture{names: map[id.RoomID]string{}}
	br, c := startFixture(t, filepath.Join(t.TempDir(), "bridge.db"), mx)
	defer br.Stop()
	// A fixture backend fails refresh and returns a valid captured snapshot.
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "scripts/history"), 0700); err != nil {
		t.Fatal(err)
	}
	script := `import json, sys
if sys.argv[-1] != 'feed': sys.exit(1)
print(json.dumps({'version':1,'source':'chatgpt','account_key':'` + strings.Repeat("a", 64) + `','conversations':[{'id':'11111111-1111-1111-1111-111111111111','revision':'` + strings.Repeat("b", 64) + `','title':'Cached','url':'https://chatgpt.com/c/11111111-1111-1111-1111-111111111111','created_at':100,'updated_at':200,'messages':[{'id':'a','role':'assistant','text':'**Cached answer**'}]}]}))
`
	if err := os.WriteFile(filepath.Join(root, "scripts/history/cli.py"), []byte(script), 0600); err != nil {
		t.Fatal(err)
	}
	c.backend = source.Backend{Python: "python3", Directory: root, Archive: root}
	if err := c.poll(context.Background()); err == nil {
		t.Fatal("failed refresh reported success")
	}
	if mx.rooms != 1 || mx.messages != 1 || c.IsLoggedIn() {
		t.Fatal("cached delivery or connection status wrong")
	}
	if err := c.poll(context.Background()); err == nil {
		t.Fatal("repeat failure reported success")
	}
	if mx.messages != 1 {
		t.Fatal("stale snapshot duplicated messages")
	}
}

func TestFailureBackoffIsBoundedAndSuccessfulPollResetsIt(t *testing.T) {
	previous := 30 * time.Second
	for i := 1; i <= 20; i++ {
		delay := nextPollDelay(30*time.Second, i)
		if delay < time.Minute || delay < previous || delay > 15*time.Minute {
			t.Fatal("invalid backoff", delay)
		}
		previous = delay
	}
	if previous != 15*time.Minute || nextPollDelay(30*time.Second, 0) != 30*time.Second {
		t.Fatal("cap or recovery wrong")
	}
	if nextPollDelay(time.Hour, 5) != time.Hour {
		t.Fatal("backoff shortened configured interval")
	}
}
