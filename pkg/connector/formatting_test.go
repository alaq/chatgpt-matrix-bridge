package connector

import (
	"context"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alaq/chatgpt-matrix-bridge/internal/delivery"
	"github.com/alaq/chatgpt-matrix-bridge/internal/source"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

type presentationTransport struct{ path string }

func (p *presentationTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	p.path = r.URL.Path
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}"))}, nil
}

func TestPresentationEditHasStableTransactionDistinctFromOriginal(t *testing.T) {
	chat := testChat()
	chat.Messages[1].Text = "**Answer**"
	m := testClient().events(chat)[2].(*sourceMessage)
	existing := []*database.Message{{Metadata: &MessageMetadata{Hash: source.StableID("assistant", "**Answer**")}}}
	baseCtx := delivery.WithMessage(context.Background(), string(m.ID))
	pathFor := func(ctx context.Context) string {
		capture := &presentationTransport{}
		req, _ := http.NewRequestWithContext(ctx, "PUT", "https://test.invalid/_hungryserv/owner/_matrix/client/v3/rooms/!room:test/send/m.room.encrypted/random", nil)
		resp, err := (delivery.Transport{Base: capture}).RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return capture.path
	}
	originalPath := pathFor(baseCtx)
	var editPath string
	for attempt := 0; attempt < 2; attempt++ {
		r, err := m.HandleExisting(baseCtx, nil, nil, existing)
		if err != nil || len(r.SubEvents) != 1 {
			t.Fatal("no migration edit")
		}
		edit := r.SubEvents[0].(*simplevent.Message[*event.MessageEventContent])
		path := pathFor(edit.MutateContextFunc(baseCtx))
		if path == originalPath || (attempt > 0 && path != editPath) {
			t.Fatal("edit transaction collides or changes on retry")
		}
		editPath = path
	}
}

func TestAssistantMarkdownAndCodeAreRichText(t *testing.T) {
	body := "**Strong** and *emphasis*\n\n- first\n- second\n\n```text\nA < B\n| box |\n```\n\n| Name | Value |\n| --- | --- |\n| A | B |"
	c := renderMessage(source.Message{Role: "assistant"}, body, "")
	if c.Format != event.FormatHTML {
		t.Fatal("missing Matrix HTML format")
	}
	for _, want := range []string{"<strong>Strong</strong>", "<em>emphasis</em>", "<ul>", "<pre><code", "A &lt; B", "<table>"} {
		if !strings.Contains(c.FormattedBody, want) {
			t.Fatalf("missing %s: %s", want, c.FormattedBody)
		}
	}
	u := renderMessage(source.Message{Role: "user"}, body, "")
	if u.Body != body || u.Format != "" {
		t.Fatal("rewrote operator content")
	}
}

func TestCitationsResolveAndUnresolvedOnesLinkToSourceConversation(t *testing.T) {
	marker := "\uE200cite\uE202turn123view0\uE201"
	unknown := "\uE200cite\uE202turn999search0\uE201"
	m := source.Message{Role: "assistant", CitationGroups: []source.CitationGroup{{Marker: marker, Sources: []source.CitationSource{
		{Title: "Docs", URL: "https://example.com/docs"}, {Title: "bad", URL: "javascript:alert(1)"},
		{Title: "duplicate", URL: "https://example.com/docs"},
	}}}}
	c := renderMessage(m, "Supported "+marker+" unknown "+unknown, "https://chatgpt.com/c/example")
	if !strings.Contains(c.FormattedBody, `<a href="https://example.com/docs">Docs</a>`) || !strings.Contains(c.FormattedBody, `>source in ChatGPT</a>`) {
		t.Fatal(c.FormattedBody)
	}
	if strings.Contains(c.Body+c.FormattedBody, "turn123") || strings.Contains(c.Body+c.FormattedBody, "turn999") || strings.Contains(c.FormattedBody, "javascript:") || strings.Contains(c.FormattedBody, "duplicate") {
		t.Fatal("unsafe or unresolved citation leaked")
	}
}

func TestSourceHTMLUnsafeURLsAndMentionsAreNotActivated(t *testing.T) {
	body := `<script>alert(1)</script> <img src=x onerror=alert(1)> [bad](javascript:alert(1)) [data](data:text/html,hi) [mention](https://matrix.to/#/@someone:example.com) @room`
	c := renderMessage(source.Message{Role: "assistant"}, body, "")
	for _, bad := range []string{"<script>", "<img src=x", `href="javascript:`, `href="data:`} {
		if strings.Contains(c.FormattedBody, bad) {
			t.Fatalf("unsafe HTML: %s", c.FormattedBody)
		}
	}
	if c.Mentions == nil || c.Mentions.Room || len(c.Mentions.UserIDs) > 0 {
		t.Fatal("generated a mention notification")
	}
}

func TestFrameworkPresentationMigrationRetriesAndSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bridge.db")
	mx := &matrixFixture{names: map[id.RoomID]string{}}
	br, c := startFixture(t, path, mx)
	chat := testChat()
	chat.Messages[1].Text = "**An existing answer**"
	feedFixture(t, br, c, chat)
	ctx := context.Background()
	assistantID := c.events(chat)[2].(*sourceMessage).ID
	// Model a database written by the plain-text release, without changing source.
	parts, err := br.DB.Message.GetAllPartsByID(ctx, c.login.ID, assistantID)
	if err != nil || len(parts) != 1 {
		t.Fatalf("get original: %v", err)
	}
	originalID := parts[0].MXID
	parts[0].Metadata.(*MessageMetadata).PresentationHash = ""
	if err := br.DB.Message.Update(ctx, parts[0]); err != nil {
		t.Fatal(err)
	}
	mx.failNext = true
	if err := c.dispatch(ctx, chat); err == nil {
		t.Fatal("presentation failure not surfaced")
	}
	parts, _ = br.DB.Message.GetAllPartsByID(ctx, c.login.ID, assistantID)
	if parts[0].Metadata.(*MessageMetadata).PresentationHash != "" {
		t.Fatal("failed edit advanced migration")
	}
	feedFixture(t, br, c, chat)
	if mx.rooms != 1 || mx.messages != 3 {
		t.Fatalf("expected one in-place edit: %d", mx.messages)
	}
	last := mx.contents[len(mx.contents)-1]
	if last.NewContent == nil || last.NewContent.FormattedBody != "<strong>An existing answer</strong>" || last.RelatesTo.EventID != originalID {
		t.Fatal("edit did not replace original event")
	}
	br.Stop()
	br, c = startFixture(t, path, mx)
	defer br.Stop()
	feedFixture(t, br, c, chat)
	if mx.messages != 3 || mx.uploads != 1 {
		t.Fatal("restart duplicated edits or icon upload")
	}
	parts, _ = br.DB.Message.GetAllPartsByID(ctx, c.login.ID, assistantID)
	if parts[0].MXID != originalID {
		t.Fatal("lost original event identity")
	}
	chat.Messages[1].Text = "Changed source"
	if err := c.dispatch(ctx, chat); err == nil {
		t.Fatal("presentation bypassed source edit detection")
	}
}

func TestLegacyOperatorAndPlainAssistantDoNotGetPresentationEdits(t *testing.T) {
	for _, role := range []string{"user", "assistant"} {
		chat := testChat()
		chat.Messages = []source.Message{{ID: "old", Role: role, Text: "Plain text"}}
		m := testClient().events(chat)[1].(*sourceMessage)
		existing := []*database.Message{{Metadata: &MessageMetadata{Hash: source.StableID(role, "Plain text")}}}
		r, err := m.HandleExisting(context.Background(), nil, nil, existing)
		if err != nil || len(r.SubEvents) > 0 {
			t.Fatalf("unnecessary %s edit", role)
		}
	}
}
