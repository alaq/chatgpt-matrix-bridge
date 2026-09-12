package connector

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/alaq/chatgpt-matrix-bridge/internal/source"
	xhtml "golang.org/x/net/html"
	"golang.org/x/net/html/atom"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

func htmlText(t *testing.T, content string) string {
	t.Helper()
	nodes, err := xhtml.ParseFragment(strings.NewReader(content), &xhtml.Node{Type: xhtml.ElementNode, Data: "div", DataAtom: atom.Div})
	if err != nil {
		t.Fatal(err)
	}
	var result strings.Builder
	var walk func(*xhtml.Node)
	walk = func(n *xhtml.Node) {
		if n.Type == xhtml.TextNode {
			result.WriteString(n.Data)
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	for _, node := range nodes {
		walk(node)
	}
	return result.String()
}

func assertFitsEncryptedEvent(t *testing.T, content *event.MessageEventContent) {
	t.Helper()
	data, err := json.Marshal(content)
	if err != nil {
		t.Fatal(err)
	}
	// Conservative allowance for Megolm framing, room/type/sender, ciphertext
	// base64, bridge metadata and the encrypted JSON envelope.
	if base64.StdEncoding.EncodedLen(len(data)+2048)+2048 >= 64*1024 {
		t.Fatalf("content leaves insufficient encryption headroom: %d", len(data))
	}
	if !utf8.ValidString(content.Body) || !utf8.ValidString(content.FormattedBody) {
		t.Fatal("split UTF-8")
	}
	if content.Mentions == nil || content.Mentions.Room || len(content.Mentions.UserIDs) > 0 {
		t.Fatal("mentions activated")
	}
}

func TestSplitOversizedRichTextPreservesTextAndBalancedFormatting(t *testing.T) {
	marker := "\uE200cite\uE202turn123search0\uE201"
	m := source.Message{Role: "assistant", CitationGroups: []source.CitationGroup{{Marker: marker, Sources: []source.CitationSource{{Title: "Travel source", URL: "https://example.com/article?one=1&two=2"}}}}}
	for _, body := range []string{
		strings.Repeat("## Destination\n\n**杭州 🧳** and *details* "+marker+"\n\n", 600),
		"```python\n" + strings.Repeat("print('杭州 🧳 <>&')\n", 4000) + "```",
		"| Place | Details |\n| --- | --- |\n" + strings.Repeat("| **杭州** | Scenic & pleasant |\n", 2000),
		":::writing{variant=\"chat_message\" id=\"123\"}\n" + strings.Repeat("Draft paragraph **with emphasis**.\n\n", 3000) + ":::",
	} {
		original := renderMessage(m, body, "https://chatgpt.com/c/example")
		parts := splitMessage(original)
		if len(parts) < 2 {
			t.Fatal("oversized answer was not split")
		}
		var text strings.Builder
		for _, part := range parts {
			assertFitsEncryptedEvent(t, part)
			text.WriteString(htmlText(t, part.FormattedBody))
			if strings.Contains(body, "```python") && (!strings.Contains(part.FormattedBody, "<pre><code") || !strings.Contains(part.FormattedBody, "</code></pre>")) {
				t.Fatal("lost code formatting")
			}
		}
		if text.String() != htmlText(t, original.FormattedBody) {
			t.Fatal("visible rich text lost or duplicated")
		}
	}
}

func TestSplitPlainUnicodeEscapingAndHugeLink(t *testing.T) {
	body := strings.Repeat("杭州 🧳 <>&\"\n", 10000)
	original := renderMessage(source.Message{Role: "user"}, body, "")
	parts := splitMessage(original)
	var joined strings.Builder
	for _, part := range parts {
		assertFitsEncryptedEvent(t, part)
		joined.WriteString(part.Body)
	}
	if joined.String() != body {
		t.Fatal("changed operator text")
	}
	link := renderMessage(source.Message{Role: "assistant"}, "[Reference](https://example.com/"+strings.Repeat("a", 100000)+")", "")
	joined.Reset()
	for _, part := range splitMessage(link) {
		assertFitsEncryptedEvent(t, part)
		joined.WriteString(part.Body)
	}
	if joined.String() != link.Body {
		t.Fatal("lost oversized link text or URL")
	}
	short := renderMessage(source.Message{Role: "assistant"}, "**Unchanged** answer", "")
	if splitMessage(short)[0] != short {
		t.Fatal("rewrote ordinary answer")
	}
}

func TestLargePresentationEditFitsAndKeepsCompleteNewContent(t *testing.T) {
	content := renderMessage(source.Message{Role: "assistant"}, "**"+strings.Repeat("x", 15000)+"**", "")
	edit := presentationEdit(content, "$original")
	assertFitsEncryptedEvent(t, edit)
	if edit.RelatesTo.EventID != "$original" || edit.NewContent.Body != content.Body || edit.NewContent.FormattedBody != content.FormattedBody {
		t.Fatal("lost replacement text or target")
	}
}

func TestFrameworkMultipartResumesGapsAndDeduplicatesAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bridge.db")
	mx := &matrixFixture{names: map[id.RoomID]string{}, failAt: 2}
	br, c := startFixture(t, path, mx)
	chat := testChat()
	chat.Messages[1].Text = strings.Repeat("A long **answer** with 杭州.\n\n", 4000)
	chat.Messages = append(chat.Messages, source.Message{ID: "u2", Role: "user", Text: "Today's follow-up"}, source.Message{ID: "a2", Role: "assistant", Text: "Today's answer"})
	ctx := context.Background()
	if err := c.dispatch(ctx, chat); err == nil {
		t.Fatal("part failure not surfaced")
	}
	if mx.messages != 2 {
		t.Fatal("advanced beyond failed part")
	}
	firstPath := mx.paths[1]
	br.Stop()
	br, c = startFixture(t, path, mx)
	defer br.Stop()
	feedFixture(t, br, c, chat)
	expected := len(c.events(chat)) - 1
	if mx.rooms != 1 || mx.messages != expected {
		t.Fatalf("missing or duplicated parts: got %d want %d", mx.messages, expected)
	}
	if mx.contents[len(mx.contents)-1].Body != "Today's answer" {
		t.Fatal("missing current continuation")
	}
	for _, content := range mx.contents {
		assertFitsEncryptedEvent(t, content)
	}
	messageID := c.events(chat)[2].(*sourceMessage).ID
	parts, err := br.DB.Message.GetAllPartsByID(ctx, c.login.ID, messageID)
	if err != nil || len(parts) < 2 {
		t.Fatalf("parts not mapped to source identity: %v", err)
	}
	firstID := parts[0].MXID
	// Accepted by Matrix, but its local association was lost. Retry exactly that
	// part under its existing transaction, without hiding the remaining gap.
	if err = br.DB.Message.Delete(ctx, parts[1].RowID); err != nil {
		t.Fatal(err)
	}
	feedFixture(t, br, c, chat)
	if mx.messages != expected {
		t.Fatal("lost-commit retry duplicated a part")
	}
	parts, _ = br.DB.Message.GetAllPartsByID(ctx, c.login.ID, messageID)
	if parts[0].MXID != firstID || mx.paths[1] != firstPath {
		t.Fatal("first event identity changed")
	}
	feedFixture(t, br, c, chat)
	if mx.messages != expected {
		t.Fatal("repeat duplicated content")
	}
	chat.Messages[1].Text += "Edited source"
	if err = c.dispatch(ctx, chat); err == nil {
		t.Fatal("multipart bypassed source edit detection")
	}
}

func TestRecoveredWholeOperatorMessageDoesNotEchoSplitParts(t *testing.T) {
	chat := testChat()
	chat.Messages = chat.Messages[:1]
	chat.Messages[0].Text = strings.Repeat("<", 11000)
	events := testClient().events(chat)
	if len(events) < 3 {
		t.Fatal("fixture not oversized after JSON escaping")
	}
	existing := []*database.Message{{Metadata: &MessageMetadata{Hash: source.StableID("user", chat.Messages[0].Text)}}}
	for _, e := range events[1:] {
		res, err := e.(*sourceMessage).HandleExisting(context.Background(), nil, nil, existing)
		if err != nil || res.ContinueMessageHandling {
			t.Fatal("echoed existing outbound message")
		}
	}
}
