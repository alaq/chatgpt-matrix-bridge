package connector

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alaq/chatgpt-matrix-bridge/internal/source"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/format"
	"maunium.net/go/mautrix/id"
)

func TestWritingBlocksRenderAsDraftsWithoutMetadata(t *testing.T) {
	body := "Here is the suggested reply.\n\n:::writing{variant=\"chat_message\" id=\"12345\"}\nHello **there**.\n\nLet's meet tomorrow.\n:::\n\nAn explanation after the draft."
	c := renderMessage(source.Message{Role: "assistant"}, body, "")
	for _, want := range []string{"<strong>Draft message</strong>", "<blockquote>", "Hello <strong>there</strong>.", "<p>Let's meet tomorrow.</p>", "An explanation after the draft."} {
		if !strings.Contains(c.FormattedBody, want) {
			t.Fatalf("missing %q: %s", want, c.FormattedBody)
		}
	}
	for _, bad := range []string{":::writing", "variant=", "12345", ":::"} {
		if strings.Contains(c.Body+c.FormattedBody, bad) {
			t.Fatalf("writing metadata leaked: %s", c.FormattedBody)
		}
	}
	if strings.Index(c.FormattedBody, "</blockquote>") > strings.Index(c.FormattedBody, "An explanation") {
		t.Fatal("draft swallowed following explanation")
	}
	u := renderMessage(source.Message{Role: "user"}, body, "")
	if u.Body != body || u.Format != "" {
		t.Fatal("rewrote operator text")
	}
}

func TestWritingBlocksPreserveCodeAndIncompleteSyntax(t *testing.T) {
	block := ":::writing{variant=\"chat_message\" id=\"123\"}\nExample\n:::"
	for _, body := range []string{"```text\n" + block + "\n```", "~~~\n" + block + "\n~~~", "    " + strings.ReplaceAll(block, "\n", "\n    "), ":::writing{variant=\"chat_message\"}\nIncomplete", ":::other{key=\"value\"}\nUntouched\n:::"} {
		if got := renderWritingBlocks(body); got != body {
			t.Fatalf("rewrote literal/incomplete syntax: %q", got)
		}
	}
	body := ":::writing{variant=\"email\"}\nIntro\n\n```text\n:::\n```\n\nAfter code\n:::"
	c := renderMessage(source.Message{Role: "assistant"}, body, "")
	if !strings.Contains(c.FormattedBody, "<strong>Email draft</strong>") || !strings.Contains(c.FormattedBody, "<pre><code") || !strings.Contains(c.FormattedBody, ":::") || !strings.Contains(c.FormattedBody, "After code</p>\n</blockquote>") {
		t.Fatal(c.FormattedBody)
	}
}

func TestWritingBlocksHandleMultipleNestedAndUnsafeContent(t *testing.T) {
	body := ":::writing{variant=\"chat_message\"}\nFirst\n:::\n\n:::writing{variant=\"unknown\" id=\"hide-me\"}\n<script>no</script> [bad](javascript:alert(1))\n\n:::writing{variant=\"email\"}\nNested\n:::\n:::"
	c := renderMessage(source.Message{Role: "assistant"}, body, "")
	if strings.Count(c.FormattedBody, "<blockquote>") != 3 || strings.Contains(c.FormattedBody, "hide-me") || strings.Contains(c.FormattedBody, "<script>") || strings.Contains(c.FormattedBody, `href="javascript:`) || strings.Contains(c.FormattedBody, ":::writing") {
		t.Fatal(c.FormattedBody)
	}
}

func TestFrameworkUpgradesOnlyWritingPresentationOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bridge.db")
	mx := &matrixFixture{names: map[id.RoomID]string{}}
	br, c := startFixture(t, path, mx)
	chat := testChat()
	chat.Messages[1].Text = ":::writing{variant=\"chat_message\" id=\"12345\"}\nHello there.\n:::"
	chat.Messages = append(chat.Messages, source.Message{ID: "a2", Role: "assistant", Text: "Unchanged answer"})
	feedFixture(t, br, c, chat)
	messageID := c.events(chat)[2].(*sourceMessage).ID
	parts, err := br.DB.Message.GetAllPartsByID(context.Background(), c.login.ID, messageID)
	if err != nil || len(parts) != 1 {
		t.Fatalf("get original: %v", err)
	}
	originalID := parts[0].MXID
	rawHash := parts[0].Metadata.(*MessageMetadata).Hash
	// Represent the prior renderer's literal writing markers in persisted state.
	old := format.RenderMarkdownCustom(chat.Messages[1].Text, markdownRenderer)
	old.Mentions = &event.Mentions{}
	parts[0].Metadata.(*MessageMetadata).PresentationHash = presentationHash(&old)
	if err := br.DB.Message.Update(context.Background(), parts[0]); err != nil {
		t.Fatal(err)
	}
	feedFixture(t, br, c, chat, chat)
	if mx.messages != 4 || mx.rooms != 1 {
		t.Fatalf("expected one presentation edit: messages=%d rooms=%d", mx.messages, mx.rooms)
	}
	edit := mx.contents[len(mx.contents)-1]
	if edit.NewContent == nil || edit.RelatesTo.EventID != originalID || !strings.Contains(edit.NewContent.FormattedBody, "<blockquote>") || strings.Contains(edit.NewContent.Body+edit.NewContent.FormattedBody, ":::writing") {
		t.Fatal("writing draft did not replace the original presentation")
	}
	br.Stop()
	br, c = startFixture(t, path, mx)
	defer br.Stop()
	feedFixture(t, br, c, chat)
	parts, err = br.DB.Message.GetAllPartsByID(context.Background(), c.login.ID, messageID)
	if err != nil || len(parts) != 1 || parts[0].MXID != originalID || parts[0].Metadata.(*MessageMetadata).Hash != rawHash || mx.messages != 4 {
		t.Fatal("restart changed source identity or repeated the presentation edit")
	}
}
