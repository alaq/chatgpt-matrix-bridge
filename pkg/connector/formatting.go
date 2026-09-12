package connector

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/alaq/chatgpt-matrix-bridge/internal/delivery"
	"github.com/alaq/chatgpt-matrix-bridge/internal/source"
	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/renderer/html"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/format"
	"maunium.net/go/mautrix/format/mdext"
)

var citationMarker = regexp.MustCompile("\uE200(?:cite|filecite)(?:\uE202[^\uE200-\uE2FF]+)+\uE201")

// Escape source HTML and keep Goldmark's unsafe URL filtering enabled. The
// framework's default Markdown renderer enables unsafe URLs along with HTML.
var markdownRenderer = goldmark.New(
	goldmark.WithExtensions(extension.Strikethrough, extension.Table, mdext.EscapeHTML),
	goldmark.WithRendererOptions(html.WithHardWraps()),
)

func safeWebURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && (u.Scheme == "https" || u.Scheme == "http") && u.Hostname() != "" && u.User == nil &&
		!strings.ContainsFunc(raw, func(r rune) bool { return unicode.IsControl(r) || unicode.IsSpace(r) })
}

func renderMessage(m source.Message, body, conversationURL string) *event.MessageEventContent {
	if m.Role != "assistant" {
		content := format.TextToContent(body)
		return &content
	}
	body = renderWritingBlocks(body)
	replacements := map[string]string{}
	for _, group := range m.CitationGroups {
		if citationMarker.FindString(group.Marker) != group.Marker || group.Marker == "" {
			continue
		}
		var links []string
		seen := map[string]bool{}
		for _, citation := range group.Sources {
			if !safeWebURL(citation.URL) || seen[citation.URL] {
				continue
			}
			seen[citation.URL] = true
			title := strings.Join(strings.Fields(citation.Title), " ")
			if title == "" {
				u, _ := url.Parse(citation.URL)
				title = u.Hostname()
			}
			links = append(links, format.MarkdownLink(title, citation.URL))
		}
		if len(links) > 0 {
			replacements[group.Marker] = "(" + strings.Join(links, ", ") + ")"
		}
	}
	body = citationMarker.ReplaceAllStringFunc(body, func(marker string) string {
		if replacement, ok := replacements[marker]; ok {
			return replacement
		}
		// Never invent a URL from internal turn IDs. Older/unsupported references
		// point to the original conversation instead of exposing broken glyphs.
		if safeWebURL(conversationURL) {
			return "(" + format.MarkdownLink("source in ChatGPT", conversationURL) + ")"
		}
		return "[source in ChatGPT]"
	})
	content := format.RenderMarkdownCustom(body, markdownRenderer)
	content.Mentions = &event.Mentions{}
	return &content
}

func presentationHash(content *event.MessageEventContent) string {
	encoded, _ := json.Marshal(content)
	return source.StableID("matrix-presentation-v2", string(encoded))
}

// Each upsert carries one part, keeping framework sends serialized and giving
// each part its own delivery transaction. A partial import resumes only gaps.
func messagePartUpsert(messageID networkid.MessageID, partID networkid.PartID, partCount int, role, rawBody, hash string, content *event.MessageEventContent) func(context.Context, *bridgev2.Portal, bridgev2.MatrixAPI, []*database.Message) (bridgev2.UpsertResult, error) {
	return func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message) (bridgev2.UpsertResult, error) {
		var target *database.Message
		for _, part := range existing {
			old, ok := part.Metadata.(*MessageMetadata)
			if !ok || old.Hash != hash {
				return bridgev2.UpsertResult{}, errors.New("source message edited; edit reconciliation is not implemented in this pilot")
			}
			if old.PartCount > 1 && old.PartCount != partCount {
				return bridgev2.UpsertResult{}, errors.New("message part layout changed; reconciliation is required")
			}
			// A recovered outgoing user message already contains the whole text.
			if role == "user" && part.PartID == "" && old.PartCount == 0 {
				return bridgev2.UpsertResult{}, nil
			}
			if part.PartID == partID {
				target = part
			}
		}
		if target == nil {
			return bridgev2.UpsertResult{ContinueMessageHandling: true}, nil
		}
		res, err := presentationUpsert(messageID, role, rawBody, hash, content)(ctx, portal, intent, []*database.Message{target})
		if err == nil && partCount > 1 && target.Metadata.(*MessageMetadata).PartCount != partCount {
			target.Metadata.(*MessageMetadata).PartCount = partCount
			res.SaveParts = true
		}
		return res, err
	}
}

// Update only our assistant's presentation. Preserve the original source hash
// and Matrix event ID; a real source edit still fails closed. Operator messages
// include original Matrix sends and must never be rewritten by this migration.
func presentationUpsert(messageID networkid.MessageID, role, rawBody, hash string, content *event.MessageEventContent) func(context.Context, *bridgev2.Portal, bridgev2.MatrixAPI, []*database.Message) (bridgev2.UpsertResult, error) {
	return func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message) (bridgev2.UpsertResult, error) {
		for _, part := range existing {
			old, ok := part.Metadata.(*MessageMetadata)
			if !ok || old.Hash != hash {
				return bridgev2.UpsertResult{}, errors.New("source message edited; edit reconciliation is not implemented in this pilot")
			}
		}
		if role != "assistant" {
			return bridgev2.UpsertResult{}, nil
		}
		if len(existing) != 1 {
			return bridgev2.UpsertResult{}, errors.New("presentation update requires exactly one existing part")
		}
		renderHash := presentationHash(content)
		old := existing[0].Metadata.(*MessageMetadata)
		if old.PresentationHash == renderHash {
			return bridgev2.UpsertResult{}, nil
		}
		updated := &MessageMetadata{Hash: hash, PresentationHash: renderHash, PartCount: old.PartCount}
		if old.PresentationHash == "" && content.Body == rawBody && content.Format == "" {
			existing[0].Metadata = updated
			return bridgev2.UpsertResult{SaveParts: true}, nil
		}
		if intent.GetMXID() != existing[0].SenderMXID || existing[0].Room != portal.PortalKey || existing[0].HasFakeMXID() {
			return bridgev2.UpsertResult{}, errors.New("presentation target sender or room mismatch")
		}
		copyContent := presentationEdit(content, existing[0].MXID)
		// Nested bridgev2 subevents do not apply their context mutator. Send the
		// replacement here, inside the serialized portal handler, with an explicit
		// transaction key distinct from the original. Encryption stays in MatrixAPI.
		editKey := source.StableID("presentation", string(messageID), renderHash)
		if existing[0].PartID != "" {
			editKey = source.StableID("presentation-part-v1", string(messageID), string(existing[0].PartID), renderHash)
		}
		editCtx := delivery.WithMessage(ctx, editKey)
		_, err := intent.SendMessage(editCtx, portal.MXID, event.EventMessage, &event.Content{Parsed: copyContent}, &bridgev2.MatrixSendExtra{Timestamp: time.Now(), MessageMeta: existing[0]})
		if err != nil {
			return bridgev2.UpsertResult{}, err
		}
		// Only advance after acceptance. If the DB commit fails, the same edit
		// transaction is reused on the next poll, retaining the original event ID.
		existing[0].Metadata = updated
		return bridgev2.UpsertResult{SaveParts: true}, nil
	}
}
