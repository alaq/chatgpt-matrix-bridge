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
	"maunium.net/go/mautrix/bridgev2/simplevent"
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
	return source.StableID("matrix-presentation-v1", string(encoded))
}

// Update only our assistant's presentation. Preserve the original source hash
// and Matrix event ID; a real source edit still fails closed. Operator messages
// include original Matrix sends and must never be rewritten by this migration.
func presentationUpsert(meta simplevent.EventMeta, messageID networkid.MessageID, role, rawBody, hash string, content *event.MessageEventContent) func(context.Context, *bridgev2.Portal, bridgev2.MatrixAPI, []*database.Message) (bridgev2.UpsertResult, error) {
	return func(_ context.Context, _ *bridgev2.Portal, _ bridgev2.MatrixAPI, existing []*database.Message) (bridgev2.UpsertResult, error) {
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
		updated := &MessageMetadata{Hash: hash, PresentationHash: renderHash}
		if old.PresentationHash == "" && content.Body == rawBody && content.Format == "" {
			existing[0].Metadata = updated
			return bridgev2.UpsertResult{SaveParts: true}, nil
		}
		edit := &simplevent.Message[*event.MessageEventContent]{
			EventMeta:     meta.WithType(bridgev2.RemoteEventEdit).WithTimestamp(time.Now()),
			TargetMessage: messageID,
			Data:          content,
			ConvertEditFunc: func(_ context.Context, _ *bridgev2.Portal, _ bridgev2.MatrixAPI, parts []*database.Message, rendered *event.MessageEventContent) (*bridgev2.ConvertedEdit, error) {
				if len(parts) != 1 {
					return nil, errors.New("presentation target changed")
				}
				// Copy metadata: a failed send must leave the old persisted rendering
				// eligible for retry. bridgev2 saves this only after Matrix accepts it.
				part := *parts[0]
				part.Metadata = updated
				copyContent := *rendered
				return &bridgev2.ConvertedEdit{ModifiedParts: []*bridgev2.ConvertedEditPart{{Part: &part, Type: event.EventMessage, Content: &copyContent}}}, nil
			},
		}
		edit.MutateContextFunc = func(ctx context.Context) context.Context {
			return delivery.WithMessage(ctx, source.StableID("presentation", string(messageID), renderHash))
		}
		return bridgev2.UpsertResult{SubEvents: []bridgev2.RemoteEvent{edit}}, nil
	}
}
