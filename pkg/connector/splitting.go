package connector

import (
	"encoding/json"
	"fmt"
	"html"
	"strings"

	xhtml "golang.org/x/net/html"
	"golang.org/x/net/html/atom"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/format"
	"maunium.net/go/mautrix/id"
)

// Matrix limits encrypted events to 64 KiB. Budget the serialized body AND
// formatted_body, leaving room for Megolm/base64 expansion and bridge metadata.
const maxMessageContentBytes = 40 * 1024

func contentBytes(content *event.MessageEventContent) int {
	encoded, _ := json.Marshal(content)
	return len(encoded)
}

// Split only oversized content. Work on rendered HTML so citations, writing
// containers, tables and code blocks keep balanced markup in each part.
func splitMessage(content *event.MessageEventContent) []*event.MessageEventContent {
	if contentBytes(content) <= maxMessageContentBytes {
		return []*event.MessageEventContent{content}
	}
	if content.Format == event.FormatHTML {
		nodes, err := xhtml.ParseFragment(strings.NewReader(content.FormattedBody), &xhtml.Node{Type: xhtml.ElementNode, Data: "div", DataAtom: atom.Div})
		if err == nil {
			return splitHTML(nodes, "", "", 0)
		}
	}
	return splitPlain(content.Body)
}

func splitPlain(body string) []*event.MessageEventContent {
	content := format.TextToContent(body)
	if contentBytes(&content) <= maxMessageContentBytes {
		return []*event.MessageEventContent{&content}
	}
	text := []rune(body)
	mid := len(text) / 2
	return append(splitPlain(string(text[:mid])), splitPlain(string(text[mid:]))...)
}

func splitHTML(nodes []*xhtml.Node, prefix, suffix string, depth int) []*event.MessageEventContent {
	var rendered strings.Builder
	rendered.WriteString(prefix)
	for _, node := range nodes {
		_ = xhtml.Render(&rendered, node)
	}
	rendered.WriteString(suffix)
	content := format.HTMLToContent(rendered.String())
	content.Mentions = &event.Mentions{}
	if contentBytes(&content) <= maxMessageContentBytes {
		return []*event.MessageEventContent{&content}
	}
	if depth > 64 {
		return splitPlain(content.Body)
	}
	if len(prefix)+len(suffix) > maxMessageContentBytes/2 {
		return splitPlain(content.Body)
	}
	if len(nodes) > 1 {
		mid := len(nodes) / 2
		return append(splitHTML(nodes[:mid], prefix, suffix, depth+1), splitHTML(nodes[mid:], prefix, suffix, depth+1)...)
	}
	if len(nodes) == 1 {
		node := nodes[0]
		if node.Type == xhtml.TextNode && len([]rune(node.Data)) > 1 {
			text := []rune(node.Data)
			mid := len(text) / 2
			left := &xhtml.Node{Type: xhtml.TextNode, Data: string(text[:mid])}
			right := &xhtml.Node{Type: xhtml.TextNode, Data: string(text[mid:])}
			return append(splitHTML([]*xhtml.Node{left}, prefix, suffix, depth+1), splitHTML([]*xhtml.Node{right}, prefix, suffix, depth+1)...)
		}
		if node.Type == xhtml.ElementNode && node.FirstChild != nil {
			var opening strings.Builder
			fmt.Fprintf(&opening, "<%s", node.Data)
			for _, attr := range node.Attr {
				fmt.Fprintf(&opening, " %s=\"%s\"", attr.Key, html.EscapeString(attr.Val))
			}
			opening.WriteString(">")
			var children []*xhtml.Node
			for child := node.FirstChild; child != nil; child = child.NextSibling {
				children = append(children, child)
			}
			return splitHTML(children, prefix+opening.String(), "</"+node.Data+">"+suffix, depth+1)
		}
	}
	// An indivisible oversized attribute (for example an enormous URL) cannot
	// remain an HTML link. Preserve its readable body as bounded plain text.
	return splitPlain(content.Body)
}

func presentationEdit(content *event.MessageEventContent, target id.EventID) *event.MessageEventContent {
	copyContent := *content
	copyContent.SetEdit(target)
	if contentBytes(&copyContent) > maxMessageContentBytes {
		// Matrix edits duplicate the content in their legacy fallback. Keep the
		// complete formatted answer in m.new_content without doubling its size.
		copyContent.Body = "* Updated message"
		copyContent.Format = ""
		copyContent.FormattedBody = ""
	}
	return &copyContent
}
