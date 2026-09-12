package connector

import (
	"regexp"
	"sort"
	"strings"

	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/text"
)

var writingOpen = regexp.MustCompile(`^ {0,3}:::writing(?:\{[^\r\n]*\})?[ \t]*\r?$`)
var writingClose = regexp.MustCompile(`^ {0,3}:::[ \t]*\r?$`)
var writingVariant = regexp.MustCompile(`(?:^|[\s{])variant="([^"]*)"`)

// Convert complete writing containers into labelled blockquotes. Keep literal
// code examples and incomplete containers intact; attributes never become HTML.
func renderWritingBlocks(body string) string {
	if !strings.Contains(body, ":::writing") {
		return body
	}
	lines := strings.Split(body, "\n")
	starts := make([]int, len(lines))
	for i := 1; i < len(lines); i++ {
		starts[i] = starts[i-1] + len(lines[i-1]) + 1
	}
	code := make([]bool, len(lines))
	// Use Markdown's own code boundaries, including nested fenced examples.
	doc := markdownRenderer.Parser().Parse(text.NewReader([]byte(body)))
	_ = ast.Walk(doc, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		if !entering || (n.Kind() != ast.KindCodeBlock && n.Kind() != ast.KindFencedCodeBlock) {
			return ast.WalkContinue, nil
		}
		for i := 0; i < n.Lines().Len(); i++ {
			segment := n.Lines().At(i)
			line := sort.Search(len(starts), func(j int) bool { return starts[j] > segment.Start }) - 1
			for line >= 0 && line < len(lines) && starts[line] < segment.Stop {
				code[line] = true
				line++
			}
		}
		return ast.WalkSkipChildren, nil
	})
	var render func(int, int, int) string
	render = func(start, end, depth int) string {
		if depth >= 16 {
			return strings.Join(lines[start:end], "\n")
		}
		var out []string
		for i := start; i < end; i++ {
			if code[i] || !writingOpen.MatchString(lines[i]) {
				out = append(out, lines[i])
				continue
			}
			closing, nested := -1, 1
			for j := i + 1; j < end; j++ {
				if code[j] {
					continue
				}
				if writingOpen.MatchString(lines[j]) {
					nested++
				} else if writingClose.MatchString(lines[j]) {
					nested--
					if nested == 0 {
						closing = j
						break
					}
				}
			}
			if closing < 0 {
				out = append(out, lines[i:end]...)
				break
			}
			label := "Draft"
			if variant := writingVariant.FindStringSubmatch(lines[i]); len(variant) == 2 {
				switch variant[1] {
				case "chat_message":
					label = "Draft message"
				case "email":
					label = "Email draft"
				}
			}
			out = append(out, "", "**"+label+"**", "")
			for _, line := range strings.Split(render(i+1, closing, depth+1), "\n") {
				out = append(out, "> "+line)
			}
			out = append(out, "")
			i = closing
		}
		return strings.Join(out, "\n")
	}
	return render(0, len(lines), 0)
}
