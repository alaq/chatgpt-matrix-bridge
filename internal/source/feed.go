// Package source consumes the versioned, visible-only feed from the shared backend.
package source

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const MaxFeedBytes = 64 << 20

var accountPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
var idPattern = regexp.MustCompile(`^[a-fA-F0-9]{8}(-[a-fA-F0-9]{4}){3}-[a-fA-F0-9]{12}$`)

type Message struct {
	ID              string          `json:"id"`
	Role            string          `json:"role"`
	Text            string          `json:"text"`
	CreatedAt       *float64        `json:"created_at"`
	AttachmentCount int             `json:"attachment_count"`
	Attachments     []Attachment    `json:"attachments,omitempty"`
	CitationGroups  []CitationGroup `json:"citation_groups,omitempty"`
}

type CitationSource struct {
	Title string `json:"title"`
	URL   string `json:"url"`
}

type CitationGroup struct {
	Marker  string           `json:"marker"`
	Sources []CitationSource `json:"sources"`
}

type Conversation struct {
	Kind      string    `json:"kind,omitempty"`
	Running   bool      `json:"running,omitempty"`
	ID        string    `json:"id"`
	Revision  string    `json:"revision"`
	Title     string    `json:"title"`
	URL       string    `json:"url"`
	CreatedAt float64   `json:"created_at"`
	UpdatedAt float64   `json:"updated_at"`
	Messages  []Message `json:"messages"`
}

type Snapshot struct {
	Version            int            `json:"version"`
	Source             string         `json:"source"`
	AccountKey         string         `json:"account_key"`
	CompletedWatermark *float64       `json:"completed_watermark"`
	Coverage           string         `json:"coverage"`
	Conversations      []Conversation `json:"conversations"`
}

func Decode(data []byte) (*Snapshot, error) {
	if len(data) > MaxFeedBytes {
		return nil, errors.New("source feed exceeds size limit")
	}
	var s Snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, errors.New("invalid source JSON")
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return &s, nil
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) && v > 0 }

func (s *Snapshot) Validate() error {
	if s.Version != 1 || s.Source != "chatgpt" || !accountPattern.MatchString(s.AccountKey) {
		return errors.New("unsupported source contract or account identity")
	}
	seen := map[string]bool{}
	for _, c := range s.Conversations {
		validID := idPattern.MatchString(c.ID) && c.URL == "https://chatgpt.com/c/"+c.ID && (c.Kind == "" || c.Kind == "chatgpt" || c.Kind == "work")
		if c.Kind == "codex" {
			validID = strings.HasPrefix(c.ID, "codex:") && idPattern.MatchString(strings.TrimPrefix(c.ID, "codex:")) && c.URL == "codex://threads/"+strings.TrimPrefix(c.ID, "codex:")
		}
		if !validID || seen[c.ID] || !accountPattern.MatchString(c.Revision) || !finite(c.CreatedAt) || !finite(c.UpdatedAt) {
			return errors.New("invalid or duplicate conversation identity")
		}
		seen[c.ID] = true
		messages := map[string]bool{}
		for _, m := range c.Messages {
			if m.ID == "" || messages[m.ID] || (m.Role != "user" && m.Role != "assistant") || m.AttachmentCount < 0 || (m.CreatedAt != nil && !finite(*m.CreatedAt)) {
				return errors.New("invalid or duplicate visible message")
			}
			messages[m.ID] = true
		}
	}
	return nil
}

// StableID is namespaced and length-safe: no title matching or delimiter ambiguity.
func StableID(parts ...string) string {
	data, _ := json.Marshal(parts)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func PortalID(account, conversation string) string {
	return "chatgpt_" + StableID("chatgpt", account, conversation)
}
func MessageID(account, conversation, message string) string {
	return "chatgpt_" + StableID("chatgpt", account, conversation, message)
}

// Select includes old conversations updated after activation, in stable update order.
// Replay is intentional: the Matrix framework's database owns delivery deduplication.
func Select(s *Snapshot, account string, since float64) ([]Conversation, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	if s.AccountKey != account {
		return nil, errors.New("source account changed; refusing to mix rooms")
	}
	if !finite(since) {
		return nil, errors.New("missing activation boundary")
	}
	var out []Conversation
	for _, c := range s.Conversations {
		if c.UpdatedAt >= since {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAt == out[j].UpdatedAt {
			return out[i].ID < out[j].ID
		}
		return out[i].UpdatedAt < out[j].UpdatedAt
	})
	return out, nil
}

type Backend struct {
	Python, Directory, Archive, Descriptor string
	MaxConversations                       int
	AllowConversations                     []string
}

func (b Backend) Validate() error {
	if b.Python == "" || !filepath.IsAbs(b.Directory) || !filepath.IsAbs(b.Archive) || (b.Descriptor != "" && !filepath.IsAbs(b.Descriptor)) {
		return errors.New("configure python and absolute backend/archive/descriptor paths")
	}
	return nil
}

type boundedBuffer struct {
	bytes.Buffer
	limit int
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if b.Len()+len(p) > b.limit {
		return 0, errors.New("source output limit exceeded")
	}
	return b.Buffer.Write(p)
}

func (b Backend) run(ctx context.Context, command string, args ...string) ([]byte, error) {
	if err := b.Validate(); err != nil {
		return nil, err
	}
	argv := []string{filepath.Join(b.Directory, "scripts/history/cli.py"), "--archive", b.Archive}
	if b.Descriptor != "" {
		argv = append(argv, "--descriptor", b.Descriptor)
	}
	argv = append(argv, command)
	argv = append(argv, args...)
	cmd := exec.CommandContext(ctx, b.Python, argv...)
	cmd.Dir = b.Directory
	stdout := &boundedBuffer{limit: MaxFeedBytes}
	cmd.Stdout = stdout
	// Raw errors can include source data or local credentials. Surface only an action code.
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("backend %s failed; check local source health", command)
	}
	return stdout.Bytes(), nil
}

func (b Backend) Read(ctx context.Context) (*Snapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	limit := b.MaxConversations
	if limit == 0 {
		limit = 10
	}
	args := []string{"--max-conversations", strconv.Itoa(limit)}
	for _, id := range b.AllowConversations {
		args = append(args, "--allow-conversation", id)
	}
	data, err := b.run(ctx, "feed", args...)
	if err != nil {
		return nil, err
	}
	return Decode(data)
}

func (b Backend) Refresh(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	_, err := b.run(ctx, "sync", "--max-batches", "5", "--bridge-progress")
	// A partial window exits nonzero. The next poll resumes the collector checkpoint.
	return err
}
