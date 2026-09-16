package source

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// LocalTasks uses the existing task store and desktop owner. It never creates a
// second agent process or changes a task's model, tools, or approval settings.
type LocalTasks struct {
	Python, Adapter, Home, Journal string
	Since                          float64
	MaxConversations               int
	AllowConversations             []string
}

func (b LocalTasks) Enabled() bool { return b.Home != "" }
func (b LocalTasks) Validate() error {
	if !b.Enabled() {
		return nil
	}
	if b.Python == "" || !filepath.IsAbs(b.Adapter) || !filepath.IsAbs(b.Home) || !filepath.IsAbs(b.Journal) || !finite(b.Since) {
		return errors.New("local tasks require absolute adapter/home/journal paths and an activation date")
	}
	return nil
}
func (b LocalTasks) run(ctx context.Context, op string, request *SendRequest, known map[string]Conversation) ([]byte, error) {
	if !b.Enabled() {
		return nil, errors.New("local tasks are disabled")
	}
	if err := b.Validate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	limit := b.MaxConversations
	if limit == 0 {
		limit = 10
	}
	argv := []string{b.Adapter, "--codex-home", b.Home, "--journal", b.Journal, "--since", strconv.FormatFloat(b.Since, 'f', 3, 64), "--max-conversations", strconv.Itoa(limit)}
	for _, id := range b.AllowConversations {
		argv = append(argv, "--allow-conversation", id)
	}
	ids := make([]string, 0, len(known))
	for id, chat := range known {
		if strings.HasPrefix(id, "codex:") && accountPattern.MatchString(chat.DeliveryFingerprint) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		argv = append(argv, "--known-conversation", id+"="+known[id].DeliveryFingerprint)
	}
	argv = append(argv, op)
	cmd := exec.CommandContext(ctx, b.Python, argv...)
	if request != nil {
		data, _ := json.Marshal(request)
		cmd.Stdin = bytes.NewReader(data)
	}
	out := &boundedBuffer{limit: MaxFeedBytes}
	cmd.Stdout = out
	diagnostic := &boundedBuffer{limit: 4096}
	cmd.Stderr = diagnostic
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("local task source unavailable (%s): %w", op, ctx.Err())
		}
		status := "process_failed"
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			status = "process_exit_" + strconv.Itoa(exitErr.ExitCode())
		} else if err.Error() == "source output limit exceeded" {
			status = "output_limit"
		}
		var errno syscall.Errno
		if errors.As(err, &errno) {
			status += " errno=" + strconv.Itoa(int(errno))
		}
		return nil, fmt.Errorf("local task source unavailable (%s, %s%s)", op, status, localTaskDiagnostic(diagnostic.Bytes(), op))
	}
	return out.Bytes(), nil
}

func localTaskDiagnostic(raw []byte, operation string) string {
	var diagnostic struct {
		Version   int    `json:"version"`
		Operation string `json:"operation"`
		Code      string `json:"code"`
		Line      int    `json:"line"`
		Errno     int    `json:"errno"`
	}
	if len(raw) > 4096 || json.Unmarshal(raw, &diagnostic) != nil || diagnostic.Version != 1 || diagnostic.Operation != operation || diagnostic.Line < 0 || diagnostic.Line > 10000 || diagnostic.Errno < 0 || diagnostic.Errno > 4096 {
		return ""
	}
	switch diagnostic.Code {
	case "catalog", "catalog_open", "catalog_busy", "catalog_readonly", "catalog_corrupt", "filesystem", "encoding", "missing_field", "record_shape", "invalid_value", "internal":
		return fmt.Sprintf(", code=%s line=%d errno=%d", diagnostic.Code, diagnostic.Line, diagnostic.Errno)
	default:
		return ""
	}
}
func (b LocalTasks) Read(ctx context.Context, account string, known map[string]Conversation) ([]Conversation, error) {
	data, err := b.run(ctx, "feed", nil, known)
	if err != nil {
		return nil, err
	}
	var chats []Conversation
	if json.Unmarshal(data, &chats) != nil {
		return nil, errors.New("invalid local task feed")
	}
	for _, c := range chats {
		if c.Kind != "codex" {
			return nil, errors.New("unexpected local task kind")
		}
	}
	s := Snapshot{Version: 1, Source: "chatgpt", AccountKey: account, Conversations: chats}
	if err := HydrateUnchanged(&s, known); err != nil {
		return nil, err
	}
	return Select(&s, account, b.Since)
}
func (b LocalTasks) Send(ctx context.Context, request SendRequest) (*SendResult, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	if !strings.HasPrefix(request.ConversationID, "codex:") || len(request.Attachments) > 0 {
		return nil, errors.New("local tasks support text replies only")
	}
	data, err := b.run(ctx, "send", &request, nil)
	if err != nil {
		return nil, err
	}
	return DecodeSendResult(data)
}
