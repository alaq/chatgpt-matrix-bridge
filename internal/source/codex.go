package source

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// LocalTasks uses the existing task store and desktop owner. It never creates a
// second agent process or changes a task's model, tools, or approval settings.
type LocalTasks struct {
	Python, Adapter, Home, Journal string
	Since                          float64
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
func (b LocalTasks) run(ctx context.Context, op string, request *SendRequest) ([]byte, error) {
	if !b.Enabled() {
		return nil, errors.New("local tasks are disabled")
	}
	if err := b.Validate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, b.Python, b.Adapter, "--codex-home", b.Home, "--journal", b.Journal, "--since", strconv.FormatFloat(b.Since, 'f', 3, 64), op)
	if request != nil {
		data, _ := json.Marshal(request)
		cmd.Stdin = bytes.NewReader(data)
	}
	out := &boundedBuffer{limit: MaxFeedBytes}
	cmd.Stdout = out
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		return nil, errors.New("local task source unavailable")
	}
	return out.Bytes(), nil
}
func (b LocalTasks) Read(ctx context.Context, account string) ([]Conversation, error) {
	data, err := b.run(ctx, "feed", nil)
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
	return Select(&s, account, b.Since)
}
func (b LocalTasks) Send(ctx context.Context, request SendRequest) (*SendResult, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	if !strings.HasPrefix(request.ConversationID, "codex:") || len(request.Attachments) > 0 {
		return nil, errors.New("local tasks support text replies only")
	}
	data, err := b.run(ctx, "send", &request)
	if err != nil {
		return nil, err
	}
	return DecodeSendResult(data)
}
