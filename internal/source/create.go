package source

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type CreateRequest struct {
	Version       int    `json:"version"`
	AccountKey    string `json:"accountKey"`
	TransactionID string `json:"transactionId"`
	Text          string `json:"text"`
}
type CreateResult struct {
	Version        int    `json:"version"`
	Status         string `json:"status"`
	ConversationID string `json:"conversationId"`
	UserMessageID  string `json:"userMessageId"`
}

func (b Backend) Create(ctx context.Context, request CreateRequest) (*CreateResult, error) {
	if request.Version != 1 || !accountPattern.MatchString(request.AccountKey) || !accountPattern.MatchString(request.TransactionID) || strings.TrimSpace(request.Text) == "" || len(request.Text) > 12000 {
		return nil, errors.New("invalid creation request")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	raw, _ := json.Marshal(request)
	cmd := exec.CommandContext(ctx, b.Python, filepath.Join(b.Directory, "scripts/history/create.py"), "--descriptor", b.Descriptor)
	cmd.Stdin, cmd.Stderr = bytes.NewReader(raw), io.Discard
	out := &boundedBuffer{limit: 8192}
	cmd.Stdout = out
	if err := cmd.Run(); err != nil {
		return nil, errors.New("creation needs reconciliation")
	}
	var result CreateResult
	if json.Unmarshal(out.Bytes(), &result) != nil || result.Version != 1 || result.Status != "accepted" || !idPattern.MatchString(result.ConversationID) || !idPattern.MatchString(result.UserMessageID) {
		return nil, errors.New("creation needs reconciliation; original request retained")
	}
	return &result, nil
}
