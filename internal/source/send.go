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

type SendRequest struct {
	Version        int    `json:"version"`
	AccountKey     string `json:"accountKey"`
	ConversationID string `json:"conversationId"`
	TransactionID  string `json:"transactionId"`
	Text           string `json:"text"`
}
type SendResult struct {
	Version       int    `json:"version"`
	Status        string `json:"status"`
	UserMessageID string `json:"userMessageId"`
	Error         string `json:"error"`
}

func (r SendRequest) Validate() error {
	if r.Version != 1 || !accountPattern.MatchString(r.AccountKey) || !accountPattern.MatchString(r.TransactionID) || !idPattern.MatchString(r.ConversationID) || strings.TrimSpace(r.Text) == "" || len(r.Text) > 12000 || strings.ContainsRune(r.Text, 0) {
		return errors.New("invalid saved-send request")
	}
	return nil
}
func (b Backend) Send(ctx context.Context, r SendRequest) (*SendResult, error) {
	if err := b.Validate(); err != nil {
		return nil, err
	}
	if err := r.Validate(); err != nil {
		return nil, err
	}
	if b.Descriptor == "" {
		return nil, errors.New("saved sending requires an explicit DEV descriptor")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	data, _ := json.Marshal(r)
	cmd := exec.CommandContext(ctx, b.Python, filepath.Join(b.Directory, "scripts/history/send.py"), "--descriptor", b.Descriptor)
	cmd.Dir = b.Directory
	cmd.Stdin = bytes.NewReader(data)
	stdout := &boundedBuffer{limit: 8192}
	cmd.Stdout = stdout
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		return nil, errors.New("saved-send transport failed; reconcile the original transaction before retrying")
	}
	return DecodeSendResult(stdout.Bytes())
}
func DecodeSendResult(data []byte) (*SendResult, error) {
	var r SendResult
	if len(data) > 8192 || json.Unmarshal(data, &r) != nil || r.Version != 1 || (r.Status != "accepted" && r.Status != "not_sent" && r.Status != "uncertain") || (r.Status == "accepted" && !idPattern.MatchString(r.UserMessageID)) {
		return nil, errors.New("invalid saved-send receipt; delivery is uncertain")
	}
	return &r, nil
}
