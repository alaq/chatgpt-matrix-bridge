package source

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type SendRequest struct {
	Version        int              `json:"version"`
	AccountKey     string           `json:"accountKey"`
	ConversationID string           `json:"conversationId"`
	TransactionID  string           `json:"transactionId"`
	Text           string           `json:"text"`
	Attachments    []SendAttachment `json:"attachments,omitempty"`
}
type SendAttachment struct {
	Name     string `json:"name"`
	MimeType string `json:"mimeType"`
	SHA256   string `json:"sha256"`
	Data     string `json:"data"`
}
type SendResult struct {
	Version       int      `json:"version"`
	Status        string   `json:"status"`
	UserMessageID string   `json:"userMessageId"`
	Error         string   `json:"error"`
	AttachmentIDs []string `json:"attachmentIDs,omitempty"`
}

func (r SendRequest) Validate() error {
	validID := idPattern.MatchString(r.ConversationID) || strings.HasPrefix(r.ConversationID, "codex:") && idPattern.MatchString(strings.TrimPrefix(r.ConversationID, "codex:"))
	if r.Version != 1 || !accountPattern.MatchString(r.AccountKey) || !accountPattern.MatchString(r.TransactionID) || !validID || strings.TrimSpace(r.Text) == "" && len(r.Attachments) == 0 || len(r.Text) > 12000 || strings.ContainsRune(r.Text, 0) || len(r.Attachments) > 1 {
		return errors.New("invalid saved-send request")
	}
	for _, a := range r.Attachments {
		if a.Name == "" || len(a.Name) > 240 || strings.ContainsAny(a.Name, "/\\\x00\r\n") || !accountPattern.MatchString(a.SHA256) || len(a.Data) > 28<<20 {
			return errors.New("invalid send attachment")
		}
		data, err := base64.StdEncoding.Strict().DecodeString(a.Data)
		hash := sha256.Sum256(data)
		if err != nil || len(data) == 0 || len(data) > 20<<20 || hex.EncodeToString(hash[:]) != a.SHA256 {
			return errors.New("invalid attachment content")
		}
	}
	return nil
}
func (b Backend) Send(ctx context.Context, r SendRequest) (*SendResult, error) {
	if !idPattern.MatchString(r.ConversationID) {
		return nil, errors.New("saved browser sender requires a ChatGPT conversation")
	}
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
