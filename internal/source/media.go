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
	"time"
)

type Attachment struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	MimeType string `json:"mime_type"`
	Size     int    `json:"size"`
}
type Media struct {
	Version  int    `json:"version"`
	ID       string `json:"attachmentId"`
	Name     string `json:"name"`
	MimeType string `json:"mimeType"`
	Size     int    `json:"size"`
	SHA256   string `json:"sha256"`
	Data     string `json:"data"`
}

func (b Backend) Download(ctx context.Context, account, conversation, message string, attachment Attachment) (*Media, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	raw, _ := json.Marshal(map[string]string{"accountKey": account, "conversationId": conversation, "messageId": message, "attachmentId": attachment.ID})
	cmd := exec.CommandContext(ctx, b.Python, filepath.Join(b.Directory, "scripts/history/media.py"), "--descriptor", b.Descriptor)
	cmd.Stdin, cmd.Stderr = bytes.NewReader(raw), io.Discard
	out := &boundedBuffer{limit: 29 << 20}
	cmd.Stdout = out
	if err := cmd.Run(); err != nil {
		return nil, nil, errors.New("source attachment unavailable")
	}
	var media Media
	if json.Unmarshal(out.Bytes(), &media) != nil || media.Version != 1 || media.ID != attachment.ID || media.Size > 20<<20 || media.Size < 0 {
		return nil, nil, errors.New("invalid media envelope")
	}
	data, err := base64.StdEncoding.Strict().DecodeString(media.Data)
	hash := sha256.Sum256(data)
	if err != nil || len(data) != media.Size || hex.EncodeToString(hash[:]) != media.SHA256 {
		return nil, nil, errors.New("attachment content verification failed")
	}
	media.Data = ""
	return &media, data, nil
}
