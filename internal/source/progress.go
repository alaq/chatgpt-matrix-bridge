package source

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os/exec"
	"path/filepath"
	"time"
)

func (b Backend) SendStatus(ctx context.Context, account, conversation string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	raw, _ := json.Marshal(map[string]string{"accountKey": account, "conversationId": conversation})
	cmd := exec.CommandContext(ctx, b.Python, filepath.Join(b.Directory, "scripts/history/send_status.py"), "--descriptor", b.Descriptor)
	cmd.Stdin, cmd.Stderr = bytes.NewReader(raw), io.Discard
	out := &boundedBuffer{limit: 2048}
	cmd.Stdout = out
	if err := cmd.Run(); err != nil {
		return "", errors.New("send progress unavailable")
	}
	var state struct {
		Version int    `json:"version"`
		Phase   string `json:"phase"`
	}
	if json.Unmarshal(out.Bytes(), &state) != nil || state.Version != 1 {
		return "", errors.New("invalid send progress")
	}
	switch state.Phase {
	case "idle", "preparing", "accepted", "generating", "complete", "needs_recovery":
		return state.Phase, nil
	}
	return "", errors.New("unknown send progress")
}
