// Package delivery gives replayed source messages stable Matrix transaction IDs.
package delivery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
)

type messageKey struct{}

// WithMessage must be applied only to a single-part source message event. Room
// creation, state, receipts and unrelated bot messages must not inherit this key.
func WithMessage(ctx context.Context, sourceMessageID string) context.Context {
	return context.WithValue(ctx, messageKey{}, sourceMessageID)
}

// Transport changes only the transaction segment of Matrix timeline PUTs.
// Encryption and signing remain entirely in mautrix; the body is never inspected.
// Matrix deduplicates transactions within the sender's access-token/device scope.
type Transport struct{ Base http.RoundTripper }

func (t Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}
	key, _ := req.Context().Value(messageKey{}).(string)
	parts := strings.Split(req.URL.EscapedPath(), "/")
	// Beeper prefixes Matrix URLs with /_hungryserv/<account>.
	if len(parts) > 9 {
		parts = parts[len(parts)-9:]
	}
	if key != "" && req.Method == http.MethodPut && len(parts) == 9 &&
		parts[1] == "_matrix" && parts[2] == "client" &&
		(parts[3] == "v3" || parts[3] == "r0") && parts[4] == "rooms" &&
		parts[6] == "send" && (parts[7] == "m.room.message" || parts[7] == "m.room.encrypted") {
		// Include room and source ID; the source ID already includes account and
		// conversation. Do not include body, timestamp or random encryption bytes.
		sum := sha256.Sum256([]byte("chatgpt-matrix-v1\x00" + parts[5] + "\x00" + key))
		txn := "chatgpt_" + hex.EncodeToString(sum[:])
		copyReq := req.Clone(req.Context())
		copyURL := *req.URL
		copyURL.Path = copyURL.Path[:strings.LastIndex(copyURL.Path, "/")+1] + txn
		if copyURL.RawPath != "" {
			copyURL.RawPath = copyURL.RawPath[:strings.LastIndex(copyURL.RawPath, "/")+1] + txn
		}
		copyReq.URL = &copyURL
		req = copyReq
	}
	return base.RoundTrip(req)
}
