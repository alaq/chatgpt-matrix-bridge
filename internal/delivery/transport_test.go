package delivery

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRetryAfterAcceptanceUsesSameTransaction(t *testing.T) {
	seen := map[string]string{}
	accepted := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/m.room.encrypted/chatgpt_") {
			t.Error("transaction not replaced")
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != "ciphertext" || r.Header.Get("Authorization") != "Bearer fixture" {
			t.Error("payload/header changed")
		}
		if _, ok := seen[r.URL.Path]; !ok {
			accepted++
			seen[r.URL.Path] = "$accepted"
		}
		_, _ = w.Write([]byte(seen[r.URL.Path]))
	}))
	defer server.Close()
	for _, randomTxn := range []string{"first-process", "restarted-process"} {
		// Recreating the client simulates losing the first response before a local
		// commit. Only the remote transaction cache persists between attempts.
		client := &http.Client{Transport: Transport{}}
		req, _ := http.NewRequestWithContext(WithMessage(context.Background(), "source-message"), "PUT", server.URL+"/_hungryserv/owner/_matrix/client/v3/rooms/!room:test/send/m.room.encrypted/"+randomTxn, strings.NewReader("ciphertext"))
		req.Header.Set("Authorization", "Bearer fixture")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(body) != "$accepted" {
			t.Fatal("wrong event")
		}
		if !strings.HasSuffix(req.URL.Path, randomTxn) {
			t.Fatal("original request mutated")
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted %d messages", accepted)
	}
}

type captureTransport struct{ path string }

func (c *captureTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c.path = r.URL.EscapedPath()
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(""))}, nil
}

func TestUnrelatedRequestsUnchangedAndIdentityIsolated(t *testing.T) {
	for _, path := range []string{
		"/_matrix/client/v3/rooms/!room:test/state/m.room.name/", "/_matrix/client/v3/createRoom",
		"/_matrix/client/v3/rooms/!room:test/send/m.reaction/random",
		"/_matrix/client/v3/rooms/!room:test/redact/$event/random",
	} {
		c := &captureTransport{}
		r, _ := http.NewRequestWithContext(WithMessage(context.Background(), "source"), "PUT", "https://test.invalid"+path, nil)
		_, _ = (Transport{Base: c}).RoundTrip(r)
		if c.path != r.URL.EscapedPath() {
			t.Fatalf("changed unrelated %s", path)
		}
	}
	paths := map[string]bool{}
	for _, key := range []string{"a", "b"} {
		for _, room := range []string{"!room:test", "!other:test"} {
			c := &captureTransport{}
			r, _ := http.NewRequestWithContext(WithMessage(context.Background(), key), "PUT", "https://test.invalid/_matrix/client/v3/rooms/"+room+"/send/m.room.message/random", nil)
			_, _ = (Transport{Base: c}).RoundTrip(r)
			if paths[c.path] {
				t.Fatal("identity collision")
			}
			paths[c.path] = true
		}
	}
	c := &captureTransport{}
	r, _ := http.NewRequest("PUT", "https://test.invalid/_matrix/client/v3/rooms/!room:test/send/m.room.message/random", nil)
	_, _ = (Transport{Base: c}).RoundTrip(r)
	if c.path != r.URL.EscapedPath() {
		t.Fatal("unkeyed bot message changed")
	}
}
