package source

import "testing"

func TestSendReceiptRequiresKnownStatusAndSourceIdentity(t *testing.T) {
	for _, raw := range []string{`{}`, `{"version":1,"status":"ok"}`, `{"version":1,"status":"accepted","userMessageId":""}`, `{"version":1,"status":"accepted","userMessageId":"../../"}`} {
		if _, err := DecodeSendResult([]byte(raw)); err == nil {
			t.Fatal("invalid receipt accepted")
		}
	}
	if _, err := DecodeSendResult([]byte(`{"version":1,"status":"uncertain"}`)); err != nil {
		t.Fatal(err)
	}
}
