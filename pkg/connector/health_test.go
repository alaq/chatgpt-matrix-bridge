package connector

import (
	"context"
	"encoding/json"
	"errors"
	"maunium.net/go/mautrix/id"
	"os"
	"path/filepath"
	"testing"
)

func TestHealthSeparatesSourceOutageFromDeliveryAndDoesNotExportContent(t *testing.T) {
	mx := &matrixFixture{names: map[id.RoomID]string{}}
	root := t.TempDir()
	br, c := startFixture(t, filepath.Join(root, "bridge.db"), mx)
	defer br.Stop()
	c.connector.Config.HealthPath = filepath.Join(root, "health.json")
	c.recordHealth(context.Background(), nil, nil)
	previous := c.health.LastSourceSuccess
	c.recordHealth(context.Background(), errors.New("private source diagnostic"), nil)
	data, err := os.ReadFile(c.connector.Config.HealthPath)
	if err != nil {
		t.Fatal(err)
	}
	var health map[string]any
	if json.Unmarshal(data, &health) != nil || health["source_available"] != false || health["delivery_available"] != true {
		t.Fatal("health conflated source and delivery")
	}
	if !c.health.LastSourceSuccess.Equal(previous) {
		t.Fatal("failure advanced success timestamp")
	}
	info, _ := os.Stat(c.connector.Config.HealthPath)
	if info.Mode().Perm() != 0600 {
		t.Fatal("health file is not private")
	}
}
