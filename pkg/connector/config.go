package connector

import (
	"errors"
	"github.com/alaq/chatgpt-matrix-bridge/internal/source"
	"go.mau.fi/util/configupgrade"
	"path/filepath"
	"time"
)

type Config struct {
	Enabled            bool     `yaml:"enabled"`
	Python             string   `yaml:"python"`
	BackendDir         string   `yaml:"backend_dir"`
	ArchiveDir         string   `yaml:"archive_dir"`
	Descriptor         string   `yaml:"descriptor"`
	PollSeconds        int      `yaml:"poll_seconds"`
	Since              string   `yaml:"since"`
	AllowConversations []string `yaml:"allow_conversations"`
	SendEnabled        bool     `yaml:"send_enabled"`
	AutoLoginUser      string   `yaml:"auto_login_user"`
	HealthPath         string   `yaml:"health_path"`
	SyncEdits          bool     `yaml:"sync_edits"`
	SyncMedia          bool     `yaml:"sync_media"`
	MediaSince         string   `yaml:"media_since"`
	CodexHome          string   `yaml:"codex_home"`
	CodexAdapter       string   `yaml:"codex_adapter"`
	CodexJournal       string   `yaml:"codex_journal"`
	CodexSince         string   `yaml:"codex_since"`
	CodexSendEnabled   bool     `yaml:"codex_send_enabled"`
}

const exampleConfig = `# Enable only after configuring a dedicated private Matrix bridge.
enabled: false
python: python3
backend_dir: /absolute/path/to/codex-chatgpt-web
archive_dir: /absolute/path/to/private/archive
descriptor: /absolute/path/to/launcher-browser.json
poll_seconds: 60
# Empty starts with conversations updated after first login. Persisted across restarts.
# An explicit RFC3339 date opts into an older window at first login only.
since: ""
# Optional bounded pilot; empty discovers all eligible conversations.
allow_conversations: []
# Requires the separate saved-send capability in the DEV launcher.
send_enabled: false
# Optional single-operator bootstrap; must be allowed to log in by bridge.permissions.
auto_login_user: ""
# Optional private JSON status file. Contains timestamps/counts, never message text.
health_path: ""
# Reconcile source edits and label branch changes while preserving previous history.
sync_edits: true
# Download visible source attachments up to 20 MiB and upload them encrypted.
sync_media: true
# Optional attachment activation date; old messages keep their source links.
media_since: ""
# Optional local Work/Codex tasks. Separate activation avoids importing all history.
codex_home: ""
codex_adapter: ""
codex_journal: ""
codex_since: ""
# Experimental: the installed desktop app must expose an existing task owner.
codex_send_enabled: false
`

func (c Config) LocalTasks() source.LocalTasks {
	start, _ := time.Parse(time.RFC3339, c.CodexSince)
	return source.LocalTasks{Python: c.Python, Home: c.CodexHome, Adapter: c.CodexAdapter, Journal: c.CodexJournal, Since: float64(start.Unix())}
}

func (c Config) Backend() source.Backend {
	return source.Backend{Python: c.Python, Directory: c.BackendDir, Archive: c.ArchiveDir, Descriptor: c.Descriptor}
}
func (c Config) validate() error {
	if !c.Enabled {
		return errors.New("network.enabled is false; configure the bridge before enabling it")
	}
	if c.PollSeconds < 15 || c.PollSeconds > 3600 {
		return errors.New("poll_seconds must be between 15 and 3600")
	}
	if c.Since != "" {
		if _, err := time.Parse(time.RFC3339, c.Since); err != nil {
			return errors.New("since must be an RFC3339 timestamp")
		}
	}
	if c.HealthPath != "" && !filepath.IsAbs(c.HealthPath) {
		return errors.New("health_path must be absolute")
	}
	if c.MediaSince != "" {
		if _, err := time.Parse(time.RFC3339, c.MediaSince); err != nil {
			return errors.New("media_since must be RFC3339")
		}
	}
	if err := c.LocalTasks().Validate(); err != nil {
		return err
	}
	return c.Backend().Validate()
}
func upgradeConfig(h configupgrade.Helper) {
	h.Copy(configupgrade.Bool, "enabled")
	for _, k := range []string{"python", "backend_dir", "archive_dir", "descriptor", "since", "health_path", "codex_home", "codex_adapter", "codex_journal", "codex_since", "media_since"} {
		h.Copy(configupgrade.Str, k)
	}
	h.Copy(configupgrade.Int, "poll_seconds")
	h.Copy(configupgrade.Bool, "send_enabled")
	h.Copy(configupgrade.Bool, "sync_edits")
	h.Copy(configupgrade.Bool, "sync_media")
	h.Copy(configupgrade.Bool, "codex_send_enabled")
	h.Copy(configupgrade.Str, "auto_login_user")
	h.Copy(configupgrade.List, "allow_conversations")
}

func (c Config) allows(id string) bool {
	if len(c.AllowConversations) == 0 {
		return true
	}
	for _, candidate := range c.AllowConversations {
		if candidate == id {
			return true
		}
	}
	return false
}
