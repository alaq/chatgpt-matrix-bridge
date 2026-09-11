package connector

import (
	"errors"
	"github.com/alaq/chatgpt-matrix-bridge/internal/source"
	"go.mau.fi/util/configupgrade"
	"time"
)

type Config struct {
	Enabled     bool   `yaml:"enabled"`
	Python      string `yaml:"python"`
	BackendDir  string `yaml:"backend_dir"`
	ArchiveDir  string `yaml:"archive_dir"`
	Descriptor  string `yaml:"descriptor"`
	PollSeconds int    `yaml:"poll_seconds"`
	Since       string `yaml:"since"`
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
`

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
	return c.Backend().Validate()
}
func upgradeConfig(h configupgrade.Helper) {
	h.Copy(configupgrade.Bool, "enabled")
	for _, k := range []string{"python", "backend_dir", "archive_dir", "descriptor", "since"} {
		h.Copy(configupgrade.Str, k)
	}
	h.Copy(configupgrade.Int, "poll_seconds")
}
