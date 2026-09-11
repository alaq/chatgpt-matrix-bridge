package main

import (
	"context"
	"github.com/alaq/chatgpt-matrix-bridge/internal/delivery"
	"github.com/alaq/chatgpt-matrix-bridge/pkg/connector"
	"maunium.net/go/mautrix/bridgev2/matrix/mxmain"
	"syscall"
)

var Tag, Commit, BuildTime string

func main() {
	syscall.Umask(0o077)
	m := mxmain.BridgeMain{Name: "chatgpt-matrix-bridge", Description: "A Matrix bridge for saved ChatGPT conversations", URL: "https://github.com/alaq/chatgpt-matrix-bridge", Version: "0.1.0", Connector: &connector.Connector{}}
	m.InitVersion(Tag, Commit, BuildTime)
	m.PostInit = func() {
		delivery.Install(m.Bridge, m.Matrix)
	}
	m.PostStart = func() {
		if err := m.Connector.(*connector.Connector).AutoLogin(context.Background()); err != nil {
			m.Log.Error().Err(err).Msg("Configured operator login failed")
		}
	}
	m.Run()
}
