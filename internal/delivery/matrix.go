package delivery

import (
	"context"
	"net/http"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/id"
)

// Install runs before Matrix startup. Custom user intents may allocate a separate
// HTTP client, so wrapping only the appservice client would protect assistant
// messages while leaving mirrored user messages with random transactions.
func Install(br *bridgev2.Bridge, mx *matrix.Connector) {
	wrapHTTP(mx.AS.HTTPClient)
	br.Matrix = &matrixConnector{Connector: mx}
}
func wrapHTTP(client *http.Client) {
	if _, ok := client.Transport.(Transport); !ok {
		client.Transport = Transport{Base: client.Transport}
	}
}

type matrixConnector struct{ *matrix.Connector }

func (m *matrixConnector) NewUserIntent(ctx context.Context, user id.UserID, token string) (bridgev2.MatrixAPI, string, error) {
	intent, newToken, err := m.Connector.NewUserIntent(ctx, user, token)
	if concrete, ok := intent.(*matrix.ASIntent); ok && err == nil {
		wrapHTTP(concrete.Matrix.Client.Client)
	}
	return intent, newToken, err
}
