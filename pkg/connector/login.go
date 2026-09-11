package connector

import (
	"context"
	"errors"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
	"time"
)

func (c *Connector) AutoLogin(ctx context.Context) error {
	if c.Config.AutoLoginUser == "" {
		return nil
	}
	user, err := c.br.GetUserByMXID(ctx, id.UserID(c.Config.AutoLoginUser))
	if err != nil || user == nil || !user.Permissions.Login {
		return errors.New("auto_login_user must have explicit login permission")
	}
	_, err = (&loginProcess{connector: c, user: user}).Start(ctx)
	return err
}

func (c *Connector) GetLoginFlows() []bridgev2.LoginFlow {
	return []bridgev2.LoginFlow{{ID: "local-collector", Name: "Local ChatGPT collector", Description: "Use the operator-configured, signed-in local collector"}}
}
func (c *Connector) CreateLogin(_ context.Context, user *bridgev2.User, flow string) (bridgev2.LoginProcess, error) {
	if flow != "local-collector" {
		return nil, errors.New("unsupported login flow")
	}
	return &loginProcess{connector: c, user: user}, nil
}

type loginProcess struct {
	connector *Connector
	user      *bridgev2.User
}

func (p *loginProcess) Cancel() {}
func (p *loginProcess) Start(ctx context.Context) (*bridgev2.LoginStep, error) {
	if err := p.connector.Config.validate(); err != nil {
		return nil, err
	}
	since := time.Now()
	if p.connector.Config.Since != "" {
		since, _ = time.Parse(time.RFC3339, p.connector.Config.Since)
	}
	backend := p.connector.Config.Backend()
	if err := backend.Refresh(ctx); err != nil {
		return nil, err
	}
	snapshot, err := backend.Read(ctx)
	if err != nil {
		return nil, err
	}
	id := networkid.UserLoginID("chatgpt_" + snapshot.AccountKey)
	// Relogin preserves the original activation boundary and never transfers ownership.
	if existing := p.connector.br.GetCachedUserLoginByID(id); existing != nil {
		if existing.UserMXID != p.user.MXID {
			return nil, errors.New("collector account already belongs to another Matrix user")
		}
		return p.complete(existing), nil
	}
	ul, err := p.user.NewLogin(ctx, &database.UserLogin{ID: id, RemoteName: "ChatGPT", Metadata: &LoginMetadata{AccountKey: snapshot.AccountKey, Since: float64(since.UnixMilli()) / 1000}}, nil)
	if err != nil {
		return nil, err
	}
	return p.complete(ul), nil
}

func (p *loginProcess) complete(ul *bridgev2.UserLogin) *bridgev2.LoginStep {
	ul.Client.Connect(p.connector.br.BackgroundCtx)
	return &bridgev2.LoginStep{Type: bridgev2.LoginStepTypeComplete, StepID: "chatgpt.local.complete", Instructions: "Connected to the local ChatGPT collector. New and updated conversations will be mirrored automatically.", CompleteParams: &bridgev2.LoginCompleteParams{UserLoginID: ul.ID, UserLogin: ul}}
}
