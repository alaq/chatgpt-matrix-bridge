package connector

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alaq/chatgpt-matrix-bridge/internal/delivery"
	"github.com/alaq/chatgpt-matrix-bridge/internal/source"
	"go.mau.fi/util/configupgrade"
	"go.mau.fi/util/ptr"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
)

type Connector struct {
	br     *bridgev2.Bridge
	Config Config
	icon   atomic.Value // id.ContentURIString; GetName is also called during startup.
}

var _ bridgev2.NetworkConnector = (*Connector)(nil)

func (c *Connector) Init(br *bridgev2.Bridge) {
	bridgev2.PortalEventBuffer = 0
	c.br = br
}
func (c *Connector) Start(ctx context.Context) error {
	if c.br.Config.AsyncEvents || !c.br.Config.SplitPortals {
		return errors.New("this bridge requires bridge.async_events=false and bridge.split_portals=true")
	}
	if err := c.Config.validate(); err != nil {
		return err
	}
	return c.initializeIcon(ctx)
}
func (c *Connector) GetBridgeInfoVersion() (int, int) { return 2, 1 }
func (c *Connector) GetCapabilities() *bridgev2.NetworkGeneralCapabilities {
	return &bridgev2.NetworkGeneralCapabilities{}
}
func (c *Connector) GetName() bridgev2.BridgeName {
	return bridgev2.BridgeName{DisplayName: "ChatGPT", NetworkURL: "https://chatgpt.com", NetworkIcon: c.iconURI(), NetworkID: "chatgpt", BeeperBridgeType: "github.com/alaq/chatgpt-matrix-bridge", DefaultPort: 29345}
}
func (c *Connector) GetConfig() (string, any, configupgrade.Upgrader) {
	return exampleConfig, &c.Config, configupgrade.SimpleUpgrader(upgradeConfig)
}
func (c *Connector) GetDBMetaTypes() database.MetaTypes {
	return database.MetaTypes{UserLogin: func() any { return &LoginMetadata{} }, Message: func() any { return &MessageMetadata{} }}
}

type LoginMetadata struct {
	AccountKey string  `json:"account_key"`
	Since      float64 `json:"since"`
}
type MessageMetadata struct {
	Hash             string `json:"hash"`
	PresentationHash string `json:"presentation_hash,omitempty"`
}

func (c *Connector) LoadUserLogin(_ context.Context, login *bridgev2.UserLogin) error {
	meta, ok := login.Metadata.(*LoginMetadata)
	if !ok || string(login.ID) != "chatgpt_"+meta.AccountKey || meta.Since <= 0 {
		return errors.New("invalid saved login identity")
	}
	login.Client = &Client{login: login, connector: c, backend: c.Config.Backend(), chats: map[string]source.Conversation{}}
	return nil
}

type Client struct {
	login     *bridgev2.UserLogin
	connector *Connector
	backend   source.Backend
	connected atomic.Bool
	pollMu    sync.Mutex
	lifecycle sync.Mutex
	cancel    context.CancelFunc
	cacheMu   sync.RWMutex
	chats     map[string]source.Conversation
	sendFunc  func(context.Context, source.SendRequest) (*source.SendResult, error)
}

var _ bridgev2.NetworkAPI = (*Client)(nil)

func (c *Client) Connect(ctx context.Context) {
	c.lifecycle.Lock()
	defer c.lifecycle.Unlock()
	if c.cancel != nil {
		return
	}
	ctx, c.cancel = context.WithCancel(ctx)
	go func() {
		failures := 0
		for {
			if err := c.poll(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				failures++
				c.connected.Store(false)
				c.login.Log.Warn().Err(err).Msg("ChatGPT source synchronization paused")
				c.login.BridgeState.Send(status.BridgeState{StateEvent: status.StateTransientDisconnect, Error: "chatgpt-source-unavailable", Message: "Source refresh is unavailable. Showing the last saved history; retries will back off automatically."})
			} else {
				failures = 0
				c.connected.Store(true)
				c.login.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnected})
			}
			timer := time.NewTimer(nextPollDelay(time.Duration(c.connector.Config.PollSeconds)*time.Second, failures))
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
}
func (c *Client) Disconnect() {
	c.lifecycle.Lock()
	defer c.lifecycle.Unlock()
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
	c.connected.Store(false)
}
func (c *Client) IsLoggedIn() bool                                       { return c.connected.Load() }
func (c *Client) LogoutRemote(context.Context)                           { c.Disconnect() }
func (c *Client) IsThisUser(_ context.Context, id networkid.UserID) bool { return id == c.userID() }
func (c *Client) userID() networkid.UserID                               { return networkid.UserID(string(c.login.ID) + "_user") }
func (c *Client) assistantID() networkid.UserID {
	return networkid.UserID(string(c.login.ID) + "_assistant")
}

func (c *Client) poll(ctx context.Context) (result error) {
	c.pollMu.Lock()
	defer c.pollMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	refreshErr := c.backend.Refresh(ctx)
	// The account-bound archive remains useful while the source is unavailable.
	// Finish local delivery/presentation updates, but report the refresh failure
	// and back off instead of claiming that the source is synchronized.
	defer func() { result = errors.Join(refreshErr, result) }()
	return c.deliverArchive(ctx)
}

func (c *Client) deliverArchive(ctx context.Context) error {
	snapshot, err := c.backend.Read(ctx)
	if err != nil {
		return err
	}
	meta := c.login.Metadata.(*LoginMetadata)
	chats, err := source.Select(snapshot, meta.AccountKey, meta.Since)
	if err != nil {
		return err
	}
	c.cacheMu.Lock()
	for _, chat := range chats {
		c.chats[source.PortalID(meta.AccountKey, chat.ID)] = chat
	}
	c.cacheMu.Unlock()
	var failures []error
	for _, chat := range chats {
		if !c.connector.Config.allows(chat.ID) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.dispatch(ctx, chat); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func nextPollDelay(normal time.Duration, failures int) time.Duration {
	if failures == 0 {
		return normal
	}
	delay := max(normal, time.Minute)
	for i := 1; i < failures && delay < 15*time.Minute; i++ {
		delay *= 2
	}
	return max(normal, min(delay, 15*time.Minute))
}

func (c *Client) dispatch(ctx context.Context, chat source.Conversation) error {
	for _, evt := range c.events(chat) {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Propagate poll cancellation into synchronous Matrix delivery.
		switch e := evt.(type) {
		case *simplevent.ChatResync:
			e.MutateContextFunc = func(context.Context) context.Context { return ctx }
		case *simplevent.PreConvertedMessage:
			e.MutateContextFunc = func(context.Context) context.Context { return delivery.WithMessage(ctx, string(e.ID)) }
		case *sourceMessage:
			e.MutateContextFunc = func(context.Context) context.Context { return delivery.WithMessage(ctx, string(e.ID)) }
		}
		result := c.login.QueueRemoteEvent(evt)
		if !result.Success {
			return errors.New("Matrix delivery failed; this conversation will be retried")
		}
	}
	return nil
}

func (c *Client) GetChatInfo(_ context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	c.cacheMu.RLock()
	chat, ok := c.chats[string(portal.ID)]
	c.cacheMu.RUnlock()
	if !ok {
		return nil, errors.New("conversation is not in the current verified source feed")
	}
	return c.chatInfo(chat), nil
}
func (c *Client) GetUserInfo(_ context.Context, ghost *bridgev2.Ghost) (*bridgev2.UserInfo, error) {
	name := "ChatGPT"
	if ghost.ID == c.userID() {
		name = "You (ChatGPT)"
	} else if ghost.ID != c.assistantID() {
		return nil, errors.New("unknown ChatGPT sender")
	}
	info := &bridgev2.UserInfo{Name: ptr.Ptr(name)}
	if ghost.ID == c.assistantID() {
		info.Avatar = c.connector.avatar()
	}
	return info, nil
}
func (c *Client) GetCapabilities(context.Context, *bridgev2.Portal) *event.RoomFeatures {
	if c.connector.Config.SendEnabled {
		return &event.RoomFeatures{ID: "chatgpt-saved-text-v1", MaxTextLength: 12000, Edit: event.CapLevelRejected, Delete: event.CapLevelRejected, Thread: event.CapLevelRejected, Reply: event.CapLevelRejected}
	}
	return &event.RoomFeatures{ID: "chatgpt-readonly-v1"}
}
