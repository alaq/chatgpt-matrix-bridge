package connector

import (
	"context"
	"database/sql"
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
	c.registerCommands()
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
	Hash             string   `json:"hash"`
	PresentationHash string   `json:"presentation_hash,omitempty"`
	PartCount        int      `json:"part_count,omitempty"`
	EditGeneration   int      `json:"edit_generation,omitempty"`
	AttachmentIDs    []string `json:"attachment_ids,omitempty"`
}

func (c *Connector) LoadUserLogin(_ context.Context, login *bridgev2.UserLogin) error {
	meta, ok := login.Metadata.(*LoginMetadata)
	if !ok || string(login.ID) != "chatgpt_"+meta.AccountKey || meta.Since <= 0 {
		return errors.New("invalid saved login identity")
	}
	client := &Client{login: login, connector: c, backend: c.Config.Backend(), chats: map[string]source.Conversation{}, refreshRequested: make(chan struct{}, 1)}
	client.loginValid.Store(true)
	login.Client = client
	return nil
}

type Client struct {
	health           syncHealth
	login            *bridgev2.UserLogin
	connector        *Connector
	backend          source.Backend
	connected        atomic.Bool
	loginValid       atomic.Bool
	pollMu           sync.Mutex
	lifecycle        sync.Mutex
	cancel           context.CancelFunc
	cacheMu          sync.RWMutex
	chats            map[string]source.Conversation
	refreshRequested chan struct{}
	sendFunc         func(context.Context, source.SendRequest) (*source.SendResult, error)
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
			case <-c.refreshRequested:
				timer.Stop()
			}
		}
	}()
}

func (c *Client) requestRefresh() {
	select {
	case c.refreshRequested <- struct{}{}:
	default:
	}
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

// A persisted collector login remains configured across transient sync failures.
// Every actual send independently verifies the fresh source account before any UI
// submission. Sync health must not make bridgev2 discard the owner's follow-up.
func (c *Client) IsLoggedIn() bool                                       { return c.loginValid.Load() }
func (c *Client) LogoutRemote(context.Context)                           { c.loginValid.Store(false); c.Disconnect() }
func (c *Client) IsThisUser(_ context.Context, id networkid.UserID) bool { return id == c.userID() }
func (c *Client) userID() networkid.UserID                               { return networkid.UserID(string(c.login.ID) + "_user") }
func (c *Client) assistantID() networkid.UserID {
	return networkid.UserID(string(c.login.ID) + "_assistant")
}
func (c *Client) sourceAssistantID(chat source.Conversation) networkid.UserID {
	if chat.Kind == "codex" {
		return networkid.UserID(string(c.login.ID) + "_codex")
	}
	return c.assistantID()
}

func (c *Client) poll(ctx context.Context) (result error) {
	c.pollMu.Lock()
	defer c.pollMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	c.health.mu.Lock()
	c.health.PendingAttachments = 0
	c.health.mu.Unlock()
	refreshErr := c.backend.Refresh(ctx)
	creationErr := c.recoverCreations(ctx)
	// The account-bound archive remains useful while the source is unavailable.
	// Finish local delivery/presentation updates, but report the refresh failure
	// and back off instead of claiming that the source is synchronized.
	defer func() {
		c.recordHealth(ctx, refreshErr, errors.Join(creationErr, result))
		result = errors.Join(refreshErr, creationErr, result)
	}()
	return c.deliverArchive(ctx)
}

func (c *Client) deliverArchive(ctx context.Context) error {
	meta := c.login.Metadata.(*LoginMetadata)
	var chats []source.Conversation
	var failures []error
	snapshot, err := c.backend.Read(ctx)
	if err == nil {
		chats, err = source.Select(snapshot, meta.AccountKey, meta.Since)
	}
	if err != nil {
		failures = append(failures, err)
	}
	local := c.connector.Config.LocalTasks()
	if local.Enabled() {
		// Persist source identity: a changed local store must never reuse old rooms.
		key := "chatgpt_local_source_" + meta.AccountKey
		identity := source.StableID("local-task-store-v1", local.Home)
		var saved string
		err = c.connector.br.DB.QueryRow(ctx, "SELECT value FROM kv_store WHERE bridge_id=$1 AND key=$2", c.connector.br.ID, key).Scan(&saved)
		if errors.Is(err, sql.ErrNoRows) {
			_, err = c.connector.br.DB.Exec(ctx, "INSERT INTO kv_store (bridge_id,key,value) VALUES ($1,$2,$3)", c.connector.br.ID, key, identity)
			saved = identity
		}
		if err == nil && saved != identity {
			err = errors.New("local task store changed; refusing to mix rooms")
		}
		if err == nil {
			var tasks []source.Conversation
			tasks, err = local.Read(ctx, meta.AccountKey)
			chats = append(chats, tasks...)
		}
		if err != nil {
			failures = append(failures, err)
		}
	}
	c.cacheMu.Lock()
	for _, chat := range chats {
		c.chats[source.PortalID(meta.AccountKey, chat.ID)] = chat
	}
	c.cacheMu.Unlock()
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
	events := c.events(chat)
	var branchKey string
	var branchData []byte
	if c.connector.Config.SyncEdits {
		var changed bool
		var err error
		branchKey, branchData, changed, err = c.branchState(ctx, chat)
		if err != nil {
			return err
		}
		if changed {
			meta := events[0].(*simplevent.ChatResync).EventMeta.WithType(bridgev2.RemoteEventMessage).WithSender(bridgev2.EventSender{Sender: c.sourceAssistantID(chat)})
			notice := &simplevent.PreConvertedMessage{EventMeta: meta, ID: networkid.MessageID(source.StableID("branch-change", branchKey, string(branchData))), Data: &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{Type: event.EventMessage, Content: &event.MessageEventContent{MsgType: event.MsgNotice, Body: "The active conversation branch changed in ChatGPT. Earlier messages remain as history; subsequent messages follow the selected branch.", Mentions: &event.Mentions{}}, DBMetadata: &MessageMetadata{Hash: source.StableID("branch-change", string(branchData))}}}}}
			events = append(events[:1], append([]bridgev2.RemoteEvent{notice}, events[1:]...)...)
		}
	}
	var mediaFailures []error
	for _, evt := range events {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Propagate poll cancellation into synchronous Matrix delivery.
		switch e := evt.(type) {
		case *simplevent.Message[source.Attachment]:
			e.MutateContextFunc = func(context.Context) context.Context { return delivery.WithMessage(ctx, string(e.ID)) }
		case *simplevent.ChatResync:
			e.MutateContextFunc = func(context.Context) context.Context { return ctx }
		case *simplevent.PreConvertedMessage:
			e.MutateContextFunc = func(context.Context) context.Context { return delivery.WithMessage(ctx, string(e.ID)) }
		case *sourceMessage:
			e.MutateContextFunc = func(context.Context) context.Context {
				key := string(e.ID)
				if part := e.Data.Parts[0].ID; part != "" {
					key = source.StableID("message-part-v1", key, string(part))
				}
				return delivery.WithMessage(ctx, key)
			}
		}
		result := c.login.QueueRemoteEvent(evt)
		if e, ok := evt.(*sourceMessage); ok && e.preError != nil {
			return e.preError
		}
		if !result.Success {
			if _, ok := evt.(*simplevent.Message[source.Attachment]); ok {
				mediaFailures = append(mediaFailures, errors.New("attachment delivery pending"))
				continue
			}
			return errors.New("Matrix delivery failed; this conversation will be retried")
		}
	}
	if len(mediaFailures) > 0 {
		c.health.mu.Lock()
		c.health.PendingAttachments += len(mediaFailures)
		c.health.mu.Unlock()
		c.login.Log.Warn().Int("attachments", len(mediaFailures)).Msg("Some source attachments remain unavailable; text synchronization continues")
	}
	if branchKey != "" {
		if err := c.saveBranch(ctx, branchKey, branchData); err != nil {
			return err
		}
	}
	if chat.Kind == "codex" || chat.Kind == "work" || chat.Running {
		c.observeTaskState(ctx, chat)
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
	} else if ghost.ID == c.sourceAssistantID(source.Conversation{Kind: "codex"}) {
		name = "Codex"
	} else if ghost.ID != c.assistantID() {
		return nil, errors.New("unknown ChatGPT sender")
	}
	info := &bridgev2.UserInfo{Name: ptr.Ptr(name)}
	if ghost.ID == c.assistantID() {
		info.Avatar = c.connector.avatar()
	}
	return info, nil
}
func (c *Client) GetCapabilities(_ context.Context, portal *bridgev2.Portal) *event.RoomFeatures {
	if c.connector.Config.SendEnabled {
		files := event.FileFeatureMap{}
		for _, typ := range []event.CapabilityMsgType{event.CapabilityMsgType("m.image"), event.CapabilityMsgType("m.file"), event.CapabilityMsgType("m.audio"), event.CapabilityMsgType("m.video")} {
			files[typ] = &event.FileFeatures{MimeTypes: map[string]event.CapabilitySupportLevel{"*/*": event.CapLevelPartialSupport}, Caption: event.CapLevelFullySupported, MaxCaptionLength: 12000, MaxSize: 20 << 20}
		}
		if portal != nil {
			c.cacheMu.RLock()
			chat := c.chats[string(portal.ID)]
			c.cacheMu.RUnlock()
			if chat.Kind == "codex" {
				if !c.connector.Config.CodexSendEnabled {
					return &event.RoomFeatures{ID: "codex-readonly-v1"}
				}
				files = nil
			}
		}
		return &event.RoomFeatures{ID: "chatgpt-saved-media-v2", File: files, MaxTextLength: 12000, Edit: event.CapLevelRejected, Delete: event.CapLevelRejected, DeleteForMe: true, Thread: event.CapLevelRejected, Reply: event.CapLevelRejected}
	}
	return &event.RoomFeatures{ID: "chatgpt-readonly-v1"}
}
