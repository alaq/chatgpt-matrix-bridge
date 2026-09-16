package connector

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/alaq/chatgpt-matrix-bridge/internal/source"
	"maunium.net/go/mautrix/bridgev2/status"
)

type syncSource int

const (
	remoteSource syncSource = iota
	localSource
)

func conversationSource(id string) syncSource {
	if strings.HasPrefix(id, "codex:") {
		return localSource
	}
	return remoteSource
}

func (c *Client) Connect(ctx context.Context) {
	c.lifecycle.Lock()
	defer c.lifecycle.Unlock()
	if c.cancel != nil {
		return
	}
	ctx, c.cancel = context.WithCancel(ctx)
	c.workers.Add(1)
	go c.syncLoop(ctx, time.Duration(c.connector.Config.PollSeconds)*time.Second, c.refreshRequested, c.poll)
	if c.connector.Config.LocalTasks().Enabled() {
		c.workers.Add(1)
		go c.syncLoop(ctx, c.connector.Config.localPollInterval(), c.localRefreshRequested, c.pollLocal)
	}
}

func (c *Client) syncLoop(ctx context.Context, interval time.Duration, wake <-chan struct{}, poll func(context.Context) error) {
	defer c.workers.Done()
	failures := 0
	for {
		err := poll(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			failures++
			c.login.Log.Warn().Err(err).Msg("Source synchronization paused; independent sources continue")
		} else {
			failures = 0
		}
		c.publishSyncState()
		timer := time.NewTimer(nextPollDelay(interval, failures))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (c *Client) publishSyncState() {
	c.health.mu.Lock()
	defer c.health.mu.Unlock()
	ready := sourceState(&c.health.ChatGPT, time.Duration(c.connector.Config.PollSeconds)*time.Second) == "connected"
	if c.connector.Config.LocalTasks().Enabled() {
		ready = ready && sourceState(c.health.source(localSource), c.connector.Config.localPollInterval()) == "connected"
	}
	c.connected.Store(ready)
	state := status.BridgeState{StateEvent: status.StateConnected}
	if !ready {
		state = status.BridgeState{StateEvent: status.StateTransientDisconnect, Error: "chatgpt-source-unavailable", Message: "A source is starting or needs recovery. Independent sources continue; use status for details."}
	}
	c.login.BridgeState.Send(state)
}

func requestWake(wake chan<- struct{}) {
	select {
	case wake <- struct{}{}:
	default:
	}
}

// Creation and remote generation completion only wake the ChatGPT collector.
func (c *Client) requestRefresh() { requestWake(c.refreshRequested) }

func (c *Client) requestConversationRefresh(id string) {
	if conversationSource(id) == localSource {
		requestWake(c.localRefreshRequested)
	} else {
		c.requestRefresh()
	}
}

func (c *Client) Disconnect() {
	c.lifecycle.Lock()
	defer c.lifecycle.Unlock()
	if c.cancel != nil {
		c.cancel()
		c.workers.Wait()
		c.cancel = nil
	}
	c.connected.Store(false)
}

// Only this lane performs remote refresh and creation/outbox recovery for ChatGPT.
// A failed refresh can still deliver the previously verified local archive.
func (c *Client) poll(ctx context.Context) (result error) {
	c.pollMu.Lock()
	defer c.pollMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	c.resetPendingAttachments(remoteSource)
	refreshErr := c.backend.Refresh(ctx)
	creationErr := c.recoverCreations(ctx)
	defer func() {
		sourceErr, deliveryErr := splitSyncFailures(result)
		if ctx.Err() == nil {
			c.recordSourceHealth(ctx, remoteSource, errors.Join(refreshErr, sourceErr), errors.Join(creationErr, deliveryErr))
		}
		result = errors.Join(refreshErr, creationErr, result)
	}()
	return c.deliverSource(ctx, remoteSource)
}

func (c *Client) pollLocal(ctx context.Context) (result error) {
	c.localPollMu.Lock()
	defer c.localPollMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if !c.connector.Config.LocalTasks().Enabled() {
		return nil
	}
	c.resetPendingAttachments(localSource)
	defer func() {
		sourceErr, deliveryErr := splitSyncFailures(result)
		if ctx.Err() == nil {
			c.recordSourceHealth(ctx, localSource, sourceErr, deliveryErr)
		}
	}()
	return c.deliverSource(ctx, localSource)
}

func (c *Client) readLocal(ctx context.Context, known map[string]source.Conversation) ([]source.Conversation, error) {
	meta := c.login.Metadata.(*LoginMetadata)
	local := c.connector.Config.LocalTasks()
	key := "chatgpt_local_source_" + meta.AccountKey
	identity := source.StableID("local-task-store-v1", local.Home)
	var saved string
	err := c.connector.br.DB.QueryRow(ctx, "SELECT value FROM kv_store WHERE bridge_id=$1 AND key=$2", c.connector.br.ID, key).Scan(&saved)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = c.connector.br.DB.Exec(ctx, "INSERT INTO kv_store (bridge_id,key,value) VALUES ($1,$2,$3)", c.connector.br.ID, key, identity)
		saved = identity
	}
	if err != nil {
		return nil, err
	}
	if saved != identity {
		return nil, errors.New("local task store changed; refusing to mix rooms")
	}
	return local.Read(ctx, meta.AccountKey, known)
}

func (c *Client) deliverSource(ctx context.Context, lane syncSource) error {
	meta := c.login.Metadata.(*LoginMetadata)
	known, err := c.loadDelivered(ctx)
	if err != nil {
		return err
	}
	var chats []source.Conversation
	if lane == localSource {
		chats, err = c.readLocal(ctx, known)
	} else {
		var snapshot *source.Snapshot
		snapshot, err = c.backend.Read(ctx, known)
		if err == nil {
			chats, err = source.Select(snapshot, meta.AccountKey, meta.Since)
		}
	}
	if err != nil {
		return sourceReadFailure{err}
	}
	// Fail closed if an adapter crosses source boundaries: a local tick must
	// never recover a remote outbox or deliver another lane's conversations.
	for _, chat := range chats {
		if conversationSource(chat.ID) != lane {
			return sourceReadFailure{errors.New("unexpected conversation source")}
		}
	}
	chats = c.updateSourceCache(meta.AccountKey, lane, chats)
	var failures []error
	for _, chat := range chats {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.dispatchConversation(ctx, chat); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// Keep bounded metadata from both sources to apply one combined active cap.
// The other lane's payloads are never redispatched or retained here.
func (c *Client) updateSourceCache(account string, lane syncSource, chats []source.Conversation) []source.Conversation {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	chats = c.connector.Config.selectActive(chats)
	metadata := make([]source.Conversation, len(chats))
	for i, chat := range chats {
		chat.Messages = nil
		metadata[i] = chat
	}
	c.candidates[lane] = metadata
	combined := append(append([]source.Conversation{}, c.candidates[remoteSource]...), c.candidates[localSource]...)
	active := c.connector.Config.selectActive(combined)
	c.chats = make(map[string]source.Conversation, len(active))
	for _, chat := range active {
		c.chats[source.PortalID(account, chat.ID)] = chat
	}
	selected := make([]source.Conversation, 0, len(chats))
	for _, chat := range chats {
		if _, ok := c.chats[source.PortalID(account, chat.ID)]; ok {
			selected = append(selected, chat)
		}
	}
	return selected
}
