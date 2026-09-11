package connector

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/alaq/chatgpt-matrix-bridge/internal/source"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// One durable outbox per portal. Portal event handling serializes access. Store
// the original Matrix event before submitting, then recover its source association
// before any incoming mirror can create an echo. This private DB contains text.
type outbound struct {
	Request     source.SendRequest `json:"request"`
	MatrixEvent id.EventID         `json:"matrix_event"`
	Sender      id.UserID          `json:"sender"`
	Timestamp   int64              `json:"timestamp"`
}

func (c *Client) outboxKey(portal *bridgev2.Portal) string {
	return "chatgpt_outbox_" + source.StableID(string(c.login.ID), string(portal.ID))
}
func (c *Client) getOutbound(ctx context.Context, portal *bridgev2.Portal) (*outbound, error) {
	var raw string
	err := c.connector.br.DB.QueryRow(ctx, "SELECT value FROM kv_store WHERE bridge_id=$1 AND key=$2", c.connector.br.ID, c.outboxKey(portal)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && raw == "") {
		return nil, nil
	}
	if err != nil {
		return nil, errors.New("failed to read saved-send outbox")
	}
	var out outbound
	if len(raw) > 80*1024 || json.Unmarshal([]byte(raw), &out) != nil || out.Request.Validate() != nil || out.Request.AccountKey != c.login.Metadata.(*LoginMetadata).AccountKey || source.PortalID(out.Request.AccountKey, out.Request.ConversationID) != string(portal.ID) || out.Sender != c.login.UserMXID || out.MatrixEvent == "" || out.Timestamp <= 0 {
		return nil, errors.New("invalid saved-send outbox identity")
	}
	return &out, nil
}
func (c *Client) setOutbound(ctx context.Context, portal *bridgev2.Portal, out *outbound) error {
	var err error
	if out == nil {
		_, err = c.connector.br.DB.Exec(ctx, "DELETE FROM kv_store WHERE bridge_id=$1 AND key=$2", c.connector.br.ID, c.outboxKey(portal))
	} else {
		raw, _ := json.Marshal(out)
		_, err = c.connector.br.DB.Exec(ctx, "INSERT INTO kv_store (bridge_id,key,value) VALUES ($1,$2,$3) ON CONFLICT(bridge_id,key) DO UPDATE SET value=$3", c.connector.br.ID, c.outboxKey(portal), string(raw))
	}
	if err != nil {
		return errors.New("failed to persist saved-send outbox")
	}
	return nil
}
func (c *Client) send(ctx context.Context, req source.SendRequest) (*source.SendResult, error) {
	if c.sendFunc != nil {
		return c.sendFunc(ctx, req)
	}
	return c.backend.Send(ctx, req)
}
func (c *Client) outboundMessage(portal *bridgev2.Portal, out *outbound, sourceID string) *database.Message {
	return &database.Message{ID: networkid.MessageID(source.MessageID(out.Request.AccountKey, out.Request.ConversationID, sourceID)), MXID: out.MatrixEvent, Room: portal.PortalKey, SenderID: c.userID(), SenderMXID: out.Sender, Timestamp: time.UnixMilli(out.Timestamp), Metadata: &MessageMetadata{Hash: source.StableID("user", out.Request.Text)}}
}
func (c *Client) recoverOutbound(ctx context.Context, portal *bridgev2.Portal) error {
	out, err := c.getOutbound(ctx, portal)
	if err != nil || out == nil {
		return err
	}
	existing, err := c.connector.br.DB.Message.GetPartByMXID(ctx, out.MatrixEvent)
	if err != nil {
		return err
	}
	if existing != nil {
		return c.setOutbound(ctx, portal, nil)
	}
	if !c.connector.Config.SendEnabled {
		return errors.New("outbound recovery is paused while sending is disabled")
	}
	// Replay uses exactly the original transaction and payload. The backend either
	// returns its saved receipt, reconciles the source, or refuses an uncertain send.
	result, err := c.send(ctx, out.Request)
	if err != nil || result.Status != "accepted" {
		return errors.New("saved send is unresolved; mirroring this room is paused to prevent an echo")
	}
	msg := c.outboundMessage(portal, out, result.UserMessageID)
	if _, err = c.connector.br.GetGhostByID(ctx, c.userID()); err != nil {
		return err
	}
	if err = c.connector.br.DB.Message.Insert(ctx, msg); err != nil {
		return errors.New("failed to recover source-to-Matrix message association")
	}
	return c.setOutbound(ctx, portal, nil)
}
func sendError(message string, certain bool) error {
	return bridgev2.WrapErrorInStatus(errors.New(message)).WithIsCertain(certain).WithErrorAsMessage()
}
func (c *Client) HandleMatrixMessage(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	if !c.connector.Config.SendEnabled {
		return nil, sendError("Sending into saved ChatGPT conversations is disabled in this bridge.", true)
	}
	if msg == nil || msg.Event == nil || msg.Content == nil || msg.Portal == nil || msg.Event.Sender != c.login.UserMXID || msg.Portal.Receiver != c.login.ID || msg.OrigSender != nil {
		return nil, sendError("This bridge only accepts its owner's messages in a verified conversation room.", true)
	}
	if msg.Content.MsgType != event.MsgText || msg.ReplyTo != nil || msg.ThreadRoot != nil || msg.Content.RelatesTo != nil {
		return nil, sendError("This pilot supports plain text continuation only.", true)
	}
	c.cacheMu.RLock()
	chat, ok := c.chats[string(msg.Portal.ID)]
	c.cacheMu.RUnlock()
	account := c.login.Metadata.(*LoginMetadata).AccountKey
	if !ok || !c.connector.Config.allows(chat.ID) || source.PortalID(account, chat.ID) != string(msg.Portal.ID) {
		return nil, sendError("Wait for this conversation to synchronize before sending.", true)
	}
	req := source.SendRequest{Version: 1, AccountKey: account, ConversationID: chat.ID, TransactionID: source.StableID("matrix-send", account, string(msg.Portal.MXID), string(msg.Event.ID)), Text: msg.Content.Body}
	if err := req.Validate(); err != nil {
		return nil, sendError("Send non-empty text of at most 12,000 UTF-8 bytes.", true)
	}
	if err := c.recoverOutbound(ctx, msg.Portal); err != nil {
		return nil, sendError("A previous send needs reconciliation before this room can continue.", false)
	}
	out := &outbound{Request: req, MatrixEvent: msg.Event.ID, Sender: msg.Event.Sender, Timestamp: msg.Event.Timestamp}
	if err := c.setOutbound(ctx, msg.Portal, out); err != nil {
		return nil, sendError("Could not record this send safely.", true)
	}
	result, err := c.send(ctx, req)
	if err != nil || result.Status == "uncertain" {
		return nil, sendError("Delivery to ChatGPT is uncertain. The original attempt is retained for reconciliation; do not resend with a new message.", false)
	}
	if result.Status != "accepted" {
		if err := c.setOutbound(ctx, msg.Portal, nil); err != nil {
			return nil, sendError("The send was rejected but its local state needs reconciliation.", false)
		}
		return nil, sendError("ChatGPT did not accept this send. Check the signed-in session and whether another turn is running.", true)
	}
	return &bridgev2.MatrixMessageResponse{DB: c.outboundMessage(msg.Portal, out, result.UserMessageID), PostSave: func(ctx context.Context, _ *database.Message) { _ = c.setOutbound(ctx, msg.Portal, nil) }}, nil
}

type sourceMessage struct {
	*simplevent.PreConvertedMessage
	client   *Client
	preError error
}

func (e *sourceMessage) PreHandle(ctx context.Context, portal *bridgev2.Portal) {
	e.preError = e.client.recoverOutbound(ctx, portal)
}
func (e *sourceMessage) ConvertMessage(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI) (*bridgev2.ConvertedMessage, error) {
	if e.preError != nil {
		return nil, e.preError
	}
	return e.PreConvertedMessage.ConvertMessage(ctx, portal, intent)
}
func (e *sourceMessage) HandleExisting(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message) (bridgev2.UpsertResult, error) {
	if e.preError != nil {
		return bridgev2.UpsertResult{}, e.preError
	}
	return e.PreConvertedMessage.HandleExisting(ctx, portal, intent, existing)
}
