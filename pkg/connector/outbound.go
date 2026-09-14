package connector

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
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
	Request             source.SendRequest `json:"request"`
	MatrixEvent         id.EventID         `json:"matrix_event"`
	MatrixTransactionID string             `json:"matrix_transaction_id,omitempty"`
	Sender              id.UserID          `json:"sender"`
	Timestamp           int64              `json:"timestamp"`
	AttachmentIDs       []string           `json:"attachment_ids,omitempty"`
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
	if len(raw) > 29<<20 || json.Unmarshal([]byte(raw), &out) != nil || out.Request.Validate() != nil || out.Request.AccountKey != c.login.Metadata.(*LoginMetadata).AccountKey || source.PortalID(out.Request.AccountKey, out.Request.ConversationID) != string(portal.ID) || out.Sender != c.login.UserMXID || out.MatrixEvent == "" || out.Timestamp <= 0 {
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
	if strings.HasPrefix(req.ConversationID, "codex:") {
		if !c.connector.Config.CodexSendEnabled {
			return &source.SendResult{Version: 1, Status: "not_sent", Error: "codex_read_only"}, nil
		}
		return c.connector.Config.LocalTasks().Send(ctx, req)
	}
	return c.backend.Send(ctx, req)
}
func (c *Client) outboundMessage(portal *bridgev2.Portal, out *outbound, sourceID string) *database.Message {
	return &database.Message{ID: networkid.MessageID(source.MessageID(out.Request.AccountKey, out.Request.ConversationID, sourceID)), MXID: out.MatrixEvent, Room: portal.PortalKey, SenderID: c.userID(), SenderMXID: out.Sender, Timestamp: time.UnixMilli(out.Timestamp), Metadata: &MessageMetadata{Hash: source.StableID("user", out.Request.Text), AttachmentIDs: out.AttachmentIDs}}
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
		return c.finishRecovery(ctx, portal, out)
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
	out.AttachmentIDs = result.AttachmentIDs
	msg.Metadata.(*MessageMetadata).AttachmentIDs = result.AttachmentIDs
	if _, err = c.connector.br.GetGhostByID(ctx, c.userID()); err != nil {
		return err
	}
	if err = c.connector.br.DB.Message.Insert(ctx, msg); err != nil {
		return errors.New("failed to recover source-to-Matrix message association")
	}
	return c.finishRecovery(ctx, portal, out)
}

func (c *Client) finishRecovery(ctx context.Context, portal *bridgev2.Portal, out *outbound) error {
	transactionID := out.MatrixTransactionID
	if transactionID == "" {
		// Older outboxes did not retain the client's transaction. Resolve it from
		// the exact original event so mobile clients can settle their local echo.
		original, err := c.connector.br.Bot.GetEvent(ctx, portal.MXID, out.MatrixEvent)
		if err != nil || original == nil || original.ID != out.MatrixEvent || original.Sender != out.Sender || (original.RoomID != "" && original.RoomID != portal.MXID) {
			return errors.New("cannot verify original event for recovery receipt")
		}
		transactionID = original.Unsigned.TransactionID
	}
	c.connector.br.Matrix.SendMessageStatus(ctx, &bridgev2.MessageStatus{Status: event.MessageStatusSuccess}, &bridgev2.MessageStatusEventInfo{
		RoomID: portal.MXID, SourceEventID: out.MatrixEvent, TransactionID: transactionID, Sender: out.Sender, EventType: event.EventMessage, MessageType: event.MsgText,
	})
	return c.setOutbound(ctx, portal, nil)
}
func sendError(message string, certain bool) error {
	return bridgev2.WrapErrorInStatus(errors.New(message)).WithIsCertain(certain).WithErrorAsMessage().WithSendNotice(true)
}
func (c *Client) HandleMatrixMessage(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	if !c.connector.Config.SendEnabled {
		return nil, sendError("Sending into saved ChatGPT conversations is disabled in this bridge.", true)
	}
	if msg == nil || msg.Event == nil || msg.Content == nil || msg.Portal == nil || msg.Event.Sender != c.login.UserMXID || msg.Portal.Receiver != c.login.ID || msg.OrigSender != nil {
		return nil, sendError("This bridge only accepts its owner's messages in a verified conversation room.", true)
	}
	isMedia := msg.Content.MsgType == event.MsgImage || msg.Content.MsgType == event.MsgFile || msg.Content.MsgType == event.MsgAudio || msg.Content.MsgType == event.MsgVideo
	if (msg.Content.MsgType != event.MsgText && !isMedia) || msg.ReplyTo != nil || msg.ThreadRoot != nil || msg.Content.RelatesTo != nil {
		return nil, sendError("Send text, an image, or a file as a normal message.", true)
	}
	c.cacheMu.RLock()
	chat, ok := c.chats[string(msg.Portal.ID)]
	c.cacheMu.RUnlock()
	account := c.login.Metadata.(*LoginMetadata).AccountKey
	if !ok || !c.connector.Config.allows(chat.ID) || source.PortalID(account, chat.ID) != string(msg.Portal.ID) {
		return nil, sendError("Wait for this conversation to synchronize before sending.", true)
	}
	req := source.SendRequest{Version: 1, AccountKey: account, ConversationID: chat.ID, TransactionID: source.StableID("matrix-send", account, string(msg.Portal.MXID), string(msg.Event.ID)), Text: msg.Content.Body}
	if isMedia {
		if msg.Content.Info == nil || msg.Content.Info.Size <= 0 || msg.Content.Info.Size > 20<<20 {
			return nil, sendError("Send an attachment up to 20 MiB with a known size.", true)
		}
		data, err := c.connector.br.Bot.DownloadMedia(ctx, msg.Content.URL, msg.Content.File)
		if err != nil || len(data) == 0 || len(data) > 20<<20 {
			return nil, sendError("Could not download this attachment.", true)
		}
		name := msg.Content.FileName
		if name == "" {
			name = msg.Content.Body
		}
		name = filepath.Base(name)
		if name == "" || name == "." {
			name = "attachment"
		}
		mime := msg.Content.Info.MimeType
		if mime == "" {
			mime = "application/octet-stream"
		}
		hash := sha256.Sum256(data)
		req.Attachments = []source.SendAttachment{{Name: name, MimeType: mime, SHA256: hex.EncodeToString(hash[:]), Data: base64.StdEncoding.EncodeToString(data)}}
		if msg.Content.Body == name {
			req.Text = ""
		}
	}
	if err := req.Validate(); err != nil {
		return nil, sendError("Send non-empty text of at most 12,000 UTF-8 bytes.", true)
	}
	if err := c.recoverOutbound(ctx, msg.Portal); err != nil {
		return nil, sendError("A previous send needs reconciliation before this room can continue.", false)
	}
	out := &outbound{Request: req, MatrixEvent: msg.Event.ID, MatrixTransactionID: msg.Event.Unsigned.TransactionID, Sender: msg.Event.Sender, Timestamp: msg.Event.Timestamp}
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
		return nil, sendError(rejectedSendMessage(result.Error), true)
	}
	out.AttachmentIDs = result.AttachmentIDs
	return &bridgev2.MatrixMessageResponse{DB: c.outboundMessage(msg.Portal, out, result.UserMessageID), PostSave: func(ctx context.Context, _ *database.Message) {
		_ = c.setOutbound(ctx, msg.Portal, nil)
		c.requestRefresh()
		c.observeGeneration(msg.Portal, chat.ID)
	}}, nil
}

func rejectedSendMessage(code string) string {
	switch code {
	case "codex_read_only":
		return "This local task is mirrored read-only. Open it in the desktop app to continue it."
	case "codex_owner_unavailable":
		return "The desktop app could not route this reply to the original task. Nothing was submitted. Open the task in the app, then retry the original message."
	case "codex_turn_ended":
		return "The Codex turn finished before this reply could join it. Nothing was submitted; retry the original message to continue the same task."
	case "saved_send_draft_mismatch":
		return "The ChatGPT editor did not preserve this message exactly. Nothing was submitted; retry the original message after the sender is fixed."
	case "saved_send_existing_draft":
		return "This ChatGPT conversation already has a different unsent draft. Resolve it in ChatGPT, then retry this message."
	case "saved_send_source_busy", "saved_send_busy":
		return "ChatGPT is still handling another turn. Nothing was submitted; retry this message when that turn finishes."
	case "saved_send_account_mismatch":
		return "The browser is signed into a different ChatGPT account. Nothing was submitted."
	case "saved_send_challenge", "saved_send_login_required":
		return "ChatGPT needs login or browser verification in the DEV browser. Nothing was submitted; retry this message after completing it."
	default:
		return "ChatGPT did not accept this send. Check the signed-in session and whether another turn is running."
	}
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
		return nil, errors.Join(bridgev2.ErrIgnoringRemoteEvent, e.preError)
	}
	return e.PreConvertedMessage.ConvertMessage(ctx, portal, intent)
}
func (e *sourceMessage) HandleExisting(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message) (bridgev2.UpsertResult, error) {
	if e.preError != nil {
		return bridgev2.UpsertResult{}, e.preError
	}
	return e.PreConvertedMessage.HandleExisting(ctx, portal, intent, existing)
}
