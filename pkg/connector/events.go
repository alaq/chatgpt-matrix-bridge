package connector

import (
	"fmt"
	"github.com/alaq/chatgpt-matrix-bridge/internal/source"
	"go.mau.fi/util/ptr"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"
	"strings"
	"time"
)

func (c *Client) chatInfo(chat source.Conversation) *bridgev2.ChatInfo {
	label := "ChatGPT"
	if chat.Kind == "codex" {
		label = "Codex"
		if !c.connector.Config.CodexSendEnabled {
			label = "Codex (read-only)"
		}
	} else if chat.Kind == "work" {
		label = "ChatGPT Work"
	}
	name := label + " · " + strings.Join(strings.Fields(chat.Title), " ")
	if len([]rune(name)) > 200 {
		name = string([]rune(name)[:200])
	}
	participant := label
	if chat.Kind == "codex" {
		participant = "Codex"
	}
	if chat.Kind == "work" {
		participant = "ChatGPT"
	}
	topic := chat.URL
	if chat.Kind == "codex" && !c.connector.Config.CodexSendEnabled {
		topic = "Read-only task mirror. Open the original task in the desktop app to continue: " + chat.URL
	}
	return &bridgev2.ChatInfo{Name: ptr.Ptr(name), Avatar: c.connector.avatar(), Topic: ptr.Ptr(topic), Type: ptr.Ptr(database.RoomTypeDefault), JoinRule: &event.JoinRulesEventContent{JoinRule: event.JoinRuleInvite}, Members: &bridgev2.ChatMemberList{IsFull: true, Members: []bridgev2.ChatMember{
		{EventSender: bridgev2.EventSender{IsFromMe: true, Sender: c.userID(), SenderLogin: c.login.ID}, Membership: event.MembershipJoin, PowerLevel: ptr.Ptr(50)},
		{EventSender: bridgev2.EventSender{Sender: c.sourceAssistantID(chat)}, UserInfo: &bridgev2.UserInfo{Name: ptr.Ptr(participant), Avatar: c.connector.avatar()}, Membership: event.MembershipJoin, PowerLevel: ptr.Ptr(50)},
	}}, ExcludeChangesFromTimeline: true}
}

func (c *Client) events(chat source.Conversation) []bridgev2.RemoteEvent {
	account := c.login.Metadata.(*LoginMetadata).AccountKey
	key := networkid.PortalKey{ID: networkid.PortalID(source.PortalID(account, chat.ID)), Receiver: c.login.ID}
	meta := simplevent.EventMeta{PortalKey: key, CreatePortal: true, Timestamp: time.UnixMilli(int64(chat.UpdatedAt * 1000))}
	// Resync creates rooms even for conversations without a visible assistant response.
	result := []bridgev2.RemoteEvent{&simplevent.ChatResync{EventMeta: meta.WithType(bridgev2.RemoteEventChatResync), ChatInfo: c.chatInfo(chat)}}
	for _, m := range chat.Messages {
		sender := bridgev2.EventSender{Sender: c.sourceAssistantID(chat)}
		if m.Role == "user" {
			sender = bridgev2.EventSender{Sender: c.userID(), SenderLogin: c.login.ID, IsFromMe: true}
		}
		ts := chat.CreatedAt
		if m.CreatedAt != nil {
			ts = *m.CreatedAt
		}
		body := m.Text
		if m.AttachmentCount > 0 {
			body += fmt.Sprintf("\n\n[%d attachment(s); open the original conversation to view]", m.AttachmentCount)
		}
		if body == "" {
			body = "[Empty message]"
		}
		hash := source.StableID(m.Role, body)
		content := renderMessage(m, body, chat.URL)
		messageID := networkid.MessageID(source.MessageID(account, chat.ID, m.ID))
		messageMeta := meta.WithType(bridgev2.RemoteEventMessageUpsert).WithSender(sender).WithTimestamp(time.UnixMilli(int64(ts * 1000)))
		contents := splitMessage(content)
		for i, part := range contents {
			partID := networkid.PartID("")
			metadata := &MessageMetadata{Hash: hash}
			if len(contents) > 1 {
				metadata.PartCount = len(contents)
			}
			if i > 0 {
				partID = networkid.PartID(fmt.Sprintf("text-%06d", i))
			}
			if m.Role == "assistant" {
				metadata.PresentationHash = presentationHash(part)
			}
			upsert := messagePartUpsert(messageID, partID, len(contents), m.Role, body, hash, part)
			if c.connector.Config.SyncEdits {
				upsert = reconciledPartUpsert(messageID, partID, len(contents), m.Role, body, hash, part)
			}
			result = append(result, &sourceMessage{client: c, PreConvertedMessage: &simplevent.PreConvertedMessage{
				EventMeta:          messageMeta,
				ID:                 messageID,
				Data:               &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{ID: partID, Type: event.EventMessage, Content: part, DBMetadata: metadata}}},
				HandleExistingFunc: upsert,
			}})
		}
		mediaStart, _ := time.Parse(time.RFC3339, c.connector.Config.MediaSince)
		if c.connector.Config.SyncMedia && (c.connector.Config.MediaSince == "" || ts >= float64(mediaStart.Unix())) {
			for _, attachment := range m.Attachments {
				result = append(result, c.attachmentEvent(messageMeta, chat, m, attachment))
			}
		}
	}
	return result
}
