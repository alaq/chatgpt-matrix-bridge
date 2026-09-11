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
	name := "ChatGPT · " + strings.Join(strings.Fields(chat.Title), " ")
	if len([]rune(name)) > 200 {
		name = string([]rune(name)[:200])
	}
	return &bridgev2.ChatInfo{Name: ptr.Ptr(name), Avatar: c.connector.avatar(), Topic: ptr.Ptr(chat.URL), Type: ptr.Ptr(database.RoomTypeDefault), JoinRule: &event.JoinRulesEventContent{JoinRule: event.JoinRuleInvite}, Members: &bridgev2.ChatMemberList{IsFull: true, Members: []bridgev2.ChatMember{
		{EventSender: bridgev2.EventSender{IsFromMe: true, Sender: c.userID(), SenderLogin: c.login.ID}, Membership: event.MembershipJoin, PowerLevel: ptr.Ptr(50)},
		{EventSender: bridgev2.EventSender{Sender: c.assistantID()}, UserInfo: &bridgev2.UserInfo{Name: ptr.Ptr("ChatGPT"), Avatar: c.connector.avatar()}, Membership: event.MembershipJoin, PowerLevel: ptr.Ptr(50)},
	}}, ExcludeChangesFromTimeline: true}
}

func (c *Client) events(chat source.Conversation) []bridgev2.RemoteEvent {
	account := c.login.Metadata.(*LoginMetadata).AccountKey
	key := networkid.PortalKey{ID: networkid.PortalID(source.PortalID(account, chat.ID)), Receiver: c.login.ID}
	meta := simplevent.EventMeta{PortalKey: key, CreatePortal: true, Timestamp: time.UnixMilli(int64(chat.UpdatedAt * 1000))}
	// Resync creates rooms even for conversations without a visible assistant response.
	result := []bridgev2.RemoteEvent{&simplevent.ChatResync{EventMeta: meta.WithType(bridgev2.RemoteEventChatResync), ChatInfo: c.chatInfo(chat)}}
	for _, m := range chat.Messages {
		sender := bridgev2.EventSender{Sender: c.assistantID()}
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
		metadata := &MessageMetadata{Hash: hash}
		if m.Role == "assistant" {
			metadata.PresentationHash = presentationHash(content)
		}
		messageID := networkid.MessageID(source.MessageID(account, chat.ID, m.ID))
		messageMeta := meta.WithType(bridgev2.RemoteEventMessageUpsert).WithSender(sender).WithTimestamp(time.UnixMilli(int64(ts * 1000)))
		result = append(result, &sourceMessage{client: c, PreConvertedMessage: &simplevent.PreConvertedMessage{
			EventMeta:          messageMeta,
			ID:                 messageID,
			Data:               &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{Type: event.EventMessage, Content: content, DBMetadata: metadata}}},
			HandleExistingFunc: presentationUpsert(messageMeta, messageID, m.Role, body, hash, content),
		}})
	}
	return result
}
