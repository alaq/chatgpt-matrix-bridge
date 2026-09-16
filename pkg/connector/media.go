package connector

import (
	"context"
	"errors"
	"strings"

	"github.com/alaq/chatgpt-matrix-bridge/internal/delivery"
	"github.com/alaq/chatgpt-matrix-bridge/internal/source"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"
)

func (c *Client) attachmentEvent(meta simplevent.EventMeta, chat source.Conversation, message source.Message, attachment source.Attachment) bridgev2.RemoteEvent {
	account := c.login.Metadata.(*LoginMetadata).AccountKey
	messageID := networkid.MessageID(source.MessageID(account, chat.ID, message.ID+":attachment:"+attachment.ID))
	meta.MutateContextFunc = func(ctx context.Context) context.Context { return delivery.WithMessage(ctx, string(messageID)) }
	return &simplevent.Message[source.Attachment]{EventMeta: meta, ID: messageID, Data: attachment,
		HandleExistingFunc: func(context.Context, *bridgev2.Portal, bridgev2.MatrixAPI, []*database.Message, source.Attachment) (bridgev2.UpsertResult, error) {
			return bridgev2.UpsertResult{}, nil
		},
		ConvertMessageFunc: func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, a source.Attachment) (*bridgev2.ConvertedMessage, error) {
			// bridgev2 emits a timeline error notice for ordinary conversion errors.
			// That would consume this attachment's deterministic transaction before
			// its actual file could be sent. Leave unavailable files retryable.
			unavailable := func(err error) (*bridgev2.ConvertedMessage, error) {
				c.addPendingAttachments(conversationSource(chat.ID), 1)
				return nil, errors.Join(bridgev2.ErrIgnoringRemoteEvent, err)
			}
			if err := c.recoverOutbound(ctx, portal); err != nil {
				return unavailable(err)
			}
			original, err := c.connector.br.DB.Message.GetPartByID(ctx, portal.Receiver, networkid.MessageID(source.MessageID(account, chat.ID, message.ID)), "")
			if err != nil {
				return unavailable(err)
			}
			if original != nil {
				if meta, ok := original.Metadata.(*MessageMetadata); ok {
					for _, id := range meta.AttachmentIDs {
						if id == a.ID {
							return &bridgev2.ConvertedMessage{}, nil
						}
					}
				}
			}
			media, data, err := c.backend.Download(ctx, account, chat.ID, message.ID, a)
			if err != nil {
				return unavailable(err)
			}
			uri, encrypted, err := intent.UploadMedia(ctx, portal.MXID, data, media.Name, media.MimeType)
			if err != nil {
				return unavailable(err)
			}
			typ := event.MsgFile
			if strings.HasPrefix(media.MimeType, "image/") {
				typ = event.MsgImage
			} else if strings.HasPrefix(media.MimeType, "audio/") {
				typ = event.MsgAudio
			} else if strings.HasPrefix(media.MimeType, "video/") {
				typ = event.MsgVideo
			}
			content := &event.MessageEventContent{MsgType: typ, Body: media.Name, FileName: media.Name, URL: uri, File: encrypted, Info: &event.FileInfo{MimeType: media.MimeType, Size: media.Size}, Mentions: &event.Mentions{}}
			return &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{Type: event.EventMessage, Content: content, DBMetadata: &MessageMetadata{Hash: media.SHA256}}}}, nil
		}}
}
