package connector

import (
	"context"
	"github.com/alaq/chatgpt-matrix-bridge/internal/source"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"strings"
	"time"
)

// Only show typing when the source has observed generation. Acceptance itself
// is not evidence of generation, and silence/timeouts never imply completion.
func (c *Client) observeGeneration(portal *bridgev2.Portal, conversationID string) {
	if strings.HasPrefix(conversationID, "codex:") {
		c.requestRefresh()
		return
	}
	if c.backend.Descriptor == "" {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(c.connector.br.BackgroundCtx, 4*time.Minute)
		defer cancel()
		ghost, err := c.connector.br.GetGhostByID(ctx, c.assistantID())
		if err != nil {
			return
		}
		defer func() {
			done, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = ghost.Intent.MarkTyping(done, portal.MXID, bridgev2.TypingTypeText, 0)
		}()
		for {
			phase, err := c.backend.SendStatus(ctx, c.login.Metadata.(*LoginMetadata).AccountKey, conversationID)
			if err != nil {
				return
			}
			if phase == "generating" {
				_ = ghost.Intent.MarkTyping(ctx, portal.MXID, bridgev2.TypingTypeText, 12*time.Second)
			}
			if phase == "complete" {
				c.requestRefresh()
				return
			}
			if phase == "idle" || phase == "needs_recovery" {
				return
			}
			timer := time.NewTimer(3 * time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
}

func (c *Client) observeTaskState(ctx context.Context, chat source.Conversation) {
	key := networkid.PortalKey{ID: networkid.PortalID(source.PortalID(c.login.Metadata.(*LoginMetadata).AccountKey, chat.ID)), Receiver: c.login.ID}
	portal, err := c.connector.br.GetPortalByKey(ctx, key)
	if err != nil || portal == nil || portal.MXID == "" {
		return
	}
	ghost, err := c.connector.br.GetGhostByID(ctx, c.sourceAssistantID(chat))
	if err != nil {
		return
	}
	duration := time.Duration(0)
	if chat.Running {
		duration = time.Duration(c.connector.Config.PollSeconds+15) * time.Second
	}
	_ = ghost.Intent.MarkTyping(ctx, portal.MXID, bridgev2.TypingTypeText, duration)
}
