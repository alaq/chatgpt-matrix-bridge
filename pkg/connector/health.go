package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"maunium.net/go/mautrix/bridgev2/commands"
)

type sourceReadFailure struct{ error }

func (e sourceReadFailure) Unwrap() error { return e.error }

// Preserve independent source and delivery failures, including mixed results.
func splitSyncFailures(err error) (sourceErr, deliveryErr error) {
	if err == nil {
		return nil, nil
	}
	if group, ok := err.(interface{ Unwrap() []error }); ok {
		for _, part := range group.Unwrap() {
			s, d := splitSyncFailures(part)
			sourceErr, deliveryErr = errors.Join(sourceErr, s), errors.Join(deliveryErr, d)
		}
		return
	}
	if source, ok := err.(sourceReadFailure); ok {
		return source.error, nil
	}
	return nil, err
}

type syncHealth struct {
	mu                  sync.Mutex
	LastAttempt         time.Time `json:"last_attempt"`
	LastSourceSuccess   time.Time `json:"last_source_success"`
	LastDeliverySuccess time.Time `json:"last_delivery_success"`
	SourceAvailable     bool      `json:"source_available"`
	DeliveryAvailable   bool      `json:"delivery_available"`
	PendingSends        int       `json:"pending_sends"`
	PendingAttachments  int       `json:"pending_attachments"`
	Rooms               int       `json:"rooms"`
}

func (c *Client) recordHealth(ctx context.Context, sourceErr, deliveryErr error) {
	h := &c.health
	h.mu.Lock()
	defer h.mu.Unlock()
	h.LastAttempt = time.Now().UTC()
	h.SourceAvailable, h.DeliveryAvailable = sourceErr == nil, deliveryErr == nil
	if sourceErr == nil {
		h.LastSourceSuccess = h.LastAttempt
	}
	if deliveryErr == nil {
		h.LastDeliverySuccess = h.LastAttempt
	}
	_ = c.connector.br.DB.QueryRow(ctx, "SELECT COUNT(*) FROM kv_store WHERE bridge_id=$1 AND key LIKE 'chatgpt_outbox_%' AND value<>''", c.connector.br.ID).Scan(&h.PendingSends)
	c.cacheMu.RLock()
	h.Rooms = len(c.chats)
	c.cacheMu.RUnlock()
	if c.connector.Config.HealthPath != "" {
		data, err := json.Marshal(h)
		if err == nil {
			file, err := os.CreateTemp(filepath.Dir(c.connector.Config.HealthPath), ".health-*")
			if err == nil {
				name := file.Name()
				defer os.Remove(name)
				_, writeErr := file.Write(data)
				syncErr := file.Sync()
				closeErr := file.Close()
				if writeErr == nil && syncErr == nil && closeErr == nil {
					_ = os.Rename(name, c.connector.Config.HealthPath)
				}
			}
		}
	}
}

func (c *Client) healthSummary() string {
	c.health.mu.Lock()
	defer c.health.mu.Unlock()
	h := &c.health
	when := "not yet completed"
	if !h.LastSourceSuccess.IsZero() {
		when = h.LastSourceSuccess.Format(time.RFC3339)
	}
	state := "connected"
	if h.LastAttempt.IsZero() {
		state = "starting"
	} else if !h.SourceAvailable {
		state = "source unavailable; check the browser and local task reader"
	} else if !h.DeliveryAvailable {
		state = "delivery needs recovery"
	} else if time.Since(h.LastAttempt) > max(5*time.Minute, 3*time.Duration(c.connector.Config.PollSeconds)*time.Second) {
		state = "sync delayed"
	}
	return fmt.Sprintf("Status: %s\nLast successful source sync: %s\nConversations: %d\nPending sends: %d\nUnavailable attachments: %d", state, when, h.Rooms, h.PendingSends, h.PendingAttachments)
}

func (c *Connector) registerCommands() {
	proc, ok := c.br.Commands.(*commands.Processor)
	if !ok {
		return
	}
	proc.AddHandler(&commands.FullHandler{Name: "new", RequiresLogin: true, Help: commands.HelpMeta{Section: commands.HelpSectionGeneral, Description: "Start a new saved ChatGPT conversation: new <first message>."}, Func: func(ce *commands.Event) {
		for _, login := range ce.User.GetUserLogins() {
			if client, ok := login.Client.(*Client); ok {
				client.createFromCommand(ce)
				return
			}
		}
	}})
	proc.AddHandlers(&commands.FullHandler{Name: "status", RequiresLogin: true,
		Help: commands.HelpMeta{Section: commands.HelpSectionGeneral, Description: "Show source sync and pending delivery health."},
		Func: func(ce *commands.Event) {
			for _, login := range ce.User.GetUserLogins() {
				if client, ok := login.Client.(*Client); ok {
					ce.Reply("%s", client.healthSummary())
				}
			}
		}}, &commands.FullHandler{Name: "retry", RequiresLogin: true,
		Help: commands.HelpMeta{Section: commands.HelpSectionGeneral, Description: "Reconcile pending sends using their original transactions."},
		Func: func(ce *commands.Event) {
			for _, login := range ce.User.GetUserLogins() {
				if client, ok := login.Client.(*Client); ok {
					client.requestRefresh()
				}
			}
			ce.Reply("Recovery requested. Pending sends will use their original attempts; an uncertain submission will not be sent again.")
		}})
}
