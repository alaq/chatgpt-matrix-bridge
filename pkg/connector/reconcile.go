package connector

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/alaq/chatgpt-matrix-bridge/internal/delivery"
	"github.com/alaq/chatgpt-matrix-bridge/internal/source"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
)

func reconciledPartUpsert(messageID networkid.MessageID, partID networkid.PartID, partCount int, role, rawBody, hash string, content *event.MessageEventContent) func(context.Context, *bridgev2.Portal, bridgev2.MatrixAPI, []*database.Message) (bridgev2.UpsertResult, error) {
	return func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message) (bridgev2.UpsertResult, error) {
		var target *database.Message
		changed := false
		for _, part := range existing {
			old, ok := part.Metadata.(*MessageMetadata)
			if !ok {
				return bridgev2.UpsertResult{}, errors.New("unknown source metadata")
			}
			if len(old.AttachmentIDs) > 0 {
				return bridgev2.UpsertResult{}, nil
			}
			// The original outgoing Matrix message can contain the complete body.
			if role == "user" && part.PartID == "" && old.PartCount == 0 && old.Hash == hash {
				return bridgev2.UpsertResult{}, nil
			}
			if part.PartID == partID {
				target = part
			}
			if partID == "" && strings.HasPrefix(string(part.PartID), "text-") {
				index, err := strconv.Atoi(strings.TrimPrefix(string(part.PartID), "text-"))
				if err == nil && index >= partCount {
					tombstone := &event.MessageEventContent{MsgType: event.MsgText, Body: "[This part was superseded by an updated message.]", Mentions: &event.Mentions{}}
					updated, err := applySourceEdit(ctx, portal, intent, part, hash, partCount, tombstone)
					if err != nil {
						return bridgev2.UpsertResult{}, err
					}
					changed = changed || updated
				}
			}
		}
		if target == nil {
			return bridgev2.UpsertResult{ContinueMessageHandling: true, SaveParts: changed}, nil
		}
		old := target.Metadata.(*MessageMetadata)
		if old.Hash == hash && old.PresentationHash == presentationHash(content) && (old.PartCount == partCount || partCount == 1 && old.PartCount == 0) {
			return bridgev2.UpsertResult{SaveParts: changed}, nil
		}
		if old.Hash == hash && role == "user" && old.PresentationHash == "" {
			return bridgev2.UpsertResult{SaveParts: changed}, nil
		}
		updated, err := applySourceEdit(ctx, portal, intent, target, hash, partCount, content)
		return bridgev2.UpsertResult{SaveParts: changed || updated}, err
	}
}

func applySourceEdit(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, target *database.Message, hash string, count int, content *event.MessageEventContent) (bool, error) {
	old, ok := target.Metadata.(*MessageMetadata)
	if !ok {
		return false, errors.New("unknown source metadata")
	}
	renderHash := presentationHash(content)
	if old.Hash == hash && old.PresentationHash == renderHash && old.PartCount == count {
		return false, nil
	}
	if portal == nil || intent == nil || target.SenderMXID != intent.GetMXID() || target.Room != portal.PortalKey || target.HasFakeMXID() {
		return false, errors.New("edit source sender or room mismatch")
	}
	key := source.StableID("source-edit-v2", string(target.ID), string(target.PartID), strconv.Itoa(old.EditGeneration+1), hash, renderHash)
	_, err := intent.SendMessage(delivery.WithMessage(ctx, key), portal.MXID, event.EventMessage, &event.Content{Parsed: presentationEdit(content, target.MXID)}, &bridgev2.MatrixSendExtra{Timestamp: time.Now(), MessageMeta: target})
	if err != nil {
		return false, err
	}
	target.Metadata = &MessageMetadata{Hash: hash, PresentationHash: renderHash, PartCount: count, EditGeneration: old.EditGeneration + 1}
	return true, nil
}

// Keep obsolete branches as history, with one explicit branch transition notice.
// Source deletion is not interpreted as permission to erase Matrix history.
func (c *Client) branchState(ctx context.Context, chat source.Conversation) (string, []byte, bool, error) {
	key := "chatgpt_branch_" + source.PortalID(c.login.Metadata.(*LoginMetadata).AccountKey, chat.ID)
	ids := make([]string, 0, len(chat.Messages))
	current := map[string]bool{}
	for _, m := range chat.Messages {
		ids = append(ids, m.ID)
		current[m.ID] = true
	}
	type checkpoint struct {
		IDs        []string `json:"ids"`
		Generation int      `json:"generation"`
	}
	state := checkpoint{IDs: ids}
	encoded, _ := json.Marshal(state)
	var raw string
	if err := c.connector.br.DB.QueryRow(ctx, "SELECT value FROM kv_store WHERE bridge_id=$1 AND key=$2", c.connector.br.ID, key).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return key, encoded, false, nil
		}
		return "", nil, false, err
	}
	var previous checkpoint
	if json.Unmarshal([]byte(raw), &previous) != nil { // pre-release checkpoints were ID arrays.
		if json.Unmarshal([]byte(raw), &previous.IDs) != nil {
			return "", nil, false, errors.New("invalid branch checkpoint")
		}
	}
	state.Generation = previous.Generation
	for _, id := range previous.IDs {
		if !current[id] {
			state.Generation++
			encoded, _ = json.Marshal(state)
			return key, encoded, true, nil
		}
	}
	encoded, _ = json.Marshal(state)
	return key, encoded, false, nil
}

func (c *Client) saveBranch(ctx context.Context, key string, data []byte) error {
	_, err := c.connector.br.DB.Exec(ctx, "INSERT INTO kv_store (bridge_id,key,value) VALUES ($1,$2,$3) ON CONFLICT(bridge_id,key) DO UPDATE SET value=$3", c.connector.br.ID, key, string(data))
	if err != nil {
		return fmt.Errorf("save active branch checkpoint: %w", err)
	}
	return nil
}
