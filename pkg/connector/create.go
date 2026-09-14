package connector

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/alaq/chatgpt-matrix-bridge/internal/source"
	"maunium.net/go/mautrix/bridgev2/commands"
	"strings"
)

type pendingCreation struct {
	Request source.CreateRequest `json:"request"`
	Room    string               `json:"room"`
	Event   string               `json:"event"`
	Result  *source.CreateResult `json:"result,omitempty"`
}

func (c *Client) createFromCommand(ce *commands.Event) {
	text := strings.TrimSpace(ce.RawArgs)
	if !c.connector.Config.SendEnabled || text == "" || len(text) > 12000 {
		ce.Reply("Use `new <first message>` with up to 12,000 UTF-8 bytes.")
		return
	}
	c.pollMu.Lock()
	defer c.pollMu.Unlock()
	request := source.CreateRequest{Version: 1, AccountKey: c.login.Metadata.(*LoginMetadata).AccountKey, TransactionID: source.StableID("matrix-create", string(ce.RoomID), string(ce.EventID)), Text: text}
	key := "chatgpt_create_" + request.TransactionID
	var previous string
	err := c.connector.br.DB.QueryRow(ce.Ctx, "SELECT value FROM kv_store WHERE bridge_id=$1 AND key=$2", c.connector.br.ID, key).Scan(&previous)
	out := pendingCreation{Request: request, Room: string(ce.RoomID), Event: string(ce.EventID)}
	if err == nil {
		if json.Unmarshal([]byte(previous), &out) != nil || out.Request != request {
			ce.Reply("Creation record does not match this request.")
			return
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		ce.Reply("Could not read creation state.")
		return
	}
	if out.Result == nil {
		raw, _ := json.Marshal(out)
		if _, err = c.connector.br.DB.Exec(ce.Ctx, "INSERT INTO kv_store (bridge_id,key,value) VALUES ($1,$2,$3) ON CONFLICT(bridge_id,key) DO UPDATE SET value=$3", c.connector.br.ID, key, string(raw)); err != nil {
			ce.Reply("Could not record this creation safely.")
			return
		}
		out.Result, err = c.backend.Create(ce.Ctx, request)
		if err != nil {
			ce.Reply("Creation needs recovery. The original request is saved; use `retry` instead of sending another `new` command.")
			return
		}
		raw, _ = json.Marshal(out)
		if _, err = c.connector.br.DB.Exec(ce.Ctx, "UPDATE kv_store SET value=$3 WHERE bridge_id=$1 AND key=$2", c.connector.br.ID, key, string(raw)); err != nil {
			ce.Reply("Created in ChatGPT; local confirmation needs recovery.")
			return
		}
	}
	c.requestRefresh()
	ce.Reply("Created [your ChatGPT conversation](https://chatgpt.com/c/%s). Its Beeper room will appear after synchronization.", out.Result.ConversationID)
}
func (c *Client) recoverCreations(ctx context.Context) error {
	rows, err := c.connector.br.DB.Query(ctx, "SELECT key,value FROM kv_store WHERE bridge_id=$1 AND key LIKE 'chatgpt_create_%'", c.connector.br.ID)
	if err != nil {
		return err
	}
	type entry struct{ key, raw string }
	var entries []entry
	for rows.Next() {
		var e entry
		if err = rows.Scan(&e.key, &e.raw); err != nil {
			rows.Close()
			return err
		}
		entries = append(entries, e)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	var failures []error
	for _, e := range entries {
		var out pendingCreation
		if json.Unmarshal([]byte(e.raw), &out) != nil || out.Request.AccountKey != c.login.Metadata.(*LoginMetadata).AccountKey {
			continue
		}
		if out.Result != nil {
			continue
		}
		if !c.connector.Config.SendEnabled {
			return errors.New("creation recovery paused while sending disabled")
		}
		out.Result, err = c.backend.Create(ctx, out.Request)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		raw, _ := json.Marshal(out)
		_, err = c.connector.br.DB.Exec(ctx, "UPDATE kv_store SET value=$3 WHERE bridge_id=$1 AND key=$2", c.connector.br.ID, e.key, string(raw))
		if err != nil {
			return err
		}
	}
	return errors.Join(failures...)
}
