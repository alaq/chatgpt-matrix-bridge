package connector

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"fmt"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
)

//go:embed assets/chatgpt.png
var iconPNG []byte

var iconHash = sha256.Sum256(iconPNG)
var iconID = "chatgpt-" + hex.EncodeToString(iconHash[:])

func (c *Connector) iconURI() id.ContentURIString {
	uri, _ := c.icon.Load().(id.ContentURIString)
	return uri
}

func (c *Connector) avatar() *bridgev2.Avatar {
	return &bridgev2.Avatar{ID: networkid.AvatarID(iconID), MXC: c.iconURI(), Hash: iconHash}
}

func (c *Connector) initializeIcon(ctx context.Context) error {
	key := database.Key("chatgpt_icon_" + iconID)
	uri := id.ContentURIString(c.br.DB.KV.Get(ctx, key))
	if parsed, err := uri.Parse(); err != nil || parsed.IsEmpty() {
		var err error
		uri, _, err = c.br.Bot.UploadMedia(ctx, "", iconPNG, "chatgpt.png", "image/png")
		if err != nil {
			return fmt.Errorf("upload ChatGPT icon: %w", err)
		}
		c.br.DB.KV.Set(ctx, key, string(uri))
	}
	c.icon.Store(uri)
	return c.br.Bot.SetAvatarURL(ctx, uri)
}
