package state

import (
	"context"

	"github.com/ShadyDonkey/portcullis/internal/gateway"
)

type Global struct {
	Intents   gateway.Intent `json:"intents"`
	NumShards int            `json:"num_shards"`
}

type Store interface {
	GetSession(shardID int) (*gateway.Session, error)
	PutSession(shardID int, session gateway.Session) error
	DeleteSession(shardID int) error
	GetGlobal() (*Global, error)
	PutGlobal(global Global) error
	Close(ctx context.Context) error
}
