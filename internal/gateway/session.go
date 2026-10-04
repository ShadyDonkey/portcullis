package gateway

import (
	"context"
)

type Session struct {
	ID               string
	ResumeGatewayURL string
	LastSeq          *int
}

type SessionStore interface {
	GetSession(ctx context.Context, shardID int) (*Session, error)
	PutSession(ctx context.Context, shardID int, session Session) error
}
