package gateway

import (
	"context"
)

type Session struct {
	ID               string `json:"session_id"`
	ResumeGatewayURL string `json:"resume_gateway_url"`
	LastSequence     *int   `json:"last_sequence"`
}

type SessionStore interface {
	GetSession(ctx context.Context, shardID int) (*Session, error)
	PutSession(ctx context.Context, shardID int, session Session) error
	DeleteSession(ctx context.Context, shardID int) error
}
