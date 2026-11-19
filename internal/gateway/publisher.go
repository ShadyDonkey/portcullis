package gateway

import (
	"context"
)

type EventPublisher interface {
	Publish(ctx context.Context, eventType string, frame []byte) error
}
