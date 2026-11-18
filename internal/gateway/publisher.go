package gateway

import (
	"context"
)

type EventPublisher interface {
	Publish(ctx context.Context, eventType [EventTypeMaxLength]byte, eventTypeLen int, frame []byte) error
}
