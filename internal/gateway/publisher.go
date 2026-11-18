package gateway

import (
	"context"
)

type EventPublisher interface {
	Publish(ctx context.Context, eventType []byte, frame []byte) error
}
