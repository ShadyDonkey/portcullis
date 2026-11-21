package gateway

import (
	"github.com/ShadyDonkey/portcullis/internal/bridge"
)

type EventPublisher interface {
	Send(event *bridge.Event)
}
