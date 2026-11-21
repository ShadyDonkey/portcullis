package bridge

import (
	"context"
	"log/slog"
)

type Bridge interface {
	Deliver(e *Event) error
	Close(ctx context.Context) error
}

type InFlightReporter interface {
	InFlightBytes() int64
}

type Env struct {
	Name   string
	Logger *slog.Logger
	Fatal  func(error)
}

type Base struct {
	Enabled bool `toml:"enabled"`
}

func (b *Base) IsEnabled() bool {
	return b.Enabled
}

func (b *Base) Validate() error {
	return nil
}
