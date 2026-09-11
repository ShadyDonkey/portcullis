package jetstream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	eventStreamName     = "DISCORD_EVENTS"
	eventSubjectPattern = "events.*"
)

type Config struct {
	URL string

	// TODO: these may be replaced later by NKey or JWT depending on which path I go
	User     string
	Password string
}

type JetStream struct {
	nc *nats.Conn
	js jetstream.JetStream
}

func New(ctx context.Context, config Config) (*JetStream, error) {
	if config.URL == "" {
		return nil, errors.New("jetstream config URL is empty")
	}

	if config.User == "" || config.Password == "" {
		return nil, errors.New("jetstream config user or password is empty")
	}

	nc, err := nats.Connect(config.URL, nats.UserInfo(config.User, config.Password))
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}

	_, err = js.CreateOrUpdateStream(
		ctx, jetstream.StreamConfig{
			Name:      eventStreamName,
			Subjects:  []string{eventSubjectPattern},
			Retention: jetstream.WorkQueuePolicy,
			Storage:   jetstream.FileStorage,
		},
	)

	if err != nil {
		nc.Close()
		js.Conn().Close()
		return nil, err
	}

	return &JetStream{
		nc: nc,
		js: js,
	}, nil
}

func (j *JetStream) Publish(ctx context.Context, eventType string, payload []byte) error {
	if eventType == "" {
		return fmt.Errorf("publish: event type is empty")
	}

	subject := fmt.Sprintf("events.%s", eventType)

	if _, err := j.js.Publish(ctx, subject, payload); err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	return nil
}

func (j *JetStream) Close() {
	err := j.nc.Drain()
	if err != nil {
		slog.Error("failed to drain nc connection", "err", err)
		return
	}

	err = j.js.Conn().Drain()
	if err != nil {
		slog.Error("failed to drain js connection", "err", err)
		return
	}
}
