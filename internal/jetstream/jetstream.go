package jetstream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	eventStreamName     = "DISCORD_EVENTS"
	eventSubjectPattern = "events.*"

	// per shard session state for proxy use for fresh or resume on restart?
	shardSessionBucketName    = "PORTCULLIS_STATE"
	shardSessionBucketHistory = 1 // TODO: do I need this defined here?

	// key for holding info on the global state like intents, number of shards, etc
	globalStateKey = "global-state"
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
	kv jetstream.KeyValue
}

func New(ctx context.Context, config Config) (*JetStream, error) {
	if config.URL == "" {
		return nil, errors.New("jetstream config URL is empty")
	}

	if config.User == "" || config.Password == "" {
		return nil, errors.New("jetstream config user or password is empty")
	}

	nc, err := nats.Connect(
		config.URL, nats.UserInfo(config.User, config.Password), nats.FlusherTimeout(10*time.Second),
	)
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(
		nc,
		jetstream.WithPublishAsyncMaxPending(4096),
		jetstream.WithPublishAsyncTimeout(10*time.Second),
		jetstream.WithPublishAsyncErrHandler(
			func(_ jetstream.JetStream, m *nats.Msg, err error) {
				slog.Error("jetstream publish error", "subject", m.Subject, "err", err)
			},
		),
	)
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
		return nil, err
	}

	kv, err := js.CreateOrUpdateKeyValue(
		ctx, jetstream.KeyValueConfig{
			Bucket:  shardSessionBucketName,
			History: shardSessionBucketHistory,
			Storage: jetstream.FileStorage,
		},
	)

	if err != nil {
		nc.Close()
		return nil, err
	}

	return &JetStream{
		nc: nc,
		js: js,
		kv: kv,
	}, nil
}

func (j *JetStream) Publish(ctx context.Context, eventType string, payload []byte) error {
	if eventType == "" {
		return fmt.Errorf("publish: event type is empty")
	}

	subject := fmt.Sprintf("events.%s", eventType)

	if _, err := j.js.PublishAsync(subject, payload); err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	return nil
}

func (j *JetStream) Close() {
	<-j.js.PublishAsyncComplete()
	if err := j.nc.Drain(); err != nil {
		slog.Error("failed to drain NATS connection", "err", err)
	}
}

func shardSessionKey(shardID int) string {
	return fmt.Sprintf("shard-%d", shardID)
}
