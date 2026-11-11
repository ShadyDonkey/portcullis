package jetstream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// per shard session state for proxy use for fresh or resume on restart?
	shardSessionBucketHistory = 1 // TODO: do I need this defined here?
	// key for holding info on the global state like intents, number of shards, etc
	globalStateKey            = "global-state"
	natsMaxReconnects         = 60
	natsReconnectWait         = 2 * time.Second
	publishAsyncAckTimeout    = 10 * time.Second
	publishCompleteWait       = publishAsyncAckTimeout + time.Second
	publishStallWait          = 1 * time.Millisecond
	publishFailureLogInterval = 10 * time.Second
	publishMaxPending         = 4096
)

type Config struct {
	URL            string
	ConnectionName string
	StateBucket    string
	StreamName     string
	SubjectPrefix  string
	MaxAge         time.Duration
	MaxBytes       int64

	// TODO: these may be replaced later by NKey or JWT depending on which path I go
	User     string
	Password string
}

type JetStream struct {
	nc              *nats.Conn
	js              jetstream.JetStream
	kv              jetstream.KeyValue
	subjectPrefix   string
	closing         atomic.Bool
	unexpectedClose chan error
	stalledDrops    atomic.Uint64
	syncErrors      atomic.Uint64
	ackErrors       atomic.Uint64
	stallFailures   failureLog
	syncFailures    failureLog
	ackFailures     failureLog
}

func New(ctx context.Context, config Config) (*JetStream, error) {
	if config.URL == "" {
		return nil, errors.New("jetstream config URL is empty")
	}

	if config.User == "" || config.Password == "" {
		return nil, errors.New("jetstream config user or password is empty")
	}

	j := &JetStream{
		subjectPrefix:   config.SubjectPrefix,
		unexpectedClose: make(chan error, 1),
	}

	nc, err := nats.Connect(
		config.URL,
		nats.UserInfo(config.User, config.Password),
		nats.FlusherTimeout(10*time.Second),
		nats.Name(config.ConnectionName),
		nats.MaxReconnects(natsMaxReconnects),
		nats.ReconnectWait(natsReconnectWait),
		nats.ReconnectJitter(100*time.Millisecond, time.Second),
		nats.ErrorHandler(
			func(_ *nats.Conn, _ *nats.Subscription, err error) {
				slog.Error("NATS error", "err", err)
			},
		),
		nats.DisconnectErrHandler(
			func(_ *nats.Conn, err error) {
				if j.closing.Load() {
					return
				}

				slog.Warn("NATS connection severed, reconnecting...", "err", err)
			},
		),
		nats.ReconnectHandler(
			func(nc *nats.Conn) {
				slog.Info("NATS re-connected", "url", nc.ConnectedUrl())
			},
		),
		nats.ClosedHandler(
			func(nc *nats.Conn) {
				if j.closing.Load() {
					slog.Info("NATS connection closed via intentional shutdown.")
					return
				}

				cause := nc.LastError()

				if cause == nil {
					cause = errors.New("nats: connection closed, no cause recorded")
				}

				slog.Error("NATS connection closed.", "err", cause)

				select {
				case j.unexpectedClose <- fmt.Errorf("nats: connection closed unexpectedly: %w", cause):
				default:
				}
			},
		),
	)

	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(
		nc,
		jetstream.WithPublishAsyncMaxPending(publishMaxPending),
		jetstream.WithPublishAsyncTimeout(publishAsyncAckTimeout),
		jetstream.WithPublishAsyncErrHandler(
			func(_ jetstream.JetStream, m *nats.Msg, err error) {
				j.ackErrors.Add(1)
				j.ackFailures.record("async_ack", err, "subject", m.Subject)
			},
		),
	)

	if err != nil {
		j.closing.Store(true)
		nc.Close()
		return nil, err
	}

	_, err = js.CreateOrUpdateStream(
		ctx, jetstream.StreamConfig{
			Name: config.StreamName,
			Subjects: []string{
				fmt.Sprintf("%s.*", config.SubjectPrefix),
			},
			Retention: jetstream.WorkQueuePolicy,
			Storage:   jetstream.FileStorage,
			MaxBytes:  config.MaxBytes,
			MaxAge:    config.MaxAge,
		},
	)

	if err != nil {
		nc.Close()
		return nil, err
	}

	kv, err := js.CreateOrUpdateKeyValue(
		ctx, jetstream.KeyValueConfig{
			Bucket:  config.StateBucket,
			History: shardSessionBucketHistory,
			Storage: jetstream.FileStorage,
		},
	)

	if err != nil {
		j.closing.Store(true)
		nc.Close()
		return nil, err
	}

	j.nc = nc
	j.kv = kv
	j.js = js

	return j, nil
}

func (j *JetStream) UnexpectedClose() <-chan error {
	return j.unexpectedClose
}

func (j *JetStream) Publish(ctx context.Context, eventType string, payload []byte) error {
	if eventType == "" {
		err := fmt.Errorf("publish: event type is empty")
		j.syncErrors.Add(1)
		j.syncFailures.record("sync_publish", err)

		return err
	}

	if j.js.PublishAsyncPending() >= publishMaxPending {
		j.stalledDrops.Add(1)
		j.stallFailures.record("stalled_drop", jetstream.ErrTooManyStalledMsgs)

		return fmt.Errorf("publish: %w", jetstream.ErrTooManyStalledMsgs)
	}

	subject := fmt.Sprintf("%s.%s", j.subjectPrefix, eventType)

	if _, err := j.js.PublishAsync(subject, payload, jetstream.WithStallWait(publishStallWait)); err != nil {
		if errors.Is(err, jetstream.ErrTooManyStalledMsgs) {
			j.stalledDrops.Add(1)
			j.stallFailures.record("stalled_drop", err)
		} else {
			j.syncErrors.Add(1)
			j.syncFailures.record("sync_publish", err)
		}

		return fmt.Errorf("publish: %w", err)
	}

	return nil
}

type PublishStats struct {
	StalledDrops uint64
	SyncErrors   uint64
	AckErrors    uint64
}

func (j *JetStream) PublishStats() PublishStats {
	return PublishStats{
		StalledDrops: j.stalledDrops.Load(),
		SyncErrors:   j.syncErrors.Load(),
		AckErrors:    j.ackErrors.Load(),
	}
}

func (j *JetStream) Close() {
	j.closing.Store(true)

	select {
	case <-j.js.PublishAsyncComplete():
	case <-time.After(publishCompleteWait):
		slog.Error(
			"timed out waiting for pending publishes to complete, forcing drain", "pending", j.js.PublishAsyncPending(),
		)
	}

	if j.nc.Status() == nats.CLOSED {
		slog.Info("NATS connection previously closed, skipping drain.")
		return
	}

	if err := j.nc.Drain(); err != nil {
		slog.Error("NATS connection drain failed", "err", err)
	}
}

func shardSessionKey(shardID int) string {
	return fmt.Sprintf("shard-%d", shardID)
}
