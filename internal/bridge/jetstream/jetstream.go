package jetstream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ShadyDonkey/portcullis/internal/bridge"
	"github.com/ShadyDonkey/portcullis/internal/metrics"
	"github.com/nats-io/nats.go"
	natsjs "github.com/nats-io/nats.go/jetstream"
)

const (
	maxReconnects          = 60
	reconnectWait          = 2 * time.Second
	publishAsyncAckTimeout = 10 * time.Second
	publishStallWait       = 1 * time.Millisecond
	publishMaxPending      = 4096
)

func init() {
	bridge.Register(
		bridge.Factory{
			Name:      "jetstream",
			NewConfig: func() bridge.Config { return defaultConfig() },
			New:       newBridge,
		},
	)
}

type pendingEvent struct {
	event      *bridge.Event
	enqueuedAt time.Time
}

type Bridge struct {
	nc           *nats.Conn
	js           natsjs.JetStream
	name         string
	prefix       string
	mu           sync.Mutex
	pending      map[uintptr]pendingEvent
	pendingBytes atomic.Int64
	room         chan struct{}
	stop         chan struct{}
	stopped      atomic.Bool
	closing      atomic.Bool
	ackFailures  *bridge.FailureLog
}

func newBridge(ctx context.Context, c bridge.Config, env bridge.Env) (bridge.Bridge, error) {
	cfg, ok := c.(*Config)
	if !ok {
		return nil, fmt.Errorf("jetstream: unexpected config type %T", c)
	}

	b := &Bridge{
		name:        env.Name,
		prefix:      cfg.Stream.SubjectPrefix,
		pending:     make(map[uintptr]pendingEvent),
		room:        make(chan struct{}, 1),
		stop:        make(chan struct{}),
		ackFailures: bridge.NewFailureLog(env.Logger),
	}

	nc, err := b.connect(cfg, env)
	if err != nil {
		return nil, err
	}

	b.nc = nc

	js, err := natsjs.New(
		nc,
		natsjs.WithPublishAsyncMaxPending(publishMaxPending),
		natsjs.WithPublishAsyncTimeout(publishAsyncAckTimeout),
		natsjs.WithPublishAsyncAckHandler(b.onAck),
		natsjs.WithPublishAsyncErrHandler(b.onError),
	)
	if err != nil {
		nc.Close()
		return nil, err
	}

	_, err = js.CreateOrUpdateStream(
		ctx, natsjs.StreamConfig{
			Name:      cfg.Stream.Name,
			Subjects:  []string{fmt.Sprintf("%s.*", cfg.Stream.SubjectPrefix)},
			Retention: natsjs.WorkQueuePolicy,
			Storage:   natsjs.FileStorage,
			MaxBytes:  int64(cfg.Stream.MaxBytes),
			MaxAge:    time.Duration(cfg.Stream.MaxAge),
		},
	)
	if err != nil {
		nc.Close()
		return nil, err
	}

	b.js = js

	return b, nil
}

func (b *Bridge) connect(cfg *Config, env bridge.Env) (*nats.Conn, error) {
	log := env.Logger

	return nats.Connect(
		cfg.URL.String(),
		nats.UserInfo(cfg.Username.String(), cfg.Password.Reveal()),
		nats.Name(cfg.ConnectionName),
		nats.FlusherTimeout(10*time.Second),
		nats.MaxReconnects(maxReconnects),
		nats.ReconnectWait(reconnectWait),
		nats.ReconnectJitter(100*time.Millisecond, time.Second),
		nats.ErrorHandler(
			func(_ *nats.Conn, _ *nats.Subscription, err error) {
				log.Error("NATS error", "err", err)
			},
		),
		nats.DisconnectErrHandler(
			func(_ *nats.Conn, err error) {
				if !b.closing.Load() {
					log.Warn("NATS connection severed, reconnecting...", "err", err)
				}
			},
		),
		nats.ReconnectHandler(
			func(nc *nats.Conn) {
				metrics.NATSReconnect()
				log.Info("NATS re-connected", "url", nc.ConnectedUrl())
			},
		),
		nats.ClosedHandler(
			func(nc *nats.Conn) {
				if b.closing.Load() {
					return
				}

				cause := nc.LastError()
				if cause == nil {
					cause = errors.New("connection closed, no cause recorded")
				}

				log.Error("NATS connection closed", "err", cause)
				env.Fatal(fmt.Errorf("nats connection lost: %w", cause))
			},
		),
	)
}

func (b *Bridge) Deliver(e *bridge.Event) error {
	if len(e.Frame) == 0 {
		e.Release()
		return nil
	}

	for b.js.PublishAsyncPending() >= publishMaxPending {
		select {
		case <-b.room:
		case <-b.stop:
			metrics.BridgeDrop(b.name, metrics.DropShutdown)
			e.Release()
			return errors.New("jetstream: bridge closed")
		}
	}

	subject := b.prefix + "." + e.Type

	b.track(e)

	if _, err := b.js.PublishAsync(subject, e.Frame, natsjs.WithStallWait(publishStallWait)); err != nil {
		b.untrack(e)

		if errors.Is(err, natsjs.ErrTooManyStalledMsgs) {
			metrics.BridgeDrop(b.name, metrics.DropStalled)
		} else {
			metrics.BridgeDrop(b.name, metrics.DropSyncError)
		}

		e.Release()

		return fmt.Errorf("publish %s: %w", subject, err)
	}

	metrics.Payload(b.name, len(e.Frame))
	metrics.Published(b.name, e.Type)

	return nil
}

func (b *Bridge) Close(ctx context.Context) error {
	if b.stopped.Swap(true) {
		return nil
	}

	b.closing.Store(true)
	close(b.stop)

	var err error

	select {
	case <-b.js.PublishAsyncComplete():
	case <-ctx.Done():
		err = fmt.Errorf("%d publishes still unacknowledged at shutdown", b.js.PublishAsyncPending())
		b.js.CleanupPublisher()
	}

	// Anything still tracked will never get an ack or error callback.
	b.mu.Lock()
	leftover := b.pending
	b.pending = make(map[uintptr]pendingEvent)
	b.mu.Unlock()

	var leftoverBytes int64
	for _, p := range leftover {
		leftoverBytes += int64(len(p.event.Frame))
		metrics.BridgeDrop(b.name, metrics.DropShutdown)
		p.event.Release()
	}

	b.pendingBytes.Add(-leftoverBytes)

	b.nc.Close()

	return err
}

func (b *Bridge) onAck(_ natsjs.JetStream, msg *nats.Msg, _ *natsjs.PubAck) {
	if p, ok := b.take(msg.Data); ok {
		metrics.Ack(b.name, p.event.Type, time.Since(p.enqueuedAt))
		p.event.Release()
	}

	b.signalRoom()
}

func (b *Bridge) onError(_ natsjs.JetStream, msg *nats.Msg, err error) {
	b.ackFailures.Record("async_ack", err, "subject", msg.Subject)
	metrics.BridgeDrop(b.name, metrics.DropAckError)

	if p, ok := b.take(msg.Data); ok {
		p.event.Release()
	}

	b.signalRoom()
}

func (b *Bridge) track(e *bridge.Event) {
	b.mu.Lock()
	b.pending[frameKey(e.Frame)] = pendingEvent{event: e, enqueuedAt: time.Now()}
	b.mu.Unlock()

	b.pendingBytes.Add(int64(len(e.Frame)))
}

func (b *Bridge) untrack(e *bridge.Event) {
	b.mu.Lock()
	delete(b.pending, frameKey(e.Frame))
	b.mu.Unlock()

	b.pendingBytes.Add(-int64(len(e.Frame)))
}

func (b *Bridge) take(frame []byte) (pendingEvent, bool) {
	if len(frame) == 0 {
		return pendingEvent{}, false
	}

	key := frameKey(frame)

	b.mu.Lock()
	p, ok := b.pending[key]
	delete(b.pending, key)
	b.mu.Unlock()

	if ok {
		b.pendingBytes.Add(-int64(len(frame)))
	}

	return p, ok
}

func (b *Bridge) InFlightBytes() int64 {
	return b.pendingBytes.Load()
}

func (b *Bridge) signalRoom() {
	select {
	case b.room <- struct{}{}:
	default:
	}
}

func frameKey(frame []byte) uintptr {
	return uintptr(unsafe.Pointer(unsafe.SliceData(frame)))
}
