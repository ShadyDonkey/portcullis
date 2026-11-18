package jetstream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	natsMaxReconnects         = 60
	natsReconnectWait         = 2 * time.Second
	publishAsyncAckTimeout    = 10 * time.Second
	publishCompleteWait       = publishAsyncAckTimeout + time.Second
	publishStallWait          = 1 * time.Millisecond
	publishFailureLogInterval = 10 * time.Second
	publishMaxPending         = 4096
	inFlightByteCap           = 128 << 20
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
	natsReconnects  atomic.Uint64
	stallFailures   failureLog
	syncFailures    failureLog
	ackFailures     failureLog

	pubMu         sync.Mutex
	ring          [publishMaxPending]ringSlot
	ringHead      int
	ringLen       int
	ringSignal    chan struct{}
	ringOccupancy atomic.Int64

	inFlightBytes atomic.Int64

	publishSignal chan struct{}
	stopCh        chan struct{}
	drainStopped  chan struct{}
	drainDone     chan struct{}

	ringEvictions atomic.Uint64
	byteCapDrops  atomic.Uint64
	dropFailures  failureLog
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
		ringSignal:      make(chan struct{}, 1),
		publishSignal:   make(chan struct{}, 1),
		stopCh:          make(chan struct{}),
		drainStopped:    make(chan struct{}),
		drainDone:       make(chan struct{}),
	}

	nc, err := dialNATS(config, j)
	if err != nil {
		return nil, err
	}

	ready := false
	defer func() {
		if !ready {
			j.closing.Store(true)
			nc.Close()
		}
	}()

	js, err := newPublisher(nc, j)
	if err != nil {
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
		return nil, err
	}

	j.nc = nc
	j.kv = kv
	j.js = js

	go j.publishDrain()
	ready = true

	return j, nil
}

func (j *JetStream) UnexpectedClose() <-chan error {
	return j.unexpectedClose
}

func (j *JetStream) NATSReconnects() uint64 {
	return j.natsReconnects.Load()
}

func (j *JetStream) Close() {
	if j.closing.Swap(true) {
		return
	}

	j.pubMu.Lock()
	close(j.stopCh)
	j.pubMu.Unlock()

	select {
	case <-j.drainDone:
	case <-time.After(publishCompleteWait):
		slog.Error("timed out draining publish ring", "ring", j.ringOccupancy.Load())
	}

	close(j.drainStopped)

	select {
	case <-j.js.PublishAsyncComplete():
	case <-time.After(publishCompleteWait):
		slog.Error(
			"timed out waiting for pending publishes, cleaning up publisher",
			"pending", j.js.PublishAsyncPending(),
		)
		j.js.CleanupPublisher()

		select {
		case <-j.js.PublishAsyncComplete():
		case <-time.After(time.Second):
			slog.Error("publisher cleanup did not complete")
		}
	}

	if j.nc.Status() == nats.CLOSED {
		slog.Info("NATS connection previously closed, skipping drain.")
		return
	}

	if err := j.nc.Drain(); err != nil {
		slog.Error("NATS connection drain failed", "err", err)
	}
}
