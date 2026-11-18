package jetstream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ShadyDonkey/portcullis/internal/framepool"
	"github.com/ShadyDonkey/portcullis/internal/gateway"
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
	inFlightByteCap           = 128 << 20
	pendingCap                = publishMaxPending + 1
	reaperTick                = 1 * time.Millisecond
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

type ringSlot struct {
	eventType    [gateway.EventTypeMaxLength]byte
	eventTypeLen int
	frame        []byte
}

type pendingSlot struct {
	ok    <-chan *jetstream.PubAck
	err   <-chan error
	frame []byte
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

	pubMu         sync.Mutex
	ring          [publishMaxPending]ringSlot
	ringHead      int
	ringLen       int
	ringSignal    chan struct{}
	ringOccupancy atomic.Int64

	pendMu     sync.Mutex
	pending    [pendingCap]pendingSlot
	pendHead   int
	pendLen    int
	pendSignal chan struct{}

	inFlightBytes atomic.Int64

	stopCh       chan struct{}
	drainStopped chan struct{}
	drainDone    chan struct{}
	reaperDone   chan struct{}

	ringEvictions          atomic.Uint64
	byteCapDrops           atomic.Uint64
	eventTypeOverflowDrops atomic.Uint64
	dropFailures           failureLog
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
		pendSignal:      make(chan struct{}, 1),
		stopCh:          make(chan struct{}),
		drainStopped:    make(chan struct{}),
		drainDone:       make(chan struct{}),
		reaperDone:      make(chan struct{}),
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
		j.closing.Store(true)
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

	go j.publishDrain()
	go j.publishReaper()

	return j, nil
}

func (j *JetStream) UnexpectedClose() <-chan error {
	return j.unexpectedClose
}

func (j *JetStream) buildSubject(eventType []byte) string {
	return fmt.Sprintf("%s.%s", j.subjectPrefix, eventType)
}

func (j *JetStream) Publish(
	ctx context.Context, eventType [gateway.EventTypeMaxLength]byte, eventTypeLen int, frame []byte,
) error {
	if eventTypeLen == 0 || eventTypeLen > len(eventType) {
		j.eventTypeOverflowDrops.Add(1)
		err := fmt.Errorf("publish: event type length %d out of range", eventTypeLen)
		// TODO: maybe add these strings to a const list
		j.dropFailures.record("event_type_overflow", err)
		j.releaseFrame(frame, true)

		return err
	}

	size := int64(len(frame))
	var logKind string
	var logErr error
	var byteCapEvicted bool

	j.pubMu.Lock()

	if j.isStopping() {
		j.pubMu.Unlock()
		j.dropFailures.record("publish_after_close", errors.New("publisher is closed"))
		j.releaseFrame(frame, true)

		return errors.New("publish: publisher is closed")
	}

	for j.inFlightBytes.Load()+size > inFlightByteCap {
		if j.ringLen == 0 {
			j.byteCapDrops.Add(1)
			j.pubMu.Unlock()
			j.dropFailures.record("byte_cap_drop", jetstream.ErrTooManyStalledMsgs)
			j.releaseFrame(frame, true)
			return fmt.Errorf("publish: %w", jetstream.ErrTooManyStalledMsgs)
		}

		j.evictOldestLocked(true)
		byteCapEvicted = true
	}

	if j.ringLen == len(j.ring) {
		j.evictOldestLocked(false)
		logKind = "ring_eviction"
		logErr = jetstream.ErrTooManyStalledMsgs
	}

	slot := &j.ring[(j.ringHead+j.ringLen)%len(j.ring)]
	slot.eventType = eventType
	slot.eventTypeLen = eventTypeLen
	slot.frame = frame

	j.ringLen++
	j.ringOccupancy.Store(int64(j.ringLen))
	j.inFlightBytes.Add(size)
	j.pubMu.Unlock()

	if logKind != "" {
		j.dropFailures.record(logKind, logErr)
	}

	if byteCapEvicted {
		j.dropFailures.record("byte_cap_eviction", jetstream.ErrTooManyStalledMsgs)
	}

	signal(j.ringSignal)

	return nil
}

func (j *JetStream) evictOldestLocked(byteCap bool) {
	slot := &j.ring[j.ringHead]
	frame := slot.frame
	*slot = ringSlot{}
	j.ringHead = (j.ringHead + 1) % len(j.ring)
	j.ringLen--
	j.ringOccupancy.Store(int64(j.ringLen))
	j.ringEvictions.Add(1)

	if byteCap {
		j.byteCapDrops.Add(1)
	}

	j.releaseFrame(frame, true)
}

func (j *JetStream) releaseFrame(frame []byte, pool bool) {
	if len(frame) == 0 {
		return
	}

	if pool {
		framepool.Put(frame)
	}

	j.inFlightBytes.Add(-int64(len(frame)))
}

func (j *JetStream) tryPopRing() (ringSlot, bool) {
	j.pubMu.Lock()
	defer j.pubMu.Unlock()

	if j.ringLen == 0 {
		return ringSlot{}, false
	}

	slot := j.ring[j.ringHead]
	j.ring[j.ringHead] = ringSlot{}
	j.ringHead = (j.ringHead + 1) % len(j.ring)
	j.ringLen--
	j.ringOccupancy.Store(int64(j.ringLen))

	return slot, true
}

func (j *JetStream) popRing() (ringSlot, bool) {
	for {
		if slot, ok := j.tryPopRing(); ok {
			return slot, true
		}

		select {
		case <-j.ringSignal:
		case <-j.stopCh:
			return ringSlot{}, false
		}
	}
}

func (j *JetStream) hasSubmitRoom() bool {
	j.pendMu.Lock()
	room := j.pendLen < len(j.pending)
	j.pendMu.Unlock()

	return room && j.js.PublishAsyncPending() < publishMaxPending
}

func (j *JetStream) submit(slot ringSlot) {
	if j.noMoreSubmits() {
		j.dropSlot(slot, "shutdown_drop")
		return
	}

	subject := j.buildSubject(slot.eventType[:slot.eventTypeLen])
	future, err := j.js.PublishAsync(subject, slot.frame, jetstream.WithStallWait(publishStallWait))

	if err != nil {
		if errors.Is(err, jetstream.ErrTooManyStalledMsgs) {
			j.stalledDrops.Add(1)
			j.stallFailures.record("stalled_drop", err)
		} else {
			j.syncErrors.Add(1)
			j.syncFailures.record("sync_publish", err, "subject", subject)
		}

		j.releaseFrame(slot.frame, true)
		return
	}

	j.pushPending(future.Ok(), future.Err(), slot.frame)
}

func (j *JetStream) pushPending(ok <-chan *jetstream.PubAck, err <-chan error, frame []byte) {
	j.pendMu.Lock()

	if j.pendLen == len(j.pending) {
		j.pendMu.Unlock()
		j.releaseFrame(frame, false)
		slog.Error("pending registry full, dropped publish", "pending", pendingCap)
		return
	}

	slot := &j.pending[(j.pendHead+j.pendLen)%len(j.pending)]
	slot.ok = ok
	slot.err = err
	slot.frame = frame

	j.pendLen++
	j.pendMu.Unlock()
	signal(j.pendSignal)
}

func (j *JetStream) dropSlot(slot ringSlot, kind string) {
	j.dropFailures.record(kind, errors.New("publish dropped at shutdown"))
	j.releaseFrame(slot.frame, true)
}

func (j *JetStream) publishDrain() {
	ticker := time.NewTicker(publishStallWait)
	defer ticker.Stop()
	defer close(j.drainDone)

	for {
		slot, ok := j.popRing()

		if !ok {
			return
		}

		for !j.hasSubmitRoom() {
			select {
			case <-j.pendSignal:
			case <-ticker.C:
			case <-j.stopCh:
				j.submit(slot)
				j.drainRemaining()
				return
			}
		}

		j.submit(slot)
	}
}

func (j *JetStream) drainRemaining() {
	for {
		slot, ok := j.tryPopRing()

		if !ok {
			return
		}

		if !j.hasSubmitRoom() {
			j.dropSlot(slot, "shutdown_drop")
			continue
		}

		j.submit(slot)
	}
}

func futureResolved(ok <-chan *jetstream.PubAck, err <-chan error) bool {
	select {
	case <-ok:
		return true
	case <-err:
		return true
	default:
		return false
	}
}

func (j *JetStream) publishReaper() {
	ticker := time.NewTicker(reaperTick)
	defer ticker.Stop()
	defer close(j.reaperDone)

	drainFinished := false

	for {
		j.pendMu.Lock()

		for j.pendLen > 0 {
			head := &j.pending[j.pendHead]
			if !futureResolved(head.ok, head.err) {
				break
			}

			frame := head.frame
			*head = pendingSlot{}
			j.pendHead = (j.pendHead + 1) % len(j.pending)
			j.pendLen--
			j.pendMu.Unlock()
			j.releaseFrame(frame, true)
			signal(j.pendSignal)
			j.pendMu.Lock()
		}

		empty := j.pendLen == 0
		j.pendMu.Unlock()

		if j.isStopping() {
			if !drainFinished {
				select {
				case <-j.drainStopped:
					drainFinished = true
				case <-ticker.C:
				}
			} else {
				<-ticker.C
			}

			if drainFinished && empty {
				return
			}

			continue
		}

		<-ticker.C
	}

}

func (j *JetStream) forceReleaseAll() {
	j.pendMu.Lock()
	n := j.pendLen

	for i := 0; i < n; i++ {
		slot := &j.pending[j.pendHead]
		frame := slot.frame
		*slot = pendingSlot{}
		j.pendHead = (j.pendHead + 1) % len(j.pending)
		j.pendLen--
		j.pendMu.Unlock()
		j.releaseFrame(frame, false)
		j.pendMu.Lock()
	}

	j.pendMu.Unlock()
	slog.Error("forced release of pending publishes at shutdown", "dropped", n)
}

func (j *JetStream) isStopping() bool {
	select {
	case <-j.stopCh:
		return true
	default:
		return false
	}
}

func (j *JetStream) noMoreSubmits() bool {
	select {
	case <-j.drainStopped:
		return true
	default:
		return false
	}
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

type PublishStats struct {
	StalledDrops           uint64
	SyncErrors             uint64
	AckErrors              uint64
	RingEvictions          uint64
	ByteCapDrops           uint64
	EventTypeOverflowDrops uint64
	InFlightBytes          uint64
	RingOccupancy          uint64
}

func (j *JetStream) PublishStats() PublishStats {
	return PublishStats{
		StalledDrops:           j.stalledDrops.Load(),
		SyncErrors:             j.syncErrors.Load(),
		AckErrors:              j.ackErrors.Load(),
		RingEvictions:          j.ringEvictions.Load(),
		ByteCapDrops:           j.byteCapDrops.Load(),
		EventTypeOverflowDrops: j.eventTypeOverflowDrops.Load(),
		InFlightBytes:          uint64(j.inFlightBytes.Load()),
		RingOccupancy:          uint64(j.ringOccupancy.Load()),
	}
}

func (j *JetStream) Close() {
	if j.closing.Swap(true) {
		return
	}

	close(j.stopCh)

	select {
	case <-j.drainDone:
	case <-time.After(publishCompleteWait):
		slog.Error("timed out draining publish ring", "ring", j.ringOccupancy.Load())
	}

	close(j.drainStopped)

	select {
	case <-j.reaperDone:
	case <-time.After(publishCompleteWait):
		slog.Error(
			"timed out waiting for pending publishes to complete, forcing drain", "pending", j.js.PublishAsyncPending(),
		)
		j.forceReleaseAll()

		select {
		case <-j.reaperDone:
		case <-time.After(time.Second):
			slog.Error("reaper did not exit after forced release")
		}
	}

	select {
	case <-j.js.PublishAsyncComplete():
	default:
		slog.Error("pending publishes still unresolved at shutdown", "pending", j.js.PublishAsyncPending())
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
