package jetstream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ShadyDonkey/portcullis/internal/framepool"
	"github.com/ShadyDonkey/portcullis/internal/metrics"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	dropReasonByteCapDrop       = "byte_cap_drop"
	dropReasonPublishAfterClose = "publish_after_close"
	dropReasonShutdownDrop      = "shutdown_drop"
)

type ringSlot struct {
	eventType string
	frame     []byte
}

func (j *JetStream) buildSubject(eventType string) string {
	return j.subjectPrefix + "." + eventType
}

func (j *JetStream) Publish(ctx context.Context, eventType string, frame []byte) error {
	size := int64(len(frame))
	metrics.Payload(len(frame))

	j.pubMu.Lock()

	if j.isStopping() {
		j.pubMu.Unlock()
		j.dropFailures.record(dropReasonPublishAfterClose, errors.New("publisher is closed"))
		metrics.PublishDrop(metrics.DropAfterClose)
		j.recycleFrame(frame)
		return errors.New("publish: publisher is closed")
	}

	var byteCapEvicted bool
	var ringEvicted bool

	for j.inFlightBytes.Load()+size > inFlightByteCap {
		if j.ringLen == 0 {
			j.pubMu.Unlock()
			j.dropFailures.record(dropReasonByteCapDrop, jetstream.ErrTooManyStalledMsgs)
			metrics.PublishDrop(metrics.DropByteCap)
			j.recycleFrame(frame)
			return fmt.Errorf("publish: %w", jetstream.ErrTooManyStalledMsgs)
		}

		j.evictOldestLocked(metrics.DropByteCap)
		byteCapEvicted = true
	}

	if j.ringLen == len(j.ring) {
		j.evictOldestLocked(metrics.DropRingEvicted)
		ringEvicted = true
	}

	slot := &j.ring[(j.ringHead+j.ringLen)%len(j.ring)]
	slot.eventType = eventType
	slot.frame = frame

	j.ringLen++
	j.ringOccupancy.Store(int64(j.ringLen))
	j.inFlightBytes.Add(size)
	j.trackAck(eventType, frame, time.Now())
	j.pubMu.Unlock()

	if byteCapEvicted {
		j.dropFailures.record("byte_cap_eviction", jetstream.ErrTooManyStalledMsgs)
	}

	if ringEvicted {
		j.dropFailures.record("ring_eviction", jetstream.ErrTooManyStalledMsgs)
	}

	signal(j.ringSignal)
	return nil
}

func (j *JetStream) evictOldestLocked(reason string) {
	slot := &j.ring[j.ringHead]
	frame := slot.frame
	*slot = ringSlot{}
	j.ringHead = (j.ringHead + 1) % len(j.ring)
	j.ringLen--
	j.ringOccupancy.Store(int64(j.ringLen))
	metrics.PublishDrop(reason)
	j.releaseFrame(frame)
}

func (j *JetStream) recycleFrame(frame []byte) {
	framepool.Put(frame)
}

func (j *JetStream) releaseFrame(frame []byte) {
	j.untrackAck(frame)
	framepool.Put(frame)
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
	return j.js.PublishAsyncPending() < publishMaxPending
}

func (j *JetStream) submit(slot ringSlot) {
	if j.noMoreSubmits() {
		j.dropSlot(slot, dropReasonShutdownDrop)
		return
	}

	subject := j.buildSubject(slot.eventType)
	_, err := j.js.PublishAsync(subject, slot.frame, jetstream.WithStallWait(publishStallWait))
	if err != nil {
		if errors.Is(err, jetstream.ErrTooManyStalledMsgs) {
			j.stallFailures.record("stalled_drop", err)
			metrics.PublishDrop(metrics.DropStalled)
		} else {
			j.syncFailures.record("sync_publish", err, "subject", subject)
			metrics.PublishDrop(metrics.DropSyncError)
		}

		j.releaseFrame(slot.frame)
	}
}

func (j *JetStream) dropSlot(slot ringSlot, kind string) {
	j.dropFailures.record(kind, errors.New("publish dropped at shutdown"))
	metrics.PublishDrop(metrics.DropShutdown)
	j.releaseFrame(slot.frame)
}

func (j *JetStream) publishDrain() {
	defer close(j.drainDone)

	for {
		slot, ok := j.popRing()
		if !ok {
			j.drainRemaining()
			return
		}

		for !j.hasSubmitRoom() {
			select {
			case <-j.publishSignal:
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

		if j.noMoreSubmits() || !j.hasSubmitRoom() {
			j.dropSlot(slot, dropReasonShutdownDrop)
			continue
		}

		j.submit(slot)
	}
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

func (j *JetStream) InFlightBytes() int64 {
	return j.inFlightBytes.Load()
}
