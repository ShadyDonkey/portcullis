package jetstream

import (
	"time"
	"unsafe"

	"github.com/ShadyDonkey/portcullis/internal/metrics"
)

type ackInfo struct {
	enqueuedAt time.Time
	eventType  string
}

func ackKey(frame []byte) uintptr {
	return uintptr(unsafe.Pointer(unsafe.SliceData(frame)))
}

func (j *JetStream) trackAck(eventType string, frame []byte, at time.Time) {
	if len(frame) == 0 {
		return
	}

	info := ackInfo{enqueuedAt: at, eventType: eventType}

	j.ackMu.Lock()
	j.ackPending[ackKey(frame)] = info
	j.ackMu.Unlock()
}

func (j *JetStream) untrackAck(frame []byte) {
	if len(frame) == 0 {
		return
	}

	j.ackMu.Lock()
	delete(j.ackPending, ackKey(frame))
	j.ackMu.Unlock()
}

func (j *JetStream) completeAck(frame []byte) {
	if len(frame) == 0 {
		return
	}

	j.ackMu.Lock()
	info, tracked := j.ackPending[ackKey(frame)]
	j.ackMu.Unlock()

	if !tracked {
		return
	}

	metrics.Ack(info.eventType, time.Since(info.enqueuedAt))
}
