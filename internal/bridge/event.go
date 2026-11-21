package bridge

import (
	"sync"
	"sync/atomic"

	"github.com/ShadyDonkey/portcullis/internal/framepool"
)

type Event struct {
	Frame      []byte
	Type       string
	Shard      int
	Sequence   int
	ReceivedAt int64

	refs atomic.Int64
}

var eventPool = sync.Pool{
	New: func() any {
		return new(Event)
	},
}

func NewEvent(frame []byte, eventType string, shard int, sequence int, receivedAt int64) *Event {
	e := eventPool.Get().(*Event)
	e.Frame = frame
	e.Type = eventType
	e.Shard = shard
	e.Sequence = sequence
	e.ReceivedAt = receivedAt

	e.refs.Store(1)

	return e
}

func (e *Event) Retain() {
	if e.refs.Add(1) <= 1 {
		// TODO: don't use panic?
		panic("bridge: retain called on a released Event")
	}
}

func (e *Event) Release() {
	n := e.refs.Add(-1)

	switch {
	case n > 0:
		return
	case n < 0:
		panic("bridge: Event released more times than it was retained")
	}

	framepool.Put(e.Frame)

	e.Frame = nil
	e.Type = ""
	e.Shard = 0
	e.Sequence = 0
	e.ReceivedAt = 0

	eventPool.Put(e)
}
