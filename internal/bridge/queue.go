package bridge

import "sync"

const (
	DefaultQueueSlots   = 4096
	DefaultQueueByteCap = 128 << 20
)

const (
	DropByteCap     = "byte_cap"
	DropRingEvicted = "ring_evicted"
	DropAfterClose  = "after_close"
	DropShutdown    = "shutdown"
)

type queue struct {
	mu      sync.Mutex
	ring    []*Event
	head    int
	n       int
	bytes   int64
	byteCap int64
	closed  bool

	wake   chan struct{}
	onDrop func(reason string)
}

func newQueue(slots int, byteCap int64, onDrop func(string)) *queue {
	if onDrop == nil {
		onDrop = func(string) {}
	}

	return &queue{
		ring:    make([]*Event, slots),
		byteCap: byteCap,
		wake:    make(chan struct{}, 1),
		onDrop:  onDrop,
	}
}

func (q *queue) push(e *Event) {
	size := int64(len(e.Frame))

	q.mu.Lock()

	if q.closed {
		q.mu.Unlock()
		q.dropIncoming(DropAfterClose, e)
		return
	}

	if size > q.byteCap {
		q.mu.Unlock()
		q.dropIncoming(DropByteCap, e)
		return
	}

	for q.bytes+size > q.byteCap {
		q.evictOldestLocked(DropByteCap)
	}

	if q.n == len(q.ring) {
		q.evictOldestLocked(DropRingEvicted)
	}

	q.ring[(q.head+q.n)%len(q.ring)] = e
	q.n++
	q.bytes += size
	q.mu.Unlock()

	q.signal()
}

func (q *queue) pop() (*Event, bool) {
	for {
		q.mu.Lock()

		if q.n > 0 {
			e := q.takeOldestLocked()
			q.mu.Unlock()
			return e, true
		}

		closed := q.closed
		q.mu.Unlock()

		if closed {
			return nil, false
		}

		<-q.wake
	}
}

func (q *queue) close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.signal()
}

func (q *queue) abandon() {
	q.mu.Lock()
	defer q.mu.Unlock()

	for q.n > 0 {
		q.evictOldestLocked(DropShutdown)
	}
}

func (q *queue) queuedBytes() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.bytes
}

func (q *queue) takeOldestLocked() *Event {
	e := q.ring[q.head]
	q.ring[q.head] = nil
	q.head = (q.head + 1) % len(q.ring)
	q.n--
	q.bytes -= int64(len(e.Frame))

	return e
}

func (q *queue) evictOldestLocked(reason string) {
	e := q.takeOldestLocked()
	q.onDrop(reason)
	e.Release()
}

func (q *queue) dropIncoming(reason string, e *Event) {
	q.onDrop(reason)
	e.Release()
}

func (q *queue) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}
