package bridge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

type Entry struct {
	Name   string
	Bridge Bridge
}

type Options struct {
	Slots   int
	ByteCap int64
	OnDrop  func(reason string)
	Logger  *slog.Logger
}

type Fanout struct {
	runners []*runner
}

type runner struct {
	name string
	b    Bridge
	q    *queue
	fail *FailureLog
	done chan struct{}
}

func NewFanout(entries []Entry, opts Options) (*Fanout, error) {
	if len(entries) == 0 {
		return nil, errors.New("bridge: fanout needs at least one bridge")
	}

	slots := opts.Slots
	if slots <= 0 {
		slots = DefaultQueueSlots
	}

	byteCap := opts.ByteCap
	if byteCap <= 0 {
		byteCap = DefaultQueueByteCap
	}

	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	f := &Fanout{runners: make([]*runner, 0, len(entries))}

	for _, entry := range entries {
		r := &runner{
			name: entry.Name,
			b:    entry.Bridge,
			q:    newQueue(slots, byteCap, opts.OnDrop),
			fail: NewFailureLog(logger.With("bridge", entry.Name)),
			done: make(chan struct{}),
		}

		f.runners = append(f.runners, r)
		go r.run()
	}

	return f, nil
}

func (r *runner) run() {
	defer close(r.done)

	for {
		e, ok := r.q.pop()
		if !ok {
			return
		}

		if err := r.b.Deliver(e); err != nil {
			r.fail.Record("deliver", err)
		}
	}
}

func (f *Fanout) Send(e *Event) {
	for range len(f.runners) - 1 {
		e.Retain()
	}

	for _, r := range f.runners {
		r.q.push(e)
	}
}

func (f *Fanout) InFlightBytes() int64 {
	var total int64
	for _, r := range f.runners {
		total += r.q.queuedBytes()
	}

	return total
}

func (f *Fanout) Close(ctx context.Context) error {
	for _, r := range f.runners {
		r.q.close()
	}

	for _, r := range f.runners {
		select {
		case <-r.done:
		case <-ctx.Done():
			r.q.abandon()
		}
	}

	var errs []error
	for _, r := range f.runners {
		if err := r.b.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("bridge %s: %w", r.name, err))
		}
	}

	return errors.Join(errs...)
}
