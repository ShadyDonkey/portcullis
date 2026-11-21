package bridge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/ShadyDonkey/portcullis/internal/metrics"
)

type Entry struct {
	Name   string
	Bridge Bridge
}

type Options struct {
	Slots   int
	ByteCap int64
	OnDrop  func(bridge, reason string)
	OnFatal func(error)
	Logger  *slog.Logger
}

type Fanout struct {
	runners []*runner
	fatal   func(error)
	logger  *slog.Logger
}

type runner struct {
	name     string
	b        Bridge
	q        *queue
	fail     *FailureLog
	log      *slog.Logger
	fatal    func(error)
	done     chan struct{}
	inFlight atomic.Int64
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

	onDrop := opts.OnDrop
	if onDrop == nil {
		onDrop = func(string, string) {}
	}

	f := &Fanout{
		runners: make([]*runner, 0, len(entries)),
		fatal:   opts.OnFatal,
		logger:  logger,
	}

	for _, entry := range entries {
		bridgeLog := logger.With("bridge", entry.Name)

		r := &runner{
			name: entry.Name,
			b:    entry.Bridge,
			q: newQueue(
				slots, byteCap,
				func(reason string) {
					onDrop(entry.Name, reason)
				},
			),
			fail:  NewFailureLog(bridgeLog),
			log:   bridgeLog,
			fatal: opts.OnFatal,
			done:  make(chan struct{}),
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

		if !r.deliverOnce(e) {
			return
		}
	}
}

func (r *runner) deliverOnce(e *Event) (ok bool) {
	defer func() {
		if rec := recover(); rec != nil {
			FatalInvariant(r.fatal, r.log, r.name, "deliver", rec)
			ok = false
		}
	}()

	metrics.BridgeConsumed(r.name, e.Type)

	size := int64(len(e.Frame))
	r.inFlight.Add(size)
	err := r.b.Deliver(e)
	r.inFlight.Add(-size)

	if err != nil {
		r.fail.Record("deliver", err)
	}

	return true
}

func (f *Fanout) Send(e *Event) {
	defer func() {
		FatalInvariant(f.fatal, f.logger, "fanout", "Send", recover())
	}()

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
		total += r.inFlight.Load()

		if rep, ok := r.b.(InFlightReporter); ok {
			total += rep.InFlightBytes()
		}
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
			func() {
				defer func() {
					if rec := recover(); rec != nil {
						LogInvariant(f.logger, r.name, "queue abandon", rec)
					}
				}()

				r.q.abandon()
			}()
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
