package jetstream

import (
	"log/slog"
	"sync"
	"time"
)

type failureLog struct {
	mu        sync.Mutex
	lastAt    time.Time
	sinceLast uint64
}

func (l *failureLog) record(kind string, err error, kv ...any) {
	l.mu.Lock()
	now := time.Now()
	l.sinceLast++
	count := l.sinceLast
	gap := now.Sub(l.lastAt)
	loud := l.lastAt.IsZero() || gap >= 2*publishFailureLogInterval
	due := loud || gap >= publishFailureLogInterval

	if due {
		l.lastAt = now
		l.sinceLast = 0
	}

	l.mu.Unlock()

	if !due {
		return
	}

	if loud {
		args := append([]any{"kind", kind, "err", err}, kv...)
		slog.Error("publish failure", args...)
		return
	}

	slog.Warn("publish failures ongoing, throttled", "kind", kind, "count", count, "err", err)
}
