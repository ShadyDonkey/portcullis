package jetstream

import (
	"log/slog"
	"sync"
	"time"
)

type failureLog struct {
	mu        sync.Mutex
	logged    bool
	lastAt    time.Time
	sinceLast uint64
}

func (l *failureLog) record(kind string, err error, kv ...any) {
	l.mu.Lock()
	l.sinceLast++
	count := l.sinceLast
	gap := time.Since(l.lastAt)
	loud := !l.logged || gap >= 2*publishFailureLogInterval
	due := loud || gap >= publishFailureLogInterval

	if due {
		l.logged = true
		l.lastAt = time.Now()
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
