package bridge

import (
	"log/slog"
	"sync"
	"time"
)

const failureLogInterval = 10 * time.Second

type FailureLog struct {
	logger    *slog.Logger
	mu        sync.Mutex
	lastAt    time.Time
	sinceLast uint64
}

func NewFailureLog(logger *slog.Logger) *FailureLog {
	if logger == nil {
		logger = slog.Default()
	}

	return &FailureLog{
		logger: logger,
	}
}

func (l *FailureLog) Record(kind string, err error, kv ...any) {
	l.mu.Lock()
	now := time.Now()
	l.sinceLast++
	count := l.sinceLast
	gap := now.Sub(l.lastAt)
	loud := l.lastAt.IsZero() || gap >= 2*failureLogInterval
	due := loud || gap >= failureLogInterval

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
		l.logger.Error("delivery failure", args...)
		return
	}

	l.logger.Warn("delivery failures ongoing, throttled", "kind", kind, "count", count, "err", err)
}
