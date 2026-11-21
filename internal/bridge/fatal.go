package bridge

import (
	"fmt"
	"log/slog"
	"runtime/debug"

	"github.com/ShadyDonkey/portcullis/internal/metrics"
)

func FatalInvariant(hook func(error), logger *slog.Logger, component, where string, r any) {
	if r == nil {
		return
	}

	err := reportInvariant(logger, component, where, r)

	if hook == nil {
		panic(err)
	}

	hook(err)
}

func LogInvariant(logger *slog.Logger, component, where string, r any) {
	if r == nil {
		return
	}

	_ = reportInvariant(logger, component, where, r)
}

func reportInvariant(logger *slog.Logger, component, where string, r any) error {
	err := fmt.Errorf("%s: event lifetime invariant violated in %s: %v", component, where, r)

	metrics.InvariantViolation(component)
	logger.Error("event lifetime tripwire fired", "err", err, "stack", string(debug.Stack()))

	return err
}
