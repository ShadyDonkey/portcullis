package gateway

import (
	"errors"
	"math/rand/v2"
	"time"
)

type ExitAction int

const (
	ExitStop ExitAction = iota
	ExitResume
	ExitReidentify
)

func (a ExitAction) String() string {
	switch a {
	case ExitStop:
		return "stop"
	case ExitResume:
		return "resume"
	case ExitReidentify:
		return "reidentify"
	default:
		return "unknown"
	}
}

func ErrorToExitAction(err error) ExitAction {
	if closeErr, ok := errors.AsType[*CloseError](err); ok {
		switch {
		case isFatalCloseCode(closeErr.Code):
			return ExitStop
		case closeErr.Resumable:
			return ExitResume
		default:
			return ExitReidentify
		}
	}

	if invalid, ok := errors.AsType[*InvalidSessionError](err); ok {
		if invalid.Resumable {
			return ExitResume
		}

		return ExitReidentify
	}

	return ExitResume
}

func ReidentifyDelay() time.Duration {
	return time.Second + rand.N(4*time.Second)
}
