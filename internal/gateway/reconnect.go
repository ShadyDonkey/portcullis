package gateway

import (
	"errors"
	"math/rand/v2"
	"time"
)

type exitAction int

const (
	exitStop exitAction = iota
	exitResume
	exitReidentify
)

func (a exitAction) String() string {
	switch a {
	case exitStop:
		return "stop"
	case exitResume:
		return "resume"
	case exitReidentify:
		return "reidentify"
	default:
		return "unknown"
	}
}

func errorToExitAction(err error) exitAction {
	var closeErr *CloseError
	if errors.As(err, &closeErr) {
		switch {
		case isFatalCloseCode(closeErr.Code):
			return exitStop
		case closeErr.Resumable:
			return exitResume
		default:
			return exitReidentify
		}
	}

	var invalid *InvalidSessionError
	if errors.As(err, &invalid) {
		if invalid.Resumable {
			return exitResume
		}

		return exitReidentify
	}

	return exitResume
}

type backoff struct {
	currentDelay time.Duration
}

func (b *backoff) reset() {
	b.currentDelay = 0
}

func (b *backoff) next() time.Duration {
	if b.currentDelay == 0 {
		b.currentDelay = reconnectMinDelay
	} else {
		b.currentDelay = min(b.currentDelay*2, reconnectMaxDelay)
	}

	return b.currentDelay/2 + rand.N(b.currentDelay/2)
}

func reidentifyDelay() time.Duration {
	return time.Second + rand.N(4*time.Second)
}
