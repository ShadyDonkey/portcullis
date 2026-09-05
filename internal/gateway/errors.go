package gateway

import (
	"errors"
	"fmt"
)

var (
	ErrNoHeartbeatAck     = errors.New("heartbeat ack not received")
	ErrReconnectRequested = errors.New("gateway requested reconnect")
	ErrHelloTimeout       = errors.New("hello not received in time")
)

type InvalidSessionError struct {
	Resumable bool
}

func (e *InvalidSessionError) Error() string {
	if e.Resumable {
		return "invalid session, resumable"
	}
	return "invalid session, re-identify required"
}

type CloseError struct {
	Code      int
	Resumable bool
	Err       error
}

func (e *CloseError) Error() string {
	return fmt.Sprintf("gateway close code %d (resumable=%t): %v", e.Code, e.Resumable, e.Err)
}

func (e *CloseError) Unwrap() error { return e.Err }

// isResumableCloseCode reports whether Discord documents the given gateway
// close code as one the client may resume/reconnect from.
// Source: https://docs.discord.com/developers/topics/opcodes-and-status-codes#gateway-gateway-close-event-codes
func isResumableCloseCode(code int) bool {
	switch code {
	case 4000, 4001, 4002, 4003, 4005, 4007, 4008, 4009:
		return true
	case 4004, 4006, 4010, 4011, 4012, 4013, 4014:
		return false
	default:
		return false

	}
}
