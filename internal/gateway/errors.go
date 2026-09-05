package gateway

import "errors"

var (
	ErrNoHeartbeatAck     = errors.New("heartbeat ack not received")
	ErrReconnectRequested = errors.New("gateway requested reconnect")
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
