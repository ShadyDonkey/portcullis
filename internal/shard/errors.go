package shard

import (
	"errors"
)

var (
	ErrNoHeartbeatAck     = errors.New("heartbeat ack not received")
	ErrReconnectRequested = errors.New("gateway requested reconnect")
	ErrHelloTimeout       = errors.New("hello not received in time")
	ErrControlChannelFull = errors.New("control channel full")
)
