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

// isFatalCloseCode reports if reconnecting can not succeed without human
// intervention. Docs say to stop attempting to reconnect on these to avoid
// infinite loops.
func isFatalCloseCode(code int) bool {
	switch code {
	case 4004, 4010, 4011, 4012, 4013, 4014:
		return true
	default:
		return false
	}
}

// closeCodeHint returns one actionable sentence for the gateway close
// codes for the end user.
// Source: https://docs.discord.com/developers/topics/opcodes-and-status-codes#gateway-gateway-close-event-codes
func closeCodeHint(code int) string {
	switch code {
	case 4004:
		return "the bot token is invalid or was reset"
	case 4010:
		return "the shard id / shard count sent when identifying is invalid"
	case 4011:
		return "the bot is in too many guilds for the current shard count"
	case 4012:
		return "invalid API version"
	case 4013:
		return "the intents bitmask is invalid"
	case 4014:
		return "enable the privileged intents for this application in the Discord Developer Portal or remove them from the configured intents"
	default:
		return "unknown close code"
	}
}
