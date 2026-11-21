package shard

import (
	"time"
)

const (
	helloTimeout    = 10 * time.Second
	controlChanSize = 16

	reconnectMinDelay       = 1 * time.Second
	reconnectMaxDelay       = 60 * time.Second
	reconnectStableAfter    = 2 * time.Minute
	shardShutdownTimeout    = 15 * time.Second
	websocketMaxMessageSize = 128 << 20

	// If this many consecutive connections each live shorter than reconnectStableAfter
	// then reconnecting cannot succeed, stop there instead of burning the daily
	// identify budget on fresh identifies.
	maxConsecutiveUnstableExits = 5

	// Max retries on websocket connections to force stopping when Discord GW is down
	// or some other network failure.
	maxDialRetries = 30
)
