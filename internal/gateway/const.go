package gateway

import (
	"time"
)

// TODO: Clean up these unused consts?
const (
	Version                 = 10
	DefaultEncoding         = "json"
	writeTimeout            = 5 * time.Second
	helloTimeout            = 10 * time.Second
	identifyJitterMax       = 1 * time.Second
	identifyInterval        = 5*time.Second + 500*time.Millisecond
	reconnectMinDelay       = 1 * time.Second
	reconnectMaxDelay       = 60 * time.Second
	reconnectStableAfter    = 2 * time.Minute
	shardShutdownTimeout    = 15 * time.Second
	websocketMaxMessageSize = 128 << 20
	sessionStoreTimeout     = 1 * time.Second
)
