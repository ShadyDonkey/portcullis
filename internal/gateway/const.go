package gateway

import (
	"time"
)

const (
	Version            = 10
	DefaultEncoding    = "json"
	identifyInterval   = 5*time.Second + 500*time.Millisecond
	EventTypeMaxLength = 40
)
