package shard

import (
	"sync/atomic"
)

var (
	parseErrors   atomic.Uint64
	eventOverflow atomic.Uint64
)

func ParseErrors() uint64 {
	return parseErrors.Load()
}

func EventTypeOverflows() uint64 {
	return eventOverflow.Load()
}
