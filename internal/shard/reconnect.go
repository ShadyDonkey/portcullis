package shard

import (
	"math/rand/v2"
	"time"
)

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
