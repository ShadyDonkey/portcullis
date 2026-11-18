package gateway

import (
	"context"
	"time"
)

type identifyBucket struct {
	sem  chan struct{}
	last time.Time
}

type IdentifyLimiter struct {
	buckets  []*identifyBucket
	interval time.Duration
}

func NewIdentifyLimiter(maxConcurrency int) *IdentifyLimiter {
	if maxConcurrency < 1 {
		maxConcurrency = 1
	}

	limiter := &IdentifyLimiter{
		buckets:  make([]*identifyBucket, maxConcurrency),
		interval: identifyInterval,
	}

	for i := range limiter.buckets {
		limiter.buckets[i] = &identifyBucket{
			sem: make(chan struct{}, 1),
		}
	}

	return limiter
}

func (l *IdentifyLimiter) Wait(ctx context.Context, shardID int) error {
	b := l.buckets[shardID%len(l.buckets)]

	select {
	case b.sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-b.sem }()

	if wait := l.interval - time.Since(b.last); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()

		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	b.last = time.Now()
	return nil
}
