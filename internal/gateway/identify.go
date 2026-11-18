package gateway

import (
	"context"
	"time"
)

type identifyBucket struct {
	sem  chan struct{}
	last time.Time
}

type bucketLimiter struct {
	buckets  []*identifyBucket
	interval time.Duration
}

func newIdentifyBucketLimiter(maxConcurrency int) *bucketLimiter {
	if maxConcurrency < 1 {
		maxConcurrency = 1
	}

	bl := &bucketLimiter{
		buckets:  make([]*identifyBucket, maxConcurrency),
		interval: identifyInterval,
	}

	for i := range bl.buckets {
		bl.buckets[i] = &identifyBucket{
			sem: make(chan struct{}, 1),
		}
	}

	return bl
}

func (bl *bucketLimiter) Wait(ctx context.Context, shardID int) error {
	b := bl.buckets[shardID%len(bl.buckets)]

	select {
	case b.sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-b.sem }()

	if wait := bl.interval - time.Since(b.last); wait > 0 {
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
