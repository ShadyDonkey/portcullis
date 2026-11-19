package metrics

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
)

const (
	DropFiltered   = "filtered"
	DropParseError = "parse_error"

	DropAfterClose  = "after_close"
	DropShutdown    = "shutdown"
	DropStalled     = "stalled"
	DropSyncError   = "sync_error"
	DropAckError    = "ack_error"
	DropRingEvicted = "ring_evicted"
	DropByteCap     = "byte_cap"
)

var measureCtx = context.Background()

func Consumed(eventType string) {
	eventsConsumed.Add(measureCtx, 1, otelmetric.WithAttributes(attribute.String("event_type", eventType)))
}

func Published(eventType string) {
	eventsPublished.Add(measureCtx, 1, otelmetric.WithAttributes(attribute.String("event_type", eventType)))
}

func Dispatch(eventType string, d time.Duration) {
	dispatchDuration.Record(
		measureCtx,
		float64(d)/float64(time.Millisecond),
		otelmetric.WithAttributes(attribute.String("event_type", eventType)),
	)
}

func Payload(size int) {
	payloadBytes.Record(measureCtx, int64(size))
}

func Ack(eventType string, d time.Duration) {
	ackDuration.Record(
		measureCtx,
		float64(d)/float64(time.Millisecond),
		otelmetric.WithAttributes(attribute.String("event_type", eventType)),
	)
}

func ShardDrop(reason string) {
	shardDrops.Add(measureCtx, 1, otelmetric.WithAttributes(attribute.String("reason", reason)))
}

func PublishDrop(reason string) {
	publishDrops.Add(measureCtx, 1, otelmetric.WithAttributes(attribute.String("reason", reason)))
}

func ShardReconnect(shardID int) {
	shardReconnects.Add(measureCtx, 1, otelmetric.WithAttributes(attribute.Int("shard_id", shardID)))
}

func NATSReconnect() {
	natsReconnects.Add(measureCtx, 1)
}
