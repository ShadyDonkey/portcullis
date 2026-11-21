package metrics

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
)

var frameSizeBuckets = []float64{
	512,
	1 << 10, 4 << 10, 16 << 10, 64 << 10, 256 << 10,
	1 << 20, 4 << 20, 16 << 20, 64 << 20, 128 << 20,
}

var dispatchDurationBuckets = []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500}

var ackDurationBuckets = []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2000, 5000}

var (
	eventsConsumed       otelmetric.Int64Counter
	bridgeConsumed       otelmetric.Int64Counter
	eventsPublished      otelmetric.Int64Counter
	shardDrops           otelmetric.Int64Counter
	bridgeDrops          otelmetric.Int64Counter
	invariantViolations  otelmetric.Int64Counter
	shardReconnects      otelmetric.Int64Counter
	natsReconnects       otelmetric.Int64Counter
	payloadBytes         otelmetric.Int64Histogram
	dispatchDuration     otelmetric.Float64Histogram
	ackDuration          otelmetric.Float64Histogram
)

func Setup(m otelmetric.Meter) error {
	var err error

	if eventsConsumed, err = m.Int64Counter("portcullis.shard.events_consumed"); err != nil {
		return fmt.Errorf("create shard.events_consumed metric: %w", err)
	}

	if bridgeConsumed, err = m.Int64Counter("portcullis.bridge.events_consumed"); err != nil {
		return fmt.Errorf("create bridge.events_consumed metric: %w", err)
	}

	if eventsPublished, err = m.Int64Counter("portcullis.bridge.events_published"); err != nil {
		return fmt.Errorf("create bridge.events_published metric: %w", err)
	}

	if shardDrops, err = m.Int64Counter("portcullis.shard.dropped_events"); err != nil {
		return fmt.Errorf("create shard.dropped_events metric: %w", err)
	}

	if bridgeDrops, err = m.Int64Counter("portcullis.bridge.dropped_events"); err != nil {
		return fmt.Errorf("create bridge.dropped_events metric: %w", err)
	}

	if invariantViolations, err = m.Int64Counter("portcullis.bridge.invariant_violations"); err != nil {
		return fmt.Errorf("create bridge.invariant_violations metric: %w", err)
	}

	if shardReconnects, err = m.Int64Counter("portcullis.shard.reconnects"); err != nil {
		return fmt.Errorf("create shard.reconnects metric: %w", err)
	}

	if natsReconnects, err = m.Int64Counter("portcullis.nats.reconnects"); err != nil {
		return fmt.Errorf("create nats.reconnects metric: %w", err)
	}

	if payloadBytes, err = m.Int64Histogram(
		"portcullis.bridge.payload_bytes",
		otelmetric.WithUnit("By"),
		otelmetric.WithExplicitBucketBoundaries(frameSizeBuckets...),
	); err != nil {
		return fmt.Errorf("create bridge.payload_bytes metric: %w", err)
	}

	if dispatchDuration, err = m.Float64Histogram(
		"portcullis.shard.dispatch_duration",
		otelmetric.WithUnit("ms"),
		otelmetric.WithExplicitBucketBoundaries(dispatchDurationBuckets...),
	); err != nil {
		return fmt.Errorf("create shard.dispatch_duration metric: %w", err)
	}

	if ackDuration, err = m.Float64Histogram(
		"portcullis.bridge.ack_duration",
		otelmetric.WithUnit("ms"),
		otelmetric.WithExplicitBucketBoundaries(ackDurationBuckets...),
	); err != nil {
		return fmt.Errorf("create bridge.ack_duration metric: %w", err)
	}

	return nil
}

type Sources struct {
	ShardUp       func() map[int]bool
	InFlightBytes func() int64
}

func RegisterGauges(m otelmetric.Meter, sources Sources) error {
	up, err := m.Int64ObservableGauge("portcullis.shard.up")
	if err != nil {
		return fmt.Errorf("create shard.up metric: %w", err)
	}

	inFlight, err := m.Int64ObservableGauge("portcullis.bridge.in_flight_bytes")
	if err != nil {
		return fmt.Errorf("create bridge.in_flight_bytes metric: %w", err)
	}

	_, err = m.RegisterCallback(
		func(_ context.Context, ob otelmetric.Observer) error {
			for id, isUp := range sources.ShardUp() {
				v := int64(0)
				if isUp {
					v = 1
				}

				ob.ObserveInt64(up, v, otelmetric.WithAttributes(attribute.Int("shard_id", id)))
			}

			ob.ObserveInt64(inFlight, sources.InFlightBytes())
			return nil
		},
		up,
		inFlight,
	)
	if err != nil {
		return fmt.Errorf("register gauges callback: %w", err)
	}

	return nil
}
