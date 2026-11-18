package observability

import (
	"context"
	"fmt"

	"github.com/ShadyDonkey/portcullis/internal/gateway"
	"github.com/ShadyDonkey/portcullis/internal/jetstream"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
)

type MetricsSources struct {
	PublishStats      func() jetstream.PublishStats
	ShardStatuses     func() map[int]gateway.ShardStatus
	ParseErrors       func() uint64
	EventTypeOverflow func() uint64
	OversizedFrames   func() uint64
}

func (o *Observability) RegisterMetrics(sources MetricsSources) error {
	if o.meter == nil {
		return nil
	}

	m := o.meter.Meter("github.com/ShadyDonkey/portcullis")

	drops, err := m.Int64ObservableCounter("portcullis.publisher.stalled_drops")
	if err != nil {
		return fmt.Errorf("create stalled_drops metric: %w", err)
	}

	syncErrors, err := m.Int64ObservableCounter("portcullis.publisher.sync_errors")
	if err != nil {
		return fmt.Errorf("create sync_errors metric: %w", err)
	}

	ackErrors, err := m.Int64ObservableCounter("portcullis.publisher.ack_errors")
	if err != nil {
		return fmt.Errorf("create ack_errors metric: %w", err)
	}

	up, err := m.Int64ObservableGauge("portcullis.gateway.shard.up")
	if err != nil {
		return fmt.Errorf("create shard_up metric: %w", err)
	}

	ringEvictions, err := m.Int64ObservableCounter("portcullis.publisher.ring_evictions")
	if err != nil {
		return fmt.Errorf("create ring_evictions metric: %w", err)
	}

	byteCapDrops, err := m.Int64ObservableCounter("portcullis.publisher.byte_cap_drops")
	if err != nil {
		return fmt.Errorf("create byte_cap_drops metric: %w", err)
	}

	oversized, err := m.Int64ObservableCounter("portcullis.publisher.oversized_frames")
	if err != nil {
		return fmt.Errorf("create oversized_frames metric: %w", err)
	}

	parseErrors, err := m.Int64ObservableCounter("portcullis.gateway.parse_errors")
	if err != nil {
		return fmt.Errorf("create parse_errors metric: %w", err)
	}

	overflow, err := m.Int64ObservableCounter("portcullis.publisher.event_type_overflow_drops")
	if err != nil {
		return fmt.Errorf("create event_type_overflow_drops metric: %w", err)
	}

	inFlight, err := m.Int64ObservableGauge("portcullis.publisher.in_flight_bytes")
	if err != nil {
		return fmt.Errorf("create in_flight_bytes metric: %w", err)
	}

	ringOccupancy, err := m.Int64ObservableGauge("portcullis.publisher.ring_occupancy")
	if err != nil {
		return fmt.Errorf("create ring_occupancy metric: %w", err)
	}

	_, err = m.RegisterCallback(
		func(_ context.Context, ob otelmetric.Observer) error {
			s := sources.PublishStats()

			ob.ObserveInt64(drops, int64(s.StalledDrops))
			ob.ObserveInt64(syncErrors, int64(s.SyncErrors))
			ob.ObserveInt64(ackErrors, int64(s.AckErrors))
			ob.ObserveInt64(ringEvictions, int64(s.RingEvictions))
			ob.ObserveInt64(byteCapDrops, int64(s.ByteCapDrops))
			ob.ObserveInt64(oversized, int64(sources.OversizedFrames()))
			ob.ObserveInt64(parseErrors, int64(sources.ParseErrors()))
			ob.ObserveInt64(overflow, int64(sources.EventTypeOverflow()))
			ob.ObserveInt64(inFlight, int64(s.InFlightBytes))
			ob.ObserveInt64(ringOccupancy, int64(s.RingOccupancy))

			for id, status := range sources.ShardStatuses() {
				v := int64(0)
				if status == gateway.ShardStatusRunning {
					v = 1
				}

				ob.ObserveInt64(
					up, v,
					otelmetric.WithAttributes(
						attribute.Int("shard_id", id),
					),
				)
			}

			return nil
		},
		drops,
		syncErrors,
		ackErrors,
		up,
		ringEvictions,
		byteCapDrops,
		oversized,
		parseErrors,
		overflow,
		inFlight,
		ringOccupancy,
	)
	if err != nil {
		return fmt.Errorf("register metrics callback: %w", err)
	}

	return nil
}
