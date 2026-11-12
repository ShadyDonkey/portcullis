package observability

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ShadyDonkey/portcullis/internal/gateway"
	"github.com/ShadyDonkey/portcullis/internal/jetstream"
	"github.com/getsentry/sentry-go"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	sdkmetric "go.opentelemetry.io/otel/metric"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

type Config struct {
	OtelEnabled   bool
	SentryEnabled bool
	LogLevel      slog.Leveler
}

type Observability struct {
	base   *slog.Logger
	level  slog.Leveler
	sentry bool
	meter  *metric.MeterProvider
	logs   *sdklog.LoggerProvider
}

func Setup(ctx context.Context, base *slog.Logger, cfg Config) (*Observability, error) {
	o := &Observability{
		base:  base,
		level: cfg.LogLevel,
	}

	if cfg.SentryEnabled {
		if err := sentry.Init(sentry.ClientOptions{}); err != nil {
			return nil, fmt.Errorf("failed to start sentry: %w", err)
		}

		o.sentry = true
	}

	if cfg.OtelEnabled {
		otel.SetErrorHandler(
			otel.ErrorHandlerFunc(
				func(err error) {
					base.Error("otel", "err", err)
				},
			),
		)

		res, err := resource.New(
			ctx,
			resource.WithAttributes(semconv.ServiceName("portcullis")),
			resource.WithFromEnv(),
		)

		if err != nil {
			return nil, fmt.Errorf("failed to create resource: %w", err)
		}

		mExp, mErr := otlpmetrichttp.New(ctx)

		if mErr != nil {
			return nil, fmt.Errorf("failed to create metric exporter: %w", mErr)
		}

		o.meter = metric.NewMeterProvider(
			metric.WithResource(res),
			metric.WithReader(metric.NewPeriodicReader(mExp)),
		)

		lExp, lErr := otlploghttp.New(ctx)

		if lErr != nil {
			return nil, fmt.Errorf("failed to create log exporter: %w", lErr)
		}

		o.logs = sdklog.NewLoggerProvider(
			sdklog.WithResource(res),
			sdklog.WithProcessor(sdklog.NewBatchProcessor(lExp)),
		)
	}

	return o, nil
}

func (o *Observability) Close(runErr error) {
	if o.sentry {
		if runErr != nil {
			sentry.CaptureException(runErr)
		}

		sentry.Flush(2 * time.Second)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if o.meter != nil {
		if err := o.meter.Shutdown(ctx); err != nil {
			o.base.Error("otel metrics shutdown failure", "err", err)
		}
	}

	if o.logs != nil {
		if err := o.logs.Shutdown(ctx); err != nil {
			o.base.Error("otel logs shutdown failure", "err", err)
		}
	}
}

func (o *Observability) WrapLogger(l *slog.Logger) *slog.Logger {
	if o.logs == nil {
		return l
	}

	otelH := leveled{
		otelslog.NewHandler("portcullis", otelslog.WithLoggerProvider(o.logs)),
		o.level,
	}

	return slog.New(slog.NewMultiHandler(l.Handler(), otelH))
}

func (o *Observability) RegisterMetrics(
	publishStats func() jetstream.PublishStats,
	shardStatuses func() map[int]gateway.ShardStatus,
) error {
	if o.meter == nil {
		return nil
	}

	m := o.meter.Meter("github.com/ShadyDonkey/portcullis")
	drops, _ := m.Int64ObservableCounter("portcullis.publisher.stalled_drops")
	syncErr, _ := m.Int64ObservableCounter("portcullis.publisher.sync_errors")
	ackErr, _ := m.Int64ObservableCounter("portcullis.publisher.ack_errors")
	up, _ := m.Int64ObservableCounter("portcullis.gateway.shard.up")

	_, err := m.RegisterCallback(
		func(_ context.Context, ob sdkmetric.Observer) error {
			s := publishStats()

			ob.ObserveInt64(drops, int64(s.StalledDrops))
			ob.ObserveInt64(syncErr, int64(s.SyncErrors))
			ob.ObserveInt64(ackErr, int64(s.AckErrors))

			for id, status := range shardStatuses() {
				v := int64(0)

				if status == gateway.ShardStatusRunning {
					v = 1
				}

				ob.ObserveInt64(
					up, v,
					sdkmetric.WithAttributes(
						attribute.Int("shard_id", id),
					),
				)
			}

			return nil
		},
		drops,
		syncErr,
		ackErr,
		up,
	)

	return err
}

type leveled struct {
	slog.Handler
	min slog.Leveler
}

func (ld leveled) Enabled(ctx context.Context, l slog.Level) bool {
	return l >= ld.min.Level() && ld.Handler.Enabled(ctx, l)
}

func (ld leveled) Handle(ctx context.Context, r slog.Record) error {
	if r.Level < ld.min.Level() {
		return nil
	}

	return ld.Handler.Handle(ctx, r)
}

func (ld leveled) WithAttrs(attrs []slog.Attr) slog.Handler {
	return leveled{
		ld.Handler.WithAttrs(attrs),
		ld.min,
	}
}

func (ld leveled) WithGroup(name string) slog.Handler {
	return leveled{
		ld.Handler.WithGroup(name),
		ld.min,
	}
}
