package observability

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/getsentry/sentry-go"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
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
			resource.WithAttributes(attribute.String("service.name", "portcullis")),
			resource.WithFromEnv(),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create resource: %w", err)
		}

		mExp, err := otlpmetrichttp.New(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create metric exporter: %w", err)
		}

		o.meter = metric.NewMeterProvider(
			metric.WithResource(res),
			metric.WithReader(metric.NewPeriodicReader(mExp)),
		)

		lExp, err := otlploghttp.New(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create log exporter: %w", err)
		}

		o.logs = sdklog.NewLoggerProvider(
			sdklog.WithResource(res),
			sdklog.WithProcessor(sdklog.NewBatchProcessor(lExp)),
		)
	}

	return o, nil
}

const meterName = "github.com/ShadyDonkey/portcullis"

func (o *Observability) Meter() otelmetric.Meter {
	if o.meter != nil {
		return o.meter.Meter(meterName)
	}

	return noop.NewMeterProvider().Meter(meterName)
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

type leveled struct {
	slog.Handler
	min slog.Leveler
}

func (ld leveled) Enabled(ctx context.Context, l slog.Level) bool {
	return l >= ld.min.Level() && ld.Handler.Enabled(ctx, l)
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
