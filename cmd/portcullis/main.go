package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ShadyDonkey/portcullis/internal/config"
	"github.com/ShadyDonkey/portcullis/internal/framepool"
	"github.com/ShadyDonkey/portcullis/internal/gateway"
	"github.com/ShadyDonkey/portcullis/internal/jetstream"
	"github.com/ShadyDonkey/portcullis/internal/observability"
)

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() (returnErr error) {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	logger := newLogger(cfg.Log)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	obs, err := observability.Setup(
		ctx, logger, observability.Config{
			OtelEnabled:   cfg.Observability.OtelEnabled,
			SentryEnabled: cfg.Observability.SentryEnabled,
			LogLevel:      cfg.Log.Level,
		},
	)
	if err != nil {
		return err
	}

	defer func() {
		obs.Close(returnErr)
	}()

	logger = obs.WrapLogger(logger)
	slog.SetDefault(logger)

	intents, err := gateway.ParseIntents(cfg.Discord.Intents)
	if err != nil {
		return err
	}

	js, err := jetstream.New(
		ctx, jetstream.Config{
			URL:            cfg.NATS.URL.String(),
			User:           cfg.NATS.Username.String(),
			Password:       cfg.NATS.Password.Reveal(),
			ConnectionName: cfg.NATS.ConnectionName,
			StateBucket:    cfg.NATS.KV.StateBucket,
			StreamName:     cfg.NATS.Stream.Name,
			SubjectPrefix:  cfg.NATS.Stream.SubjectPrefix,
			MaxAge:         time.Duration(cfg.NATS.Stream.MaxAge),
			MaxBytes:       int64(cfg.NATS.Stream.MaxBytes),
		},
	)
	if err != nil {
		return err
	}
	defer js.Close()

	httpClient := newDiscordClient(cfg.Discord.Token.Reveal())

	info, err := fetchGatewayBot(ctx, httpClient)
	if err != nil {
		return err
	}

	slog.DebugContext(ctx, "Fetched gateway info", "resp", info)

	numShards, reshardInterval := resolveSharding(ctx, cfg.Discord.Sharding, info.Shards)

	slog.DebugContext(
		ctx,
		"identify budget",
		"remaining", info.SessionStartLimit.Remaining,
		"total", info.SessionStartLimit.Total,
		"reset_after", time.Duration(info.SessionStartLimit.ResetAfter)*time.Millisecond,
		"num_shards", numShards,
		"max_concurrency", info.SessionStartLimit.MaxConcurrency,
	)

	if err := checkIdentifyBudget(info.SessionStartLimit, numShards); err != nil {
		slog.ErrorContext(
			ctx,
			"refusing to start, identify budget too low",
			"remaining", info.SessionStartLimit.Remaining,
			"total", info.SessionStartLimit.Total,
			"needed", numShards+identifyBudgetMargin,
			"reset_after", time.Duration(info.SessionStartLimit.ResetAfter)*time.Millisecond,
			"fatal", true,
			"err", err,
		)

		return err
	}

	slog.DebugContext(
		ctx, "Starting shard manager", "url", info.URL, "intents", intents, "num_shards", numShards,
	)

	if err := reconcileProxyState(ctx, js, intents, numShards); err != nil {
		slog.ErrorContext(ctx, "failed to reconcile proxy state", "err", err)
		return err
	}

	shardManager := gateway.NewShardManager(
		gateway.ShardManagerConfig{
			URL:             info.URL,
			Token:           cfg.Discord.Token.Reveal(),
			Intents:         int(intents),
			NumShards:       numShards,
			Publisher:       js,
			SessionStore:    js,
			MaxConcurrency:  info.SessionStartLimit.MaxConcurrency,
			ReshardInterval: reshardInterval,
			Filter:          gateway.NewEventFilter(cfg.Portcullis.Publish),
		},
	)

	if err := obs.RegisterMetrics(
		observability.MetricsSources{
			PublishStats:      js.PublishStats,
			ShardStatuses:     shardManager.Statuses,
			ParseErrors:       gateway.ParseErrors,
			EventTypeOverflow: gateway.EventTypeOverflows,
			OversizedFrames:   framepool.OversizedFrames,
		},
	); err != nil {
		return err
	}

	if err := shardManager.Start(ctx); err != nil {
		slog.ErrorContext(ctx, "failed to start shard manager", "err", err)
		return err
	}

	fatal := make(chan error, 1)
	for id := range numShards {
		go func() {
			if err := shardManager.WaitForShard(id); err != nil {
				select {
				case fatal <- fmt.Errorf("shard %d failed: %w", id, err):
				default:
					slog.WarnContext(
						ctx, fmt.Sprintf("additional shard failure on %d", id), "shard_id", id, "err", err,
					)
				}
			}
		}()
	}

	if cfg.HTTP.Enabled {
		// TODO: this
	}

	var runErr error
	select {
	case <-ctx.Done():
		slog.Info("Shutting down")

	case runErr = <-fatal:
		slog.ErrorContext(ctx, "fatal shard error, shutting down", "err", runErr)

	case runErr = <-js.UnexpectedClose():
		slog.ErrorContext(ctx, "NATS connection closed, shutting down", "err", runErr)
	}

	stop()
	shardManager.Shutdown()

	return runErr
}

func newLogger(c config.LogConfig) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level: c.Level,
	}

	if c.Format == "text" {
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}

	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}
