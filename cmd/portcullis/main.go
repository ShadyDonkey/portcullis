package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ShadyDonkey/portcullis/internal/config"
	"github.com/ShadyDonkey/portcullis/internal/gateway"
	"github.com/ShadyDonkey/portcullis/internal/metrics"
	"github.com/ShadyDonkey/portcullis/internal/observability"
	"github.com/ShadyDonkey/portcullis/internal/shard"
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

	meter := obs.Meter()
	if err := metrics.Setup(meter); err != nil {
		return err
	}

	store, err := openStore(ctx, cfg, logger)
	if err != nil {
		return err
	}

	var closeStoreOnce sync.Once
	closeStore := func() {
		closeStoreOnce.Do(
			func() {
				closeCtx, closeCtxCancel := context.WithTimeout(context.Background(), storeCloseTimeout)
				defer closeCtxCancel()

				if err := store.Close(closeCtx); err != nil {
					slog.Error("failed to close state store cleanly", "err", err)
				}
			},
		)
	}

	defer closeStore()

	httpClient := newDiscordClient(cfg.Discord.Token.Reveal())
	info, err := fetchGatewayBot(ctx, httpClient)
	if err != nil {
		return err
	}

	slog.DebugContext(ctx, "fetched gateway bot info", "resp", info)

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

	if err := reconcileGlobalState(ctx, store, intents, numShards); err != nil {
		slog.ErrorContext(ctx, "failed to reconcile global state", "err", err)
		return err
	}

	fanout, bridgeFatal, err := startBridges(ctx, cfg, logger)
	if err != nil {
		return err
	}

	var closeFanoutOnce sync.Once
	closeFanout := func() {
		closeFanoutOnce.Do(
			func() {
				closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bridgeCloseTimeout)
				defer cancel()

				if err := fanout.Close(closeCtx); err != nil {
					slog.Error("failed to close bridges cleanly", "err", err)
				}
			},
		)
	}
	defer closeFanout()

	shardManager := shard.NewManager(
		shard.Config{
			URL:             info.URL,
			Token:           cfg.Discord.Token.Reveal(),
			Intents:         int(intents),
			NumShards:       numShards,
			Publisher:       fanout,
			SessionStore:    store,
			MaxConcurrency:  info.SessionStartLimit.MaxConcurrency,
			ReshardInterval: reshardInterval,
			Filter:          gateway.NewEventFilter(cfg.Portcullis.Publish),
		},
	)

	if err := metrics.RegisterGauges(
		meter,
		metrics.Sources{
			ShardUp: func() map[int]bool {
				statuses := shardManager.Statuses()
				up := make(map[int]bool, len(statuses))
				for id, status := range statuses {
					up[id] = status == shard.StatusRunning
				}
				return up
			},
			InFlightBytes: fanout.InFlightBytes,
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
						ctx, "additional shard failure", "shard_id", id, "err", err,
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

	case runErr = <-bridgeFatal:
		slog.ErrorContext(ctx, "bridge asked for shutdown", "err", runErr)
	}

	stop()
	shardManager.Shutdown()
	closeStore()
	closeFanout()

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
