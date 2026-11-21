package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ShadyDonkey/portcullis/internal/bridge"
	"github.com/ShadyDonkey/portcullis/internal/config"
	"github.com/ShadyDonkey/portcullis/internal/metrics"

	_ "github.com/ShadyDonkey/portcullis/internal/bridge/jetstream"
	_ "github.com/ShadyDonkey/portcullis/internal/bridge/webhook"
)

const bridgeCloseTimeout = 5 * time.Second

func startBridges(ctx context.Context, cfg *config.Config, logger *slog.Logger) (*bridge.Fanout, <-chan error, error) {
	fatal := make(chan error, 1)
	entries := make([]bridge.Entry, 0, len(cfg.Bridges))

	closeBuilt := func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bridgeCloseTimeout)
		defer cancel()

		for _, entry := range entries {
			if err := entry.Bridge.Close(closeCtx); err != nil {
				logger.ErrorContext(ctx, "failed to close bridge", "bridge", entry.Name, "err", err)
			}
		}
	}

	for _, spec := range cfg.Bridges {
		env := bridge.Env{
			Name:   spec.Name,
			Logger: logger.With("bridge", spec.Name),
			Fatal: func(err error) {
				select {
				case fatal <- fmt.Errorf("bridge %s: %w", spec.Name, err):
				default:
				}
			},
		}

		b, err := spec.Factory.New(ctx, spec.Config, env)
		if err != nil {
			closeBuilt()
			return nil, nil, fmt.Errorf("bridge.%s: %w", spec.Name, err)
		}

		entries = append(
			entries, bridge.Entry{
				Name:   spec.Name,
				Bridge: b,
			},
		)
		logger.InfoContext(ctx, "bridge started", "bridge", spec.Name)

	}

	fanout, err := bridge.NewFanout(
		entries, bridge.Options{
			OnDrop: metrics.BridgeDrop,
			Logger: logger,
		},
	)

	if err != nil {
		closeBuilt()
		return nil, nil, err
	}

	return fanout, fatal, nil
}
