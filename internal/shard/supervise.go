package shard

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ShadyDonkey/portcullis/internal/gateway"
	"github.com/ShadyDonkey/portcullis/internal/metrics"
)

func (m *Manager) supervise(ctx context.Context, id int, ms *managedShard) {
	var bo backoff
	var err error
	var consecutiveUnstable int

	for {
		startedAt := time.Now()
		err = ms.shard.Start(ctx)

		if closeErr := ms.shard.Close(); closeErr != nil && err == nil {
			slog.ErrorContext(ctx, "failed to close shard", "shard_id", id, "err", closeErr)
		}

		if ctx.Err() != nil {
			err = nil
			break
		}

		action := gateway.ErrorToExitAction(err)
		if action == gateway.ExitStop {
			if closeErr, ok := errors.AsType[*gateway.CloseError](err); ok {
				slog.ErrorContext(
					ctx, "shard exiting permanently",
					"shard_id", id, "code", closeErr.Code, "hint", gateway.CloseCodeHint(closeErr.Code), "fatal",
					true,
				)
			}
			err = fmt.Errorf("exit stop code received, not reconnecting: %w", err)
			break
		}

		forceFresh := action == gateway.ExitReidentify
		if forceFresh {
			m.discardSession(ctx, id)
		}

		if time.Since(startedAt) >= reconnectStableAfter {
			bo.reset()
			consecutiveUnstable = 0
		} else {
			consecutiveUnstable++
		}

		if consecutiveUnstable >= maxConsecutiveUnstableExits {
			err = fmt.Errorf(
				"shard flapping: %d consecutive connections each lasted under %s, last exit: %w",
				consecutiveUnstable, reconnectStableAfter, err,
			)
			slog.ErrorContext(
				ctx, "shard flapping, not reconnecting",
				"shard_id", id, "consecutive", consecutiveUnstable,
				"threshold", maxConsecutiveUnstableExits, "fatal", true,
			)
			break
		}

		shard, shardErr := m.reconnect(ctx, id, &bo, action, forceFresh, err)
		if shardErr != nil {
			err = shardErr
			if ctx.Err() != nil {
				err = nil
			}
			break
		}

		m.mu.Lock()
		ms.shard = shard
		m.mu.Unlock()

		metrics.ShardReconnect(id)
	}

	ms.cancel()
	m.mu.Lock()
	ms.err = err
	m.mu.Unlock()

	close(ms.done)

	if err != nil {
		slog.ErrorContext(ctx, "shard exited with an error", "shard_id", id, "err", err)
	} else {
		slog.InfoContext(ctx, "shard exited successfully", "shard_id", id)
	}
}

func (m *Manager) reconnect(
	ctx context.Context, id int, bo *backoff, action gateway.ExitAction, forceFresh bool, cause error,
) (
	*Shard, error,
) {
	attempts := 0
	for {
		attempts++
		delay := bo.next()

		if action == gateway.ExitReidentify {
			delay = max(delay, gateway.ReidentifyDelay())
		}

		slog.WarnContext(
			ctx, "shard disconnected, reconnecting", "shard_id", id, "action", action, "delay", delay,
			"cause", cause,
		)

		if err := waitDelay(ctx, delay); err != nil {
			return nil, err
		}

		shardCfg := m.idToShardConfig(id)
		shardCfg.ForceFreshSession = forceFresh

		shard, err := NewShard(ctx, shardCfg)
		if err == nil {
			return shard, nil
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if attempts >= maxDialRetries {
			return nil, fmt.Errorf(
				"shard: max dial retries encountered, reconnect gave up after %d attempts: %w", attempts, err,
			)
		}

		cause = err
	}
}

func waitDelay(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) discardSession(ctx context.Context, id int) error {
	deleteCtx, cancel := context.WithTimeout(ctx, sessionStoreTimeout)
	defer cancel()

	if err := m.config.SessionStore.DeleteSession(deleteCtx, id); err != nil {
		slog.ErrorContext(ctx, "failed to discard session", "shard_id", id, "err", err)
		return err
	}

	return nil
}

func (m *Manager) idToShardConfig(id int) ShardConfig {
	return ShardConfig{
		ID:              id,
		URL:             m.config.URL,
		Token:           m.config.Token,
		Intents:         m.config.Intents,
		NumShards:       m.config.NumShards,
		Publisher:       m.config.Publisher,
		SessionStore:    m.config.SessionStore,
		IdentifyLimiter: m.identify,
		Filter:          m.config.Filter,
	}
}
