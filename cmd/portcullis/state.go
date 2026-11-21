package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ShadyDonkey/portcullis/internal/config"
	"github.com/ShadyDonkey/portcullis/internal/gateway"
	"github.com/ShadyDonkey/portcullis/internal/state"
)

const storeCloseTimeout = 2 * time.Second

func openStore(ctx context.Context, cfg *config.Config, logger *slog.Logger) (state.Store, error) {
	return state.OpenBolt(cfg.Portcullis.StatePath, logger)
}

func reconcileGlobalState(ctx context.Context, store state.Store, intents gateway.Intent, numShards int) error {
	want := state.Global{
		Intents:   intents,
		NumShards: numShards,
	}

	stored, err := store.GetGlobal()
	if err != nil {
		return fmt.Errorf("failed to get global state: %w", err)
	}

	if stored != nil && *stored == want {
		slog.InfoContext(ctx, "global state unchanged, all sessions eligible to resume")
		return nil
	}

	clearUpTo := want.NumShards
	if stored != nil {
		clearUpTo = max(clearUpTo, stored.NumShards)
		slog.ErrorContext(
			ctx, "global state mismatch, clearing all sessions",
			"old_intents", stored.Intents, "new_intents", want.Intents,
			"old_num_shards", stored.NumShards, "new_num_shards", want.NumShards,
			"clear_up_to", clearUpTo,
		)
	} else {
		slog.InfoContext(ctx, "no stored global state, clearing all sessions")
	}

	for id := range clearUpTo {
		if err := store.DeleteSession(id); err != nil {
			return err
		}
	}

	if err := store.PutGlobal(want); err != nil {
		return fmt.Errorf("failed to put global state: %w", err)
	}

	return nil
}
