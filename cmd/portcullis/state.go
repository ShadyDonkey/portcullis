package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ShadyDonkey/portcullis/internal/gateway"
	"github.com/ShadyDonkey/portcullis/internal/jetstream"
)

func reconcileProxyState(
	ctx context.Context, js *jetstream.JetStream, intents gateway.Intent, numShards int,
) error {
	want := jetstream.GlobalState{
		Intents:   intents,
		NumShards: numShards,
	}

	stored, err := js.GetGlobalState(ctx)
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
		slog.WarnContext(
			ctx, "global state changed, clearing all sessions",
			"old_intents", stored.Intents, "new_intents", want.Intents,
			"old_num_shards", stored.NumShards, "new_num_shards", want.NumShards,
		)
	} else {
		slog.InfoContext(ctx, "no stored global state, clearing all sessions")
	}

	for id := range clearUpTo {
		if err := js.DeleteSession(ctx, id); err != nil {
			return err
		}
	}

	if err := js.PutGlobalState(ctx, want); err != nil {
		return fmt.Errorf("failed to put global state: %w", err)
	}

	return nil
}
