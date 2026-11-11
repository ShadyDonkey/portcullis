package main

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ShadyDonkey/portcullis/internal/config"
	"github.com/ShadyDonkey/portcullis/internal/gateway"
	"github.com/ShadyDonkey/portcullis/internal/jetstream"
	"github.com/carlmjohnson/requests"
)

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	cfg, cfgErr := config.Load()
	if cfgErr != nil {
		return cfgErr
	}

	logger := newLogger(cfg.Log)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if cfg.Observability.OtelEnabled {
		// TODO: enable otel
	}

	if cfg.Observability.SentryEnabled {
		// TODO: enable sentry
	}

	intents, intentsParseErr := gateway.ParseIntents(cfg.Discord.Intents)

	if intentsParseErr != nil {
		return intentsParseErr
	}

	js, jsErr := jetstream.New(
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

	if jsErr != nil {
		return jsErr
	}
	defer js.Close()

	httpClient := &http.Client{
		Timeout: 15 * time.Second,
		Transport: Transport{
			RoundTripper: http.DefaultTransport,
			Headers: http.Header{
				"Authorization": {fmt.Sprintf("Bot %s", cfg.Discord.Token.Reveal())},
				// TODO: replace with real build version
				"User-Agent": {fmt.Sprintf("DiscordBot (https://portcullisgw.com, %s)", "0.0.0")},
			},
		},
	}

	var resp struct {
		URL               string            `json:"url"`
		Shards            int               `json:"shards"`
		SessionStartLimit sessionStartLimit `json:"session_start_limit"`
	}

	// TODO: Add config do the base URL (ref: nirn)
	reqErr := requests.
		URL("https://discord.com/api/v10/gateway/bot").
		Client(httpClient).
		ToJSON(&resp).
		Fetch(ctx)

	if reqErr != nil {
		return reqErr
	}

	slog.DebugContext(ctx, "Fetched gateway info", "resp", resp)

	numShards := resp.Shards
	var reshardInterval time.Duration

	if o := cfg.Discord.Sharding.ShardCountOverride; o != nil {
		if *o < resp.Shards {
			slog.WarnContext(
				ctx, "shard_count_override is below Discord's recommendation", "override", *o, "recommended",
				resp.Shards,
			)
		}

		numShards = *o
	} else {
		reshardInterval = time.Duration(*cfg.Discord.Sharding.ReshardCheckInterval)
	}

	slog.DebugContext(
		ctx,
		"identify budget",
		"remaining", resp.SessionStartLimit.Remaining,
		"total", resp.SessionStartLimit.Total,
		"reset_after", time.Duration(resp.SessionStartLimit.ResetAfter)*time.Millisecond,
		"num_shards", numShards,
		"max_concurrency", resp.SessionStartLimit.MaxConcurrency,
	)

	if budgetErr := checkIdentifyBudget(resp.SessionStartLimit, numShards); budgetErr != nil {
		slog.ErrorContext(
			ctx,
			"refusing to start, identify budget too low",
			"remaining", resp.SessionStartLimit.Remaining,
			"total", resp.SessionStartLimit.Total,
			"needed", numShards+identifyBudgetMargin,
			"reset_after", time.Duration(resp.SessionStartLimit.ResetAfter)*time.Millisecond,
			"fatal", true,
			"err", budgetErr,
		)

		return budgetErr
	}

	slog.DebugContext(
		ctx, "Starting shard manager", "url", resp.URL, "intents", intents, "num_shards", numShards,
	)

	if stateErr := reconcileProxyState(ctx, js, int(intents), numShards); stateErr != nil {
		slog.ErrorContext(ctx, "failed to reconcile proxy state", "err", stateErr)

		return stateErr
	}

	shardManager := gateway.NewShardManager(
		gateway.ShardManagerConfig{
			URL:             resp.URL,
			Token:           cfg.Discord.Token.Reveal(),
			Intents:         int(intents),
			NumShards:       numShards,
			Publisher:       js,
			SessionStore:    js,
			MaxConcurrency:  resp.SessionStartLimit.MaxConcurrency,
			ReshardInterval: reshardInterval,
			Filter:          gateway.NewEventFilter(cfg.Portcullis.Publish),
		},
	)

	if startErr := shardManager.Start(ctx); startErr != nil {
		slog.ErrorContext(ctx, "failed to start shard manager", "err", startErr)
		return startErr
	}

	fatal := make(chan error, 1)
	for id := range numShards {
		go func() {
			if wErr := shardManager.WaitForShard(id); wErr != nil {
				select {
				case fatal <- fmt.Errorf("shard %d failed: %w", id, wErr):
				default:
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

type sessionStartLimit struct {
	Total          int `json:"total"`
	Remaining      int `json:"remaining"`
	ResetAfter     int `json:"reset_after"`
	MaxConcurrency int `json:"max_concurrency"`
}

func reconcileProxyState(ctx context.Context, js *jetstream.JetStream, intents, numShards int) error {
	want := jetstream.GlobalState{
		Intents:   gateway.Intent(intents),
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

	if gsErr := js.PutGlobalState(ctx, want); gsErr != nil {
		return fmt.Errorf("failed to put global state: %w", gsErr)
	}

	return nil
}

type Transport struct {
	http.RoundTripper
	Headers http.Header
}

func (t Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	maps.Copy(req.Header, t.Headers)
	return t.RoundTripper.RoundTrip(req)
}

// identifyBudgetMargin is headroom for reconnect re-identifies: the guard
// requires enough budget for every shard to identify plus this many
// emergency re-identifies after reconnects.
const identifyBudgetMargin = 10

func checkIdentifyBudget(limit sessionStartLimit, numShards int) error {
	needed := numShards + identifyBudgetMargin
	if limit.Remaining < needed {
		return fmt.Errorf("identify budget nearly exhausted, not starting")
	}

	return nil
}
