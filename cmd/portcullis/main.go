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

	"github.com/ShadyDonkey/portcullis/internal/gateway"
	"github.com/ShadyDonkey/portcullis/internal/jetstream"
	"github.com/carlmjohnson/requests"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	token := os.Getenv("DISCORD_TOKEN")

	// TODO: get this from config
	slog.SetDefault(
		slog.New(
			slog.NewTextHandler(
				os.Stdout, &slog.HandlerOptions{
					Level: slog.LevelDebug,
				},
			),
		),
	)

	js, err := jetstream.New(
		ctx, jetstream.Config{
			URL:      "localhost:4222",
			User:     "portcullis",
			Password: "supersecret",
		},
	)

	if err != nil {
		slog.Error("Failed to connect to jetstream", "err", err)
		os.Exit(1)
	}

	defer js.Close()

	httpClient := &http.Client{
		Transport: Transport{
			RoundTripper: http.DefaultTransport,
			Headers: http.Header{
				"Authorization": {fmt.Sprintf("Bot %s", token)},
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

	err = requests.
		URL("https://discord.com/api/v10/gateway/bot").
		Client(httpClient).
		ToJSON(&resp).
		Fetch(ctx)

	if err != nil {
		slog.Error("Failed to fetch", "err", err)
		os.Exit(1)
	}

	slog.DebugContext(ctx, "Fetched gateway info", "resp", resp)

	slog.Info(
		"identify budget",
		"remaining", resp.SessionStartLimit.Remaining,
		"total", resp.SessionStartLimit.Total,
		"reset_after", time.Duration(resp.SessionStartLimit.ResetAfter)*time.Millisecond,
		"num_shards", resp.Shards,
		"max_concurrency", resp.SessionStartLimit.MaxConcurrency,
	)

	if err = checkIdentifyBudget(resp.SessionStartLimit, resp.Shards); err != nil {
		slog.Error(
			"refusing to start, identify budget too low",
			"remaining", resp.SessionStartLimit.Remaining,
			"total", resp.SessionStartLimit.Total,
			"needed", resp.Shards+identifyBudgetMargin,
			"reset_after", time.Duration(resp.SessionStartLimit.ResetAfter)*time.Millisecond,
			"fatal", true,
			"err", err,
		)

		js.Close()
		os.Exit(1)
	}

	// TODO: get intents from config
	const intents = gateway.IntentGuilds | gateway.IntentGuildMessages | gateway.IntentMessageContent

	slog.DebugContext(
		ctx, "Starting shard manager", "url", resp.URL, "intents", intents, "numShards", resp.Shards,
	)

	if err = reconcileProxyState(ctx, js, int(intents), resp.Shards); err != nil {
		slog.Error("failed to reconcile proxy state", "err", err)
		os.Exit(1)
	}

	manager := gateway.NewShardManager(
		gateway.ShardManagerConfig{
			URL:            resp.URL,
			Token:          token,
			Intents:        int(intents),
			NumShards:      resp.Shards,
			Publisher:      js,
			SessionStore:   js,
			MaxConcurrency: resp.SessionStartLimit.MaxConcurrency,
		},
	)

	if mErr := manager.Start(ctx); mErr != nil {
		slog.Error("Failed to start shard manager", "err", mErr)
		os.Exit(1)
	}

	fatal := make(chan error, 1)
	for id := range resp.Shards {
		go func() {
			if wErr := manager.WaitForShard(id); wErr != nil {
				select {
				case fatal <- fmt.Errorf("failed to wait for shard %d: %w", id, wErr):
				default:
				}
			}
		}()
	}

	var fatalErr error
	select {
	case <-ctx.Done():
		slog.Info("Shutting down")
	case fatalErr = <-fatal:
		slog.Error("Shard failed permanently, shutting down", "err", fatalErr)
	}

	manager.Shutdown()

	if fatalErr != nil {
		js.Close()
		os.Exit(1)
	}
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
	maps.Copy(req.Header, t.Headers)
	return t.RoundTripper.RoundTrip(req)
}

// TODO: make this configurable once config loading exists

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
