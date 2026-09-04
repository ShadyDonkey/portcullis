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

	"github.com/ShadyDonkey/portcullis/internal/gateway"
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

	httpClient := &http.Client{
		Transport: Transport{
			RoundTripper: http.DefaultTransport,
			Headers: http.Header{
				"Authorization": {fmt.Sprintf("Bot %s", token)},
				// TODO: replace with real build version and domain name
				"User-Agent": {fmt.Sprintf("DiscordBot (https://github.com/ShadyDonkey/portcullis, %s)", "0.0.0")},
			},
		},
	}

	var resp struct {
		URL               string `json:"url"`
		Shards            int    `json:"shards"`
		SessionStartLimit struct {
			Total          int `json:"total"`
			Remaining      int `json:"remaining"`
			ResetAfter     int `json:"reset_after"`
			MaxConcurrency int `json:"max_concurrency"`
		} `json:"session_start_limit"`
	}

	err := requests.
		URL("https://discord.com/api/v10/gateway/bot").
		Client(httpClient).
		ToJSON(&resp).
		Fetch(ctx)

	if err != nil {
		slog.Error("Failed to fetch", "err", err)
		os.Exit(1)
	}

	slog.DebugContext(ctx, "Fetched gateway info", "resp", resp)

	// TODO: get intents from config
	const intents = gateway.IntentGuilds

	shard, err := gateway.NewShard(
		ctx, gateway.ShardConfig{
			URL:       resp.URL,
			ID:        0,
			Token:     token,
			Intents:   int(intents),
			NumShards: resp.Shards,
		},
	)
	if err != nil {
		slog.Error("Failed to create shard", "err", err)
		os.Exit(1)
	}

	defer func(shard *gateway.Shard) {
		err = shard.Close()
		if err != nil {
			slog.Error("Failed to close shard", "err", err)
		}
	}(shard)

	if sErr := shard.Start(ctx); sErr != nil {
		slog.Error("error occurred on shard", "err", sErr)
		os.Exit(1)
	}

}

type Transport struct {
	http.RoundTripper
	Headers http.Header
}

func (t Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	maps.Copy(req.Header, t.Headers)
	return t.RoundTripper.RoundTrip(req)
}
