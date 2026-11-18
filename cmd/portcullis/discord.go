package main

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"time"

	"github.com/ShadyDonkey/portcullis/internal/config"
	"github.com/carlmjohnson/requests"
)

const identifyBudgetMargin = 10

type sessionStartLimit struct {
	Total          int `json:"total"`
	Remaining      int `json:"remaining"`
	ResetAfter     int `json:"reset_after"`
	MaxConcurrency int `json:"max_concurrency"`
}

type gatewayBotResponse struct {
	URL               string            `json:"url"`
	Shards            int               `json:"shards"`
	SessionStartLimit sessionStartLimit `json:"session_start_limit"`
}

func newDiscordClient(token string) *http.Client {
	return &http.Client{
		Timeout: 15 * time.Second,
		Transport: Transport{
			RoundTripper: http.DefaultTransport,
			Headers: http.Header{
				"Authorization": {fmt.Sprintf("Bot %s", token)},
				// TODO: replace with real build version
				"User-Agent": {fmt.Sprintf("DiscordBot (https://portcullisgw.com, %s)", "0.0.0")},
			},
		},
	}
}

func fetchGatewayBot(ctx context.Context, client *http.Client) (gatewayBotResponse, error) {
	var resp gatewayBotResponse

	// TODO: Add config do the base URL (ref: nirn)
	err := requests.
		URL("https://discord.com/api/v10/gateway/bot").
		Client(client).
		ToJSON(&resp).
		Fetch(ctx)

	return resp, err
}

func resolveSharding(
	ctx context.Context, sharding config.ShardingConfig, recommended int,
) (numShards int, reshardInterval time.Duration) {
	if override := sharding.ShardCountOverride; override != nil {
		if *override < recommended {
			slog.WarnContext(
				ctx, "shard_count_override is below Discord's recommendation", "override", *override, "recommended",
				recommended,
			)
		}

		return *override, 0
	}

	return recommended, time.Duration(*sharding.ReshardCheckInterval)
}

func checkIdentifyBudget(limit sessionStartLimit, numShards int) error {
	needed := numShards + identifyBudgetMargin
	if limit.Remaining < needed {
		return fmt.Errorf("identify budget nearly exhausted, not starting")
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
