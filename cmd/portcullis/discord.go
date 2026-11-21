package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ShadyDonkey/portcullis/internal/config"
	"github.com/ShadyDonkey/portcullis/internal/gateway"
	"github.com/carlmjohnson/requests"
)

const (
	identifyBudgetMargin    = 10
	gatewayBotMaxAttempts   = 4
	gatewayBotBaseBackoff   = 1 * time.Second
	gatewayBotRetryAfterCap = 30 * time.Second

	defaultDiscordAPIBase = "https://discord.com/api"
)

var version = "dev"

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
				"User-Agent":    {fmt.Sprintf("DiscordBot (https://portcullisgw.com, %s)", version)},
			},
		},
	}
}

func fetchGatewayBot(ctx context.Context, client *http.Client, apiBaseURL string) (gatewayBotResponse, error) {
	if apiBaseURL == "" {
		apiBaseURL = defaultDiscordAPIBase
	}

	endpoint := fmt.Sprintf("%s/v%d/gateway/bot", strings.TrimSuffix(apiBaseURL, "/"), gateway.Version)

	var resp gatewayBotResponse
	backoff := gatewayBotBaseBackoff

	for attempt := 1; ; attempt++ {
		headers := map[string][]string{}

		err := requests.
			URL(endpoint).
			Client(client).
			AddValidator(requests.ChainHandlers(requests.CopyHeaders(headers), requests.DefaultValidator)).
			ToJSON(&resp).
			Fetch(ctx)

		if err == nil {
			break
		}

		if ctxErr := ctx.Err(); ctxErr != nil {
			return gatewayBotResponse{}, ctxErr
		}

		var se *requests.ResponseError
		switch {
		case errors.As(err, &se) && se.StatusCode == http.StatusUnauthorized:
			return gatewayBotResponse{}, fmt.Errorf("discord rejected the bot token (HTTP 401): %w", err)

		case errors.As(err, &se) && se.StatusCode == http.StatusTooManyRequests:
			if attempt == gatewayBotMaxAttempts {
				return gatewayBotResponse{}, fmt.Errorf("fetch gateway/bot failed after %d attempts: %w", attempt, err)
			}
			delay := retryAfterDelay(headers, backoff)
			slog.WarnContext(ctx, "gateway/bot rate limited, retrying", "attempt", attempt, "delay", delay)
			if waitErr := sleepCtx(ctx, delay); waitErr != nil {
				return gatewayBotResponse{}, waitErr
			}

		case errors.As(err, &se) && se.StatusCode >= 500, errors.Is(err, requests.ErrTransport):
			if attempt == gatewayBotMaxAttempts {
				return gatewayBotResponse{}, fmt.Errorf("fetch gateway/bot failed after %d attempts: %w", attempt, err)
			}
			slog.WarnContext(
				ctx, "gateway/bot fetch failed, retrying", "attempt", attempt, "delay", backoff, "err", err,
			)
			if waitErr := sleepCtx(ctx, backoff); waitErr != nil {
				return gatewayBotResponse{}, waitErr
			}
			backoff *= 2

		default:
			return gatewayBotResponse{}, fmt.Errorf("fetch gateway/bot failed: %w", err)
		}
	}

	if err := validateGatewayBot(resp); err != nil {
		return gatewayBotResponse{}, err
	}

	return resp, nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func retryAfterDelay(headers map[string][]string, fallback time.Duration) time.Duration {
	if vals := headers["Retry-After"]; len(vals) > 0 {
		if secs, err := strconv.ParseFloat(vals[0], 64); err == nil && secs > 0 {
			return min(time.Duration(secs*float64(time.Second)), gatewayBotRetryAfterCap)
		}
	}
	return fallback
}

func validateGatewayBot(resp gatewayBotResponse) error {
	if resp.URL == "" {
		return errors.New("gateway/bot response missing gateway URL")
	}

	if u, err := url.Parse(resp.URL); err != nil || u.Scheme != "wss" || u.Host == "" {
		return fmt.Errorf("gateway/bot response has invalid gateway URL %q", resp.URL)
	}

	if resp.Shards < 1 {
		return fmt.Errorf("gateway/bot response has invalid shard count %d", resp.Shards)
	}

	l := resp.SessionStartLimit

	if l.Total < 1 || l.MaxConcurrency < 1 || l.ResetAfter < 0 {
		return fmt.Errorf("gateway/bot response has invalid session_start_limit %+v", l)
	}

	return nil
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
