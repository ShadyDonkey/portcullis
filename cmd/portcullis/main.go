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

	"github.com/carlmjohnson/requests"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	token := os.Getenv("DISCORD_TOKEN")

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

	slog.Info("Connected to Discord gateway", "resp", resp)

	//
	// c, _, dialErr := websocket.Dial(ctx, fmt.Sprintf("%s?v=10&encoding=json", resp.URL), nil)
	// if dialErr != nil {
	// 	slog.Error("Failed to connect", "err", dialErr)
	// 	os.Exit(1)
	// }
	//
	// defer func(c *websocket.Conn) {
	// 	err = c.CloseNow()
	// 	if err != nil {
	// 		slog.Error("Failed to close connection", "err", err)
	// 	}
	// }(c)

	// for {
	// 	msgType, data, readErr := c.Read(ctx)
	// 	if readErr != nil {
	// 		slog.Error("Failed to read message", "err", readErr)
	// 		break
	// 	}
	//
	// 	var env Envelope
	// 	err = json.Unmarshal(data, &env)
	// 	if err != nil {
	// 		slog.Error("Failed to unmarshal message", "err", err)
	// 		break
	// 	}
	//
	// 	slog.Info("Read message", "op", env.Op, "seq", env.S, "type", env.T, "data", string(env.D), "msgType", msgType)
	// }
}

type Transport struct {
	http.RoundTripper
	Headers http.Header
}

func (t Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	maps.Copy(req.Header, t.Headers)
	return t.RoundTripper.RoundTrip(req)
}
