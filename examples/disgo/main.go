package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/disgoorg/disgo"
	"github.com/disgoorg/disgo/bot"
	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/events"
	"github.com/disgoorg/disgo/gateway"
	"github.com/disgoorg/disgo/rest"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type shim struct {
	gateway.Gateway
	shardID int
}

func (s shim) ShardID() int { return s.shardID }

func main() {
	token := os.Getenv("DISCORD_TOKEN")
	natsURL := os.Getenv("NATS_URL")
	natsUser := os.Getenv("NATS_USERNAME")
	natsPass := os.Getenv("NATS_PASSWORD")
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client, err := disgo.New(
		token,
		bot.WithLogger(log),
		bot.WithEventListeners(
			bot.NewListenerFunc(
				func(e *events.MessageCreate) {
					log.Info(
						"message_create",
						slog.Any("guild", e.GuildID),
						slog.Any("channel", e.ChannelID),
						slog.String("author", e.Message.Author.Username),
						slog.String("content", e.Message.Content),
					)
				},
			),
			bot.NewListenerFunc(
				func(e *events.GuildJoin) {
					log.Info("guild_join", slog.Any("guild", e.GuildID), slog.String("name", e.Guild.Name))
				},
			),
		),
	)

	if err != nil {
		log.Error("error while creating disgo instance", "error", err)
		os.Exit(1)
	}

	bootCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	self, err := client.Rest.GetUser(client.ApplicationID, rest.WithCtx(bootCtx))
	cancel()

	if err != nil {
		log.Error("error while fetching user", "error", err)
		os.Exit(1)
	}

	client.Caches.SetSelfUser(discord.OAuth2User{User: *self})

	nc, err := nats.Connect(
		natsURL,
		nats.UserInfo(natsUser, natsPass),
		nats.Name("consumer"),
		nats.MaxReconnects(-1),
	)

	if err != nil {
		log.Error("error while connecting to nats", "error", err)
		os.Exit(1)
	}

	js, err := jetstream.New(nc)

	if err != nil {
		log.Error("error while creating jetstream", "error", err)
		os.Exit(1)
	}

	consumer, err := js.CreateOrUpdateConsumer(
		ctx, "DISCORD_EVENTS", jetstream.ConsumerConfig{
			Durable:       "disgo-consumer",
			FilterSubject: "events.>",
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       30 * time.Second,
		},
	)

	if err != nil {
		log.Error("error while creating consumer", "error", err)
		os.Exit(1)
	}

	cc, err := consumer.Consume(
		func(frame jetstream.Msg) {
			var m gateway.Message

			if err := json.Unmarshal(frame.Data(), &m); err != nil {
				log.Warn("dropping unparsable frame", "error", err)
				_ = frame.Term()
				return
			}

			data, ok := m.D.(gateway.EventData)

			if !ok {
				log.Warn("dropping non-dispatch frame", "op", m.Op, "type", m.T)
				_ = frame.Term()
				return
			}

			if _, unknown := data.(gateway.EventUnknown); unknown {
				log.Warn("dropping unknown event", "type", m.T, "seq", m.S)
				_ = frame.Term()
				return
			}

			log.Debug("handling gateway event", "type", m.T, "seq", m.S)
			client.EventManager.HandleGatewayEvent(
				shim{shardID: 0},
				m.T,
				m.S,
				data,
			)

			// _ = m.Term()
			// _ = m.Nak()
			_ = frame.Ack()
		},
		jetstream.ConsumeErrHandler(
			func(_ jetstream.ConsumeContext, err error) {
				log.Warn("error while consuming message", "error", err)
			},
		),
	)

	if err != nil {
		log.Error("error while setting up consumer", "error", err)
		os.Exit(1)
	}

	<-ctx.Done()
	log.Info("shutting down")
	cc.Stop()
	closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client.Close(closeCtx)
	if err := nc.Drain(); err != nil {
		log.Error("error while shutting down consumer", "error", err)
		os.Exit(1)
	}
}
