package jetstream

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ShadyDonkey/portcullis/internal/metrics"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func dialNATS(config Config, j *JetStream) (*nats.Conn, error) {
	return nats.Connect(
		config.URL,
		nats.UserInfo(config.User, config.Password),
		nats.FlusherTimeout(10*time.Second),
		nats.Name(config.ConnectionName),
		nats.MaxReconnects(natsMaxReconnects),
		nats.ReconnectWait(natsReconnectWait),
		nats.ReconnectJitter(100*time.Millisecond, time.Second),
		nats.ErrorHandler(
			func(_ *nats.Conn, _ *nats.Subscription, err error) {
				slog.Error("NATS error", "err", err)
			},
		),
		nats.DisconnectErrHandler(
			func(_ *nats.Conn, err error) {
				if j.closing.Load() {
					return
				}

				slog.Warn("NATS connection severed, reconnecting...", "err", err)
			},
		),
		nats.ReconnectHandler(
			func(nc *nats.Conn) {
				metrics.NATSReconnect()
				slog.Info("NATS re-connected", "url", nc.ConnectedUrl())
			},
		),
		nats.ClosedHandler(
			func(nc *nats.Conn) {
				if j.closing.Load() {
					slog.Info("NATS connection closed via intentional shutdown.")
					return
				}

				cause := nc.LastError()
				if cause == nil {
					cause = errors.New("nats: connection closed, no cause recorded")
				}

				slog.Error("NATS connection closed.", "err", cause)

				select {
				case j.unexpectedClose <- fmt.Errorf("nats: connection closed unexpectedly: %w", cause):
				default:
				}
			},
		),
	)
}

func newPublisher(nc *nats.Conn, j *JetStream) (jetstream.JetStream, error) {
	return jetstream.New(
		nc,
		jetstream.WithPublishAsyncMaxPending(publishMaxPending),
		jetstream.WithPublishAsyncTimeout(publishAsyncAckTimeout),
		jetstream.WithPublishAsyncAckHandler(
			func(_ jetstream.JetStream, msg *nats.Msg, _ *jetstream.PubAck) {
				j.completeAck(msg.Data)
				j.releaseFrame(msg.Data)
				signal(j.publishSignal)
			},
		),
		jetstream.WithPublishAsyncErrHandler(
			func(_ jetstream.JetStream, msg *nats.Msg, err error) {
				j.ackFailures.record("async_ack", err, "subject", msg.Subject)
				metrics.PublishDrop(metrics.DropAckError)
				j.releaseFrame(msg.Data)
				signal(j.publishSignal)
			},
		),
	)
}
