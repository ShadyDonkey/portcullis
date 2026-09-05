package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"runtime"
	"time"

	"github.com/coder/websocket"
)

const (
	Version         = 10
	DefaultEncoding = "json"
	writeTimeout    = 5 * time.Second
)

type IncomingPayload struct {
	Op       Opcode          `json:"op"`
	Data     json.RawMessage `json:"d"`
	Sequence *int            `json:"s"`
	Type     string          `json:"t"`
}

type Shard struct {
	id        int
	conn      *websocket.Conn
	inbound   chan IncomingPayload
	lastSeq   *int
	token     string
	intents   int
	numShards int
}

type ShardConfig struct {
	URL       string
	ID        int
	Token     string
	Intents   int
	NumShards int
}

func NewShard(ctx context.Context, config ShardConfig) (*Shard, error) {
	c, _, dialErr := websocket.Dial(ctx, fmt.Sprintf("%s?v=%d&encoding=%s", config.URL, Version, DefaultEncoding), nil)

	if dialErr != nil {
		return nil, fmt.Errorf("failed to dial websocket: %w", dialErr)
	}

	shard := &Shard{
		id:        config.ID,
		conn:      c,
		inbound:   make(chan IncomingPayload),
		lastSeq:   nil,
		token:     config.Token,
		intents:   config.Intents,
		numShards: config.NumShards,
	}

	return shard, nil
}

func (s *Shard) Close() error {
	return s.conn.CloseNow()
}

func (s *Shard) Start(ctx context.Context) error {
	errCh := make(chan error, 1)
	go s.readFaucet(ctx, errCh)

	var heartbeatCh <-chan time.Time
	ackReceived := true
	identified := false

	for {
		select {
		case i := <-s.inbound:
			{
				switch i.Op {
				case OpHello:
					{
						var hello RecvHelloData
						if err := json.Unmarshal(i.Data, &hello); err != nil {
							return fmt.Errorf("failed to unmarshal hello data: %w", err)
						}

						heartbeatCh = s.defibrillate(ctx, hello.HeartbeatInterval)
					}
				case OpHeartbeatAck:
					ackReceived = true

				case OpHeartbeat:
					{
						ackReceived = false
						err := s.sendHeartbeat(ctx)
						if err != nil {
							return err
						}
					}

				case OpDispatch:
					{
						s.lastSeq = i.Sequence

						if i.Type == "READY" || i.Type == "RESUMED" {
							// TODO: do something with this?
							continue
						}

						// TODO: handle dispatch
						slog.Debug("received event", "type", i.Type, "data", i.Data, "seq", i.Sequence)
					}

				case OpReconnect, OpInvalidSession:
					return fmt.Errorf("gateway requested reconnect, op %d", i.Op)
				default:
					slog.WarnContext(ctx, "unhandled opcode case", "op", i.Op)
				}
			}

		case <-heartbeatCh:
			{
				if !ackReceived {
					slog.ErrorContext(ctx, "heartbeat ack not received")
					return fmt.Errorf("heartbeat ack not received")
				}
				ackReceived = false
				slog.DebugContext(ctx, "sending heartbeat")
				if err := s.sendHeartbeat(ctx); err != nil {
					return err
				}

				if !identified {
					if err := s.identify(ctx); err != nil {
						return fmt.Errorf("failed to identify: %w", err)
					}
					identified = true
				}
			}

		case err := <-errCh:
			return fmt.Errorf("faucet failure: %w", err)

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Shard) readFaucet(ctx context.Context, errChan chan<- error) {
	for {
		// TODO: will need message type if I add compression
		_, data, err := s.conn.Read(ctx)
		if err != nil {
			select {
			case errChan <- err:
			case <-ctx.Done():
			}
			return
		}

		var payload IncomingPayload
		if unmarshalErr := json.Unmarshal(data, &payload); unmarshalErr != nil {
			slog.ErrorContext(ctx, "failed to unmarshal payload", "err", unmarshalErr)
			continue
		}

		select {
		case s.inbound <- payload:
		case <-ctx.Done():
			return
		}
	}
}

func (s *Shard) sendHeartbeat(ctx context.Context) error {
	payload := struct {
		Op   Opcode `json:"op"`
		Data *int   `json:"d"`
	}{
		Op:   OpHeartbeat,
		Data: s.lastSeq,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	writeCtx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()

	if err = s.conn.Write(writeCtx, websocket.MessageText, data); err != nil {
		return fmt.Errorf("failed to write payload: %w", err)
	}

	return nil
}

func (s *Shard) defibrillate(ctx context.Context, intervalMs int) <-chan time.Time {
	interval := time.Duration(intervalMs) * time.Millisecond
	jitter := rand.Float64()
	firstDelay := time.Duration(jitter * float64(interval))
	ch := make(chan time.Time)

	go func() {
		slog.DebugContext(ctx, "starting the heart", "interval", interval, "firstDelay", firstDelay)
		timer := time.NewTimer(firstDelay)

		select {
		case t := <-timer.C:
			{
				select {
				case ch <- t:
				case <-ctx.Done():
					return
				}
			}
		case <-ctx.Done():
			timer.Stop()
			return
		}

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case t := <-ticker.C:
				{
					select {
					case ch <- t:
					case <-ctx.Done():
						return
					}
				}

			case <-ctx.Done():
				return
			}
		}
	}()

	return ch
}

func (s *Shard) identify(ctx context.Context) error {
	payload := struct {
		Op   Opcode           `json:"op"`
		Data SendIdentifyData `json:"d"`
	}{
		Op: OpIdentify,
		Data: SendIdentifyData{
			Token: s.token,
			Properties: SendIdentifyProperties{
				OS: runtime.GOOS,
				// TODO: make these better
				Browser: "portcullisgw.com",
				Device:  "portcullis",
			},
			Intents: s.intents,
			Shard:   &[2]int{s.id, s.numShards},
		},
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	writeCtx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()

	if err = s.conn.Write(writeCtx, websocket.MessageText, data); err != nil {
		return fmt.Errorf("failed to write identify payload: %w", err)
	}

	return nil
}
