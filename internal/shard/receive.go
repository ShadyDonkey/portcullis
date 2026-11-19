package shard

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	"fmt"
	"log/slog"
	"time"

	"github.com/ShadyDonkey/portcullis/internal/framepool"
	"github.com/ShadyDonkey/portcullis/internal/gateway"
	"github.com/ShadyDonkey/portcullis/internal/metrics"
	"github.com/ShadyDonkey/portcullis/internal/ws"
)

const (
	eventReady   = "READY"
	eventResumed = "RESUMED"
)

var decoderOptions = []jsontext.Options{
	jsontext.AllowInvalidUTF8(true),
	jsontext.AllowDuplicateNames(true),
}

type envelope struct {
	op        gateway.Opcode
	seq       int64
	eventType string
}

func resetDecoder(dec *jsontext.Decoder, r *bytes.Reader, frame []byte) {
	r.Reset(frame)
	dec.Reset(r, decoderOptions...)
}

func parseEnvelope(dec *jsontext.Decoder, r *bytes.Reader, frame []byte) (envelope, error) {
	var env envelope

	resetDecoder(dec, r, frame)

	tok, err := dec.ReadToken()
	if err != nil {
		return env, err
	}

	if tok.Kind() != jsontext.KindBeginObject {
		return env, fmt.Errorf("invalid envelope kind %d, expected JSON object", tok.Kind())
	}

	for {
		nameTok, err := dec.ReadToken()
		if err != nil {
			return env, err
		}

		if nameTok.Kind() == jsontext.KindEndObject {
			return env, nil
		}

		switch name := nameTok.String(); name {
		case "op":
			opTok, err := dec.ReadToken()
			if err != nil {
				return env, err
			}

			v, err := opTok.Int()
			if err != nil {
				return env, err
			}

			env.op = gateway.Opcode(v)

		case "s":
			if dec.PeekKind() == jsontext.KindNull {
				if _, err := dec.ReadToken(); err != nil {
					return env, err
				}

				env.seq = 0
			} else {
				seqTok, err := dec.ReadToken()
				if err != nil {
					return env, err
				}

				v, err := seqTok.Int()
				if err != nil {
					return env, err
				}

				env.seq = v
			}

		case "t":
			tTok, err := dec.ReadToken()
			if err != nil {
				return env, err
			}

			if tTok.Kind() == jsontext.KindString {
				env.eventType = tTok.String()
			}

		default:
			if err := dec.SkipValue(); err != nil {
				return env, err
			}
		}
	}
}

func decodeControlData(dec *jsontext.Decoder, r *bytes.Reader, frame []byte, dst any) error {
	resetDecoder(dec, r, frame)

	tok, err := dec.ReadToken()
	if err != nil {
		return err
	}

	if tok.Kind() != jsontext.KindBeginObject {
		return fmt.Errorf("invalid token kind %v, expected JSON object", tok.Kind())
	}

	for {
		nameTok, err := dec.ReadToken()
		if err != nil {
			return err
		}

		if nameTok.Kind() == jsontext.KindEndObject {
			return fmt.Errorf("control frame has no 'd'")
		}

		if nameTok.String() != "d" {
			if err := dec.SkipValue(); err != nil {
				return err
			}

			continue
		}

		v, err := dec.ReadValue()
		if err != nil {
			return err
		}

		return json.Unmarshal(v, dst)
	}
}

func (s *Shard) sendControl(msg controlMessage) bool {
	select {
	case s.control <- msg:
		return true
	default:
		return false
	}
}

func (s *Shard) failRead(ctx context.Context, errChan chan<- error, frame []byte, err error) {
	framepool.Put(frame)

	select {
	case errChan <- err:
	case <-ctx.Done():
	}
}

func (s *Shard) readLoop(ctx context.Context, errChan chan<- error) {
	dec := jsontext.NewDecoder(bytes.NewReader(nil), decoderOptions...)
	var frameReader bytes.Reader

	for {
		frame, err := s.conn.ReadFrame(ctx)
		if err != nil {
			if code := ws.CloseStatus(err); code != -1 {
				err = gateway.NewCloseError(code, err)
			}

			s.failRead(ctx, errChan, nil, err)
			return
		}

		readDone := time.Now()

		env, err := parseEnvelope(dec, &frameReader, frame)
		if err != nil {
			metrics.ShardDrop(metrics.DropParseError)
			slog.ErrorContext(ctx, "failed to parse payload", "err", err)
			framepool.Put(frame)
			continue
		}

		if env.op == gateway.OpHeartbeatAck {
			s.ackReceived.Store(true)
			framepool.Put(frame)
			continue
		}

		if env.op == gateway.OpDispatch {
			if !s.handleDispatch(ctx, dec, &frameReader, env, frame, errChan) {
				return
			}

			metrics.Dispatch(env.eventType, time.Since(readDone))

			continue
		}

		msg := controlMessage{op: env.op}

		switch env.op {
		case gateway.OpHello:
			var hello gateway.RecvHelloData
			if err := decodeControlData(dec, &frameReader, frame, &hello); err != nil {
				metrics.ShardDrop(metrics.DropParseError)
				s.failRead(
					ctx, errChan, frame, fmt.Errorf("failed to decode control data for hello: %w", err),
				)
				return
			}

			msg.heartbeatInterval = hello.HeartbeatInterval

		case gateway.OpInvalidSession:
			var resumable bool
			if err := decodeControlData(dec, &frameReader, frame, &resumable); err != nil {
				metrics.ShardDrop(metrics.DropParseError)
				slog.ErrorContext(
					ctx, "failed to decode control data for invalid session", "op", msg.op, "err", err,
				)
			}

			msg.resumable = resumable
		}

		if !s.sendControl(msg) {
			s.failRead(ctx, errChan, frame, ErrControlChannelFull)
			return
		}

		framepool.Put(frame)
	}
}

func (s *Shard) handleDispatch(
	ctx context.Context, dec *jsontext.Decoder, r *bytes.Reader, env envelope, frame []byte,
	errChan chan<- error,
) bool {
	s.lastSequence.Store(env.seq)

	if env.eventType == "" {
		metrics.ShardDrop(metrics.DropParseError)
		framepool.Put(frame)
		return true
	}

	eventType := env.eventType
	metrics.Consumed(eventType)

	if eventType == eventReady {
		var ready gateway.RecvReadyData
		if err := decodeControlData(dec, r, frame, &ready); err != nil {
			metrics.ShardDrop(metrics.DropParseError)
			s.failRead(
				ctx, errChan, frame, fmt.Errorf("failed to decode control data for ready: %w", err),
			)
			return false
		}

		msg := controlMessage{
			op:               gateway.OpDispatch,
			dispatch:         dispatchReady,
			sessionID:        ready.SessionID,
			resumeGatewayURL: ready.ResumeGatewayURL,
		}

		if ready.Shard != nil {
			msg.shard = *ready.Shard
			msg.hasShard = true
		}

		if !s.sendControl(msg) {
			s.failRead(ctx, errChan, frame, ErrControlChannelFull)
			return false
		}

		framepool.Put(frame)
		return true
	}

	if eventType == eventResumed {
		msg := controlMessage{
			op:       gateway.OpDispatch,
			dispatch: dispatchResumed,
		}

		if !s.sendControl(msg) {
			s.failRead(ctx, errChan, frame, ErrControlChannelFull)
			return false
		}

		framepool.Put(frame)
		return true
	}

	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.DebugContext(
			ctx, "received event", "type", eventType, "data", string(frame), "seq", env.seq,
		)
	}

	if !s.filter.Allow(eventType) {
		metrics.ShardDrop(metrics.DropFiltered)

		if slog.Default().Enabled(ctx, slog.LevelDebug) {
			slog.DebugContext(ctx, "disallowed event", "type", eventType, "data", string(frame))
		}

		framepool.Put(frame)
		return true
	}

	if err := s.publisher.Publish(ctx, eventType, frame); err == nil {
		metrics.Published(eventType)
	}

	return true
}
