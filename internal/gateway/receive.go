package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"

	"github.com/ShadyDonkey/portcullis/internal/framepool"
	"github.com/coder/websocket"
)

var (
	eventReady   = []byte("READY")
	eventResumed = []byte("RESUMED")

	parseErrors   atomic.Uint64
	eventOverflow atomic.Uint64

	decoderOptions = []jsontext.Options{
		jsontext.AllowInvalidUTF8(true),
		jsontext.AllowDuplicateNames(true),
	}
)

func ParseErrors() uint64 {
	return parseErrors.Load()
}

func EventTypeOverflows() uint64 {
	return eventOverflow.Load()
}

type envelope struct {
	op                Opcode
	seq               int64
	eventType         [EventTypeMaxLength]byte
	eventTypeLen      int
	eventTypeOverflow bool
}

func (s *Shard) readFrame(ctx context.Context) ([]byte, error) {
	_, r, err := s.conn.Reader(ctx)
	if err != nil {
		return nil, err
	}

	buf := framepool.Get(0)

	var readErr error
	for readErr == nil {
		if len(buf) == cap(buf) {
			if cap(buf) >= websocketMaxMessageSize {
				framepool.Put(buf)
				return nil, fmt.Errorf("gateway frame exceeds %d bytes", websocketMaxMessageSize)
			}

			bigger := framepool.Get(cap(buf) * 2)
			bigger = bigger[:cap(bigger)]
			copy(bigger, buf)
			bigger = bigger[:len(buf)]
			framepool.Put(buf)
			buf = bigger
		}

		var n int
		n, readErr = r.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]
	}

	if readErr != io.EOF {
		framepool.Put(buf)
		return nil, readErr
	}

	if cap(buf) > framepool.MaxPoolSize {
		return buf[:len(buf):len(buf)], nil
	}

	return buf, nil
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

			env.op = Opcode(v)

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

			t := tTok.String()
			env.eventTypeLen = len(t)
			env.eventTypeOverflow = len(t) > EventTypeMaxLength
			copy(env.eventType[:], t)

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
		frame, err := s.readFrame(ctx)
		if err != nil {
			if code := websocket.CloseStatus(err); code != -1 {
				err = &CloseError{
					Code:      int(code),
					Resumable: isResumableCloseCode(int(code)),
					Err:       err,
				}
			}

			s.failRead(ctx, errChan, nil, err)
			return
		}

		env, err := parseEnvelope(dec, &frameReader, frame)
		if err != nil {
			parseErrors.Add(1)
			slog.ErrorContext(ctx, "failed to parse payload", "err", err)
			framepool.Put(frame)
			continue
		}

		if env.op == OpHeartbeatAck {
			s.ackReceived.Store(true)
			framepool.Put(frame)
			continue
		}

		if env.op == OpDispatch {
			if !s.handleDispatch(ctx, dec, &frameReader, env, frame, errChan) {
				return
			}

			continue
		}

		msg := controlMessage{op: env.op}

		switch env.op {
		case OpHello:
			var hello RecvHelloData
			if err := decodeControlData(dec, &frameReader, frame, &hello); err != nil {
				parseErrors.Add(1)
				s.failRead(
					ctx, errChan, frame, fmt.Errorf("failed to decode control data for hello: %w", err),
				)
				return
			}

			msg.heartbeatInterval = hello.HeartbeatInterval

		case OpInvalidSession:
			var resumable bool
			if err := decodeControlData(dec, &frameReader, frame, &resumable); err != nil {
				parseErrors.Add(1)
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

	if env.eventTypeOverflow || env.eventTypeLen == 0 {
		eventOverflow.Add(1)
		framepool.Put(frame)
		return true
	}

	eventType := env.eventType[:env.eventTypeLen]

	if bytes.Equal(eventType, eventReady) {
		var ready RecvReadyData
		if err := decodeControlData(dec, r, frame, &ready); err != nil {
			parseErrors.Add(1)
			s.failRead(
				ctx, errChan, frame, fmt.Errorf("failed to decode control data for ready: %w", err),
			)
			return false
		}

		msg := controlMessage{
			op:               OpDispatch,
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

	if bytes.Equal(eventType, eventResumed) {
		msg := controlMessage{
			op:       OpDispatch,
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
			ctx, "received event", "type", string(eventType), "data", frame, "seq", env.seq,
		)
	}

	if !s.filter.Allow(eventType) {
		if slog.Default().Enabled(ctx, slog.LevelDebug) {
			slog.DebugContext(ctx, "disallowed event", "type", string(eventType), "data", frame)
		}

		framepool.Put(frame)
		return true
	}

	_ = s.publisher.Publish(ctx, eventType, frame)
	return true
}
