package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/ShadyDonkey/portcullis/internal/framepool"
	"github.com/coder/websocket"
)

var (
	eventReady   = []byte("READY")
	eventResumed = []byte("RESUMED")
	parseErrors  atomic.Uint64
)

func ParseErrors() uint64 {
	return parseErrors.Load()
}

type controlDispatch uint8

const (
	dispatchNone controlDispatch = iota
	dispatchReady
	dispatchResumed
)

type controlMessage struct {
	op                Opcode
	heartbeatInterval int
	sessionID         string
	resumeGatewayURL  string
	shard             [2]int
	hasShard          bool
	resumable         bool
	dispatch          controlDispatch
}

type envelope struct {
	op                Opcode
	seq               int64
	eventType         [EventTypeMaxLength]byte
	eventTypeLen      int
	eventTypeOverflow bool
}

type Shard struct {
	id           int
	conn         *websocket.Conn
	control      chan controlMessage
	ackReceived  atomic.Bool
	lastSequence atomic.Int64
	token        string
	intents      int
	numShards    int
	publisher    EventPublisher
	sessionStore SessionStore
	session      *Session
	filter       EventFilter
}

type ShardConfig struct {
	URL               string
	ID                int
	Token             string
	Intents           int
	NumShards         int
	Publisher         EventPublisher
	SessionStore      SessionStore
	IdentifyLimiter   *bucketLimiter
	Filter            EventFilter
	ForceFreshSession bool
}

func NewShard(ctx context.Context, config ShardConfig) (*Shard, error) {
	stored, err := config.SessionStore.GetSession(ctx, config.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to load session: %w", err)
	}

	dialURL := config.URL
	var session *Session
	var lastSequence int64

	if stored != nil && !config.ForceFreshSession && stored.ID != "" && stored.ResumeGatewayURL != "" && stored.LastSequence != nil {
		session = stored
		seq := *stored.LastSequence
		lastSequence = int64(seq)
		dialURL = stored.ResumeGatewayURL

		slog.InfoContext(ctx, "found stored session, will attempt to resume", "shard_id", config.ID, "sequence", seq)
	}

	if session == nil && config.IdentifyLimiter != nil {
		if err = config.IdentifyLimiter.Wait(ctx, config.ID); err != nil {
			return nil, fmt.Errorf("failed to wait for identification slot: %w", err)
		}
	}

	dialURL, err = withGatewayParams(dialURL)

	if err != nil {
		return nil, fmt.Errorf("failed to parse gateway URL: %w", err)
	}

	c, _, dialErr := websocket.Dial(ctx, dialURL, nil)

	if dialErr != nil {
		return nil, fmt.Errorf("failed to dial websocket: %w", dialErr)
	}

	c.SetReadLimit(websocketMaxMessageSize)

	shard := &Shard{
		id:           config.ID,
		conn:         c,
		control:      make(chan controlMessage, controlChanSize),
		lastSequence: atomic.Int64{},
		session:      session,
		token:        config.Token,
		intents:      config.Intents,
		numShards:    config.NumShards,
		publisher:    config.Publisher,
		sessionStore: config.SessionStore,
		filter:       config.Filter,
	}

	shard.lastSequence.Store(lastSequence)
	shard.ackReceived.Store(true)

	return shard, nil
}

func (s *Shard) Close() error {
	return s.conn.CloseNow()
}

func (s *Shard) Start(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer s.persistSession(context.WithoutCancel(ctx))

	errCh := make(chan error, 1)
	go s.readFaucet(runCtx, errCh)

	st := &loopState{}

	helloTimer := time.NewTimer(helloTimeout)
	defer helloTimer.Stop()

	for {
		select {
		case m := <-s.control:
			if m.op == OpHello {
				helloTimer.Stop()

				// A tick may have raced with hello processing; drain it so a
				// stale value can't be picked up and falsely end the shard.
				select {
				case <-helloTimer.C:
				default:
				}
			}
			if err := s.handleControl(ctx, runCtx, m, st); err != nil {
				return err
			}

		case <-st.heartbeatCh:
			if err := s.handleHeartbeatTick(ctx, st); err != nil {
				return err
			}

		case <-helloTimer.C:
			return ErrHelloTimeout

		case err := <-errCh:
			return fmt.Errorf("faucet failure: %w", err)

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type loopState struct {
	heartbeatCh <-chan time.Time
	identified  bool
}

func (s *Shard) handleControl(ctx, runCtx context.Context, m controlMessage, st *loopState) error {
	switch m.op {
	case OpHello:
		if st.heartbeatCh != nil {
			slog.WarnContext(ctx, "duplicate hello ignored")
			return nil
		}

		st.heartbeatCh = s.defibrillate(runCtx, m.heartbeatInterval)

		if !st.identified {
			if s.session != nil {
				if err := s.resume(ctx); err != nil {
					return fmt.Errorf("failed to resume session: %w", err)
				}

				slog.InfoContext(ctx, "resuming session", "shard_id", s.id, "sequence", s.lastSequence.Load())
			} else {
				if err := s.identify(ctx); err != nil {
					return fmt.Errorf("failed to identify session: %w", err)
				}
			}

			st.identified = true
		}

	case OpHeartbeat:
		if err := s.sendHeartbeat(ctx); err != nil {
			return err
		}

	case OpDispatch:
		switch m.dispatch {
		case dispatchReady:
			if m.hasShard && (m.shard[0] != s.id || m.shard[1] != s.numShards) {
				slog.WarnContext(
					ctx,
					fmt.Sprintf(
						"ready shard mismatch. expect [%d, %d], got [%d, %d]",
						s.id, m.shard[0], s.numShards, m.shard[1],
					),
					"expected_id", s.id,
					"actual_id", m.shard[0],
					"expected_num_shards", s.numShards,
					"actual_num_shards", m.shard[1],
				)
			}

			s.session = &Session{
				ID:               m.sessionID,
				ResumeGatewayURL: m.resumeGatewayURL,
			}

			s.persistSession(ctx)

			return nil

		case dispatchResumed:
			slog.InfoContext(ctx, fmt.Sprintf("resuming session on shard %d", s.id))
			return nil
		}

	case OpReconnect:
		return fmt.Errorf("%w: op %d", ErrReconnectRequested, m.op)

	case OpInvalidSession:
		if !m.resumable {
			s.session = nil
			s.lastSequence.Store(0)
		}

		return &InvalidSessionError{
			Resumable: m.resumable,
		}

	default:
		slog.WarnContext(ctx, "unhandled opcode case", "op", m.op)
	}

	return nil
}

func (s *Shard) handleHeartbeatTick(ctx context.Context, st *loopState) error {
	if !s.ackReceived.Swap(false) {
		return ErrNoHeartbeatAck
	}

	slog.DebugContext(ctx, "sending heartbeat")

	if err := s.sendHeartbeat(ctx); err != nil {
		return fmt.Errorf("failed to send heartbeat (shard ID %d): %w", s.id, err)
	}

	s.persistSession(ctx)

	return nil
}

func (s *Shard) readFrame(ctx context.Context) (websocket.MessageType, []byte, error) {
	typ, r, err := s.conn.Reader(ctx)
	if err != nil {
		return 0, nil, err
	}

	buf := framepool.Get(0)

	var readErr error
	for readErr == nil {
		if len(buf) == cap(buf) {
			if cap(buf) >= websocketMaxMessageSize {
				framepool.Put(buf)
				return 0, nil, fmt.Errorf("gateway frame exceeds %d bytes", websocketMaxMessageSize)
			}

			bigger := framepool.Get(cap(buf) * 2)
			copy(bigger[:cap(bigger)], buf)
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
		return 0, nil, readErr
	}

	if cap(buf) > framepool.MaxPoolSize {
		return typ, buf[:len(buf):len(buf)], nil
	}

	return typ, buf, nil
}

func (s *Shard) parseEnvelope(dec *jsontext.Decoder, r *bytes.Reader, frame []byte) (envelope, error) {
	var env envelope

	r.Reset(frame)
	dec.Reset(r, jsontext.AllowInvalidUTF8(true), jsontext.AllowDuplicateNames(true))

	tok, err := dec.ReadToken()

	if err != nil {
		return env, err
	}

	if tok.Kind() != jsontext.KindBeginObject {
		return env, fmt.Errorf("invalid envelope kind %d, expected JSON object", tok.Kind())
	}

	for {
		nameTok, nameErr := dec.ReadToken()

		if nameErr != nil {
			return env, nameErr
		}

		if nameTok.Kind() == jsontext.KindEndObject {
			return env, nil
		}

		switch name := nameTok.String(); name {
		case "op":
			opTok, opTokErr := dec.ReadToken()

			if opTokErr != nil {
				return env, opTokErr
			}

			v, vErr := opTok.Int()

			if vErr != nil {
				return env, vErr
			}

			env.op = Opcode(v)

		case "s":
			if dec.PeekKind() == jsontext.KindNull {
				if _, peekErr := dec.ReadToken(); peekErr != nil {
					return env, peekErr
				}

				env.seq = 0
			} else {
				seqTok, seqTokErr := dec.ReadToken()

				if seqTokErr != nil {
					return env, seqTokErr
				}

				v, vErr := seqTok.Int()

				if vErr != nil {
					return env, vErr
				}

				env.seq = v
			}

		case "t":
			tTok, tTokErr := dec.ReadToken()

			if tTokErr != nil {
				return env, tTokErr
			}

			t := tTok.String()
			env.eventTypeLen = len(t)
			env.eventTypeOverflow = len(t) > EventTypeMaxLength
			copy(env.eventType[:], t)

		default:
			if skipErr := dec.SkipValue(); skipErr != nil {
				return env, skipErr
			}
		}
	}
}

func (s *Shard) sendControl(ctx context.Context, msg controlMessage) {
	select {
	case s.control <- msg:
	default:
		slog.ErrorContext(ctx, "shard control channel full, dropping control message", "op", msg.op)
	}
}

func (s *Shard) faucetFail(ctx context.Context, errChan chan<- error, frame []byte, err error) {
	framepool.Put(frame)

	select {
	case errChan <- err:
	case <-ctx.Done():
	}
}

func (s *Shard) readFaucet(ctx context.Context, errChan chan<- error) {
	dec := jsontext.NewDecoder(
		bytes.NewReader(nil),
		jsontext.AllowInvalidUTF8(true),
		jsontext.AllowDuplicateNames(true),
	)

	var frameReader bytes.Reader

	for {
		// TODO: will need message type if I add compression
		_, frame, err := s.readFrame(ctx)

		if err != nil {
			if code := websocket.CloseStatus(err); code != -1 {
				err = &CloseError{
					Code:      int(code),
					Resumable: isResumableCloseCode(int(code)),
					Err:       err,
				}
			}

			select {
			case errChan <- err:
			case <-ctx.Done():
			}

			return
		}

		env, parseErr := s.parseEnvelope(dec, &frameReader, frame)

		if parseErr != nil {
			parseErrors.Add(1)
			slog.ErrorContext(ctx, "failed to parse payload", "err", parseErr)
			framepool.Put(frame)
			continue
		}

		if env.op == OpHeartbeatAck {
			s.ackReceived.Store(true)
			framepool.Put(frame)
			continue
		}

		if env.op == OpDispatch {
			s.lastSequence.Store(env.seq)
			n := env.eventTypeLen

			if n > EventTypeMaxLength {
				n = EventTypeMaxLength
			}

			evType := env.eventType[:n]

			if !env.eventTypeOverflow && bytes.Equal(evType, eventReady) {
				var ready RecvReadyData

				if decodeErr := decodeControlData(dec, &frameReader, frame, &ready); decodeErr != nil {
					parseErrors.Add(1)
					s.faucetFail(ctx, errChan, frame, fmt.Errorf("failed to decode control data for ready: %w", decodeErr))
					return
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

				s.sendControl(ctx, msg)
				framepool.Put(frame)
				continue
			}

			if !env.eventTypeOverflow && bytes.Equal(evType, eventResumed) {
				s.sendControl(
					ctx, controlMessage{
						op:       OpDispatch,
						dispatch: dispatchResumed,
					},
				)
				framepool.Put(frame)
				continue
			}

			if slog.Default().Enabled(ctx, slog.LevelDebug) {
				slog.DebugContext(ctx, "received event", "type", string(evType), "data", frame, "seq", env.seq)
			}

			if !env.eventTypeOverflow && !s.filter.Allow(evType) {
				if slog.Default().Enabled(ctx, slog.LevelDebug) {
					slog.DebugContext(ctx, "disallowed event", "type", string(evType), "data", frame)
				}

				// TODO: maybe I ignore this? need to test.
				framepool.Put(frame)
				continue
			}

			_ = s.publisher.Publish(ctx, env.eventType, env.eventTypeLen, frame)
			continue
		}

		msg := controlMessage{op: env.op}

		switch env.op {
		case OpHello:
			var hello RecvHelloData
			if decodeErr := decodeControlData(dec, &frameReader, frame, &hello); decodeErr != nil {
				parseErrors.Add(1)
				s.faucetFail(ctx, errChan, frame, fmt.Errorf("failed to decode control data for hello: %w", decodeErr))
				return
			}

			msg.heartbeatInterval = hello.HeartbeatInterval

		case OpInvalidSession:
			var resumable bool
			if decodeErr := decodeControlData(dec, &frameReader, frame, &resumable); decodeErr != nil {
				parseErrors.Add(1)
				slog.ErrorContext(
					ctx, fmt.Sprintf("failed to decode control data for invalid session: %s", decodeErr), "op", msg.op,
					"err", decodeErr,
				)
			}

			msg.resumable = resumable
		}

		s.sendControl(ctx, msg)
		framepool.Put(frame)
	}
}

func (s *Shard) lastSequencePtr() *int {
	seq := s.lastSequence.Load()

	if seq == 0 {
		return nil
	}

	v := int(seq)
	return &v
}

func (s *Shard) sendHeartbeat(ctx context.Context) error {
	payload := struct {
		Op   Opcode `json:"op"`
		Data *int   `json:"d"`
	}{
		Op:   OpHeartbeat,
		Data: s.lastSequencePtr(),
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
		Data sendIdentifyData `json:"d"`
	}{
		Op: OpIdentify,
		Data: sendIdentifyData{
			Token: s.token,
			Properties: sendIdentifyProperties{
				OS:      runtime.GOOS,
				Browser: "portcullis",
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

func (s *Shard) resume(ctx context.Context) error {
	payload := struct {
		Op   Opcode         `json:"op"`
		Data sendResumeData `json:"d"`
	}{
		Op: OpResume,
		Data: sendResumeData{
			Token:     s.token,
			SessionID: s.session.ID,
			Sequence:  int(s.lastSequence.Load()),
		},
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	writeCtx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()

	if err = s.conn.Write(writeCtx, websocket.MessageText, data); err != nil {
		return fmt.Errorf("failed to write resume payload: %w", err)
	}

	return nil
}

func (s *Shard) persistSession(ctx context.Context) {
	seq := s.lastSequence.Load()

	if s.session == nil || seq == 0 {
		return
	}

	seqCopy := int(seq)
	sess := Session{
		ID:               s.session.ID,
		ResumeGatewayURL: s.session.ResumeGatewayURL,
		LastSequence:     &seqCopy,
	}

	putCtx, cancel := context.WithTimeout(ctx, sessionStoreTimeout)
	defer cancel()

	if err := s.sessionStore.PutSession(putCtx, s.id, sess); err != nil {
		slog.WarnContext(ctx, fmt.Sprintf("failed to persist session for shard %d", s.id), "shard_id", s.id, "err", err)
	}
}
func decodeControlData(dec *jsontext.Decoder, r *bytes.Reader, frame []byte, dst any) error {
	r.Reset(frame)
	dec.Reset(r, jsontext.AllowInvalidUTF8(true), jsontext.AllowDuplicateNames(true))

	tok, err := dec.ReadToken()

	if err != nil {
		return err
	}

	if tok.Kind() != jsontext.KindBeginObject {
		return fmt.Errorf("invalid token kind %v, expected JSON object", tok.Kind())
	}

	for {
		nameTok, nameErr := dec.ReadToken()

		if nameErr != nil {
			return nameErr
		}

		if nameTok.Kind() == jsontext.KindEndObject {
			return fmt.Errorf("control frame has no 'd'")
		}

		if nameTok.String() != "d" {
			if skipErr := dec.SkipValue(); skipErr != nil {
				return skipErr
			}

			continue
		}

		v, vErr := dec.ReadValue()

		if vErr != nil {
			return vErr
		}

		return json.Unmarshal(v, dst)
	}
}
