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

type IncomingPayload struct {
	Op       Opcode          `json:"op"`
	Data     json.RawMessage `json:"d"`
	Sequence *int            `json:"s"`
	Type     string          `json:"t"`
}

type Shard struct {
	id           int
	conn         *websocket.Conn
	inbound      chan IncomingPayload
	lastSequence *int
	token        string
	intents      int
	numShards    int
	publisher    EventPublisher
	sessionStore SessionStore
	session      *Session
}

type ShardConfig struct {
	URL             string
	ID              int
	Token           string
	Intents         int
	NumShards       int
	Publisher       EventPublisher
	SessionStore    SessionStore
	IdentifyLimiter *bucketLimiter
}

func NewShard(ctx context.Context, config ShardConfig) (*Shard, error) {
	stored, err := config.SessionStore.GetSession(ctx, config.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to load session: %w", err)
	}

	dialURL := config.URL
	var session *Session
	var lastSequence *int

	if stored != nil && stored.ID != "" && stored.ResumeGatewayURL != "" && stored.LastSequence != nil {
		session = stored
		seq := *stored.LastSequence
		lastSequence = &seq
		dialURL = stored.ResumeGatewayURL

		slog.InfoContext(ctx, "found stored session, will attempt to resume", "shard_id", config.ID, "sequence", seq)
	}

	if session == nil && config.IdentifyLimiter != nil {
		if err = config.IdentifyLimiter.Wait(ctx, config.ID); err != nil {
			return nil, fmt.Errorf("failed to wait for identification slot: %w", err)
		}
	}

	c, _, dialErr := websocket.Dial(ctx, dialURL, nil)

	if dialErr != nil {
		return nil, fmt.Errorf("failed to dial websocket: %w", dialErr)
	}

	c.SetReadLimit(websocketMaxMessageSize)

	shard := &Shard{
		id:           config.ID,
		conn:         c,
		inbound:      make(chan IncomingPayload),
		lastSequence: lastSequence,
		session:      session,
		token:        config.Token,
		intents:      config.Intents,
		numShards:    config.NumShards,
		publisher:    config.Publisher,
		sessionStore: config.SessionStore,
	}

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

	st := &loopState{ackReceived: true}

	helloTimer := time.NewTimer(helloTimeout)
	defer helloTimer.Stop()

	for {
		select {
		case p := <-s.inbound:
			if p.Op == OpHello {
				helloTimer.Stop()
			}
			if err := s.handlePayload(ctx, runCtx, p, st); err != nil {
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
	ackReceived bool
	identified  bool
}

func (s *Shard) handlePayload(ctx, runCtx context.Context, p IncomingPayload, st *loopState) error {
	switch p.Op {
	case OpHello:
		if st.heartbeatCh != nil {
			slog.WarnContext(ctx, "duplicate hello ignored")
			return nil
		}

		var hello RecvHelloData
		if err := json.Unmarshal(p.Data, &hello); err != nil {
			return fmt.Errorf("failed to unmarshal hello data: %w", err)
		}

		st.heartbeatCh = s.defibrillate(runCtx, hello.HeartbeatInterval)

		if !st.identified {
			if s.session != nil {
				if err := s.resume(ctx); err != nil {
					return fmt.Errorf("failed to resume session: %w", err)
				}

				slog.InfoContext(ctx, "resuming session", "shard_id", s.id, "sequence", *s.lastSequence)
			} else {
				if err := s.identify(ctx); err != nil {
					return fmt.Errorf("failed to identify session: %w", err)
				}
			}

			st.identified = true
		}

	case OpHeartbeatAck:
		st.ackReceived = true

	case OpHeartbeat:
		if err := s.sendHeartbeat(ctx); err != nil {
			return err
		}

	case OpDispatch:
		var seqCopy *int
		if p.Sequence != nil {
			seq := *p.Sequence
			seqCopy = &seq
		}
		s.lastSequence = seqCopy

		if p.Type == "READY" {
			var ready RecvReadyData
			if err := json.Unmarshal(p.Data, &ready); err != nil {
				return fmt.Errorf("failed to unmarshal ready data: %w", err)
			}

			if ready.Shard != nil && (ready.Shard[0] != s.id || ready.Shard[1] != s.numShards) {
				slog.WarnContext(
					ctx, "ready shard mismatch", "expected_id", s.id, "actual_id", ready.Shard[0],
					"expected_num_shards", s.numShards, "actual_num_shards", ready.Shard[1],
				)
			}

			s.session = &Session{
				ID:               ready.SessionID,
				ResumeGatewayURL: ready.ResumeGatewayURL,

				// TODO: do I still need to do last sequence assignment?
				// LastSequence:     s.lastSequence,
			}

			s.persistSession(ctx)
			return nil
		}

		if p.Type == "RESUMED" {
			slog.InfoContext(ctx, "session resumed", "shard_id", s.id)
			return nil
		}

		slog.Debug("received event", "type", p.Type, "data", p.Data, "seq", p.Sequence)

		if err := s.publisher.Publish(ctx, p.Type, p.Data); err != nil {
			slog.ErrorContext(ctx, "failed to publish event", "err", err, "type", p.Type, "seq", p.Sequence)
		}

		return nil

	case OpReconnect:
		return fmt.Errorf("%w: op %d", ErrReconnectRequested, p.Op)
	case OpInvalidSession:
		var resumable bool
		_ = json.Unmarshal(p.Data, &resumable)

		if !resumable {
			s.session = nil
			s.lastSequence = nil
		}

		return &InvalidSessionError{Resumable: resumable}
	default:
		slog.WarnContext(ctx, "unhandled opcode case", "op", p.Op)
	}

	return nil
}

func (s *Shard) handleHeartbeatTick(ctx context.Context, st *loopState) error {
	if !st.ackReceived {
		return ErrNoHeartbeatAck
	}

	st.ackReceived = false
	slog.DebugContext(ctx, "sending heartbeat")

	if err := s.sendHeartbeat(ctx); err != nil {
		return fmt.Errorf("failed to send heartbeat (shard ID %d): %w", s.id, err)
	}

	s.persistSession(ctx)

	return nil
}

func (s *Shard) readFaucet(ctx context.Context, errChan chan<- error) {
	for {
		// TODO: will need message type if I add compression
		_, data, err := s.conn.Read(ctx)
		if err != nil {
			if code := websocket.CloseStatus(err); code != -1 {
				err = &CloseError{Code: int(code), Resumable: isResumableCloseCode(int(code)), Err: err}
			}
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
		Data: s.lastSequence,
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
			Sequence:  *s.lastSequence,
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
	if s.session == nil || s.lastSequence == nil {
		return
	}

	seq := *s.lastSequence
	sess := Session{
		ID:               s.session.ID,
		ResumeGatewayURL: s.session.ResumeGatewayURL,
		LastSequence:     &seq,
	}

	putCtx, cancel := context.WithTimeout(ctx, sessionStoreTimeout)
	defer cancel()

	if err := s.sessionStore.PutSession(putCtx, s.id, sess); err != nil {
		slog.ErrorContext(ctx, "failed to persist session", "shard_id", s.id, "err", err)
	}
}
