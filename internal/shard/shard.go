package shard

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/ShadyDonkey/portcullis/internal/gateway"
	"github.com/ShadyDonkey/portcullis/internal/ws"
)

type controlDispatch uint8

const (
	dispatchNone controlDispatch = iota
	dispatchReady
	dispatchResumed
)

type controlMessage struct {
	op                gateway.Opcode
	heartbeatInterval int
	sessionID         string
	resumeGatewayURL  string
	shard             [2]int
	hasShard          bool
	resumable         bool
	dispatch          controlDispatch
}

type Shard struct {
	id           int
	conn         *ws.Conn
	control      chan controlMessage
	ackReceived  atomic.Bool
	lastSequence atomic.Int64
	token        string
	intents      int
	numShards    int
	publisher    gateway.EventPublisher
	sessionStore gateway.SessionStore
	session      *gateway.Session
	filter       gateway.EventFilter
}

type ShardConfig struct {
	URL               string
	ID                int
	Token             string
	Intents           int
	NumShards         int
	Publisher         gateway.EventPublisher
	SessionStore      gateway.SessionStore
	IdentifyLimiter   *gateway.IdentifyLimiter
	Filter            gateway.EventFilter
	ForceFreshSession bool
}

func NewShard(ctx context.Context, config ShardConfig) (*Shard, error) {
	stored, err := config.SessionStore.GetSession(ctx, config.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to load session: %w", err)
	}

	dialURL := config.URL
	var session *gateway.Session
	var lastSequence int64

	if stored != nil && !config.ForceFreshSession && stored.ID != "" && stored.ResumeGatewayURL != "" &&
		stored.LastSequence != nil {
		session = stored
		seq := *stored.LastSequence
		lastSequence = int64(seq)
		dialURL = stored.ResumeGatewayURL

		slog.InfoContext(
			ctx, "found stored session, will attempt to resume", "shard_id", config.ID, "sequence", seq,
		)
	}

	if session == nil && config.IdentifyLimiter != nil {
		if err = config.IdentifyLimiter.Wait(ctx, config.ID); err != nil {
			return nil, fmt.Errorf("failed to wait for identification slot: %w", err)
		}
	}

	dialURL, err = gateway.BuildURL(dialURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse gateway URL: %w", err)
	}

	conn, err := ws.Dial(ctx, dialURL, websocketMaxMessageSize)
	if err != nil {
		return nil, fmt.Errorf("failed to dial websocket: %w", err)
	}

	shard := &Shard{
		id:           config.ID,
		conn:         conn,
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
	return s.conn.Close()
}

func (s *Shard) Start(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer s.persistSession(context.WithoutCancel(ctx))

	errCh := make(chan error, 1)
	go s.readLoop(runCtx, errCh)

	st := &loopState{}

	helloTimer := time.NewTimer(helloTimeout)
	defer helloTimer.Stop()

	for {
		select {
		case m := <-s.control:
			if m.op == gateway.OpHello {
				helloTimer.Stop()

				select {
				case <-helloTimer.C:
				default:
				}
			}

			if err := s.handleControl(ctx, runCtx, m, st); err != nil {
				return err
			}

		case <-st.heartbeatCh:
			if err := s.handleHeartbeatTick(ctx); err != nil {
				return err
			}

		case <-helloTimer.C:
			return ErrHelloTimeout

		case err := <-errCh:
			return fmt.Errorf("read failure: %w", err)

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
	case gateway.OpHello:
		if st.heartbeatCh != nil {
			slog.WarnContext(ctx, "duplicate hello ignored")
			return nil
		}

		st.heartbeatCh = s.heartbeatTicks(runCtx, m.heartbeatInterval)

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

	case gateway.OpHeartbeat:
		if err := s.sendHeartbeat(ctx); err != nil {
			return err
		}

	case gateway.OpDispatch:
		switch m.dispatch {
		case dispatchReady:
			if m.hasShard && (m.shard[0] != s.id || m.shard[1] != s.numShards) {
				slog.WarnContext(
					ctx,
					"ready shard mismatch",
					"expected_id", s.id,
					"actual_id", m.shard[0],
					"expected_num_shards", s.numShards,
					"actual_num_shards", m.shard[1],
				)
			}

			s.session = &gateway.Session{
				ID:               m.sessionID,
				ResumeGatewayURL: m.resumeGatewayURL,
			}

			s.persistSession(ctx)
			return nil

		case dispatchResumed:
			slog.InfoContext(ctx, "session resumed", "shard_id", s.id)
			return nil
		}

	case gateway.OpReconnect:
		return ErrReconnectRequested

	case gateway.OpInvalidSession:
		if !m.resumable {
			s.session = nil
			s.lastSequence.Store(0)
		}

		return &gateway.InvalidSessionError{
			Resumable: m.resumable,
		}

	default:
		slog.WarnContext(ctx, "unhandled opcode case", "op", m.op)
	}

	return nil
}

func (s *Shard) handleHeartbeatTick(ctx context.Context) error {
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

func (s *Shard) persistSession(ctx context.Context) {
	seq := s.lastSequence.Load()
	if s.session == nil || seq == 0 {
		return
	}

	seqCopy := int(seq)
	sess := *s.session
	sess.LastSequence = &seqCopy

	putCtx, cancel := context.WithTimeout(ctx, sessionStoreTimeout)
	defer cancel()

	if err := s.sessionStore.PutSession(putCtx, s.id, sess); err != nil {
		slog.WarnContext(
			ctx, "failed to persist session", "shard_id", s.id, "err", err,
		)
	}
}
