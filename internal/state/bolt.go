package state

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ShadyDonkey/portcullis/internal/gateway"
	"go.etcd.io/bbolt"
	bbolterr "go.etcd.io/bbolt/errors"
)

const (
	flushInterval        = time.Second
	lockTimeout          = time.Second
	schemaVersion uint32 = 1
)

var (
	bucketSessions = []byte("sessions")
	bucketGlobal   = []byte("global")
	bucketMeta     = []byte("meta")
	keyGlobal      = []byte("state")
	keySchema      = []byte("schema")
	errClosed      = errors.New("state store is closed")
)

type Bolt struct {
	db          *bbolt.DB
	logger      *slog.Logger
	mu          sync.Mutex
	sessions    map[int]gateway.Session
	global      *Global
	dirty       map[int]struct{}
	dirtyGlobal bool
	closed      bool
	flushNow    chan struct{}
	stop        chan struct{}
	done        chan struct{}
	closeOnce   sync.Once
	closeErr    error
}

var _ Store = (*Bolt)(nil)

func OpenBolt(path string, logger *slog.Logger) (*Bolt, error) {
	if logger == nil {
		logger = slog.Default()
	}

	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("failed to create state directory %q: %w", dir, err)
		}
	}

	db, err := openFile(path, logger)

	if err != nil {
		return nil, err
	}

	b := &Bolt{
		db:       db,
		logger:   logger,
		sessions: make(map[int]gateway.Session),
		dirty:    make(map[int]struct{}),
		flushNow: make(chan struct{}, 1),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}

	if err := b.initAndLoad(); err != nil {
		_ = db.Close()
		return nil, err
	}

	go b.loop()

	return b, nil
}

func openFile(path string, logger *slog.Logger) (*bbolt.DB, error) {
	opts := &bbolt.Options{
		Timeout: lockTimeout,
	}

	db, err := bbolt.Open(path, 0o600, opts)

	if err == nil {
		return db, nil
	}

	if errors.Is(err, bbolterr.ErrTimeout) {
		return nil, fmt.Errorf(
			"state file %s is locked by another process, check for another Portcullis instance running", path,
		)
	}

	if errors.Is(err, fs.ErrPermission) {
		return nil, fmt.Errorf("open state file %s failed: %w", path, err)
	}

	moved := fmt.Sprintf("%s.corrupt-%s", path, time.Now().Format("20060102T150405Z"))

	if renameErr := os.Rename(path, moved); renameErr != nil {
		return nil, fmt.Errorf("open state file %s failed: %w (could not move it aside: %v)", path, err, renameErr)
	}

	logger.Error(
		"state file unreadable, moved and starting fresh; sessions will not resume", "path", path, "moved_to", moved,
		"err", err,
	)

	db, err = bbolt.Open(path, 0o600, opts)

	if err != nil {
		return nil, fmt.Errorf("create state file %s failed: %w", path, err)
	}

	return db, nil
}

func (b *Bolt) initAndLoad() error {
	err := b.db.Update(
		func(tx *bbolt.Tx) error {
			for _, name := range [][]byte{bucketSessions, bucketGlobal, bucketMeta} {
				if _, err := tx.CreateBucketIfNotExists(name); err != nil {
					return err
				}
			}

			meta := tx.Bucket(bucketMeta)

			if v := meta.Get(keySchema); v != nil {
				if len(v) != 4 || binary.BigEndian.Uint32(v) > schemaVersion {
					return errors.New("state file was written by a new version of Portcullis")
				}

				return nil
			}

			var v [4]byte
			binary.BigEndian.PutUint32(v[:], schemaVersion)

			return meta.Put(keySchema, v[:])
		},
	)

	if err != nil {
		return fmt.Errorf("could not initialize state file: %w", err)
	}

	return b.db.View(
		func(tx *bbolt.Tx) error {
			err := tx.Bucket(bucketSessions).ForEach(
				func(k, v []byte) error {
					if len(k) != 4 {
						return nil
					}

					var s gateway.Session

					if err := json.Unmarshal(v, &s); err != nil {
						b.logger.Warn("skipping unreadable stored session", "key", k, "err", err)

						return nil
					}

					b.sessions[int(binary.BigEndian.Uint32(k))] = s

					return nil
				},
			)

			if err != nil {
				return fmt.Errorf("could not load sessions: %w", err)
			}

			if v := tx.Bucket(bucketGlobal).Get(keyGlobal); v != nil {
				var g Global

				if err := json.Unmarshal(v, &g); err != nil {
					b.logger.Warn("ignoring unreadable stored global state", "err", err)

					return nil
				}

				b.global = &g
			}

			return nil
		},
	)
}

func (b *Bolt) GetSession(shardID int) (*gateway.Session, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	s, ok := b.sessions[shardID]

	if !ok {
		return nil, nil
	}

	s = cloneSession(s)

	return &s, nil
}

func (b *Bolt) PutSession(shardID int, session gateway.Session) error {
	session = cloneSession(session)

	b.mu.Lock()

	if b.closed {
		b.mu.Unlock()
		return errClosed
	}

	prev, had := b.sessions[shardID]
	b.sessions[shardID] = session
	b.dirty[shardID] = struct{}{}
	b.mu.Unlock()

	if !had || prev.ID != session.ID {
		b.requestFlush()
	}

	return nil
}

func (b *Bolt) DeleteSession(shardID int) error {
	b.mu.Lock()

	if b.closed {
		b.mu.Unlock()
		return errClosed
	}

	delete(b.sessions, shardID)
	b.dirty[shardID] = struct{}{}
	b.mu.Unlock()

	b.requestFlush()

	return nil
}

func (b *Bolt) GetGlobal() (*Global, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.global == nil {
		return nil, nil
	}

	g := *b.global

	return &g, nil
}

func (b *Bolt) PutGlobal(global Global) error {
	b.mu.Lock()

	if b.closed {
		b.mu.Unlock()
		return errClosed
	}

	b.global = &global
	b.dirtyGlobal = true
	b.mu.Unlock()

	b.requestFlush()

	return nil
}

func (b *Bolt) Close(ctx context.Context) error {
	b.closeOnce.Do(
		func() {
			b.mu.Lock()
			b.closed = true
			b.mu.Unlock()

			close(b.stop)

			select {
			case <-b.done:
			case <-ctx.Done():
			}

			var errs []error

			if err := b.flush(); err != nil {
				errs = append(errs, fmt.Errorf("final state flush: %w", err))
			}

			if err := b.db.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close state file: %w", err))
			}

			b.closeErr = errors.Join(errs...)
		},
	)

	return b.closeErr
}

func (b *Bolt) loop() {
	defer close(b.done)

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
		case <-b.flushNow:
		case <-b.stop:
			return
		}

		if err := b.flush(); err != nil {
			b.logger.Warn("failed to write state, will retry", "err", err)
		}
	}
}

func (b *Bolt) requestFlush() {
	select {
	case b.flushNow <- struct{}{}:
	default:
	}
}

type sessionWrite struct {
	key  [4]byte
	id   int
	data []byte
}

func (b *Bolt) flush() error {
	b.mu.Lock()

	if len(b.dirty) == 0 && !b.dirtyGlobal {
		b.mu.Unlock()
		return nil
	}

	writes := make([]sessionWrite, 0, len(b.dirty))

	for id := range b.dirty {
		w := sessionWrite{
			id: id,
		}

		binary.BigEndian.PutUint32(w.key[:], uint32(id))

		if s, ok := b.sessions[id]; ok {
			w.data, _ = json.Marshal(s)
		}

		writes = append(writes, w)
	}

	b.dirty = make(map[int]struct{})

	var globalData []byte
	hadGlobal := b.dirtyGlobal

	if hadGlobal {
		globalData, _ = json.Marshal(b.global)
		b.dirtyGlobal = false
	}

	b.mu.Unlock()

	err := b.db.Update(
		func(tx *bbolt.Tx) error {
			sessions := tx.Bucket(bucketSessions)

			for _, w := range writes {
				var err error

				if w.data == nil {
					err = sessions.Delete(w.key[:])
				} else {
					err = sessions.Put(w.key[:], w.data)
				}

				if err != nil {
					return err
				}
			}

			if hadGlobal {
				return tx.Bucket(bucketGlobal).Put(keyGlobal, globalData)
			}

			return nil
		},
	)

	if err == nil {
		return nil
	}

	b.mu.Lock()

	for _, w := range writes {
		b.dirty[w.id] = struct{}{}
	}

	if hadGlobal {
		b.dirtyGlobal = true
	}

	b.mu.Unlock()

	return err
}

func cloneSession(session gateway.Session) gateway.Session {
	if session.LastSequence != nil {
		v := *session.LastSequence
		session.LastSequence = &v
	}

	return session
}
