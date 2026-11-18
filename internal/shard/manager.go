package shard

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ShadyDonkey/portcullis/internal/gateway"
)

type Manager struct {
	mu       sync.Mutex
	shards   map[int]*managedShard
	config   Config
	identify *gateway.IdentifyLimiter
}

type Config struct {
	Token           string
	Intents         int
	URL             string
	NumShards       int
	MaxConcurrency  int
	Publisher       gateway.EventPublisher
	SessionStore    gateway.SessionStore
	Filter          gateway.EventFilter
	ReshardInterval time.Duration
}

type managedShard struct {
	shard  *Shard
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

type Status int

const (
	StatusUnknown Status = iota
	StatusRunning
	StatusStopped
	StatusFailed
	StatusReconnecting
)

func (s Status) String() string {
	switch s {
	case StatusRunning:
		return "running"
	case StatusStopped:
		return "stopped"
	case StatusFailed:
		return "failed"
	case StatusReconnecting:
		return "reconnecting"
	default:
		return "unknown"
	}
}

func NewManager(config Config) *Manager {
	return &Manager{
		config:   config,
		shards:   make(map[int]*managedShard),
		identify: gateway.NewIdentifyLimiter(config.MaxConcurrency),
	}
}

func (m *Manager) Start(ctx context.Context) error {
	for id := range m.config.NumShards {
		if err := m.AddShard(ctx, id); err != nil {
			m.Shutdown()
			return fmt.Errorf("failed to start shard %d: %w", id, err)
		}
	}

	return nil
}

func (m *Manager) Shutdown() {
	m.mu.Lock()
	ids := make([]int, 0, len(m.shards))
	for id := range m.shards {
		ids = append(ids, id)
	}
	m.mu.Unlock()

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Go(
			func() {
				if err := m.StopShard(id); err != nil {
					slog.Error("failed to stop shard", "id", id, "err", err)
				}
			},
		)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(shardShutdownTimeout):
		m.mu.Lock()
		var stuck []int
		for id, ms := range m.shards {
			select {
			case <-ms.done:
			default:
				stuck = append(stuck, id)
			}
		}
		m.mu.Unlock()
		slog.Error(
			"shutdown timed out, shards may have not exited cleanly",
			"timeout", shardShutdownTimeout, "stuck_shards", stuck,
		)
	}
}

func (m *Manager) AddShard(ctx context.Context, id int) error {
	m.mu.Lock()
	if _, exists := m.shards[id]; exists {
		m.mu.Unlock()
		return fmt.Errorf("shard %d already exists", id)
	}
	m.mu.Unlock()

	shardCtx, cancel := context.WithCancel(ctx)
	shard, err := NewShard(shardCtx, m.idToShardConfig(id))
	if err != nil {
		cancel()
		return fmt.Errorf("failed to create shard %d: %w", id, err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.shards[id]; exists {
		cancel()
		if closeErr := shard.Close(); closeErr != nil {
			slog.Error("failed to close duplicate shard", "id", id, "err", closeErr)
		}
		return fmt.Errorf("shard %d already exists", id)
	}

	ms := &managedShard{
		shard:  shard,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	m.shards[id] = ms

	go m.supervise(shardCtx, id, ms)

	return nil
}

func (m *Manager) StopShard(id int) error {
	m.mu.Lock()
	ms, exists := m.shards[id]
	m.mu.Unlock()

	if !exists {
		return fmt.Errorf("shard %d does not exist", id)
	}

	ms.cancel()
	<-ms.done

	m.mu.Lock()
	delete(m.shards, id)
	m.mu.Unlock()

	return nil
}

func (m *Manager) WaitForShard(id int) error {
	m.mu.Lock()
	ms, exists := m.shards[id]
	m.mu.Unlock()

	if !exists {
		return fmt.Errorf("shard %d does not exist", id)
	}

	<-ms.done

	m.mu.Lock()
	err := ms.err
	m.mu.Unlock()

	return err
}

func statusOf(ms *managedShard) Status {
	select {
	case <-ms.done:
		if ms.err != nil {
			return StatusFailed
		}
		return StatusStopped
	default:
		if ms.shard.connected.Load() {
			return StatusRunning
		}
		return StatusReconnecting
	}
}

func (m *Manager) Status(id int) Status {
	m.mu.Lock()
	ms, exists := m.shards[id]
	m.mu.Unlock()

	if !exists {
		return StatusUnknown
	}

	return statusOf(ms)
}

func (m *Manager) Statuses() map[int]Status {
	m.mu.Lock()
	defer m.mu.Unlock()

	statuses := make(map[int]Status, len(m.shards))
	for id, ms := range m.shards {
		statuses[id] = statusOf(ms)
	}

	return statuses
}
