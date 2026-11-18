package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type ShardManager struct {
	mu       sync.Mutex
	shards   map[int]*managedShard
	config   ShardManagerConfig
	identify *bucketLimiter
}

type ShardManagerConfig struct {
	Token           string
	Intents         int
	URL             string
	NumShards       int
	MaxConcurrency  int
	Publisher       EventPublisher
	SessionStore    SessionStore
	Filter          EventFilter
	ReshardInterval time.Duration
}

type managedShard struct {
	shard  *Shard
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

type ShardStatus int

const (
	ShardStatusUnknown ShardStatus = iota
	ShardStatusRunning
	ShardStatusStopped
	ShardStatusFailed
)

func (s ShardStatus) String() string {
	switch s {
	case ShardStatusRunning:
		return "running"
	case ShardStatusStopped:
		return "stopped"
	case ShardStatusFailed:
		return "failed"
	default:
		return "unknown"
	}
}

func NewShardManager(config ShardManagerConfig) *ShardManager {
	return &ShardManager{
		config:   config,
		shards:   make(map[int]*managedShard),
		identify: newIdentifyBucketLimiter(config.MaxConcurrency),
	}
}

func (m *ShardManager) Start(ctx context.Context) error {
	for id := range m.config.NumShards {
		if err := m.AddShard(ctx, id); err != nil {
			m.Shutdown()
			return fmt.Errorf("failed to start shard %d: %w", id, err)
		}
	}

	return nil
}

func (m *ShardManager) Shutdown() {
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

func (m *ShardManager) AddShard(ctx context.Context, id int) error {
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

func (m *ShardManager) StopShard(id int) error {
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

func (m *ShardManager) WaitForShard(id int) error {
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

func statusOf(ms *managedShard) ShardStatus {
	select {
	case <-ms.done:
		if ms.err != nil {
			return ShardStatusFailed
		}
		return ShardStatusStopped
	default:
		return ShardStatusRunning
	}
}

func (m *ShardManager) Status(id int) ShardStatus {
	m.mu.Lock()
	ms, exists := m.shards[id]
	m.mu.Unlock()

	if !exists {
		return ShardStatusUnknown
	}

	return statusOf(ms)
}

func (m *ShardManager) Statuses() map[int]ShardStatus {
	m.mu.Lock()
	defer m.mu.Unlock()

	statuses := make(map[int]ShardStatus, len(m.shards))
	for id, ms := range m.shards {
		statuses[id] = statusOf(ms)
	}

	return statuses
}
