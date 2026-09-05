package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

type ShardManager struct {
	mu     sync.Mutex
	shards map[int]*ManagedShard
	config ShardManagerConfig
}

type ShardManagerConfig struct {
	Token     string
	Intents   int
	URL       string
	NumShards int
}

type ManagedShard struct {
	shard      *Shard
	cancel     context.CancelFunc
	done       chan struct{}
	generation int
	err        error
}

func NewShardManager(ctx context.Context, config ShardManagerConfig) *ShardManager {
	return &ShardManager{
		config: config,
		shards: make(map[int]*ManagedShard),
	}
}

func (m *ShardManager) Start(ctx context.Context) error {
	for id := 0; id < m.config.NumShards; id++ {
		// TODO: need to set the correct generation
		if err := m.AddShard(ctx, id, 0); err != nil {
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

	wg.Wait()
}

func (m *ShardManager) AddShard(ctx context.Context, id int, generation int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.shards[id]; exists {
		return fmt.Errorf("shard %d already exists", id)
	}

	shardCtx, cancel := context.WithCancel(ctx)
	shard, err := NewShard(
		shardCtx, ShardConfig{
			URL:       m.config.URL,
			ID:        id,
			Token:     m.config.Token,
			Intents:   m.config.Intents,
			NumShards: m.config.NumShards,
		},
	)

	if err != nil {
		cancel()
		return fmt.Errorf("failed to create shard %d: %w", id, err)
	}

	ms := &ManagedShard{
		shard:      shard,
		cancel:     cancel,
		done:       make(chan struct{}),
		generation: generation,
	}
	m.shards[id] = ms

	go m.supervise(shardCtx, id, ms)

	return nil
}

func (m *ShardManager) StopShard(id int) error {
	m.mu.Lock()
	ms, exists := m.shards[id]

	if !exists {
		m.mu.Unlock()
		return fmt.Errorf("shard %d does not exist", id)
	}

	m.mu.Unlock()

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

func (m *ShardManager) supervise(ctx context.Context, id int, ms *ManagedShard) {
	err := ms.shard.Start(ctx)

	if closeErr := ms.shard.Close(); closeErr != nil {
		slog.ErrorContext(ctx, "failed to close shard", "shard_id", id, "err", closeErr)
	}

	m.mu.Lock()
	ms.err = err
	m.mu.Unlock()

	close(ms.done)

	if err != nil {
		slog.ErrorContext(ctx, "shard exited with error", "shard_id", id, "err", err)
	} else {
		slog.InfoContext(ctx, "shard exited normally", "shard_id", id)
	}
}
