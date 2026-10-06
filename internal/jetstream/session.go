package jetstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ShadyDonkey/portcullis/internal/gateway"
	"github.com/nats-io/nats.go/jetstream"
)

type GlobalState struct {
	Intents   gateway.Intent `json:"intents"`
	NumShards int            `json:"num_shards"`
}

func (j *JetStream) GetSession(ctx context.Context, shardID int) (*gateway.Session, error) {
	entry, err := j.kv.Get(ctx, shardSessionKey(shardID))
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, nil
		}

		return nil, fmt.Errorf("get session err, shard %d: %w", shardID, err)
	}

	var session gateway.Session

	if err = json.Unmarshal(entry.Value(), &session); err != nil {
		return nil, fmt.Errorf("decode session err, shard %d: %w", shardID, err)
	}

	return &session, nil
}

func (j *JetStream) PutSession(ctx context.Context, shardID int, session gateway.Session) error {
	data, err := json.Marshal(session)
	if err != nil {
		return fmt.Errorf("encode session err, shard %d: %w", shardID, err)
	}

	if _, err = j.kv.Put(ctx, shardSessionKey(shardID), data); err != nil {
		return fmt.Errorf("put session err, shard %d: %w", shardID, err)
	}

	return nil
}

func (j *JetStream) DeleteSession(ctx context.Context, shardID int) error {
	if err := j.kv.Delete(ctx, shardSessionKey(shardID)); err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil
		}

		return fmt.Errorf("delete session err, shard %d: %w", shardID, err)
	}

	return nil
}

func (j *JetStream) GetGlobalState(ctx context.Context) (*GlobalState, error) {
	entry, err := j.kv.Get(ctx, globalStateKey)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get global state err: %w", err)
	}

	var state GlobalState
	if err = json.Unmarshal(entry.Value(), &state); err != nil {
		return nil, fmt.Errorf("decode global state err: %w", err)
	}

	return &state, nil
}

func (j *JetStream) PutGlobalState(ctx context.Context, state GlobalState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode global state err: %w", err)
	}

	if _, err = j.kv.Put(ctx, globalStateKey, data); err != nil {
		return fmt.Errorf("put global state err: %w", err)
	}

	return nil
}
