package shard

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"runtime"
	"time"

	"github.com/ShadyDonkey/portcullis/internal/gateway"
)

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
		Op   gateway.Opcode `json:"op"`
		Data *int           `json:"d"`
	}{
		Op:   gateway.OpHeartbeat,
		Data: s.lastSequencePtr(),
	}

	return s.conn.WriteJSON(ctx, payload)
}

func (s *Shard) identify(ctx context.Context) error {
	payload := struct {
		Op   gateway.Opcode       `json:"op"`
		Data gateway.IdentifyData `json:"d"`
	}{
		Op: gateway.OpIdentify,
		Data: gateway.IdentifyData{
			Token: s.token,
			Properties: gateway.IdentifyProperties{
				OS:      runtime.GOOS,
				Browser: "portcullis",
				Device:  "portcullis",
			},
			Intents: s.intents,
			Shard:   &[2]int{s.id, s.numShards},
		},
	}

	return s.conn.WriteJSON(ctx, payload)
}

func (s *Shard) resume(ctx context.Context) error {
	payload := struct {
		Op   gateway.Opcode     `json:"op"`
		Data gateway.ResumeData `json:"d"`
	}{
		Op: gateway.OpResume,
		Data: gateway.ResumeData{
			Token:     s.token,
			SessionID: s.session.ID,
			Sequence:  int(s.lastSequence.Load()),
		},
	}

	return s.conn.WriteJSON(ctx, payload)
}

func (s *Shard) heartbeatTicks(ctx context.Context, intervalMs int) <-chan time.Time {
	interval := time.Duration(intervalMs) * time.Millisecond
	firstDelay := time.Duration(rand.Float64() * float64(interval))
	ch := make(chan time.Time)

	go func() {
		slog.DebugContext(ctx, "starting heartbeat", "interval", interval, "first_delay", firstDelay)

		timer := time.NewTimer(firstDelay)
		defer timer.Stop()

		select {
		case t := <-timer.C:
			if !sendTick(ctx, ch, t) {
				return
			}
		case <-ctx.Done():
			return
		}

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case t := <-ticker.C:
				if !sendTick(ctx, ch, t) {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch
}

func sendTick(ctx context.Context, ch chan<- time.Time, t time.Time) bool {
	select {
	case ch <- t:
		return true
	case <-ctx.Done():
		return false
	}
}
