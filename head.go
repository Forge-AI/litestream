package litestream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

const (
	HeadMoving = "moving"
	HeadRest   = 10 * time.Second

	headSweepInterval = time.Second
	headSweepWrites   = 16
)

type HeadClient interface {
	WriteHead(ctx context.Context, key string, text []byte) error
	ReadHead(ctx context.Context, key string) ([]byte, error)
}

type headState struct {
	mu         sync.Mutex
	moving     bool
	opened     bool
	lastUpload time.Time
}

func (r *Replica) headClient() (HeadClient, bool) {
	if r.HeadKey == "" {
		return nil, false
	}
	client, ok := r.Client.(HeadClient)
	return client, ok
}

func (r *Replica) headMoves(ctx context.Context) error {
	client, ok := r.headClient()
	if !ok {
		return nil
	}
	r.head.mu.Lock()
	moving := r.head.moving
	r.head.mu.Unlock()
	if moving {
		return nil
	}
	if err := client.WriteHead(ctx, r.HeadKey, []byte(HeadMoving)); err != nil {
		return fmt.Errorf("write head: %w", err)
	}
	r.head.mu.Lock()
	r.head.moving, r.head.opened, r.head.lastUpload = true, true, time.Now()
	r.head.mu.Unlock()
	heads.add(r)
	return nil
}

func (r *Replica) headUploaded() {
	r.head.mu.Lock()
	r.head.lastUpload = time.Now()
	r.head.mu.Unlock()
}

func (r *Replica) headCaughtUp(ctx context.Context) {
	client, ok := r.headClient()
	if !ok {
		return
	}
	r.head.mu.Lock()
	opened := r.head.opened
	r.head.opened = true
	r.head.mu.Unlock()
	if opened {
		return
	}
	text, err := client.ReadHead(ctx, r.HeadKey)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err == nil && string(text) == HeadMoving {
		err = r.writeRest(ctx, client)
	}
	if err != nil {
		r.head.mu.Lock()
		r.head.opened = false
		r.head.mu.Unlock()
		r.Logger().Warn("settle the head of a reopened database; the next sync retries", slog.Any("error", err))
	}
}

func (r *Replica) writeRest(ctx context.Context, client HeadClient) error {
	text := fmt.Sprintf("%016x-%d", uint64(r.Pos().TXID), time.Now().UnixMilli())
	return client.WriteHead(ctx, r.HeadKey, []byte(text))
}

func (r *Replica) rest(ctx context.Context) error {
	client, ok := r.headClient()
	if !ok {
		return nil
	}
	r.head.mu.Lock()
	moving := r.head.moving
	r.head.mu.Unlock()
	if !moving {
		return nil
	}
	if err := r.writeRest(ctx, client); err != nil {
		return fmt.Errorf("write head: %w", err)
	}
	r.head.mu.Lock()
	r.head.moving = false
	r.head.mu.Unlock()
	heads.remove(r)
	return nil
}

type headSweep struct {
	mu     sync.Mutex
	moving map[*Replica]struct{}
	once   sync.Once
}

var heads = &headSweep{moving: make(map[*Replica]struct{})}

func (s *headSweep) add(r *Replica) {
	s.mu.Lock()
	s.moving[r] = struct{}{}
	s.mu.Unlock()
	s.once.Do(func() { go s.run() })
}

func (s *headSweep) remove(r *Replica) {
	s.mu.Lock()
	delete(s.moving, r)
	s.mu.Unlock()
}

func (s *headSweep) run() {
	ticker := time.NewTicker(headSweepInterval)
	defer ticker.Stop()
	for now := range ticker.C {
		s.sweep(context.Background(), now)
	}
}

func (s *headSweep) sweep(ctx context.Context, now time.Time) {
	s.mu.Lock()
	var due []*Replica
	for r := range s.moving {
		r.head.mu.Lock()
		if now.Sub(r.head.lastUpload) >= HeadRest {
			due = append(due, r)
		}
		r.head.mu.Unlock()
	}
	s.mu.Unlock()
	var writes errgroup.Group
	writes.SetLimit(headSweepWrites)
	for _, r := range due {
		writes.Go(func() error {
			if !r.syncSem.TryAcquire(1) {
				return nil
			}
			defer r.syncSem.Release(1)
			if err := r.rest(ctx); err != nil {
				r.Logger().Warn("rest the head; the next sweep retries", slog.Any("error", err))
			}
			return nil
		})
	}
	_ = writes.Wait()
}

func SweepHeads(ctx context.Context, now time.Time) {
	heads.sweep(ctx, now)
}

func (r *Replica) restHeld(ctx context.Context) error {
	if err := r.lockSync(ctx); err != nil {
		return err
	}
	defer r.syncSem.Release(1)
	return r.rest(ctx)
}
