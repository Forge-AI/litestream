package litestream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/superfly/ltx"
)

func TestStore_SyncDeadlineNamesInitializationAtAnyDatabaseCount(t *testing.T) {
	for _, n := range []int{1, 1000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			db := openReplicatedDB(t)
			client := &syncDeadlineReplicaClient{
				ReplicaClient: db.Replica.Client,
				entered:       make(chan struct{}),
				release:       make(chan struct{}),
			}
			db.Replica.Client = client
			logs := &syncDeadlineLog{records: make(chan map[string]any, 2)}
			db.Logger = slog.New(slog.NewJSONHandler(logs, nil))
			store := &Store{dbs: []*DB{db}}
			for i := 1; i < n; i++ {
				other := &DB{path: fmt.Sprintf("unused-%d", i)}
				other.syncDiag.Lock()
				store.dbs = append(store.dbs, other)
			}
			holderDone := make(chan error, 1)
			var requestDone chan error
			var release sync.Once
			t.Cleanup(func() {
				release.Do(func() { close(client.release) })
				for _, other := range store.dbs[1:] {
					other.syncDiag.Unlock()
				}
				select {
				case err := <-holderDone:
					if err != nil {
						t.Errorf("holder sync: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Error("holder did not finish")
				}
				if requestDone != nil {
					select {
					case err := <-requestDone:
						if !errors.Is(err, context.DeadlineExceeded) {
							t.Errorf("sync error = %v, want deadline exceeded", err)
						}
					case <-time.After(5 * time.Second):
						t.Error("request did not finish")
					}
				}
				if got := logs.count.Load(); got != 1 {
					t.Errorf("deadline warnings = %d, want 1", got)
				}
			})
			go func() { holderDone <- db.Sync(context.Background()) }()
			select {
			case <-client.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("initialization did not list the replica")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			requestDone = make(chan error, 1)
			go func() {
				_, err := store.SyncDB(ctx, db.Path(), true, false)
				requestDone <- err
			}()
			select {
			case record := <-logs.records:
				for key, want := range map[string]any{
					"level":            "WARN",
					"path":             db.Path(),
					"request_phase":    "is_open",
					"holder_active":    true,
					"holder_operation": "sync",
					"holder_phase":     "init_replica_list",
				} {
					if got := record[key]; got != want {
						t.Errorf("%s = %v, want %v", key, got, want)
					}
				}
			case <-time.After(5 * time.Second):
				t.Fatal("deadline was silent while initialization held the database lock")
			}
			if got := client.calls.Load(); got != 1 {
				t.Fatalf("replica calls while blocked = %d, want 1 at N=%d", got, n)
			}
		})
	}
}

func TestStore_SyncDeadlineReportsOnceOnReturn(t *testing.T) {
	for _, cause := range []error{context.DeadlineExceeded, context.Canceled} {
		t.Run(cause.Error(), func(t *testing.T) {
			logs := &syncDeadlineLog{records: make(chan map[string]any, 2)}
			db := &DB{path: "closed", Logger: slog.New(slog.NewJSONHandler(logs, nil))}
			store := &Store{dbs: []*DB{db}}
			ctx, cancel := context.WithCancelCause(context.Background())
			cancel(cause)
			if _, err := store.SyncDB(ctx, db.Path(), false, false); !errors.Is(err, ErrDatabaseNotOpen) {
				t.Fatalf("sync error = %v, want database not open", err)
			}
			want := int64(0)
			if errors.Is(cause, context.DeadlineExceeded) {
				want = 1
			}
			if got := logs.count.Load(); got != want {
				t.Fatalf("deadline warnings = %d, want %d", got, want)
			}
		})
	}
}

type syncDeadlineReplicaClient struct {
	ReplicaClient
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	calls   atomic.Int64
}

func (c *syncDeadlineReplicaClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	c.calls.Add(1)
	c.once.Do(func() {
		close(c.entered)
		<-c.release
	})
	return c.ReplicaClient.LTXFiles(ctx, level, seek, useMetadata)
}

type syncDeadlineLog struct {
	records chan map[string]any
	count   atomic.Int64
}

func (w *syncDeadlineLog) Write(p []byte) (int, error) {
	var record map[string]any
	if err := json.Unmarshal(p, &record); err != nil {
		return 0, err
	}
	if record["msg"] == "database sync deadline exceeded" {
		w.count.Add(1)
		w.records <- record
	}
	return len(p), nil
}
