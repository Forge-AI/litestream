package litestream

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/superfly/ltx"
)

const testStall = 200 * time.Millisecond

func TestStore_SyncRunsAsLongAsItsUploadMoves(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows int
	}{
		{"small", 1},
		{"large", 256},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openReplicatedDB(t)
			writeRows(t, db, tc.rows)
			client := &trickleClient{ReplicaClient: db.Replica.Client, gap: testStall / 4}
			db.Replica.Client = client
			ctx, cancel := db.untilStalled(t.Context(), testStall)
			defer cancel()

			started := time.Now()
			if _, err := (&Store{dbs: []*DB{db}}).SyncDB(ctx, db.Path(), true, false); err != nil {
				t.Fatalf("sync: %v", err)
			}
			if tc.rows > 1 && time.Since(started) < 5*testStall {
				t.Fatalf("the upload took %v, want past five stall windows of %v", time.Since(started), testStall)
			}
			if client.moved.Load() == 0 {
				t.Fatal("the upload moved no bytes")
			}
		})
	}
}

func TestStore_SyncEndsOnceItsUploadStopsMoving(t *testing.T) {
	db := openReplicatedDB(t)
	writeRows(t, db, 64)
	client := &trickleClient{ReplicaClient: db.Replica.Client, gap: testStall / 4, stopAfter: 4, stopped: make(chan time.Time, 1)}
	db.Replica.Client = client
	logs := &stallLog{}
	db.Logger = slog.New(slog.NewJSONHandler(logs, nil))
	ctx, cancel := db.untilStalled(t.Context(), testStall)
	defer cancel()

	_, err := (&Store{dbs: []*DB{db}}).SyncDB(ctx, db.Path(), true, false)
	ended := time.Now()
	db.Replica.Client = client.ReplicaClient

	if !errors.Is(err, ErrStalled) {
		t.Fatalf("sync error = %v, want %v", err, ErrStalled)
	}
	if idle := ended.Sub(<-client.stopped); idle < testStall {
		t.Fatalf("the sync ended %v after its upload stopped, want at least %v", idle, testStall)
	}
	if got := logs.stalls.Load(); got != 1 {
		t.Fatalf("stall warnings = %d, want 1", got)
	}
}

func writeRows(t *testing.T, db *DB, rows int) {
	t.Helper()
	sqldb, err := sql.Open("sqlite", db.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sqldb.Close() }()
	for range rows {
		if _, err := sqldb.Exec(`INSERT INTO t (id) VALUES (randomblob(4096))`); err != nil {
			t.Fatal(err)
		}
	}
}

type trickleClient struct {
	ReplicaClient
	gap       time.Duration
	stopAfter int
	stopped   chan time.Time
	moved     atomic.Int64
}

func (c *trickleClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	var body bytes.Buffer
	chunk := make([]byte, 16<<10)
	for reads := 1; ; reads++ {
		n, err := r.Read(chunk)
		body.Write(chunk[:n])
		c.moved.Add(int64(n))
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if reads == c.stopAfter {
			c.stopped <- time.Now()
			<-ctx.Done()
			return nil, context.Cause(ctx)
		}
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-time.After(c.gap):
		}
	}
	return c.ReplicaClient.WriteLTXFile(ctx, level, minTXID, maxTXID, &body)
}

type stallLog struct {
	stalls atomic.Int64
}

func (w *stallLog) Write(p []byte) (int, error) {
	var record map[string]any
	if err := json.Unmarshal(p, &record); err != nil {
		return 0, err
	}
	if record["msg"] == "database sync moved no bytes for its stall window" {
		w.stalls.Add(1)
	}
	return len(p), nil
}
