package litestream

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/superfly/ltx"
)

func TestStore_TwoWritersOnOneDatabaseShipOneNextLTXFile(t *testing.T) {
	ctx := context.Background()
	store := NewStore(nil, CompactionLevels{{Level: 0}})
	seed := filepath.Join(t.TempDir(), "chat")
	create, err := sql.Open("sqlite", seed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := create.Exec(`PRAGMA journal_mode = wal; CREATE TABLE t (id INT)`); err != nil {
		t.Fatal(err)
	}
	if err := create.Close(); err != nil {
		t.Fatal(err)
	}
	shared := t.TempDir()
	ahead := registerWriter(t, store, shared, seed)
	behind := registerWriter(t, store, shared, seed)
	for _, w := range []*writer{ahead, behind} {
		if _, err := store.SyncDB(ctx, w.path, true, false); err != nil {
			t.Fatal(err)
		}
	}
	pos := func(w *writer) ltx.TXID { return store.FindDB(w.path).Replica.Pos().TXID }
	if pos(behind) > pos(ahead) {
		ahead, behind = behind, ahead
	}
	if pos(behind) == pos(ahead) {
		ahead.write(t, store, 1)
	}
	next := pos(behind) + 1
	stored := func() []byte {
		t.Helper()
		r, err := ahead.client.OpenLTXFile(ctx, 0, next, next, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = r.Close() }()
		b, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	before := stored()

	if _, err := behind.app.Exec(`INSERT INTO t VALUES (2)`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SyncDB(ctx, behind.path, true, false); !errors.Is(err, ErrLTXFileTaken) {
		t.Fatalf("the second writer of ltx file %v synced with %v, want ErrLTXFileTaken", next, err)
	}
	ours, err := os.ReadFile(store.FindDB(behind.path).LTXPath(0, next, next))
	if err != nil {
		t.Fatal(err)
	}
	if after := stored(); !bytes.Equal(after, before) || bytes.Equal(after, ours) {
		t.Fatal("the second writer replaced the stored ltx file")
	}

	calls := behind.client.calls.Load()
	if _, err := behind.app.Exec(`INSERT INTO t VALUES (3)`); err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	body := fmt.Sprintf(`{"path": %q, "wait": true}`, behind.path)
	NewServer(store).handleSync(rec, httptest.NewRequest(http.MethodPost, "/sync", strings.NewReader(body)))
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("status=%d body=%s, want 412: a writer whose next ltx file was taken has stopped shipping", rec.Code, rec.Body)
	}
	if n := behind.client.calls.Load() - calls; n != 0 {
		t.Fatalf("the stopped writer made %d more calls to the replica, want 0", n)
	}
	ahead.write(t, store, 4)
}

type writer struct {
	path   string
	app    *sql.DB
	client *callCounter
}

func registerWriter(t *testing.T, store *Store, replica, seed string) *writer {
	t.Helper()
	path := filepath.Join(t.TempDir(), "chat")
	data, err := os.ReadFile(seed)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	inner, err := NewReplicaClientFromURL("file://" + replica)
	if err != nil {
		t.Fatal(err)
	}
	client := &callCounter{ReplicaClient: inner}
	db := NewDB(path)
	db.MonitorInterval = 0
	db.Replica = NewReplicaWithClient(db, client)
	db.Replica.MonitorEnabled = false
	if err := store.RegisterDB(db); err != nil {
		t.Fatal(err)
	}
	app, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = app.Close()
		_ = store.UnregisterDB(context.Background(), path)
	})
	return &writer{path: path, app: app, client: client}
}

func (w *writer) write(t *testing.T, store *Store, i int) {
	t.Helper()
	if _, err := w.app.Exec(`INSERT INTO t VALUES (?)`, i); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SyncDB(context.Background(), w.path, true, false); err != nil {
		t.Fatal(err)
	}
}

type callCounter struct {
	ReplicaClient
	calls atomic.Int64
}

func (c *callCounter) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	c.calls.Add(1)
	return c.ReplicaClient.LTXFiles(ctx, level, seek, useMetadata)
}

func (c *callCounter) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	c.calls.Add(1)
	return c.ReplicaClient.OpenLTXFile(ctx, level, minTXID, maxTXID, offset, size)
}

func (c *callCounter) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	c.calls.Add(1)
	return c.ReplicaClient.WriteLTXFile(ctx, level, minTXID, maxTXID, r)
}
