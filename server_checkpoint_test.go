package litestream_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
)

func TestServer_SyncCheckpointDrainsASyncedOnePageLog(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h := handOver(t, ctx, 0, 0)
	if _, err := h.sqldb.ExecContext(ctx, `UPDATE t SET body = randomblob(4000) WHERE id = 1`); err != nil {
		t.Fatal(err)
	}
	if err := h.db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if fileHoldsTheLog(t, ctx, h.sqldb, h.db.Path()) {
		t.Fatal("the synced page reached the file before any checkpoint")
	}

	mustSync(t, serveSyncs(t, h.db), litestream.SyncRequest{Path: h.db.Path(), Checkpoint: true})

	if !fileHoldsTheLog(t, ctx, h.sqldb, h.db.Path()) {
		t.Fatal("one sync with the checkpoint flag left a synced one-page log outside the file")
	}
}

func TestServer_SyncCheckpointDrainsALogAnEarlierLaneShipped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h := handOver(t, ctx, handoverRows/2, 0)
	if err := h.db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.db.Close(ctx); err != nil {
		t.Fatal(err)
	}
	lane := openHandoverPublisher(t, h.db.Path(), h.client)
	if fileHoldsTheLog(t, ctx, h.sqldb, h.db.Path()) {
		t.Fatal("the earlier lane checkpointed the log it shipped")
	}

	mustSync(t, serveSyncs(t, lane), litestream.SyncRequest{Path: lane.Path(), Checkpoint: true})

	if !fileHoldsTheLog(t, ctx, h.sqldb, h.db.Path()) {
		t.Fatal("one sync with the checkpoint flag left the log an earlier lane shipped outside the file")
	}
}

func TestServer_SyncCheckpointKeepsTheReadTransactionPastItsTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h := handOver(t, ctx, 4, 0)

	mustSync(t, serveSyncs(t, h.db), litestream.SyncRequest{Path: h.db.Path(), Checkpoint: true, Timeout: 5})

	insertHandoverRows(t, ctx, h.sqldb, handoverRows+5, 1)
	if _, err := h.sqldb.ExecContext(ctx, `PRAGMA busy_timeout = 0`); err != nil {
		t.Fatal(err)
	}
	for range 20 {
		var busy, logged, checkpointed int
		if err := h.sqldb.QueryRowContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &logged, &checkpointed); err != nil {
			t.Fatal(err)
		}
		if busy == 0 {
			t.Fatal("an outside connection truncated the log after the checkpoint request returned, so litestream no longer held its read transaction")
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestServer_SyncWithoutCheckpointLeavesTheLogAndAnIdleDatabaseShipsNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h := handOver(t, ctx, 4, 0)
	client := serveSyncs(t, h.db)
	plain := litestream.SyncRequest{Path: h.db.Path()}

	mustSync(t, client, plain)
	if fileHoldsTheLog(t, ctx, h.sqldb, h.db.Path()) {
		t.Fatal("a sync without the checkpoint flag checkpointed the log")
	}
	checkpointed := mustSync(t, client, litestream.SyncRequest{Path: h.db.Path(), Checkpoint: true})
	if !fileHoldsTheLog(t, ctx, h.sqldb, h.db.Path()) {
		t.Fatal("one sync with the checkpoint flag left the log outside the file")
	}

	for range 3 {
		if idle := mustSync(t, client, plain); idle.TXID != checkpointed.TXID {
			t.Fatalf("an idle database shipped bookkeeping after its checkpoint: checkpointed=%d idle=%d", checkpointed.TXID, idle.TXID)
		}
	}
}

func TestServer_SyncRefusesACheckpointThatWaits(t *testing.T) {
	status, answer := postSync(t, serveSyncs(t), litestream.SyncRequest{Path: "/db", Wait: true, Checkpoint: true})
	if status != http.StatusBadRequest {
		t.Fatalf("status=%d error=%q, want 400", status, answer.Error)
	}
}

func TestDB_CompactionAndSnapshotNameTheirUploadsInTheMetaDirectory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dir := t.TempDir()
	db := openHandoverPublisher(t, filepath.Join(dir, "db"), file.NewReplicaClient(filepath.Join(dir, "replica")))
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	sqldb := openServerSQLDB(t, ctx, db.Path())
	t.Cleanup(func() { _ = sqldb.Close() })
	if _, err := sqldb.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, body BLOB)`); err != nil {
		t.Fatal(err)
	}
	insertHandoverRows(t, ctx, sqldb, 1, 4)
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.Replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	l1, err := db.Compact(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}

	named, err := os.ReadFile(filepath.Join(db.MetaPath(), "compacted"))
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("{\"level\":1,\"min\":%d,\"max\":%d,\"size\":%d}\n{\"level\":%d,\"min\":%d,\"max\":%d,\"size\":%d}\n",
		l1.MinTXID, l1.MaxTXID, l1.Size, litestream.SnapshotLevel, snapshot.MinTXID, snapshot.MaxTXID, snapshot.Size)
	if string(named) != want {
		t.Fatalf("compacted file:\n%s\nwant:\n%s", named, want)
	}
}

type syncAnswer struct {
	litestream.SyncResponse
	Error string `json:"error"`
}

func serveSyncs(t *testing.T, dbs ...*litestream.DB) *http.Client {
	t.Helper()
	store := litestream.NewStore(dbs, litestream.CompactionLevels{{Level: 0}})
	store.CompactionMonitorEnabled = false
	if err := store.Open(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	server := litestream.NewServer(store)
	server.SocketPath = testSocketPath(t)
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	return newSocketClient(t, server.SocketPath)
}

func postSync(t *testing.T, client *http.Client, req litestream.SyncRequest) (int, syncAnswer) {
	t.Helper()
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Post("http://localhost/sync", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var answer syncAnswer
	if err := json.NewDecoder(resp.Body).Decode(&answer); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, answer
}

func mustSync(t *testing.T, client *http.Client, req litestream.SyncRequest) litestream.SyncResponse {
	t.Helper()
	status, answer := postSync(t, client, req)
	if status != http.StatusOK {
		t.Fatalf("sync %+v: status=%d error=%q", req, status, answer.Error)
	}
	return answer.SyncResponse
}

func fileHoldsTheLog(t *testing.T, ctx context.Context, sqldb *sql.DB, path string) bool {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	copied := filepath.Join(t.TempDir(), "db")
	if err := os.WriteFile(copied, data, 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := sql.Open("sqlite", copied)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	return rowsDigest(t, ctx, file) == rowsDigest(t, ctx, sqldb)
}

func rowsDigest(t *testing.T, ctx context.Context, sqldb *sql.DB) string {
	t.Helper()
	var digest string
	if err := sqldb.QueryRowContext(ctx, `SELECT coalesce(group_concat(id || ':' || hex(body), ','), '') FROM (SELECT id, body FROM t ORDER BY id)`).Scan(&digest); err != nil {
		t.Fatal(err)
	}
	return digest
}
