package litestream

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func holdExecutorInSnapshot(t *testing.T, db *DB) {
	t.Helper()
	mustAcquireSemaphore(db.execSem)
	t.Cleanup(func() { db.execSem.Release(1) })
	db.beginSyncDiag(diagOpSync)
	db.setSyncDiagPhase(diagPhaseWriteLTXFromDB, func(s *diagState) {
		s.txID = 9425
		s.walSize = 4120
		s.snapshotting = true
		s.reason = "wal truncated by another process"
	})
}

func TestDB_ExecutorWaitTimeoutNamesTheHolder(t *testing.T) {
	db := NewDB(filepath.Join(t.TempDir(), "db"))
	holdExecutorInSnapshot(t, db)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	err := db.Sync(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v, want deadline exceeded", err)
	}
	if !strings.Contains(err.Error(), "wait for db sync executor") {
		t.Fatalf("err=%q, want sync executor context", err)
	}
	var waitErr *ExecutorWaitError
	if !errors.As(err, &waitErr) {
		t.Fatalf("err=%T, want *ExecutorWaitError", err)
	}
	if got := waitErr.Cause(); got != "executor_deadline" {
		t.Fatalf("cause=%q, want executor_deadline", got)
	}
	holder := waitErr.Holder
	if !holder.Active || holder.Operation != "sync" || holder.Phase != "write_ltx_from_db" {
		t.Fatalf("holder=%+v, want an active sync in write_ltx_from_db", holder)
	}
	if !holder.Snapshotting || holder.Reason != "wal truncated by another process" {
		t.Fatalf("holder=%+v, want the snapshot and its reason", holder)
	}
	if holder.TXID != 9425 || holder.WALSize != 4120 {
		t.Fatalf("holder=%+v, want txid 9425 and wal size 4120", holder)
	}
	if holder.ExecutorWaiterCount != 1 {
		t.Fatalf("executor_waiter_count=%d, want the timed-out waiter counted", holder.ExecutorWaiterCount)
	}
}

func TestServer_SyncTimeoutCarriesTheExecutorHolder(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "db")
	sqldb, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`CREATE TABLE t (id INT)`); err != nil {
		t.Fatal(err)
	}
	if err := sqldb.Close(); err != nil {
		t.Fatal(err)
	}
	db := NewDB(dbPath)
	db.MonitorInterval = 0
	db.Replica = NewReplicaWithClient(db, &testReplicaClient{dir: t.TempDir()})
	db.Replica.MonitorEnabled = false
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
	server := NewServer(NewStore([]*DB{db}, CompactionLevels{{Level: 0}}))
	holdExecutorInSnapshot(t, db)

	body := fmt.Sprintf(`{"path": %q, "wait": true, "timeout": 1}`, dbPath)
	rec := httptest.NewRecorder()
	server.handleSync(rec, httptest.NewRequest(http.MethodPost, "/sync", strings.NewReader(body)))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%s, want 500", rec.Code, rec.Body)
	}
	var resp struct {
		Error   string         `json:"error"`
		Details SyncDiagnostic `json:"details"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(resp.Error, "wait for db sync executor: context deadline exceeded") {
		t.Fatalf("error=%q, want the executor deadline", resp.Error)
	}
	if resp.Details.Phase != "write_ltx_from_db" || !resp.Details.Snapshotting || resp.Details.Path != dbPath {
		t.Fatalf("details=%+v, want the snapshotting holder of %s", resp.Details, dbPath)
	}
	if resp.Details.DBSize == 0 {
		t.Fatalf("details=%+v, want the database size", resp.Details)
	}
}
