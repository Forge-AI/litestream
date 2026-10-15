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

func holdExecutor(t *testing.T, db *DB) {
	t.Helper()
	mustAcquireSemaphore(db.execSem)
	t.Cleanup(func() { db.execSem.Release(1) })
	db.beginSyncDiag(diagOpSync)
	db.setSyncDiagPhase(diagPhaseVerify)
}

func startSnapshot(db *DB) {
	db.setSyncDiagPhase(diagPhaseWriteLTXFromDB, func(s *diagState) {
		s.txID = 9425
		s.walSize = 4120
		s.snapshotting = true
		s.reason = "wal truncated by another process"
	})
}

func openReplicatedDB(t *testing.T) *DB {
	t.Helper()
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
	return db
}

func waitForExecutorWaiter(t *testing.T, db *DB, done <-chan error) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for db.SyncDiagnostic().ExecutorWaiterCount != 1 {
		select {
		case err := <-done:
			t.Fatalf("returned before waiting on the executor: %v", err)
		case <-deadline:
			t.Fatal("never waited on the executor")
		case <-ticker.C:
		}
	}
}

func expireWhileQueued(t *testing.T, db *DB, sync func(context.Context) error) error {
	t.Helper()
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	done := make(chan error, 1)
	go func() { done <- sync(ctx) }()
	waitForExecutorWaiter(t, db, done)
	cancel(context.DeadlineExceeded)
	return <-done
}

func requireSnapshotHolder(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrSnapshotting) {
		t.Fatalf("err=%v, want ErrSnapshotting", err)
	}
	var waitErr *ExecutorWaitError
	if !errors.As(err, &waitErr) {
		t.Fatalf("err=%T, want *ExecutorWaitError", err)
	}
	if got := waitErr.Cause(); got != "snapshotting" {
		t.Fatalf("cause=%q, want snapshotting", got)
	}
	if !waitErr.Holder.Snapshotting || waitErr.Holder.Phase != "write_ltx_from_db" {
		t.Fatalf("holder=%+v, want the snapshot", waitErr.Holder)
	}
}

func TestDB_ExecutorWaitTimeoutNamesTheHolder(t *testing.T) {
	db := NewDB(filepath.Join(t.TempDir(), "db"))
	holdExecutor(t, db)
	startSnapshot(db)

	err := expireWhileQueued(t, db, db.Sync)
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

func TestDB_SyncAndWaitYieldsToARunningSnapshot(t *testing.T) {
	db := openReplicatedDB(t)
	holdExecutor(t, db)
	startSnapshot(db)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	started := time.Now()
	requireSnapshotHolder(t, db.SyncAndWait(ctx))
	if elapsed := time.Since(started); elapsed < snapshotYieldAfter || elapsed > 3*snapshotYieldAfter {
		t.Fatalf("SyncAndWait took %s behind the snapshot, want about %s", elapsed, snapshotYieldAfter)
	}
}

func TestDB_SyncAndWaitYieldsWhenTheHolderStartsASnapshot(t *testing.T) {
	db := openReplicatedDB(t)
	holdExecutor(t, db)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- db.SyncAndWait(ctx) }()
	waitForExecutorWaiter(t, db, done)

	startSnapshot(db)
	select {
	case err := <-done:
		requireSnapshotHolder(t, err)
	case <-time.After(3 * snapshotYieldAfter):
		t.Fatal("SyncAndWait kept waiting after the holder's snapshot outlasted the yield delay")
	}
}

func TestDB_SyncAndWaitGivesEachSnapshotItsOwnSecond(t *testing.T) {
	db := openReplicatedDB(t)
	holdExecutor(t, db)
	startSnapshot(db)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	started := time.Now()
	done := make(chan error, 1)
	go func() { done <- db.SyncAndWait(ctx) }()
	waitForExecutorWaiter(t, db, done)

	time.Sleep(snapshotYieldAfter / 2)
	db.finishSyncDiag(nil)
	db.beginSyncDiag(diagOpSync)
	startSnapshot(db)
	nextStarted := time.Now()

	requireSnapshotHolder(t, <-done)
	if waited := time.Since(nextStarted); waited < snapshotYieldAfter {
		t.Fatalf("refused %s after the second snapshot started (%s after the first), want at least %s", waited, time.Since(started), snapshotYieldAfter)
	}
}

func TestDB_SyncAndWaitWaitsOutAShortSnapshot(t *testing.T) {
	db := openReplicatedDB(t)
	mustAcquireSemaphore(db.execSem)
	db.beginSyncDiag(diagOpSync)
	startSnapshot(db)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- db.SyncAndWait(ctx) }()
	waitForExecutorWaiter(t, db, done)

	db.finishSyncDiag(nil)
	db.execSem.Release(1)
	if err := <-done; err != nil {
		t.Fatalf("SyncAndWait after a short snapshot: %v", err)
	}
}

func TestDB_SyncAndWaitQueuesBehindAnIncrementalHolder(t *testing.T) {
	db := openReplicatedDB(t)
	holdExecutor(t, db)

	err := expireWhileQueued(t, db, db.SyncAndWait)
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrSnapshotting) {
		t.Fatalf("err=%v, want only the deadline", err)
	}
}

func syncOverHTTP(t *testing.T, db *DB, timeoutSeconds int) (int, string, SyncDiagnostic) {
	t.Helper()
	server := NewServer(NewStore([]*DB{db}, CompactionLevels{{Level: 0}}))
	body := fmt.Sprintf(`{"path": %q, "wait": true, "timeout": %d}`, db.Path(), timeoutSeconds)
	rec := httptest.NewRecorder()
	server.handleSync(rec, httptest.NewRequest(http.MethodPost, "/sync", strings.NewReader(body)))
	var resp struct {
		Error   string         `json:"error"`
		Details SyncDiagnostic `json:"details"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	return rec.Code, resp.Error, resp.Details
}

func TestServer_SyncDuringASnapshotAnswersUnavailable(t *testing.T) {
	db := openReplicatedDB(t)
	holdExecutor(t, db)
	startSnapshot(db)

	status, message, details := syncOverHTTP(t, db, 10)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("status=%d error=%q, want 503", status, message)
	}
	if !strings.Contains(message, ErrSnapshotting.Error()) {
		t.Fatalf("error=%q, want the snapshot refusal", message)
	}
	if details.Phase != "write_ltx_from_db" || !details.Snapshotting || details.Path != db.Path() || details.DBSize == 0 {
		t.Fatalf("details=%+v, want the snapshotting holder of %s with its size", details, db.Path())
	}
}

func TestServer_SyncTimeoutCarriesTheExecutorHolder(t *testing.T) {
	db := openReplicatedDB(t)
	holdExecutor(t, db)

	status, message, details := syncOverHTTP(t, db, 1)
	if status != http.StatusInternalServerError {
		t.Fatalf("status=%d error=%q, want 500", status, message)
	}
	if !strings.Contains(message, "wait for db sync executor: context deadline exceeded") {
		t.Fatalf("error=%q, want the executor deadline", message)
	}
	if details.Phase != "verify" || details.Snapshotting || details.Path != db.Path() || details.DBSize == 0 {
		t.Fatalf("details=%+v, want the verifying holder of %s with its size", details, db.Path())
	}
}
