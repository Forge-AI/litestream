package litestream

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/superfly/ltx"
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

func requireSnapshotRefusal(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrSnapshotting) {
		t.Fatalf("err=%v, want ErrSnapshotting", err)
	}
}

func TestDB_SyncAndWaitYieldsToARunningSnapshot(t *testing.T) {
	db := openReplicatedDB(t)
	holdExecutor(t, db)
	startSnapshot(db)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	started := time.Now()
	requireSnapshotRefusal(t, db.SyncAndWait(ctx))
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
		requireSnapshotRefusal(t, err)
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

	requireSnapshotRefusal(t, <-done)
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

func syncOverHTTP(t *testing.T, db *DB, timeoutSeconds int) (int, string) {
	t.Helper()
	server := NewServer(NewStore([]*DB{db}, CompactionLevels{{Level: 0}}))
	body := fmt.Sprintf(`{"path": %q, "wait": true, "timeout": %d}`, db.Path(), timeoutSeconds)
	rec := httptest.NewRecorder()
	server.handleSync(rec, httptest.NewRequest(http.MethodPost, "/sync", strings.NewReader(body)))
	var resp struct {
		Error string `json:"error"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	return rec.Code, resp.Error
}

func TestServer_SyncDuringASnapshotAnswersUnavailable(t *testing.T) {
	db := openReplicatedDB(t)
	holdExecutor(t, db)
	startSnapshot(db)

	status, message := syncOverHTTP(t, db, 10)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("status=%d error=%q, want 503", status, message)
	}
	if !strings.Contains(message, ErrSnapshotting.Error()) {
		t.Fatalf("error=%q, want the snapshot refusal", message)
	}
}

func TestDB_SyncAndWaitFinishesACheckpointItsCallerAbandons(t *testing.T) {
	db := openReplicatedDB(t)
	sqldb, err := sql.Open("sqlite", db.Path())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqldb.Close() })
	if _, err := sqldb.Exec(`INSERT INTO t (id) VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`INSERT INTO t (id) VALUES (2)`); err != nil {
		t.Fatal(err)
	}
	before, err := readWALHeader(db.WALPath())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	restarted := make(chan struct{})
	var once sync.Once
	db.openLTXFile = func(name string, flag int, perm os.FileMode) (ltxStagingFile, error) {
		if hdr, err := readWALHeader(db.WALPath()); err == nil && !bytes.Equal(hdr, before) {
			once.Do(func() { close(restarted) })
			<-ctx.Done()
		}
		return defaultOpenLTXFile(name, flag, perm)
	}
	db.MinCheckpointPageN = 1

	done := make(chan error, 1)
	go func() { done <- db.SyncAndWait(ctx) }()
	select {
	case <-restarted:
	case err := <-done:
		t.Fatalf("returned before the checkpoint restarted the wal: %v", err)
	}
	cancel(context.DeadlineExceeded)
	<-done

	after, err := readWALHeader(db.WALPath())
	if err != nil {
		t.Fatal(err)
	}
	db.openLTXFile = defaultOpenLTXFile
	db.MinCheckpointPageN = DefaultMinCheckpointPageN
	pos, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	}
	ltxPath := db.LTXPath(0, pos.TXID, pos.TXID)
	f, err := os.Open(ltxPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	dec := ltx.NewDecoder(f)
	if err := dec.DecodeHeader(); err != nil {
		t.Fatal(err)
	}
	hdr := dec.Header()
	if hdr.WALOffset != WALHeaderSize || hdr.WALSalt1 != binary.BigEndian.Uint32(after[16:]) || hdr.WALSalt2 != binary.BigEndian.Uint32(after[20:]) {
		t.Fatalf("last ltx %+v, want the copy of the restarted wal the abandoned checkpoint began", hdr)
	}
}
