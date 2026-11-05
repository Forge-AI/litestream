package litestream

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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
	db := openUncheckedDB(t)
	t.Cleanup(func() {
		if err := db.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
	return db
}

func openUncheckedDB(t *testing.T) *DB {
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

func TestDB_FirstSyncKeepsItsReadLockOnceItsCallerReturns(t *testing.T) {
	db := openReplicatedDB(t)
	ctx, cancel := context.WithCancel(t.Context())
	if err := db.SyncAndWait(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()
	time.Sleep(100 * time.Millisecond)
	if _, err := db.rtx.ExecContext(t.Context(), `SELECT COUNT(1) FROM _litestream_seq`); err != nil {
		t.Fatalf("the read lock barring outside checkpoints died with its first caller: %v", err)
	}

	outside, err := sql.Open("sqlite", db.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer outside.Close()
	if _, err := outside.Exec(`INSERT INTO t (id) VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	var busy, frames, backfilled int
	if err := outside.QueryRow(`PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &frames, &backfilled); err != nil {
		t.Fatal(err)
	}
	if busy != 1 {
		t.Fatalf("an outside TRUNCATE passed the read lock: busy=%d frames=%d backfilled=%d", busy, frames, backfilled)
	}
}

func TestDB_WritesQueuedBehindAPassiveCheckpointKeepTheNextSyncIncremental(t *testing.T) {
	db := openReplicatedDB(t)
	sqldb, err := sql.Open("sqlite", db.Path())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqldb.Close() })
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`INSERT INTO t (id) VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`INSERT INTO t (id) VALUES (2)`); err != nil {
		t.Fatal(err)
	}

	var logs bytes.Buffer
	db.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	const queuedWrites = 64
	raced := false
	db.checkpointBarrierReleased = func() {
		if raced {
			return
		}
		raced = true
		for id := 3; id < 3+queuedWrites; id++ {
			if _, err := sqldb.Exec(`INSERT INTO t (id) VALUES (?)`, id); err != nil {
				t.Error(err)
			}
		}
	}
	db.MinCheckpointPageN = 1
	if err := db.SyncAndWait(t.Context()); err != nil {
		t.Fatal(err)
	}
	db.checkpointBarrierReleased = nil
	db.MinCheckpointPageN = DefaultMinCheckpointPageN
	if !raced {
		t.Fatal("the sync took no passive checkpoint, so nothing raced it")
	}
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(logs.String(), "resuming the wal from its header") {
		t.Fatalf("the queued writes never overwrote the last synced frame: %s", logs.String())
	}
	if strings.Contains(logs.String(), "lane snapshot") {
		t.Fatalf("the race took a full snapshot: %s", logs.String())
	}

	if err := db.Replica.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(t.TempDir(), "restored.db")
	opt := NewRestoreOptions()
	opt.OutputPath = restored
	if err := NewReplicaWithClient(nil, db.Replica.Client).Restore(t.Context(), opt); err != nil {
		t.Fatal(err)
	}
	copied, err := sql.Open("sqlite", restored)
	if err != nil {
		t.Fatal(err)
	}
	defer copied.Close()
	var rows, ids int
	if err := copied.QueryRow(`SELECT count(*), sum(id) FROM t`).Scan(&rows, &ids); err != nil {
		t.Fatal(err)
	}
	last := 2 + queuedWrites
	if want := last * (last + 1) / 2; rows != last || ids != want {
		t.Fatalf("restored rows=%d ids=%d, want rows=%d ids=%d", rows, ids, last, want)
	}
}

func holdFullCopyUntilAbandoned(t *testing.T, db *DB) <-chan error {
	t.Helper()
	sqldb, err := sql.Open("sqlite", db.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer sqldb.Close()
	if _, err := sqldb.Exec(`INSERT INTO t (id) VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`INSERT INTO t (id) VALUES (2)`); err != nil {
		t.Fatal(err)
	}

	copying := make(chan struct{})
	var began sync.Once
	db.openLTXFile = func(name string, flag int, perm os.FileMode) (ltxStagingFile, error) {
		if _, running := db.runningSnapshot(); running {
			began.Do(func() { close(copying) })
			<-db.abandoned.Done()
		}
		return defaultOpenLTXFile(name, flag, perm)
	}
	checkpointed := make(chan error, 1)
	go func() { checkpointed <- db.Checkpoint(context.Background(), CheckpointModeTruncate) }()
	select {
	case <-copying:
	case <-time.After(10 * time.Second):
		t.Fatal("the checkpoint took no boundary copy")
	}
	return checkpointed
}

func TestDB_AbandonStopsAFullCopyInsideACheckpointAndTheNextProcessLosesNothing(t *testing.T) {
	db := openUncheckedDB(t)
	checkpointed := holdFullCopyUntilAbandoned(t, db)
	_, before, err := db.MaxLTX()
	if err != nil {
		t.Fatal(err)
	}

	db.Abandon()
	if err := <-checkpointed; !errors.Is(err, context.Canceled) {
		t.Fatalf("Checkpoint=%v, want the full copy stopped by the abandon", err)
	}
	_, after, err := db.MaxLTX()
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("last ltx file=%s, want %s: a stopped copy must publish no file", after, before)
	}
	db.openLTXFile = defaultOpenLTXFile
	client := db.Replica.Client
	path := db.Path()
	db.boundary.Unlock()
	if err := db.Close(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close=%v, want the owed copy reported, so no park records this position", err)
	}

	next := NewDB(path)
	next.MonitorInterval = 0
	next.Replica = NewReplicaWithClient(next, client)
	next.Replica.MonitorEnabled = false
	if err := next.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := next.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
	if err := next.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := next.Replica.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(t.TempDir(), "restored.db")
	opt := NewRestoreOptions()
	opt.OutputPath = restored
	if err := NewReplicaWithClient(nil, client).Restore(t.Context(), opt); err != nil {
		t.Fatal(err)
	}
	copied, err := sql.Open("sqlite", restored)
	if err != nil {
		t.Fatal(err)
	}
	defer copied.Close()
	var rows int
	if err := copied.QueryRow(`SELECT count(*) FROM t`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Fatalf("restored rows=%d, want 2", rows)
	}
}

func TestAbandonAll_ACheckpointInOneDatabaseDoesNotHoldACopyInAnother(t *testing.T) {
	checkpointing := openUncheckedDB(t)
	copying := openUncheckedDB(t)
	t.Cleanup(func() {
		_ = checkpointing.Close(context.Background())
		_ = copying.Close(context.Background())
	})
	checkpointed := holdFullCopyUntilAbandoned(t, copying)

	checkpointing.boundary.Lock()
	abandoned := make(chan struct{})
	go func() {
		AbandonAll([]*DB{checkpointing, copying})
		close(abandoned)
	}()
	select {
	case err := <-checkpointed:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Checkpoint=%v, want the full copy stopped", err)
		}
	case <-time.After(10 * time.Second):
		checkpointing.boundary.Unlock()
		t.Fatal("the other database's checkpoint held this database's copy")
	}
	select {
	case <-abandoned:
		t.Fatal("the abandon must still wait out the checkpoint in flight")
	default:
	}
	checkpointing.boundary.Unlock()
	<-abandoned
}
