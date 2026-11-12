package litestream

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/superfly/ltx"
)

func TestDB_AWriterDuringACheckpointOfALargeDatabaseWaitsUnderItsBusyTimeout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	setup, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := setup.Exec(`PRAGMA page_size = 512; CREATE TABLE t (id INT, body BLOB)`); err != nil {
		t.Fatal(err)
	}
	if err := setup.Close(); err != nil {
		t.Fatal(err)
	}
	db := NewDB(path)
	db.MonitorInterval = 0
	db.Replica = NewReplicaWithClient(db, &testReplicaClient{dir: t.TempDir()})
	db.Replica.MonitorEnabled = false
	var logs bytes.Buffer
	db.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}

	const busyTimeout = 5 * time.Second
	const formerTruncatePageN = 121359
	writer := openConn(t, db, busyTimeout)
	if _, err := writer.Exec(`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?) INSERT INTO t SELECT i, randomblob(400) FROM n`, formerTruncatePageN); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(db.WALPath())
	if err != nil {
		t.Fatal(err)
	}
	if frames := (fi.Size() - WALHeaderSize) / (512 + WALFrameHeaderSize); frames <= formerTruncatePageN {
		t.Fatalf("the wal holds %d frames, want more than %d", frames, formerTruncatePageN)
	}

	var waited time.Duration
	committed := make(chan error, 1)
	db.checkpointBarrierTaken = func() {
		go func() {
			started := time.Now()
			_, err := writer.Exec(`INSERT INTO t (id) VALUES (0)`)
			waited = time.Since(started)
			committed <- err
		}()
	}
	err = db.Sync(t.Context())
	db.checkpointBarrierTaken = nil
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-committed:
		if err != nil {
			t.Fatalf("a write queued behind the checkpoint barrier failed after %s: %v", waited, err)
		}
	case <-time.After(2 * busyTimeout):
		t.Fatal("the sync took no checkpoint, so no write queued behind it")
	}
	if waited >= busyTimeout {
		t.Fatalf("a write queued behind the checkpoint barrier waited %s, want under its %s busy_timeout", waited, busyTimeout)
	}

	for _, mode := range []string{"TRUNCATE", "RESTART"} {
		if n := testutil.ToFloat64(db.checkpointNCounterVec.WithLabelValues(mode)); n != 0 {
			t.Errorf("%s checkpoints=%v, want none", mode, n)
		}
	}
	if copies := laneSnapshotTXIDs(t, &logs); !slices.Equal(copies, []uint64{1}) {
		t.Fatalf("full copies at txids %v, want only the first sync's", copies)
	}
}

func TestDB_PassiveCheckpointsAcrossWALRestartsLoseNoCommitAndCopyTheDatabaseOnce(t *testing.T) {
	db := openReplicatedDB(t)
	var logs bytes.Buffer
	db.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}

	writer := openConn(t, db, 10*time.Second)
	var committed atomic.Int64
	stop, wrote := make(chan struct{}), make(chan error, 1)
	go func() {
		for id := int64(1); ; id++ {
			select {
			case <-stop:
				wrote <- nil
				return
			default:
			}
			if _, err := writer.Exec(`INSERT INTO t (id) VALUES (?)`, id); err != nil {
				wrote <- err
				return
			}
			committed.Store(id)
			time.Sleep(100 * time.Microsecond)
		}
	}()

	const wantRestarts = 50
	restarts := 0
	for deadline := time.Now().Add(30 * time.Second); restarts < wantRestarts && time.Now().Before(deadline); {
		before := walSalts(t, db)
		if err := db.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := db.Checkpoint(t.Context()); err != nil && !isSQLiteBusyError(err) {
			t.Fatal(err)
		}
		if walSalts(t, db) != before {
			restarts++
		}
	}
	close(stop)
	if err := <-wrote; err != nil {
		t.Fatal(err)
	}
	if restarts < wantRestarts {
		t.Fatalf("the wal restarted %d times in 30s, want %d", restarts, wantRestarts)
	}

	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
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
	var rows, distinct, last int64
	if err := copied.QueryRow(`SELECT count(*), count(DISTINCT id), coalesce(max(id), 0) FROM t`).Scan(&rows, &distinct, &last); err != nil {
		t.Fatal(err)
	}
	if want := committed.Load(); rows != want || distinct != want || last != want {
		t.Fatalf("restored rows=%d distinct=%d max=%d, want each of the %d committed rows once", rows, distinct, last, want)
	}

	for _, mode := range []string{"TRUNCATE", "RESTART"} {
		if n := testutil.ToFloat64(db.checkpointNCounterVec.WithLabelValues(mode)); n != 0 {
			t.Errorf("%s checkpoints=%v, want none", mode, n)
		}
	}
	if n := testutil.ToFloat64(db.checkpointNCounterVec.WithLabelValues(CheckpointModePassive)); n < wantRestarts {
		t.Errorf("PASSIVE checkpoints=%v, want at least one per restart", n)
	}
	if copies := laneSnapshotTXIDs(t, &logs); !slices.Equal(copies, []uint64{1}) {
		t.Fatalf("full copies at txids %v, want only the first sync's", copies)
	}
}

func TestDB_AWriterCommitsWithoutWaitingWhileASnapshotCopiesTheDatabase(t *testing.T) {
	frameSize := func(db *DB) int64 { return int64(db.pageSize + WALFrameHeaderSize) }
	for _, c := range []struct {
		reason string
		force  func(t *testing.T, db *DB, writer *sql.DB)
	}{
		{reason: "no local ltx position, snapshotting"},
		{
			reason: "last ltx file holds no wal position, snapshotting",
			force:  func(t *testing.T, db *DB, _ *sql.DB) { ForgetLastWALPosition(t, db) },
		},
		{
			reason: "wal truncated by another process",
			force: func(t *testing.T, db *DB, writer *sql.DB) {
				withoutReadLock(t, db, func() {
					checkpointWAL(t, writer, "TRUNCATE")
					insertRows(t, writer, 100, 100)
				})
				db.syncState.syncedToWALEnd = false
			},
		},
		{
			reason: "wal header salt reset, snapshotting",
			force: func(t *testing.T, db *DB, _ *sql.DB) {
				rewriteLastL0Header(t, db, func(h *ltx.Header) { h.WALOffset, h.WALSize, h.WALSalt1 = WALHeaderSize, 0, h.WALSalt1+1 })
			},
		},
		{
			reason: "last page does not exist in last ltx file, wal overwritten by another process",
			force: func(t *testing.T, db *DB, writer *sql.DB) {
				insertRows(t, writer, 100, 101)
				rewriteLastL0Header(t, db, func(h *ltx.Header) { h.WALSize += frameSize(db) })
			},
		},
		{
			reason: "full or restart checkpoint detected, snapshotting",
			force: func(t *testing.T, db *DB, writer *sql.DB) {
				withoutReadLock(t, db, func() {
					checkpointWAL(t, writer, "RESTART")
					insertRows(t, writer, 100, 104)
					checkpointWAL(t, writer, "RESTART")
					insertRows(t, writer, 200, 200)
				})
			},
		},
	} {
		for _, entry := range []string{"sync", "checkpoint"} {
			t.Run(entry+"/"+c.reason, func(t *testing.T) {
				db := openReplicatedDB(t)
				writer := openConn(t, db, 10*time.Second)
				if c.force != nil {
					if err := db.Sync(t.Context()); err != nil {
						t.Fatal(err)
					}
					insertRows(t, writer, 1, 20)
					if err := db.Sync(t.Context()); err != nil {
						t.Fatal(err)
					}
					c.force(t, db, writer)
				}

				probe := openConn(t, db, 0)
				var reasons []string
				db.openLTXFile = func(name string, flag int, perm os.FileMode) (ltxStagingFile, error) {
					if holder, running := db.runningSnapshot(); running {
						reasons = append(reasons, holder.Reason)
						if _, err := probe.Exec(`INSERT INTO t (id) VALUES (-1)`); err != nil {
							t.Errorf("a write during the snapshot failed: %v", err)
						}
					}
					return defaultOpenLTXFile(name, flag, perm)
				}
				run := db.Sync
				if entry == "checkpoint" {
					run = db.Checkpoint
				}
				err := run(t.Context())
				db.openLTXFile = defaultOpenLTXFile
				if err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(reasons, []string{c.reason}) {
					t.Fatalf("snapshots %q, want one for %q", reasons, c.reason)
				}
			})
		}
	}
}

func TestDB_ASnapshotOwedAtTheBarrierRunsOnlyAfterTheBarrierRollsBack(t *testing.T) {
	db := openReplicatedDB(t)
	writer := openConn(t, db, 10*time.Second)
	insertRows(t, writer, 1, 20)
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	insertRows(t, writer, 21, 21)

	probe := openConn(t, db, 0)
	var reasons []string
	db.openLTXFile = func(name string, flag int, perm os.FileMode) (ltxStagingFile, error) {
		if holder, running := db.runningSnapshot(); running {
			reasons = append(reasons, holder.Reason)
			if _, err := probe.Exec(`INSERT INTO t (id) VALUES (-1)`); err != nil {
				t.Errorf("a write during the snapshot failed: %v", err)
			}
		}
		return defaultOpenLTXFile(name, flag, perm)
	}
	t.Cleanup(func() { db.openLTXFile = defaultOpenLTXFile })
	db.checkpointBarrierTaken = func() { ForgetLastWALPosition(t, db) }
	if err := db.Checkpoint(t.Context()); err != nil {
		t.Fatal(err)
	}
	db.checkpointBarrierTaken = nil
	if len(reasons) != 0 {
		t.Fatalf("the checkpoint copied the database for %q", reasons)
	}
	if n := testutil.ToFloat64(db.checkpointNCounterVec.WithLabelValues(CheckpointModePassive)); n != 0 {
		t.Fatalf("PASSIVE checkpoints=%v over a wal the barrier could not seal, want none", n)
	}

	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if want := "last ltx file holds no wal position, snapshotting"; !slices.Equal(reasons, []string{want}) {
		t.Fatalf("snapshots %q, want one for %q", reasons, want)
	}
}

func TestDB_TheFirstSyncAfterEachWALRestartContinuesFromTheNewHeader(t *testing.T) {
	source, err := os.ReadFile("db.go")
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(source), "INSERT INTO _litestream_lock"); n != 1 {
		t.Fatalf("db.go takes the write lock in %d places, want only the checkpoint barrier", n)
	}

	db := openReplicatedDB(t)
	var logs bytes.Buffer
	db.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	writer := openConn(t, db, 10*time.Second)
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	for id := range 20 {
		insertRows(t, writer, id, id)
		before := walSalts(t, db)
		if err := db.Checkpoint(t.Context()); err != nil {
			t.Fatal(err)
		}
		after := walSalts(t, db)
		if after == before {
			t.Fatalf("checkpoint %d left the wal unrestarted", id)
		}
		minTXID, maxTXID, err := db.MaxLTX()
		if err != nil {
			t.Fatal(err)
		}
		f, err := os.Open(db.LTXPath(0, minTXID, maxTXID))
		if err != nil {
			t.Fatal(err)
		}
		dec := ltx.NewDecoder(f)
		err = dec.DecodeHeader()
		_ = f.Close()
		if err != nil {
			t.Fatal(err)
		}
		if hdr := dec.Header(); hdr.WALOffset != WALHeaderSize || [2]uint32{hdr.WALSalt1, hdr.WALSalt2} != after {
			t.Fatalf("checkpoint %d: first sync after the restart at offset %d salts %d/%d, want offset %d salts %v", id, hdr.WALOffset, hdr.WALSalt1, hdr.WALSalt2, WALHeaderSize, after)
		}
	}
	if copies := laneSnapshotTXIDs(t, &logs); !slices.Equal(copies, []uint64{1}) {
		t.Fatalf("full copies at txids %v, want only the first sync's", copies)
	}
}

func TestDB_AWriterIsRefusedOnlyWhileTheCheckpointBarrierHoldsTheWriteLock(t *testing.T) {
	db := openReplicatedDB(t)
	writer := openConn(t, db, 10*time.Second)
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	insertRows(t, writer, 1, 1)

	probe := openConn(t, db, 0)
	barrier, released := false, false
	var probed []string
	write := func(at string) {
		probed = append(probed, at)
		_, err := probe.Exec(`INSERT INTO t (id) VALUES (-1)`)
		if barrier && !isSQLiteBusyError(err) {
			t.Errorf("a write %s: %v, want SQLITE_BUSY", at, err)
		}
		if !barrier && err != nil {
			t.Errorf("a write %s failed: %v", at, err)
		}
	}
	db.openLTXFile = func(name string, flag int, perm os.FileMode) (ltxStagingFile, error) {
		switch {
		case barrier:
			write("while the barrier seals")
		case released:
			write("after the barrier")
		default:
			write("before the barrier")
		}
		return defaultOpenLTXFile(name, flag, perm)
	}
	db.checkpointBarrierTaken = func() {
		barrier = true
		write("once the barrier is taken")
	}
	db.checkpointBarrierReleased = func() {
		barrier, released = false, true
		write("once the barrier is released")
	}
	err := db.Checkpoint(t.Context())
	db.openLTXFile, db.checkpointBarrierTaken, db.checkpointBarrierReleased = defaultOpenLTXFile, nil, nil
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"before the barrier", "once the barrier is taken", "while the barrier seals", "once the barrier is released", "after the barrier"}
	if !slices.Equal(probed, want) {
		t.Fatalf("probed %q, want %q", probed, want)
	}
}

// ForgetLastWALPosition rewrites the last L0 file without its WAL position, so
// the next sync takes a full snapshot.
func ForgetLastWALPosition(t testing.TB, db *DB) {
	rewriteLastL0Header(t, db, func(h *ltx.Header) { h.WALOffset, h.WALSize, h.WALSalt1, h.WALSalt2 = 0, 0, 0, 0 })
}

func rewriteLastL0Header(t testing.TB, db *DB, edit func(*ltx.Header)) {
	t.Helper()
	minTXID, maxTXID, err := db.MaxLTX()
	if err != nil {
		t.Fatal(err)
	}
	path := db.LTXPath(0, minTXID, maxTXID)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	dec := ltx.NewDecoder(f)
	if err := dec.DecodeHeader(); err != nil {
		t.Fatal(err)
	}
	hdr := dec.Header()
	edit(&hdr)
	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.EncodeHeader(hdr); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, hdr.PageSize)
	for {
		var page ltx.PageHeader
		if err := dec.DecodePage(&page, data); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: page.Pgno}, data); err != nil {
			t.Fatal(err)
		}
	}
	if err := dec.Close(); err != nil {
		t.Fatal(err)
	}
	enc.SetPostApplyChecksum(dec.Trailer().PostApplyChecksum)
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	db.invalidatePosCache()
}

func openConn(t *testing.T, db *DB, busyTimeout time.Duration) *sql.DB {
	t.Helper()
	conn, err := sql.Open("sqlite", fmt.Sprintf("%s?_pragma=busy_timeout(%d)", db.Path(), busyTimeout.Milliseconds()))
	if err != nil {
		t.Fatal(err)
	}
	conn.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func insertRows(t *testing.T, conn *sql.DB, first, last int) {
	t.Helper()
	for id := first; id <= last; id++ {
		if _, err := conn.Exec(`INSERT INTO t (id) VALUES (?)`, id); err != nil {
			t.Fatal(err)
		}
	}
}

func withoutReadLock(t *testing.T, db *DB, asAnotherProcess func()) {
	t.Helper()
	if err := db.releaseReadLock(); err != nil {
		t.Fatal(err)
	}
	asAnotherProcess()
	if err := db.acquireReadLock(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func checkpointWAL(t *testing.T, conn *sql.DB, mode string) {
	t.Helper()
	var busy, frames, folded int
	if err := conn.QueryRow(`PRAGMA wal_checkpoint(`+mode+`)`).Scan(&busy, &frames, &folded); err != nil {
		t.Fatal(err)
	}
	if busy != 0 {
		t.Fatalf("%s checkpoint was blocked", mode)
	}
}

func walSalts(t *testing.T, db *DB) [2]uint32 {
	t.Helper()
	hdr, err := readWALHeader(db.WALPath())
	if err != nil {
		t.Fatal(err)
	}
	return [2]uint32{binary.BigEndian.Uint32(hdr[16:]), binary.BigEndian.Uint32(hdr[20:])}
}

func laneSnapshotTXIDs(t *testing.T, logs *bytes.Buffer) []uint64 {
	t.Helper()
	var txids []uint64
	for line := range bytes.Lines(logs.Bytes()) {
		var record struct {
			Msg  string          `json:"msg"`
			TXID json.RawMessage `json:"txid"`
		}
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		if record.Msg != "lane snapshot" {
			continue
		}
		var txid uint64
		if err := json.Unmarshal(record.TXID, &txid); err != nil {
			t.Fatal(err)
		}
		txids = append(txids, txid)
	}
	return txids
}
