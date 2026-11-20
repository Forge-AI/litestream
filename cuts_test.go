package litestream

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/superfly/ltx"
)

func TestDB_CutAfterSyncedOffsetEndsAnLTXFile(t *testing.T) {
	db, sqldb := openCutLane(t)
	before := mustPos(t, db)

	insertCutRow(t, sqldb)
	cut := walEndOf(t, db)
	writePendingCut(t, db, "child", cut)
	insertCutRow(t, sqldb)
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	end, landed := l0FileAt(t, db, before.TXID+1)
	if end != cut {
		t.Fatalf("the LTX file after the cut ends at %+v, want the cut %+v", end, cut)
	}
	if got, want := readCut(t, db, "child"), landedCutJSON(landed); got != want {
		t.Fatalf("cut=%s, want %s", got, want)
	}
	if got, want := mustPos(t, db).TXID, before.TXID+2; got != want {
		t.Fatalf("txid=%s, want %s: the sync continues past the cut", got, want)
	}
	if end, _ := l0FileAt(t, db, before.TXID+2); end != walEndOf(t, db) {
		t.Fatalf("the sync ends at %+v, want the WAL end %+v", end, walEndOf(t, db))
	}
}

func TestDB_CutAtSyncedOffsetResolvesWithoutAFile(t *testing.T) {
	db, sqldb := openCutLane(t)
	insertCutRow(t, sqldb)
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	synced := mustPos(t, db)
	_, landed := l0FileAt(t, db, synced.TXID)

	writePendingCut(t, db, "child", walEndOf(t, db))
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	if _, maxTXID, err := db.MaxLTX(); err != nil {
		t.Fatal(err)
	} else if maxTXID != synced.TXID {
		t.Fatalf("max txid=%s, want %s: a cut at the synced offset writes no file", maxTXID, synced.TXID)
	}
	if got, want := readCut(t, db, "child"), landedCutJSON(landed); got != want {
		t.Fatalf("cut=%s, want %s", got, want)
	}
}

func TestDB_CutsAtOneOffsetResolveToOneTransaction(t *testing.T) {
	db, sqldb := openCutLane(t)
	before := mustPos(t, db)

	insertCutRow(t, sqldb)
	cut := walEndOf(t, db)
	writePendingCut(t, db, "first", cut)
	writePendingCut(t, db, "second", cut)
	insertCutRow(t, sqldb)
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	_, landed := l0FileAt(t, db, before.TXID+1)
	want := landedCutJSON(landed)
	for _, name := range []string{"first", "second"} {
		if got := readCut(t, db, name); got != want {
			t.Fatalf("cut %s=%s, want %s", name, got, want)
		}
	}
}

func TestDB_CutNoSyncCanLandOnIsMissed(t *testing.T) {
	db, sqldb := openCutLane(t)
	ctx := context.Background()

	insertCutRow(t, sqldb)
	cut := walEndOf(t, db)
	insertCutRow(t, sqldb)
	if err := db.ResetLocalState(ctx); err != nil {
		t.Fatal(err)
	}
	writePendingCut(t, db, "snapshotted", cut)
	writePendingCut(t, db, "restarted", walPosition{Salt1: cut.Salt1 + 1, Salt2: cut.Salt2 + 1, Offset: cut.Offset})
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{"snapshotted", "restarted"} {
		if got, want := readCut(t, db, name), `{"missed":true}`; got != want {
			t.Fatalf("cut %s=%s, want %s", name, got, want)
		}
	}
}

func TestDB_CutWrittenDuringASyncIsNeverSkipped(t *testing.T) {
	db, sqldb := openCutLane(t)
	ctx := context.Background()

	insertCutRow(t, sqldb)
	var cut walPosition
	db.openLTXFile = func(name string, flag int, perm os.FileMode) (ltxStagingFile, error) {
		if cut.Offset == 0 {
			insertCutRow(t, sqldb)
			cut = walEndOf(t, db)
			writePendingCut(t, db, "child", cut)
		}
		return defaultOpenLTXFile(name, flag, perm)
	}
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if got, want := readCut(t, db, "child"), pendingCutJSON(t, cut); got != want {
		t.Fatalf("cut=%s, want it pending as %s: the sync listed cuts before it was written", got, want)
	}
	synced := mustPos(t, db)

	insertCutRow(t, sqldb)
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	end, landed := l0FileAt(t, db, synced.TXID+1)
	if end != cut {
		t.Fatalf("the LTX file after the cut ends at %+v, want the cut %+v", end, cut)
	}
	if got, want := readCut(t, db, "child"), landedCutJSON(landed); got != want {
		t.Fatalf("cut=%s, want %s", got, want)
	}
}

func openCutLane(t *testing.T) (*DB, *sql.DB) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "db")

	db := NewDB(dbPath)
	db.MonitorInterval = 0
	db.Replica = NewReplica(db)
	db.Replica.Client = &testReplicaClient{dir: t.TempDir()}
	db.Replica.MonitorEnabled = false
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	sqldb, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqldb.Close() })
	for _, stmt := range []string{`PRAGMA journal_mode = wal;`, `CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB);`} {
		if _, err := sqldb.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	return db, sqldb
}

func insertCutRow(t *testing.T, sqldb *sql.DB) {
	t.Helper()
	if _, err := sqldb.Exec(`INSERT INTO t(data) VALUES (randomblob(3000));`); err != nil {
		t.Fatal(err)
	}
}

func mustPos(t *testing.T, db *DB) ltx.Pos {
	t.Helper()
	pos, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	}
	return pos
}

func walEndOf(t *testing.T, db *DB) walPosition {
	t.Helper()
	f, err := os.Open(db.WALPath())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	rd, err := NewWALReader(f, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	_, end, _, err := rd.PageMap(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if end == 0 {
		end = WALHeaderSize
	}
	return walPosition{Salt1: rd.salt1, Salt2: rd.salt2, Offset: end}
}

func l0FileAt(t *testing.T, db *DB, txid ltx.TXID) (walPosition, landedCut) {
	t.Helper()
	f, err := os.Open(db.LTXPath(0, txid, txid))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	dec := ltx.NewDecoder(f)
	if err := dec.Verify(); err != nil {
		t.Fatal(err)
	}
	hdr := dec.Header()
	return walPosition{Salt1: hdr.WALSalt1, Salt2: hdr.WALSalt2, Offset: hdr.WALOffset + hdr.WALSize}, landedCut{TXID: uint64(txid), File: dec.Trailer().FileChecksum}
}

func writePendingCut(t *testing.T, db *DB, name string, at walPosition) {
	t.Helper()
	if err := os.MkdirAll(db.cutsPath(), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(db.cutsPath(), name), []byte(pendingCutJSON(t, at)), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readCut(t *testing.T, db *DB, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(db.cutsPath(), name))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func pendingCutJSON(t *testing.T, at walPosition) string {
	t.Helper()
	b, err := json.Marshal(at)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func landedCutJSON(cut landedCut) string {
	return fmt.Sprintf(`{"txid":%d,"file":"%s"}`, cut.TXID, cut.File)
}

func TestDB_SyncEndsAnLTXFileAtEachCutStampedWithItsWrite(t *testing.T) {
	db := openReplicatedDB(t)
	writer := openCutWriter(t, db)
	start := syncedPos(t, db)
	base := (time.Now().UnixMilli()/1000 + 1) * 1000
	var cuts []Cut
	for i := range 3 {
		insertRow(t, writer)
		cuts = append(cuts, cutAt(t, db, base+int64(i)*1000+250))
	}
	db.AddCuts(cuts)
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}

	headers := levelZeroAfter(t, db, start)
	if len(headers) != len(cuts) {
		t.Fatalf("%d level-0 files, want one per cut, %d", len(headers), len(cuts))
	}
	for i, hdr := range headers {
		if hdr.Timestamp != cuts[i].AtMS {
			t.Errorf("file %d stamped %d, want its cut's write at %d", i, hdr.Timestamp, cuts[i].AtMS)
		}
		if end, want := hdr.WALOffset+hdr.WALSize, cutWALEnd(db, cuts[i].Frames); end != want {
			t.Errorf("file %d ends at wal offset %d, want its cut's %d", i, end, want)
		}
	}
}

func TestDB_FramesPastTheLastCutWaitForTheirCutUntilASyncIsAskedFor(t *testing.T) {
	db := openReplicatedDB(t)
	writer := openCutWriter(t, db)
	start := syncedPos(t, db)
	insertRow(t, writer)
	cut := cutAt(t, db, time.Now().Add(-time.Minute).UnixMilli())
	insertRow(t, writer)
	db.AddCuts([]Cut{cut})
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	headers := levelZeroAfter(t, db, start)
	if len(headers) != 1 || headers[0].WALOffset+headers[0].WALSize != cutWALEnd(db, cut.Frames) {
		t.Fatalf("background sync wrote %+v, want one file ending at the cut", headers)
	}

	asked := time.Now().UnixMilli()
	release := db.flushCuts()
	err := db.Sync(t.Context())
	release()
	if err != nil {
		t.Fatal(err)
	}
	headers = levelZeroAfter(t, db, start)
	if len(headers) != 2 {
		t.Fatalf("%d level-0 files after the asked-for sync, want the cut's and the tail's", len(headers))
	}
	tail := headers[1]
	if end, want := tail.WALOffset+tail.WALSize, walSize(t, db); end != want {
		t.Errorf("the asked-for sync ended at wal offset %d, want the wal end %d", end, want)
	}
	if tail.Timestamp < asked {
		t.Errorf("the tail no cut names is stamped %d, want its sync's time, at least %d", tail.Timestamp, asked)
	}
}

func TestDB_CloseShipsFramesNoCutNames(t *testing.T) {
	db := openUncheckedDB(t)
	writer := openCutWriter(t, db)
	start := syncedPos(t, db)
	insertRow(t, writer)
	db.AddCuts([]Cut{cutAt(t, db, time.Now().Add(-time.Minute).UnixMilli())})
	insertRow(t, writer)
	want := walSize(t, db)
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(t.Context()); err != nil {
		t.Fatal(err)
	}

	headers := levelZeroAfter(t, db, start)
	if len(headers) != 2 {
		t.Fatalf("%d level-0 files after close, want the cut's and the tail's", len(headers))
	}
	if end := headers[1].WALOffset + headers[1].WALSize; end != want {
		t.Errorf("close synced to wal offset %d, want the wal end %d", end, want)
	}
}

func TestDB_ADatabaseNeverSentACutSyncsToTheWALEndStampedNow(t *testing.T) {
	db := openReplicatedDB(t)
	writer := openCutWriter(t, db)
	start := syncedPos(t, db)
	insertRow(t, writer)
	insertRow(t, writer)
	before := time.Now().UnixMilli()
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	after := time.Now().UnixMilli()

	headers := levelZeroAfter(t, db, start)
	if len(headers) != 1 {
		t.Fatalf("%d level-0 files, want one for both writes", len(headers))
	}
	if end, want := headers[0].WALOffset+headers[0].WALSize, walSize(t, db); end != want {
		t.Errorf("sync ended at wal offset %d, want the wal end %d", end, want)
	}
	if stamp := headers[0].Timestamp; stamp < before || stamp > after {
		t.Errorf("file stamped %d, want its sync's time in [%d, %d]", stamp, before, after)
	}
}

func TestDB_ACutStampedBeforeTheLastFileTakesThatFilesStamp(t *testing.T) {
	db := openReplicatedDB(t)
	writer := openCutWriter(t, db)
	start := syncedPos(t, db)
	at := (time.Now().UnixMilli()/1000 + 5) * 1000
	insertRow(t, writer)
	first := cutAt(t, db, at)
	insertRow(t, writer)
	db.AddCuts([]Cut{first, cutAt(t, db, at-3000)})
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}

	headers := levelZeroAfter(t, db, start)
	if len(headers) != 2 {
		t.Fatalf("%d level-0 files, want one per cut", len(headers))
	}
	if headers[1].Timestamp != at {
		t.Errorf("the later file is stamped %d, want the earlier file's %d", headers[1].Timestamp, at)
	}
}

func TestDB_AForkCutBeforeASecondCutEndsTwoFilesInWALOrderBothStampedWithThatSecond(t *testing.T) {
	db, sqldb := openCutLane(t)
	before := mustPos(t, db)
	at := (time.Now().UnixMilli()/1000 + 5) * 1000

	insertCutRow(t, sqldb)
	fork := walEndOf(t, db)
	writePendingCut(t, db, "child", fork)
	insertCutRow(t, sqldb)
	db.AddCuts([]Cut{cutAt(t, db, at)})
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	forkEnd, forked := l0FileAt(t, db, before.TXID+1)
	if forkEnd != fork {
		t.Fatalf("the first file ends at %+v, want the fork cut %+v", forkEnd, fork)
	}
	if got, want := readCut(t, db, "child"), landedCutJSON(forked); got != want {
		t.Fatalf("cut=%s, want %s", got, want)
	}
	if end, _ := l0FileAt(t, db, before.TXID+2); end != walEndOf(t, db) {
		t.Fatalf("the second file ends at %+v, want the second's cut at the WAL end %+v", end, walEndOf(t, db))
	}
	for i, hdr := range levelZeroAfter(t, db, before.TXID) {
		if hdr.Timestamp != at {
			t.Errorf("file %d stamped %d, want the second's write at %d", i, hdr.Timestamp, at)
		}
	}
}

func TestDB_AForkCutAndASecondCutAtOneOffsetEndOneFileStampedWithThatSecond(t *testing.T) {
	db, sqldb := openCutLane(t)
	before := mustPos(t, db)
	at := (time.Now().UnixMilli()/1000 + 5) * 1000

	insertCutRow(t, sqldb)
	cut := walEndOf(t, db)
	writePendingCut(t, db, "child", cut)
	db.AddCuts([]Cut{cutAt(t, db, at)})
	insertCutRow(t, sqldb)
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	end, landed := l0FileAt(t, db, before.TXID+1)
	if end != cut {
		t.Fatalf("the file ends at %+v, want both cuts at %+v", end, cut)
	}
	if got, want := readCut(t, db, "child"), landedCutJSON(landed); got != want {
		t.Fatalf("cut=%s, want %s", got, want)
	}
	if headers := levelZeroAfter(t, db, before.TXID); len(headers) != 1 || headers[0].Timestamp != at {
		t.Fatalf("level-0 files %+v, want one stamped with the second's write at %d", headers, at)
	}
}

func TestDB_AForkCutInTheUncutTailShipsUpToItWhileTheRestWaits(t *testing.T) {
	db, sqldb := openCutLane(t)
	db.AddCuts([]Cut{cutAt(t, db, time.Now().Add(-time.Minute).UnixMilli())})
	before := mustPos(t, db)

	insertCutRow(t, sqldb)
	fork := walEndOf(t, db)
	writePendingCut(t, db, "child", fork)
	insertCutRow(t, sqldb)
	shipped := time.Now().UnixMilli()
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	end, landed := l0FileAt(t, db, before.TXID+1)
	if end != fork {
		t.Fatalf("the file ends at %+v, want the fork cut %+v", end, fork)
	}
	if got, want := readCut(t, db, "child"), landedCutJSON(landed); got != want {
		t.Fatalf("cut=%s, want %s", got, want)
	}
	headers := levelZeroAfter(t, db, before.TXID)
	if len(headers) != 1 {
		t.Fatalf("%d level-0 files, want only the fork cut's while the frames after it wait", len(headers))
	}
	if headers[0].Timestamp < shipped {
		t.Errorf("the fork cut's file is stamped %d, want its sync's time, at least %d", headers[0].Timestamp, shipped)
	}
}

func openCutWriter(t *testing.T, db *DB) *sql.DB {
	t.Helper()
	writer, err := sql.Open("sqlite", db.Path())
	if err != nil {
		t.Fatal(err)
	}
	writer.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = writer.Close() })
	if _, err := writer.Exec(`PRAGMA journal_mode = wal; PRAGMA busy_timeout = 5000`); err != nil {
		t.Fatal(err)
	}
	return writer
}

func insertRow(t *testing.T, writer *sql.DB) {
	t.Helper()
	if _, err := writer.Exec(`INSERT INTO t (id) VALUES (1)`); err != nil {
		t.Fatal(err)
	}
}

func syncedPos(t *testing.T, db *DB) ltx.TXID {
	t.Helper()
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	return mustPos(t, db).TXID
}

func cutAt(t *testing.T, db *DB, atMS int64) Cut {
	t.Helper()
	end := walEndOf(t, db)
	frames := (end.Offset - WALHeaderSize) / int64(WALFrameHeaderSize+db.pageSize)
	return Cut{Salt: uint64(end.Salt1)<<32 | uint64(end.Salt2), Frames: uint32(frames), AtMS: atMS}
}

func walSize(t *testing.T, db *DB) int64 {
	t.Helper()
	fi, err := os.Stat(db.WALPath())
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

func cutWALEnd(db *DB, frames uint32) int64 {
	return WALHeaderSize + int64(frames)*int64(WALFrameHeaderSize+db.pageSize)
}

func levelZeroAfter(t *testing.T, db *DB, after ltx.TXID) []ltx.Header {
	t.Helper()
	ents, err := os.ReadDir(db.LTXLevelDir(0))
	if err != nil {
		t.Fatal(err)
	}
	var headers []ltx.Header
	for _, ent := range ents {
		minTXID, maxTXID, err := ltx.ParseFilename(ent.Name())
		if err != nil || minTXID <= after {
			continue
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
		headers = append(headers, dec.Header())
	}
	sort.Slice(headers, func(i, j int) bool { return headers[i].MinTXID < headers[j].MinTXID })
	return headers
}
