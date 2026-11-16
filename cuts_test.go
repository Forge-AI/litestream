package litestream

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

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

	writePendingCut(t, db, "child", walEndOf(t, db))
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	if _, maxTXID, err := db.MaxLTX(); err != nil {
		t.Fatal(err)
	} else if maxTXID != synced.TXID {
		t.Fatalf("max txid=%s, want %s: a cut at the synced offset writes no file", maxTXID, synced.TXID)
	}
	if got, want := readCut(t, db, "child"), landedCutJSON(synced); got != want {
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

func l0FileAt(t *testing.T, db *DB, txid ltx.TXID) (walPosition, ltx.Pos) {
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
	return walPosition{Salt1: hdr.WALSalt1, Salt2: hdr.WALSalt2, Offset: hdr.WALOffset + hdr.WALSize}, dec.PostApplyPos()
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

func landedCutJSON(pos ltx.Pos) string {
	return fmt.Sprintf(`{"txid":%d,"checksum":"%s"}`, uint64(pos.TXID), pos.PostApplyChecksum)
}
