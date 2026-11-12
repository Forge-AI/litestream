package litestream_test

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"testing"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestDB_VerifyReadsOnlyTheLastSyncedPageOfTheLastLTXFile(t *testing.T) {
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)
	ctx := t.Context()

	if _, err := sqldb.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)`); err != nil {
		t.Fatal(err)
	}
	for range 64 {
		if _, err := sqldb.ExecContext(ctx, `INSERT INTO t (data) VALUES (randomblob(1024))`); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	}

	minTXID, maxTXID, err := db.MaxLTX()
	if err != nil {
		t.Fatal(err)
	}
	ltxFile, err := os.OpenFile(db.LTXPath(0, minTXID, maxTXID), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ltxFile.Close() }()
	dec := ltx.NewDecoder(ltxFile)
	if err := dec.DecodeHeader(); err != nil {
		t.Fatal(err)
	}
	hdr := dec.Header()
	frameSize := int64(hdr.PageSize) + litestream.WALFrameHeaderSize
	wal, err := os.Open(db.WALPath())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = wal.Close() }()
	var pgno [4]byte
	if _, err := wal.ReadAt(pgno[:], hdr.WALOffset+hdr.WALSize-frameSize); err != nil {
		t.Fatal(err)
	}
	synced := binary.BigEndian.Uint32(pgno[:])
	index := localPageIndex(t, ltxFile)
	target, ok := index[synced]
	if !ok {
		t.Fatalf("the snapshot's %d pages leave out the last synced page %d", len(index), synced)
	}
	earlier := 0
	for page, elem := range index {
		if page == synced {
			continue
		}
		if elem.Offset < target.Offset {
			earlier++
		}
		if _, err := ltxFile.WriteAt(make([]byte, elem.Size), elem.Offset); err != nil {
			t.Fatal(err)
		}
	}
	if earlier == 0 {
		t.Fatalf("the last synced page %d is the snapshot's first, so a scan never passes another page", synced)
	}

	reader, err := sqldb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Rollback() }()
	var rows int
	if err := reader.QueryRowContext(ctx, `SELECT count(*) FROM t`).Scan(&rows); err != nil {
		t.Fatal(err)
	}

	if err := db.Sync(ctx); err != nil {
		t.Fatalf("a sync read LTX pages besides the last synced one: %v", err)
	}
	if err := db.Checkpoint(ctx); err != nil {
		t.Fatalf("a passive checkpoint read LTX pages besides the last synced one: %v", err)
	}
	pos, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	}
	if pos.TXID != snapshot.TXID {
		t.Fatalf("txid=%s, want %s: a verify that scanned the snapshot past the synced page snapshotted again", pos.TXID, snapshot.TXID)
	}
}

func localPageIndex(t *testing.T, f *os.File) map[uint32]ltx.PageIndexElem {
	t.Helper()
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	footer := fi.Size() - ltx.TrailerSize - 8
	var size [8]byte
	if _, err := f.ReadAt(size[:], footer); err != nil {
		t.Fatal(err)
	}
	start := footer - int64(binary.BigEndian.Uint64(size[:]))
	index, err := ltx.DecodePageIndex(bufio.NewReader(io.NewSectionReader(f, start, footer+8-start)), 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	return index
}
