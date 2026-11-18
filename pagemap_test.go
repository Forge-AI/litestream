package litestream_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestPageMap_EveryPageResolvesNewestSectionFirstToTheFrameARestoreReads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rig := openPageMapRig(t, ctx, t.TempDir())
	rig.exec(t, ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, body BLOB)`)
	insertHandoverRows(t, ctx, rig.sqldb, 1, 40)
	rig.ship(t, ctx)
	snapshot, err := rig.db.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if m := rig.requireMap(t, ctx); m.TXID != snapshot.MaxTXID || len(m.Files) != 1 {
		t.Fatalf("map after the snapshot is at %s over %d files, want %s over the snapshot alone", m.TXID, len(m.Files), snapshot.MaxTXID)
	}

	insertHandoverRows(t, ctx, rig.sqldb, 41, 20)
	rig.exec(t, ctx, `UPDATE t SET body = randomblob(4000) WHERE id % 7 = 0`)
	rig.ship(t, ctx)
	l1, err := rig.db.Compact(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	grown := rig.requireMap(t, ctx)
	if grown.TXID != l1.MaxTXID || len(grown.Files) != 2 {
		t.Fatalf("map after the compaction is at %s over %d files, want %s over the snapshot and the level 1 file", grown.TXID, len(grown.Files), l1.MaxTXID)
	}

	rig.exec(t, ctx, `DELETE FROM t WHERE id > 10`)
	rig.exec(t, ctx, `VACUUM`)
	rig.ship(t, ctx)
	if _, err := rig.db.Compact(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if shrunk := rig.requireMap(t, ctx); shrunk.Commit >= grown.Commit || len(shrunk.Files) != 3 {
		t.Fatalf("map after a vacuum holds %d pages over %d files, want fewer than %d over 3", shrunk.Commit, len(shrunk.Files), grown.Commit)
	}
}

func TestPageMap_AWriteReadsOnlyTheFileItUploadedAndMakesTheSameRequestsBehindOneCompactionOrTwentyAtAnySize(t *testing.T) {
	type requests struct {
		reads, listed, mapWrites, mapReads, deletes int
	}
	measure := func(t *testing.T, compactions, rows int) requests {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		rig := openPageMapRig(t, ctx, t.TempDir())
		rig.exec(t, ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, body BLOB)`)
		rig.exec(t, ctx, fmt.Sprintf(`WITH RECURSIVE n(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM n WHERE id < %d) INSERT INTO t (id, body) SELECT id, randomblob(4000) FROM n`, rows))
		rig.ship(t, ctx)
		if _, err := rig.db.Snapshot(ctx); err != nil {
			t.Fatal(err)
		}
		for i := range compactions {
			insertHandoverRows(t, ctx, rig.sqldb, rows+1+i, 1)
			rig.ship(t, ctx)
			if _, err := rig.db.Compact(ctx, 1); err != nil {
				t.Fatal(err)
			}
		}
		insertHandoverRows(t, ctx, rig.sqldb, rows+1+compactions, 1)
		rig.ship(t, ctx)
		rig.client.taken()
		l1, err := rig.db.Compact(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		mapWrites, mapReads, deletes := rig.client.heads()
		reads, listed := rig.client.taken()

		got := requests{listed: listed, mapWrites: mapWrites, mapReads: mapReads, deletes: len(deletes)}
		uploaded := fmt.Sprintf("1/%s", ltx.FormatFilename(l1.MinTXID, l1.MaxTXID))
		for _, read := range reads {
			if strings.HasPrefix(read, "0/") {
				continue
			}
			if read != uploaded {
				t.Fatalf("a write behind %d compactions of %d rows read %s, want only the file it uploaded, %s", compactions, rows, read, uploaded)
			}
			got.reads++
		}
		if m := rig.requireMap(t, ctx); len(m.Files) != compactions+2 {
			t.Fatalf("map behind %d compactions spans %d files, want the snapshot and %d level 1 files", compactions, len(m.Files), compactions+1)
		}
		return got
	}
	small := measure(t, 1, 256)
	if want := (requests{reads: 2, listed: small.listed, mapWrites: 1}); small != want {
		t.Fatalf("a write of a 1 MiB database made %+v, want its uploaded file's page index and header read and one put, %+v", small, want)
	}
	if deep := measure(t, 20, 256); deep != small {
		t.Fatalf("a write made %+v behind 20 compactions and %+v behind 1, want them equal", deep, small)
	}
	if large := measure(t, 1, 8192); large != small {
		t.Fatalf("a write made %+v at 32 MiB and %+v at 1 MiB, want them equal", large, small)
	}
}

func TestPageMap_ALaneThatWroteAMapHoldsNoStatePerPageOrPerRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rig := openPageMapRig(t, ctx, t.TempDir())
	rig.exec(t, ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, body BLOB)`)
	insertHandoverRows(t, ctx, rig.sqldb, 1, 40)
	rig.ship(t, ctx)
	if _, err := rig.db.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
	insertHandoverRows(t, ctx, rig.sqldb, 41, 1)
	rig.ship(t, ctx)
	if _, err := rig.db.Compact(ctx, 1); err != nil {
		t.Fatal(err)
	}
	rig.requireMap(t, ctx)

	var walk func(v reflect.Value, path string)
	walk = func(v reflect.Value, path string) {
		switch v.Kind() {
		case reflect.Struct:
			for i := range v.NumField() {
				walk(v.Field(i), path+"."+v.Type().Field(i).Name)
			}
		case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		default:
			t.Errorf("%s is a %s, which can hold state per page or per run", path, v.Kind())
		}
	}
	walk(reflect.ValueOf(rig.db).Elem().FieldByName("pageMap"), "pageMap")
}

func TestPageMap_ARestartContinuesFromTheLocalCopyAndALostCopyCostsOneBucketRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dir := t.TempDir()
	rig := openPageMapRig(t, ctx, dir)
	rig.exec(t, ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, body BLOB)`)
	insertHandoverRows(t, ctx, rig.sqldb, 1, 10)
	rig.ship(t, ctx)
	if _, err := rig.db.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
	insertHandoverRows(t, ctx, rig.sqldb, 11, 1)
	rig.ship(t, ctx)
	if _, err := rig.db.Compact(ctx, 1); err != nil {
		t.Fatal(err)
	}
	rig.requireMap(t, ctx)
	local := filepath.Join(rig.db.MetaPath(), litestream.PageMapName)
	rig.close(t)

	restart := func(id int) int {
		rig := openPageMapRig(t, ctx, dir)
		defer rig.close(t)
		insertHandoverRows(t, ctx, rig.sqldb, id, 1)
		rig.ship(t, ctx)
		rig.client.taken()
		l1, err := rig.db.Compact(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		_, mapReads, _ := rig.client.heads()
		reads, _ := rig.client.taken()
		uploaded := fmt.Sprintf("1/%s", ltx.FormatFilename(l1.MinTXID, l1.MaxTXID))
		for _, read := range reads {
			if !strings.HasPrefix(read, "0/") && read != uploaded {
				t.Fatalf("the restarted lane read %s, want only the file it uploaded, %s", read, uploaded)
			}
		}
		if m := rig.requireMap(t, ctx); m.TXID != l1.MaxTXID {
			t.Fatalf("map after the restarted lane's compaction is at %s, want %s", m.TXID, l1.MaxTXID)
		}
		return mapReads
	}
	if n := restart(12); n != 0 {
		t.Fatalf("a restart with its local copy read the bucket's map %d times, want 0", n)
	}
	if err := os.Remove(local); err != nil {
		t.Fatal(err)
	}
	if n := restart(13); n != 1 {
		t.Fatalf("a restart that lost its local copy read the bucket's map %d times, want 1", n)
	}
}

func TestPageMap_ALocalCopyWithAFlippedByteIsReplacedFromTheBucketAndNeverRepublished(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rig := openPageMapRig(t, ctx, t.TempDir())
	rig.exec(t, ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, body BLOB)`)
	insertHandoverRows(t, ctx, rig.sqldb, 1, 10)
	rig.ship(t, ctx)
	if _, err := rig.db.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
	insertHandoverRows(t, ctx, rig.sqldb, 11, 1)
	rig.ship(t, ctx)
	if _, err := rig.db.Compact(ctx, 1); err != nil {
		t.Fatal(err)
	}
	built := rig.requireMap(t, ctx)

	local := filepath.Join(rig.db.MetaPath(), litestream.PageMapName)
	b, err := os.ReadFile(local)
	if err != nil {
		t.Fatal(err)
	}
	b[litestream.PageMapHeaderSize+len(built.Files)*litestream.PageMapFileSize+8] ^= 1
	if err := os.WriteFile(local, b, 0o600); err != nil {
		t.Fatal(err)
	}

	insertHandoverRows(t, ctx, rig.sqldb, 12, 1)
	rig.ship(t, ctx)
	rig.client.taken()
	l1, err := rig.db.Compact(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, mapReads, _ := rig.client.heads()
	reads, _ := rig.client.taken()
	uploaded := fmt.Sprintf("1/%s", ltx.FormatFilename(l1.MinTXID, l1.MaxTXID))
	for _, read := range reads {
		if !strings.HasPrefix(read, "0/") && read != uploaded {
			t.Fatalf("the write after a flipped byte read %s, want only the file it uploaded, %s", read, uploaded)
		}
	}
	if mapReads != 1 {
		t.Fatalf("the write after a flipped byte read the bucket's map %d times, want 1", mapReads)
	}
	if m := rig.requireMap(t, ctx); len(m.Files) != len(built.Files)+1 || !reflect.DeepEqual(m.Files[:len(built.Files)], built.Files) {
		t.Fatalf("the sections after a flipped byte in the local copy are %+v, want the ones built before it, %+v", m.Files, built.Files)
	}
}

func TestPageMap_EachProcessDeletesTheRetiredMapPmapOnItsFirstWriteAndRetriesAFailedDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dir := t.TempDir()
	rig := openPageMapRig(t, ctx, dir)
	retired := rig.client.ObjectKey("map.pmap")
	plant := func() {
		if err := rig.client.WriteHead(ctx, retired, []byte("PMAP")); err != nil {
			t.Fatal(err)
		}
	}
	retiredExists := func() bool {
		_, err := rig.client.ReadHead(ctx, retired)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		return err == nil
	}
	write := func(id int) ([]string, error) {
		insertHandoverRows(t, ctx, rig.sqldb, id, 1)
		rig.ship(t, ctx)
		rig.client.taken()
		_, err := rig.db.Compact(ctx, 1)
		_, _, deletes := rig.client.heads()
		return deletes, err
	}
	restart := func() {
		rig.close(t)
		rig = openPageMapRig(t, ctx, dir)
	}

	plant()
	rig.exec(t, ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, body BLOB)`)
	insertHandoverRows(t, ctx, rig.sqldb, 1, 10)
	rig.ship(t, ctx)
	rig.client.taken()
	if _, err := rig.db.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, deletes := rig.client.heads(); !slices.Equal(deletes, []string{retired}) || retiredExists() {
		t.Fatalf("the first write deleted %q, want only %s, and it gone", deletes, retired)
	}

	plant()
	if deletes, err := write(11); err != nil || len(deletes) != 0 || !retiredExists() {
		t.Fatalf("a later write in the same process deleted %q (%v), want nothing", deletes, err)
	}

	restart()
	if deletes, err := write(12); err != nil || !slices.Equal(deletes, []string{retired}) || retiredExists() {
		t.Fatalf("the first write after a restart with a local copy deleted %q (%v), want only %s, and it gone", deletes, err, retired)
	}

	restart()
	refused := errors.New("the bucket refused the delete")
	rig.client.mu.Lock()
	rig.client.failDelete = refused
	rig.client.mu.Unlock()
	if _, err := write(13); !errors.Is(err, refused) {
		t.Fatalf("a write whose delete failed returned %v, want %v", err, refused)
	}
	if mapWrites, _, _ := rig.client.heads(); mapWrites != 0 {
		t.Fatalf("a write whose delete failed uploaded the map %d times, want 0", mapWrites)
	}
	rig.client.mu.Lock()
	rig.client.failDelete = nil
	rig.client.mu.Unlock()
	plant()
	if deletes, err := write(14); err != nil || !slices.Equal(deletes, []string{retired}) || retiredExists() {
		t.Fatalf("the write after a failed delete deleted %q (%v), want a retry of %s", deletes, err, retired)
	}
	if deletes, err := write(15); err != nil || len(deletes) != 0 {
		t.Fatalf("the write after the retried delete deleted %q (%v), want nothing", deletes, err)
	}
	rig.requireMap(t, ctx)
}

func TestPageMap_AFailedUploadLeavesTheLocalCopyAndTheBucketMapUnchanged(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rig := openPageMapRig(t, ctx, t.TempDir())
	rig.exec(t, ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, body BLOB)`)
	insertHandoverRows(t, ctx, rig.sqldb, 1, 10)
	rig.ship(t, ctx)
	if _, err := rig.db.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
	insertHandoverRows(t, ctx, rig.sqldb, 11, 1)
	rig.ship(t, ctx)
	if _, err := rig.db.Compact(ctx, 1); err != nil {
		t.Fatal(err)
	}
	rig.requireMap(t, ctx)
	maps := func() (local, bucket []byte) {
		local, err := os.ReadFile(filepath.Join(rig.db.MetaPath(), litestream.PageMapName))
		if err != nil {
			t.Fatal(err)
		}
		if bucket, err = rig.client.ReadHead(ctx, rig.client.ObjectKey(litestream.PageMapName)); err != nil {
			t.Fatal(err)
		}
		return local, bucket
	}
	local, bucket := maps()

	refused := errors.New("the bucket refused the put")
	rig.client.mu.Lock()
	rig.client.failMap = refused
	rig.client.mu.Unlock()
	insertHandoverRows(t, ctx, rig.sqldb, 12, 1)
	rig.ship(t, ctx)
	if _, err := rig.db.Compact(ctx, 1); !errors.Is(err, refused) {
		t.Fatalf("a compaction whose map upload failed returned %v, want %v", err, refused)
	}
	if afterLocal, afterBucket := maps(); !bytes.Equal(afterLocal, local) || !bytes.Equal(afterBucket, bucket) {
		t.Fatal("a failed upload changed the local copy or the bucket's map")
	}
	if _, err := os.Stat(filepath.Join(rig.db.MetaPath(), litestream.PageMapName+".tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat the failed write's temp file: %v, want it removed", err)
	}

	rig.client.mu.Lock()
	rig.client.failMap = nil
	rig.client.mu.Unlock()
	insertHandoverRows(t, ctx, rig.sqldb, 13, 1)
	rig.ship(t, ctx)
	l1, err := rig.db.Compact(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if m := rig.requireMap(t, ctx); m.TXID != l1.MaxTXID {
		t.Fatalf("map after the failed upload's next write is at %s, want %s", m.TXID, l1.MaxTXID)
	}
}

func TestPageMap_EveryCompactionAndSnapshotUploadRewritesTheMapWithoutMovingItBack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rig := openPageMapRig(t, ctx, t.TempDir())
	rig.exec(t, ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, body BLOB)`)
	insertHandoverRows(t, ctx, rig.sqldb, 1, 10)
	rig.ship(t, ctx)
	if _, err := rig.db.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		insertHandoverRows(t, ctx, rig.sqldb, 11+i, 1)
		rig.ship(t, ctx)
		if _, err := rig.db.Compact(ctx, 1); err != nil {
			t.Fatal(err)
		}
	}
	insertHandoverRows(t, ctx, rig.sqldb, 13, 1)
	rig.ship(t, ctx)
	snapshot, err := rig.db.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	l2, err := rig.db.Compact(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if l2.MaxTXID >= snapshot.MaxTXID {
		t.Fatalf("the level 2 compaction ends at %s, want it behind the snapshot at %s", l2.MaxTXID, snapshot.MaxTXID)
	}
	rig.client.mu.Lock()
	writes := rig.client.mapWrites
	rig.client.mu.Unlock()
	if writes != 5 {
		t.Fatalf("5 uploads (2 snapshots, 2 level 1 compactions, 1 level 2 compaction) wrote the map %d times, want 5", writes)
	}
	if m := rig.requireMap(t, ctx); m.TXID != snapshot.MaxTXID {
		t.Fatalf("map after the level 2 compaction is at %s, want the snapshot's %s", m.TXID, snapshot.MaxTXID)
	}
}

type pageMapRecorder struct {
	*file.ReplicaClient
	mu         sync.Mutex
	reads      []string
	listed     int
	mapWrites  int
	mapReads   int
	deletes    []string
	failMap    error
	failDelete error
}

func (c *pageMapRecorder) WriteHeadFrom(ctx context.Context, key string, r *io.SectionReader) error {
	if key == c.ObjectKey(litestream.PageMapName) {
		c.mu.Lock()
		c.mapWrites++
		fail := c.failMap
		c.mu.Unlock()
		if fail != nil {
			return fail
		}
	}
	return c.ReplicaClient.WriteHeadFrom(ctx, key, r)
}

func (c *pageMapRecorder) ReadHead(ctx context.Context, key string) ([]byte, error) {
	if key == c.ObjectKey(litestream.PageMapName) {
		c.mu.Lock()
		c.mapReads++
		c.mu.Unlock()
	}
	return c.ReplicaClient.ReadHead(ctx, key)
}

func (c *pageMapRecorder) DeleteHead(ctx context.Context, key string) error {
	c.mu.Lock()
	c.deletes = append(c.deletes, key)
	fail := c.failDelete
	c.mu.Unlock()
	if fail != nil {
		return fail
	}
	return c.ReplicaClient.DeleteHead(ctx, key)
}

func (c *pageMapRecorder) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	itr, err := c.ReplicaClient.LTXFiles(ctx, level, seek, useMetadata)
	if err != nil {
		return nil, err
	}
	infos, err := ltx.SliceFileIterator(itr)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.listed += len(infos)
	c.mu.Unlock()
	return ltx.NewFileInfoSliceIterator(infos), nil
}

func (c *pageMapRecorder) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	c.mu.Lock()
	c.reads = append(c.reads, fmt.Sprintf("%d/%s", level, ltx.FormatFilename(minTXID, maxTXID)))
	c.mu.Unlock()
	return c.ReplicaClient.OpenLTXFile(ctx, level, minTXID, maxTXID, offset, size)
}

func (c *pageMapRecorder) taken() ([]string, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	reads, listed := c.reads, c.listed
	c.reads, c.listed, c.mapWrites, c.mapReads, c.deletes = nil, 0, 0, 0, nil
	return reads, listed
}

func (c *pageMapRecorder) heads() (mapWrites, mapReads int, deletes []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mapWrites, c.mapReads, c.deletes
}

type pageMapRig struct {
	db     *litestream.DB
	sqldb  *sql.DB
	client *pageMapRecorder
}

func openPageMapRig(t *testing.T, ctx context.Context, dir string) *pageMapRig {
	t.Helper()
	client := &pageMapRecorder{ReplicaClient: file.NewReplicaClient(filepath.Join(dir, "replica"))}
	db := testingutil.NewDB(t, filepath.Join(dir, "db"))
	db.MonitorInterval = 0
	db.ShutdownSyncTimeout = 0
	db.L0Retention = 0
	db.Replica = litestream.NewReplicaWithClient(db, client)
	db.Replica.MonitorEnabled = false
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	rig := &pageMapRig{db: db, sqldb: openServerSQLDB(t, ctx, db.Path()), client: client}
	t.Cleanup(func() { rig.close(t) })
	return rig
}

func (r *pageMapRig) close(t *testing.T) {
	t.Helper()
	if r.sqldb == nil {
		return
	}
	if err := r.sqldb.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.db.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.sqldb = nil
}

func (r *pageMapRig) exec(t *testing.T, ctx context.Context, query string) {
	t.Helper()
	if _, err := r.sqldb.ExecContext(ctx, query); err != nil {
		t.Fatal(err)
	}
}

func (r *pageMapRig) ship(t *testing.T, ctx context.Context) {
	t.Helper()
	if err := r.db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.db.Replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}
}

type pageMap struct {
	TXID     ltx.TXID
	PageSize uint32
	Commit   uint32
	Files    []pageMapFile
}

type pageMapFile struct {
	Level    int
	Min, Max ltx.TXID
	Size     int64
	Runs     []pageMapRun
}

type pageMapRun struct {
	First, Pages uint32
	Size, Offset int64
}

func decodePageMap(t *testing.T, b []byte) *pageMap {
	t.Helper()
	if len(b) < litestream.PageMapHeaderSize+litestream.PageMapTrailerSize {
		t.Fatalf("page map of %d bytes is shorter than its header and trailer", len(b))
	}
	body := b[:len(b)-litestream.PageMapTrailerSize]
	if got, want := binary.LittleEndian.Uint64(b[len(body):]), crc64.Checksum(body, crc64.MakeTable(crc64.ISO)); got != want {
		t.Fatalf("page map checksum %x, want %x", got, want)
	}
	if string(b[:4]) != litestream.PageMapMagic || binary.LittleEndian.Uint32(b[4:]) != litestream.PageMapVersion || binary.LittleEndian.Uint32(b[28:]) != 0 {
		t.Fatalf("page map header %x, want magic %s, version %d and a zero reserved word", b[:litestream.PageMapHeaderSize], litestream.PageMapMagic, litestream.PageMapVersion)
	}
	m := &pageMap{
		PageSize: binary.LittleEndian.Uint32(b[8:]),
		Commit:   binary.LittleEndian.Uint32(b[12:]),
		TXID:     ltx.TXID(binary.LittleEndian.Uint64(b[16:])),
		Files:    make([]pageMapFile, binary.LittleEndian.Uint32(b[24:])),
	}
	table := body[litestream.PageMapHeaderSize:]
	if len(table) < len(m.Files)*litestream.PageMapFileSize {
		t.Fatalf("page map file table of %d files is cut short", len(m.Files))
	}
	runs := table[len(m.Files)*litestream.PageMapFileSize:]
	for i := range m.Files {
		e := table[i*litestream.PageMapFileSize:]
		f := pageMapFile{
			Level: int(e[0]),
			Min:   ltx.TXID(binary.LittleEndian.Uint64(e[1:])),
			Max:   ltx.TXID(binary.LittleEndian.Uint64(e[9:])),
			Size:  int64(binary.LittleEndian.Uint64(e[17:])),
		}
		for range binary.LittleEndian.Uint64(e[25:]) {
			if len(runs) < litestream.PageMapRunSize {
				t.Fatalf("section of file %d is cut short", i)
			}
			v := binary.LittleEndian.Uint32(runs[4:])
			run := pageMapRun{First: binary.LittleEndian.Uint32(runs), Pages: v&0xff + 1, Size: int64(v >> 8), Offset: int64(binary.LittleEndian.Uint64(runs[8:]))}
			if run.First == 0 || run.Size == 0 || (run.First-1)/256 != (run.First+run.Pages-2)/256 {
				t.Fatalf("file %d holds run %+v, want a run of frames inside one 256-page segment", i, run)
			}
			if n := len(f.Runs); n > 0 && f.Runs[n-1].First+f.Runs[n-1].Pages > run.First {
				t.Fatalf("file %d holds run %+v after %+v, want runs sorted by first page with no overlap", i, run, f.Runs[n-1])
			}
			f.Runs = append(f.Runs, run)
			runs = runs[litestream.PageMapRunSize:]
		}
		m.Files[i] = f
	}
	if len(runs) != 0 {
		t.Fatalf("page map has %d bytes past its last section", len(runs))
	}
	return m
}

func (r *pageMapRig) requireMap(t *testing.T, ctx context.Context) *pageMap {
	t.Helper()
	b, err := r.client.ReadHead(ctx, r.client.ObjectKey(litestream.PageMapName))
	if err != nil {
		t.Fatal(err)
	}
	local, err := os.ReadFile(filepath.Join(r.db.MetaPath(), litestream.PageMapName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(local, b) {
		t.Fatalf("the local copy of %d bytes differs from the bucket's map of %d", len(local), len(b))
	}
	m := decodePageMap(t, b)

	opt := litestream.NewRestoreOptions()
	opt.OutputPath = filepath.Join(t.TempDir(), "restored")
	opt.TXID = m.TXID
	if err := r.db.Replica.Restore(ctx, opt); err != nil {
		t.Fatal(err)
	}
	image, err := os.ReadFile(opt.OutputPath)
	if err != nil {
		t.Fatal(err)
	}
	size := int(m.PageSize)
	if len(image) != int(m.Commit)*size {
		t.Fatalf("restore at %s is %d bytes, want the map's %d pages of %d", m.TXID, len(image), m.Commit, size)
	}

	frames := make(map[[2]int]map[uint32][]byte)
	for pgno := uint32(1); pgno <= m.Commit; pgno++ {
		if pgno == ltx.LockPgno(m.PageSize) {
			continue
		}
		at := [2]int{-1, -1}
		for i := len(m.Files) - 1; i >= 0 && at[0] < 0; i-- {
			j, found := slices.BinarySearchFunc(m.Files[i].Runs, pgno, func(run pageMapRun, pgno uint32) int {
				switch {
				case pgno < run.First:
					return 1
				case pgno >= run.First+run.Pages:
					return -1
				}
				return 0
			})
			if found {
				at = [2]int{i, j}
			}
		}
		if at[0] < 0 {
			t.Fatalf("page %d has no section in the map at %s", pgno, m.TXID)
		}
		f, run := m.Files[at[0]], m.Files[at[0]].Runs[at[1]]
		if frames[at] == nil {
			frames[at] = runFrames(t, ctx, r.client, f, run)
		}
		if !bytes.Equal(frames[at][pgno], image[int(pgno-1)*size:int(pgno)*size]) {
			t.Fatalf("the map places page %d in the run %+v of %d/%s, whose frame holds other bytes than the restore", pgno, run, f.Level, ltx.FormatFilename(f.Min, f.Max))
		}
	}
	return m
}

func runFrames(t *testing.T, ctx context.Context, client litestream.ReplicaClient, f pageMapFile, run pageMapRun) map[uint32][]byte {
	t.Helper()
	rc, err := client.OpenLTXFile(ctx, f.Level, f.Min, f.Max, run.Offset, run.Size)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(b)) != run.Size {
		t.Fatalf("run %+v of %d/%s read %d bytes", run, f.Level, ltx.FormatFilename(f.Min, f.Max), len(b))
	}
	frames := make(map[uint32][]byte, run.Pages)
	for off, pgno := 0, run.First; off < len(b); pgno++ {
		if len(b)-off < ltx.PageHeaderSize+4 {
			t.Fatalf("run %+v of %d/%s ends inside a frame header", run, f.Level, ltx.FormatFilename(f.Min, f.Max))
		}
		n := ltx.PageHeaderSize + 4 + int(binary.BigEndian.Uint32(b[off+ltx.PageHeaderSize:]))
		hdr, data, err := ltx.DecodePageData(b[off:min(off+n, len(b))])
		if err != nil || hdr.Pgno != pgno || hdr.Flags&ltx.PageHeaderFlagSize == 0 {
			t.Fatalf("frame %d of run %+v of %d/%s holds page %d with flags %x (%v), want page %d", len(frames), run, f.Level, ltx.FormatFilename(f.Min, f.Max), hdr.Pgno, hdr.Flags, err, pgno)
		}
		frames[pgno] = data
		off += n
	}
	if len(frames) != int(run.Pages) {
		t.Fatalf("run %+v of %d/%s holds %d frames", run, f.Level, ltx.FormatFilename(f.Min, f.Max), len(frames))
	}
	return frames
}
