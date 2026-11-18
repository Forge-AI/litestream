package litestream_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

const pageMapExample = "504d4150000000010000100000000003000000000000002f000000020000000009000000000000000100000000000000100000000000002328010000000000000011000000000000002f00000000000014500164d20b00920ede0b01b60ccb0c763f88310110bfc0"

func TestPageMap_EncodesTheWorkedExampleByteForByte(t *testing.T) {
	m := &litestream.PageMap{
		TXID:     0x2f,
		PageSize: 4096,
		Files: []*ltx.FileInfo{
			{Level: litestream.SnapshotLevel, MinTXID: 0x1, MaxTXID: 0x10, Size: 9000},
			{Level: 1, MinTXID: 0x11, MaxTXID: 0x2f, Size: 5200},
		},
		Pages: []litestream.PageMapEntry{
			{File: 1, Offset: 100, Size: 1490},
			{File: 0, Offset: 1810, Size: 1502},
			{File: 1, Offset: 1590, Size: 1611},
		},
	}
	if got := hex.EncodeToString(m.Encode()); got != pageMapExample {
		t.Fatalf("encoded:\n%s\nwant:\n%s", got, pageMapExample)
	}
	b, _ := hex.DecodeString(pageMapExample)
	decoded, err := litestream.DecodePageMap(b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, m) {
		t.Fatalf("decoded %+v, want %+v", decoded, m)
	}
	b[60] ^= 1
	if _, err := litestream.DecodePageMap(b); err == nil {
		t.Fatal("a map with a flipped byte decoded")
	}
}

func TestPageMap_EachMapPlacesEveryPageWhereARestoreReadsIt(t *testing.T) {
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
	if shrunk := rig.requireMap(t, ctx); len(shrunk.Pages) >= len(grown.Pages) {
		t.Fatalf("map after a vacuum holds %d pages, want fewer than %d", len(shrunk.Pages), len(grown.Pages))
	}
}

func TestPageMap_AWriterReadsOnlyTheFileItUploadedAndListsTheSameBehindOneCompactionOrTwenty(t *testing.T) {
	measure := func(t *testing.T, compactions int) (restarted []string, listed int) {
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
		for i := range compactions {
			insertHandoverRows(t, ctx, rig.sqldb, 11+i, 1)
			rig.ship(t, ctx)
			if _, err := rig.db.Compact(ctx, 1); err != nil {
				t.Fatal(err)
			}
		}
		rig.close(t)

		rig = openPageMapRig(t, ctx, dir)
		compact := func(id int) (*ltx.FileInfo, []string, int) {
			insertHandoverRows(t, ctx, rig.sqldb, id, 1)
			rig.ship(t, ctx)
			rig.client.taken()
			l1, err := rig.db.Compact(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			reads, listed := rig.client.taken()
			return l1, reads, listed
		}
		l1, reads, _ := compact(11 + compactions)
		uploaded := fmt.Sprintf("1/%s", ltx.FormatFilename(l1.MinTXID, l1.MaxTXID))
		for _, read := range reads {
			if strings.HasPrefix(read, "0/") {
				continue
			}
			if read != uploaded {
				t.Fatalf("the restarted writer read %s, want only the file it uploaded, %s", read, uploaded)
			}
			restarted = append(restarted, read)
		}
		_, _, listed = compact(12 + compactions)
		rig.requireMap(t, ctx)
		return restarted, listed
	}
	oneReads, oneListed := measure(t, 1)
	manyReads, manyListed := measure(t, 20)
	if len(oneReads) != len(manyReads) {
		t.Fatalf("a restarted writer made %d reads behind 1 compaction and %d behind 20, want them equal", len(oneReads), len(manyReads))
	}
	if oneListed != manyListed {
		t.Fatalf("a compaction listed %d files behind 1 compaction and %d behind 20, want them equal", oneListed, manyListed)
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

func TestPageMap_AnUnregisterCompactsTheLevelZeroTailSoTheMapReachesTheLastWriteBehindOneWriteOrAHundred(t *testing.T) {
	unregistered := func(t *testing.T, writes int) []string {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		r := registerPageMapDB(t, ctx)
		r.write(t, ctx, 1)
		r.write(t, ctx, 2)
		l1, err := r.db.Compact(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		var last ltx.TXID
		for i := range writes {
			last = r.write(t, ctx, 3+i)
		}
		if behind := r.requireMap(t, ctx); behind.TXID != l1.MaxTXID || behind.TXID >= last {
			t.Fatalf("before the unregister the map is at %s, want the level 1 compaction's %s behind the last write at %s", behind.TXID, l1.MaxTXID, last)
		}

		r.client.taken()
		if err := r.store.UnregisterDB(ctx, r.db.Path()); err != nil {
			t.Fatal(err)
		}
		reads, _ := r.client.taken()
		if m := r.requireMap(t, ctx); m.TXID != last {
			t.Fatalf("after the unregister of %d writes past level 1 the map is at %s, want the last write's %s", writes, m.TXID, last)
		}
		return reads
	}
	one := unregistered(t, 1)
	hundred := unregistered(t, 100)
	if len(one) != len(hundred) {
		t.Fatalf("an unregister read %d files behind 1 write past level 1 and %d behind 100, want them equal: %v and %v", len(one), len(hundred), one, hundred)
	}
}

func TestPageMap_AnUnregisterOfADatabaseItsSnapshotCoversCompactsAndMapsNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r := registerPageMapDB(t, ctx)
	r.write(t, ctx, 1)
	r.client.mu.Lock()
	written := r.client.mapWrites
	r.client.mu.Unlock()

	if err := r.store.UnregisterDB(ctx, r.db.Path()); err != nil {
		t.Fatal(err)
	}
	r.client.mu.Lock()
	rewritten := r.client.mapWrites - written
	r.client.mu.Unlock()
	itr, err := r.client.LTXFiles(ctx, 1, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	l1, err := ltx.SliceFileIterator(itr)
	if err != nil {
		t.Fatal(err)
	}
	if len(l1) != 0 || rewritten != 0 {
		t.Fatalf("an unregister whose snapshot holds the last write wrote %d level 1 files and the map %d times, want neither", len(l1), rewritten)
	}
}

type registeredPageMapDB struct {
	*pageMapRig
	store *litestream.Store
}

func registerPageMapDB(t *testing.T, ctx context.Context) *registeredPageMapDB {
	t.Helper()
	dir := t.TempDir()
	client := &pageMapRecorder{ReplicaClient: file.NewReplicaClient(filepath.Join(dir, "replica"))}
	store := litestream.NewStore(nil, litestream.CompactionLevels{{Level: 0}, {Level: 1, Interval: 24 * time.Hour}})
	db := litestream.NewDB(filepath.Join(dir, "db"))
	db.SyncDriven = true
	db.MonitorInterval = 0
	db.RetentionEnabled = false
	db.Replica = litestream.NewReplicaWithClient(db, client)
	db.Replica.MonitorEnabled = false
	if err := store.RegisterDB(db); err != nil {
		t.Fatal(err)
	}
	r := &registeredPageMapDB{pageMapRig: &pageMapRig{db: db, sqldb: openServerSQLDB(t, ctx, db.Path()), client: client}, store: store}
	t.Cleanup(func() {
		_ = r.sqldb.Close()
		_ = store.UnregisterDB(context.Background(), db.Path())
	})
	r.exec(t, ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, body BLOB)`)
	return r
}

func (r *registeredPageMapDB) write(t *testing.T, ctx context.Context, id int) ltx.TXID {
	t.Helper()
	insertHandoverRows(t, ctx, r.sqldb, id, 1)
	synced, err := r.store.SyncDB(ctx, r.db.Path(), true, false)
	if err != nil {
		t.Fatal(err)
	}
	return ltx.TXID(synced.TXID)
}

type pageMapRecorder struct {
	*file.ReplicaClient
	mu        sync.Mutex
	reads     []string
	listed    int
	mapWrites int
}

func (c *pageMapRecorder) WriteHead(ctx context.Context, key string, text []byte) error {
	if key == c.PageMapKey() {
		c.mu.Lock()
		c.mapWrites++
		c.mu.Unlock()
	}
	return c.ReplicaClient.WriteHead(ctx, key, text)
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
	c.reads, c.listed = nil, 0
	return reads, listed
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

func (r *pageMapRig) requireMap(t *testing.T, ctx context.Context) *litestream.PageMap {
	t.Helper()
	b, err := r.client.ReadHead(ctx, r.client.PageMapKey())
	if err != nil {
		t.Fatal(err)
	}
	m, err := litestream.DecodePageMap(b)
	if err != nil {
		t.Fatal(err)
	}

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
	if len(image) != len(m.Pages)*size {
		t.Fatalf("restore at %s is %d bytes, want the map's %d pages of %d", m.TXID, len(image), len(m.Pages), size)
	}
	for i, e := range m.Pages {
		pgno := uint32(i + 1)
		if e.Size == 0 {
			t.Fatalf("page %d has no place in the map at %s", pgno, m.TXID)
		}
		f := m.Files[e.File]
		hdr, data, err := litestream.FetchPage(ctx, r.client, f.Level, f.MinTXID, f.MaxTXID, e.Offset, e.Size)
		if err != nil {
			t.Fatal(err)
		}
		if hdr.Pgno != pgno || !bytes.Equal(data, image[i*size:(i+1)*size]) {
			t.Fatalf("the map places page %d at %d/%s offset %d, whose frame holds page %d with other bytes than the restore", pgno, f.Level, ltx.FormatFilename(f.MinTXID, f.MaxTXID), e.Offset, hdr.Pgno)
		}
	}
	return m
}
