package litestream_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestDB_BaseTXIDContinuesAChildWhoseReplicaHoldsNoFile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dir := t.TempDir()

	parentPath := filepath.Join(dir, "parent")
	parent := openHandoverPublisher(t, parentPath, file.NewReplicaClient(filepath.Join(dir, "parent-replica")))
	sqldb := openServerSQLDB(t, ctx, parentPath)
	if _, err := sqldb.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, body BLOB)`); err != nil {
		t.Fatal(err)
	}
	insertHandoverRows(t, ctx, sqldb, 1, handoverRows)
	if err := parent.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := parent.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	testingutil.MustCloseSQLDB(t, sqldb)
	headMin, base, err := parent.MaxLTX()
	if err != nil {
		t.Fatal(err)
	}

	childPath := filepath.Join(dir, "child")
	copyFile(t, parentPath, childPath)
	child := testingutil.NewDB(t, childPath)
	if err := os.MkdirAll(child.LTXLevelDir(0), 0o755); err != nil {
		t.Fatal(err)
	}
	copyFile(t, parent.LTXPath(0, headMin, base), child.LTXPath(0, headMin, base))
	client := file.NewReplicaClient(filepath.Join(dir, "child-replica"))
	child.MonitorInterval = 0
	child.ShutdownSyncTimeout = 0
	child.BaseTXID = base
	child.Replica = litestream.NewReplicaWithClient(child, client)
	child.Replica.MonitorEnabled = false
	child.SetHandover(litestream.Handover{TXID: base})
	if err := child.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = child.Close(context.Background()) })

	proof, err := child.ProveHandover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !proof.Continued || proof.TXID != uint64(base) {
		t.Fatalf("a child at its base's TXID %s must continue: %+v", base, proof)
	}
	store := litestream.NewStore(nil, nil)
	due := &litestream.CompactionLevel{Level: litestream.SnapshotLevel, Interval: time.Nanosecond}
	if _, err := store.CompactDB(ctx, child, due); !errors.Is(err, litestream.ErrNoCompaction) {
		t.Fatalf("a child at its base's TXID %s holds its snapshot through the base, so it must snapshot nothing: %v", base, err)
	}
	childSQL := openServerSQLDB(t, ctx, childPath)
	t.Cleanup(func() { _ = childSQL.Close() })
	insertHandoverRows(t, ctx, childSQL, handoverRows+1, 4)
	if err := child.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := child.Replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompactDB(ctx, child, store.SnapshotLevel()); !errors.Is(err, litestream.ErrCompactionTooEarly) {
		t.Fatalf("a child past its base takes its first snapshot one snapshot interval after it opened on that base, as if the base were its snapshot: %v", err)
	}

	itr, err := client.LTXFiles(ctx, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	files, err := ltx.SliceFileIterator(itr)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].MinTXID != base+1 || files[0].MaxTXID != base+1 {
		t.Fatalf("the child must ship its own write alone, at %s: %v", base+1, files)
	}
	if _, err := child.Compact(ctx, 1); err != nil {
		t.Fatalf("a child with no snapshot of its own must compact: %v", err)
	}
}

func copyFile(t *testing.T, from, to string) {
	t.Helper()
	b, err := os.ReadFile(from)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(to, b, 0o600); err != nil {
		t.Fatal(err)
	}
}
