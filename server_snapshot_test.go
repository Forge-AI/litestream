package litestream_test

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
)

func TestServer_SyncSnapshotLeavesOneFileToRestoreAtAnyWriteCount(t *testing.T) {
	for _, writes := range []int{1, 100} {
		t.Run(strconv.Itoa(writes), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			db, sqldb, client, syncs := serveSnapshotSyncs(t, ctx)
			for id := 1; id <= writes; id++ {
				insertHandoverRows(t, ctx, sqldb, id, 1)
				mustSync(t, syncs, litestream.SyncRequest{Path: db.Path(), Wait: true})
			}
			if plan := latestRestorePlan(t, ctx, client); len(plan) < writes {
				t.Fatalf("restore plan before the snapshot holds %d files after %d synced writes, want one or more per write", len(plan), writes)
			}

			synced := mustSync(t, syncs, litestream.SyncRequest{Path: db.Path(), Wait: true, Snapshot: true})

			if synced.SnapshotTXID != synced.TXID {
				t.Fatalf("snapshot_txid=%d, want the synced txid %d", synced.SnapshotTXID, synced.TXID)
			}
			if plan := latestRestorePlan(t, ctx, client); len(plan) != 1 || plan[0].Level != litestream.SnapshotLevel {
				t.Fatalf("restore plan after %d writes holds %d files, want the one snapshot", writes, len(plan))
			}
		})
	}
}

func TestServer_SyncSnapshotOfAnUnchangedDatabaseWritesNoSecondSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, sqldb, client, syncs := serveSnapshotSyncs(t, ctx)
	insertHandoverRows(t, ctx, sqldb, 1, 1)
	first := mustSync(t, syncs, litestream.SyncRequest{Path: db.Path(), Wait: true, Snapshot: true})

	again := mustSync(t, syncs, litestream.SyncRequest{Path: db.Path(), Wait: true, Snapshot: true})

	if again.SnapshotTXID != first.SnapshotTXID {
		t.Fatalf("snapshot_txid moved from %d to %d with no write", first.SnapshotTXID, again.SnapshotTXID)
	}
	itr, err := client.LTXFiles(ctx, litestream.SnapshotLevel, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	var snapshots int
	for itr.Next() {
		snapshots++
	}
	if err := itr.Close(); err != nil {
		t.Fatal(err)
	}
	if snapshots != 1 {
		t.Fatalf("two snapshot syncs of an unchanged database wrote %d snapshots, want 1", snapshots)
	}
}

func serveSnapshotSyncs(t *testing.T, ctx context.Context) (*litestream.DB, *sql.DB, *file.ReplicaClient, *http.Client) {
	t.Helper()
	dir := t.TempDir()
	client := file.NewReplicaClient(filepath.Join(dir, "replica"))
	db := openHandoverPublisher(t, filepath.Join(dir, "db"), client)
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	sqldb := openServerSQLDB(t, ctx, db.Path())
	t.Cleanup(func() { _ = sqldb.Close() })
	if _, err := sqldb.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, body BLOB)`); err != nil {
		t.Fatal(err)
	}
	return db, sqldb, client, serveSyncs(t, db)
}

func latestRestorePlan(t *testing.T, ctx context.Context, client *file.ReplicaClient) []*ltx.FileInfo {
	t.Helper()
	plan, err := litestream.CalcRestorePlan(ctx, client, 0, time.Time{}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	return plan
}
