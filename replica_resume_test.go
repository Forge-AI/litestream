package litestream_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestReplica_ResumesAboveTheReplicaHead(t *testing.T) {
	singles := func(minTXID, maxTXID ltx.TXID) [][2]ltx.TXID {
		var files [][2]ltx.TXID
		for txID := minTXID; txID <= maxTXID; txID++ {
			files = append(files, [2]ltx.TXID{txID, txID})
		}
		return files
	}
	for _, tt := range []struct {
		name      string
		snapshot  bool
		localFrom ltx.TXID
		wantL0    [][2]ltx.TXID
	}{
		{name: "LevelZeroThrough32", localFrom: 1, wantL0: singles(1, 49)},
		{name: "SnapshotThrough32AndNoLevelZero", snapshot: true, localFrom: 33, wantL0: singles(33, 49)},
		{name: "LocalHistoryStartsAfterTheSnapshot", snapshot: true, localFrom: 41, wantL0: [][2]ltx.TXID{{33, 49}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			db, sqldb := testingutil.MustOpenDBs(t)
			defer testingutil.MustCloseDBs(t, db, sqldb)

			if _, err := sqldb.ExecContext(ctx, `CREATE TABLE t (x INTEGER)`); err != nil {
				t.Fatal(err)
			}
			writeThrough(ctx, t, db, sqldb, 32)
			if tt.snapshot {
				if _, err := db.Snapshot(ctx); err != nil {
					t.Fatal(err)
				}
			} else if err := db.Replica.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			writeThrough(ctx, t, db, sqldb, 49)
			for txID := ltx.TXID(1); txID < tt.localFrom; txID++ {
				if err := os.Remove(db.LTXPath(0, txID, txID)); err != nil {
					t.Fatal(err)
				}
			}
			db.Replica.SetPos(ltx.Pos{})

			if err := db.Replica.Sync(ctx); err != nil {
				t.Fatal(err)
			}

			if got := db.Replica.Pos().TXID; got != 49 {
				t.Fatalf("replica position=%s, want 49", got)
			}
			itr, err := db.Replica.Client.LTXFiles(ctx, 0, 0, false)
			if err != nil {
				t.Fatal(err)
			}
			var l0 [][2]ltx.TXID
			for itr.Next() {
				l0 = append(l0, [2]ltx.TXID{itr.Item().MinTXID, itr.Item().MaxTXID})
			}
			if err := itr.Close(); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(l0, tt.wantL0) {
				t.Fatalf("level 0 files=%v, want %v", l0, tt.wantL0)
			}
			if err := litestream.NewCompactor(db.Replica.Client, nil).VerifyLevelConsistency(ctx, 0); err != nil {
				t.Fatal(err)
			}

			opt := litestream.NewRestoreOptions()
			opt.OutputPath = filepath.Join(t.TempDir(), "restored.db")
			if err := litestream.NewReplicaWithClient(nil, db.Replica.Client).Restore(ctx, opt); err != nil {
				t.Fatal(err)
			}
			restored, err := sql.Open("sqlite", opt.OutputPath)
			if err != nil {
				t.Fatal(err)
			}
			defer restored.Close()
			if got, want := rowSummary(ctx, t, restored), rowSummary(ctx, t, sqldb); got != want {
				t.Fatalf("restored rows %v, want %v", got, want)
			}
		})
	}
}

func writeThrough(ctx context.Context, t *testing.T, db *litestream.DB, sqldb *sql.DB, txID ltx.TXID) {
	t.Helper()
	for {
		if err := db.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		pos, err := db.Pos()
		if err != nil {
			t.Fatal(err)
		}
		if pos.TXID == txID {
			return
		}
		if pos.TXID > txID {
			t.Fatalf("local TXID %s passed %s", pos.TXID, txID)
		}
		if _, err := sqldb.ExecContext(ctx, `INSERT INTO t VALUES (?)`, int64(pos.TXID)); err != nil {
			t.Fatal(err)
		}
	}
}

func rowSummary(ctx context.Context, t *testing.T, sqldb *sql.DB) [2]int64 {
	t.Helper()
	var summary [2]int64
	if err := sqldb.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(x), 0) FROM t`).Scan(&summary[0], &summary[1]); err != nil {
		t.Fatal(err)
	}
	return summary
}
