package main_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/benbjohnson/litestream"
	main "github.com/benbjohnson/litestream/cmd/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
	"github.com/superfly/ltx"
)

func TestDB_StartupHandoverResumesTheFirstFrame(t *testing.T) {
	for _, behind := range []ltx.TXID{0, 1} {
		t.Run(fmt.Sprintf("behind=%d", behind), func(t *testing.T) {
			ctx := t.Context()
			dir := t.TempDir()
			path := filepath.Join(dir, "db")
			client := file.NewReplicaClient(filepath.Join(dir, "replica"))
			before := testingutil.NewDB(t, path)
			before.MonitorInterval = 0
			before.CheckpointInterval = 0
			before.Replica = litestream.NewReplicaWithClient(before, client)
			before.Replica.MonitorEnabled = false
			if err := before.Open(); err != nil {
				t.Fatal(err)
			}
			sqldb := testingutil.MustOpenSQLDB(t, path)
			t.Cleanup(func() { _ = sqldb.Close() })
			if _, err := sqldb.ExecContext(ctx, `PRAGMA wal_autocheckpoint = 0;
				CREATE TABLE t (id INTEGER PRIMARY KEY, body BLOB);
				WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 128)
				INSERT INTO t SELECT i, randomblob(4000) FROM n`); err != nil {
				t.Fatal(err)
			}
			if err := before.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			if err := before.Checkpoint(ctx, litestream.CheckpointModePassive); err != nil {
				t.Fatal(err)
			}
			if err := before.Close(ctx); err != nil {
				t.Fatal(err)
			}
			pos, err := before.Pos()
			if err != nil {
				t.Fatal(err)
			}
			last, err := os.Open(before.LTXPath(0, pos.TXID, pos.TXID))
			if err != nil {
				t.Fatal(err)
			}
			dec := ltx.NewDecoder(last)
			if err := dec.DecodeHeader(); err != nil {
				t.Fatal(err)
			}
			hdr := dec.Header()
			_ = last.Close()
			if hdr.WALOffset != litestream.WALHeaderSize || hdr.WALSize != int64(hdr.PageSize)+litestream.WALFrameHeaderSize {
				t.Fatalf("the parked chain must end at frame 1, got %+v", hdr)
			}
			if _, err := sqldb.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
				t.Fatal(err)
			}

			config, err := main.ParseConfig(strings.NewReader(fmt.Sprintf(
				"dbs:\n  - path: %q\n    handover-txid: %d\n    monitor-interval: 0s\n    replica:\n      type: file\n      path: %q\n",
				path, pos.TXID-behind, client.Path())), false)
			if err != nil {
				t.Fatal(err)
			}
			db, err := main.NewDBFromConfig(config.DBs[0])
			if err != nil {
				t.Fatal(err)
			}
			db.Replica.MonitorEnabled = false
			db.CheckpointInterval = 0
			if err := db.Open(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close(context.Background()) })
			if err := db.SyncAndWait(ctx); err != nil {
				t.Fatal(err)
			}
			if pages, commit := shippedPages(t, db.LTXPath(0, pos.TXID+1, pos.TXID+1)); (pages == int(commit)) != (behind != 0) {
				t.Fatalf("startup claim behind=%d shipped %d of %d pages", behind, pages, commit)
			}
			if _, err := sqldb.ExecContext(ctx, `INSERT INTO t VALUES (129, randomblob(4000))`); err != nil {
				t.Fatal(err)
			}
			if err := db.SyncAndWait(ctx); err != nil {
				t.Fatal(err)
			}
			if pages, commit := shippedPages(t, db.LTXPath(0, pos.TXID+2, pos.TXID+2)); pages == int(commit) {
				t.Fatalf("the steady-state sync after a startup claim behind=%d shipped all %d pages", behind, commit)
			}
			opt := litestream.NewRestoreOptions()
			opt.OutputPath = filepath.Join(dir, "restored.db")
			if err := litestream.NewReplicaWithClient(nil, client).Restore(ctx, opt); err != nil {
				t.Fatal(err)
			}
			restored, err := sql.Open("sqlite", opt.OutputPath)
			if err != nil {
				t.Fatal(err)
			}
			defer restored.Close()
			var want, got string
			const digest = `SELECT group_concat(hex(body), '') FROM (SELECT body FROM t ORDER BY id)`
			if err := sqldb.QueryRowContext(ctx, digest).Scan(&want); err != nil {
				t.Fatal(err)
			}
			if err := restored.QueryRowContext(ctx, digest).Scan(&got); err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatal("startup continuation did not restore every row and the next write")
			}
		})
	}
}

func shippedPages(t *testing.T, path string) (int, uint32) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	dec := ltx.NewDecoder(f)
	if err := dec.DecodeHeader(); err != nil {
		t.Fatal(err)
	}
	pages := 0
	data := make([]byte, dec.Header().PageSize)
	for {
		var page ltx.PageHeader
		if err := dec.DecodePage(&page, data); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		pages++
	}
	return pages, dec.Header().Commit
}
