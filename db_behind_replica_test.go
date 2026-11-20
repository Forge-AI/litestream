package litestream_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/superfly/ltx"
	_ "modernc.org/sqlite"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

type deleteDeniedClient struct{ *file.ReplicaClient }

func (c *deleteDeniedClient) DeleteLTXFiles(ctx context.Context, files []*ltx.FileInfo) error {
	return fmt.Errorf("AccessDenied: delete of %d files refused", len(files))
}

func mustBehind(err error) {
	if err != nil {
		panic(err)
	}
}

func writeBehindFixtureDB(path string, seq int) {
	c, err := sql.Open("sqlite", path)
	mustBehind(err)
	_, err = c.Exec("CREATE TABLE applied(seq INTEGER PRIMARY KEY); CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT); INSERT INTO applied VALUES (7)")
	mustBehind(err)
	_, err = c.Exec("INSERT INTO meta VALUES ('image','01ARZ3NDEKTSV4RRFFQ69G5FAV'), ('epoch','2'), ('seq_floor',?)", seq)
	mustBehind(err)
	mustBehind(c.Close())
}

func publishBehindFixture(ctx context.Context, client litestream.ReplicaClient, level int, min, max ltx.TXID, data []byte) {
	size := uint32(data[16])<<8 | uint32(data[17])
	if size == 1 {
		size = 65536
	}
	var b bytes.Buffer
	e, err := ltx.NewEncoder(&b)
	mustBehind(err)
	mustBehind(e.EncodeHeader(ltx.Header{Version: ltx.Version, Flags: ltx.HeaderFlagNoChecksum, PageSize: size, Commit: uint32(len(data)) / size, MinTXID: min, MaxTXID: max, Timestamp: time.Now().UnixMilli(), WALOffset: litestream.WALHeaderSize}))
	for offset := 0; offset < len(data); offset += int(size) {
		mustBehind(e.EncodePage(ltx.PageHeader{Pgno: uint32(offset/int(size) + 1)}, data[offset:offset+int(size)]))
	}
	mustBehind(e.Close())
	_, err = client.WriteLTXFile(ctx, level, min, max, &b)
	mustBehind(err)
}

func TestDB_BehindReplicaTakesBaselineAboveSnapshot(t *testing.T) {
	for _, initialReplicaTXID := range []ltx.TXID{0, 9} {
		t.Run(initialReplicaTXID.String(), func(t *testing.T) {
			root := t.TempDir()
			var err error
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			old := filepath.Join(root, "old.db")
			writeBehindFixtureDB(old, 7)
			data, err := os.ReadFile(old)
			mustBehind(err)
			client := &deleteDeniedClient{file.NewReplicaClient(filepath.Join(root, "remote"))}
			for i := ltx.TXID(1); i <= 9; i++ {
				publishBehindFixture(ctx, client, 0, i, i, data)
			}

			publishBehindFixture(ctx, client, 1, 1, 9, data)
			aged := time.Now().Add(-2 * time.Minute)
			for i := ltx.TXID(1); i <= 9; i++ {
				mustBehind(os.Chtimes(client.LTXFilePath(0, i, i), aged, aged))
			}
			oldPublisher := litestream.NewDB(old)
			oldPublisher.Replica = litestream.NewReplicaWithClient(oldPublisher, client)
			oldPublisher.Replica.MonitorEnabled = false
			oldPublisher.MonitorInterval = 0
			oldLocal := file.NewReplicaClient(filepath.Dir(filepath.Dir(filepath.Dir(oldPublisher.LTXPath(0, 10, 10)))))
			publishBehindFixture(ctx, oldLocal, 0, 10, 10, data)
			mustBehind(oldPublisher.Open())
			defer oldPublisher.Close(context.Background())
			mustBehind(oldPublisher.Sync(ctx))
			snapshot, snapshotErr := oldPublisher.Snapshot(ctx)
			mustBehind(snapshotErr)
			initialRemote, initialRemoteErr := oldPublisher.Replica.MaxLTXFileInfo(ctx, 0)
			mustBehind(initialRemoteErr)
			if snapshot.MaxTXID <= initialRemote.MaxTXID {
				t.Fatal("the fixture must publish a snapshot ahead of level zero")
			}
			t.Logf("old_snapshot=%s old_remote_l0=%s", snapshot.MaxTXID, initialRemote.MaxTXID)
			current := filepath.Join(root, "primary.db")
			writeBehindFixtureDB(current, 11)
			db := litestream.NewDB(current)
			db.Replica = litestream.NewReplicaWithClient(db, client)
			db.Replica.SetPos(ltx.Pos{TXID: initialReplicaTXID})
			db.Replica.MonitorEnabled = false
			newData, err := os.ReadFile(current)
			mustBehind(err)
			local := file.NewReplicaClient(filepath.Dir(filepath.Dir(filepath.Dir(db.LTXPath(0, 1, 1)))))
			publishBehindFixture(ctx, local, 0, 1, 1, newData)
			before, err := db.Pos()
			mustBehind(err)
			db.MonitorInterval = 0
			mustBehind(db.Open())
			defer db.Close(context.Background())
			err = db.SyncAndWait(ctx)
			mustBehind(err)
			t.Logf("local_before=%s replicated=%s", before.TXID, db.Replica.Pos().TXID)
			if db.Replica.Pos().TXID <= snapshot.MaxTXID {
				t.Errorf("replicated transaction %s reuses history covered by old snapshot %s", db.Replica.Pos().TXID, snapshot.MaxTXID)
			}
			if err := litestream.NewCompactor(client, nil).VerifyLevelConsistency(ctx, 0); err != nil {
				t.Errorf("the new publisher left level zero skipping transactions under old snapshot %s: %v", snapshot.MaxTXID, err)
			}
			db.L0Retention = time.Minute
			retentionErr := db.EnforceL0RetentionByTime(ctx)
			remote, remoteErr := db.Replica.MaxLTXFileInfo(ctx, 0)
			mustBehind(remoteErr)
			if retentionErr == nil || !strings.Contains(retentionErr.Error(), "AccessDenied") {
				t.Fatalf("the fixture must deny retention: %v", retentionErr)
			}
			t.Logf("retention=%v remote_max=%s", retentionErr, remote.MaxTXID)
			restored := filepath.Join(root, "replica.db")
			opt := litestream.NewRestoreOptions()
			opt.OutputPath = restored
			mustBehind(litestream.NewReplicaWithClient(nil, client).Restore(ctx, opt))
			c, err := sql.Open("sqlite", restored)
			mustBehind(err)
			defer c.Close()
			var seq int
			var image, epoch string
			mustBehind(c.QueryRow(`SELECT
        (SELECT value FROM meta WHERE key = 'image'),
        (SELECT value FROM meta WHERE key = 'epoch'),
        max((SELECT COALESCE(MAX(seq), 0) FROM applied),
            COALESCE((SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'seq_floor'), 0))`).Scan(&image, &epoch, &seq))
			if image != "01ARZ3NDEKTSV4RRFFQ69G5FAV" || epoch != "2" {
				t.Fatalf("restored identity %s/%s differs from the unchanged binding", image, epoch)
			}
			if seq != 11 {
				t.Fatalf("restored applied sequence %d, want 11", seq)
			}

		})
	}
}

type offlineClient struct {
	litestream.ReplicaClient
	offline atomic.Bool
}

func (c *offlineClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	if c.offline.Load() {
		return nil, errors.New("offline")
	}
	return c.ReplicaClient.WriteLTXFile(ctx, level, minTXID, maxTXID, r)
}

func TestDB_BrokenEarlierUploadFileReuploadsFromTheReplica(t *testing.T) {
	for name, breakFile := range map[string]func(string) error{
		"missing": os.Remove,
		"truncated": func(path string) error {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			return os.WriteFile(path, data[:len(data)/2], 0o600)
		},
		"zero tail": func(path string) error {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			clear(data[len(data)/2:])
			return os.WriteFile(path, data, 0o600)
		},
	} {
		for _, pending := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s pending=%t", name, pending), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				root := t.TempDir()
				path := filepath.Join(root, "primary.db")
				client := &offlineClient{ReplicaClient: file.NewReplicaClient(filepath.Join(root, "remote"))}
				c, err := sql.Open("sqlite", path)
				mustBehind(err)
				defer c.Close()
				_, err = c.Exec("PRAGMA journal_mode = wal; PRAGMA wal_autocheckpoint = 0; CREATE TABLE t(x)")
				mustBehind(err)
				open := func(autoRecover bool) *litestream.DB {
					db := litestream.NewDB(path)
					db.SyncDriven = true
					db.MonitorInterval = 0
					db.ShutdownSyncTimeout = 0
					db.Replica = litestream.NewReplicaWithClient(db, client)
					db.Replica.MonitorEnabled = false
					db.Replica.AutoRecoverEnabled = autoRecover
					mustBehind(db.Open())
					return db
				}
				insert := func(x int) {
					_, err := c.Exec("INSERT INTO t VALUES (?)", x)
					mustBehind(err)
				}

				before := open(false)
				insert(1)
				mustBehind(before.SyncAndWait(ctx))
				_, replicated, err := before.MaxLTX()
				mustBehind(err)
				client.offline.Store(true)
				insert(2)
				mustBehind(before.Sync(ctx))
				insert(3)
				mustBehind(before.Sync(ctx))
				_, newest, err := before.MaxLTX()
				mustBehind(err)
				if newest < replicated+2 {
					t.Fatalf("the fixture left transactions %s to %s pending, want two", replicated+1, newest)
				}
				earlier := before.LTXPath(0, replicated+1, replicated+1)
				if !pending {
					earlier = before.LTXPath(0, replicated, replicated)
				}
				_ = before.Close(ctx)
				mustBehind(breakFile(earlier))
				client.offline.Store(false)

				after := open(true)
				defer after.Close(ctx)
				insert(4)
				mustBehind(after.SyncAndWait(ctx))
				if _, err := after.Compact(ctx, 1); err != nil {
					t.Errorf("compaction after recovery: %v", err)
				}
				if err := litestream.NewCompactor(client, nil).VerifyLevelConsistency(ctx, 0); err != nil {
					t.Errorf("recovery left level zero skipping transactions: %v", err)
				}
				opt := litestream.NewRestoreOptions()
				opt.OutputPath = filepath.Join(root, "restored.db")
				mustBehind(litestream.NewReplicaWithClient(nil, client).Restore(ctx, opt))
				restored, err := sql.Open("sqlite", opt.OutputPath)
				mustBehind(err)
				defer restored.Close()
				var rows int
				mustBehind(restored.QueryRow("SELECT count(*) FROM t").Scan(&rows))
				if rows != 4 {
					t.Fatalf("the cloud copy holds %d rows after recovery, want 4", rows)
				}
			})
		}
	}
}

func TestDB_BrokenLastUploadFileReuploadsFromTheReplica(t *testing.T) {
	for name, breakFile := range map[string]func([]byte) []byte{
		"empty":     func([]byte) []byte { return nil },
		"truncated": func(b []byte) []byte { return b[:len(b)/2] },
		"zeroed":    func(b []byte) []byte { return make([]byte, len(b)) },
		"zero tail": func(b []byte) []byte { clear(b[len(b)/2:]); return b },
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			root := t.TempDir()
			path := filepath.Join(root, "primary.db")
			client := file.NewReplicaClient(filepath.Join(root, "remote"))
			c, err := sql.Open("sqlite", path)
			mustBehind(err)
			defer c.Close()
			_, err = c.Exec("PRAGMA journal_mode = wal; PRAGMA wal_autocheckpoint = 0; CREATE TABLE t(x)")
			mustBehind(err)
			open := func(autoRecover bool) *litestream.DB {
				db := litestream.NewDB(path)
				db.SyncDriven = true
				db.MonitorInterval = 0
				db.Replica = litestream.NewReplicaWithClient(db, client)
				db.Replica.MonitorEnabled = false
				db.Replica.AutoRecoverEnabled = autoRecover
				mustBehind(db.Open())
				return db
			}
			insert := func(x int) {
				_, err := c.Exec("INSERT INTO t VALUES (?)", x)
				mustBehind(err)
			}

			before := open(false)
			insert(1)
			mustBehind(before.SyncAndWait(ctx))
			insert(2)
			mustBehind(before.SyncAndWait(ctx))
			minTXID, maxTXID, err := before.MaxLTX()
			mustBehind(err)
			upload := before.LTXPath(0, minTXID, maxTXID)
			mustBehind(before.Close(ctx))
			data, err := os.ReadFile(upload)
			mustBehind(err)
			mustBehind(os.WriteFile(upload, breakFile(data), 0o600))

			stuck := open(false)
			if err := stuck.SyncAndWait(ctx); !errors.Is(err, litestream.ErrLTXCorrupted) {
				t.Fatalf("a chat without auto-recover synced over its broken file: %v", err)
			}
			_ = stuck.Close(ctx)

			after := open(true)
			defer after.Close(ctx)
			insert(3)
			mustBehind(after.SyncAndWait(ctx))
			if err := litestream.NewCompactor(client, nil).VerifyLevelConsistency(ctx, 0); err != nil {
				t.Errorf("recovery left level zero skipping transactions: %v", err)
			}
			opt := litestream.NewRestoreOptions()
			opt.OutputPath = filepath.Join(root, "restored.db")
			mustBehind(litestream.NewReplicaWithClient(nil, client).Restore(ctx, opt))
			restored, err := sql.Open("sqlite", opt.OutputPath)
			mustBehind(err)
			defer restored.Close()
			var rows int
			mustBehind(restored.QueryRow("SELECT count(*) FROM t").Scan(&rows))
			if rows != 3 {
				t.Fatalf("the cloud copy holds %d rows after recovery, want 3", rows)
			}
		})
	}
}

func TestReplica_LostPositionUnderSnapshotKeepsL0Contiguous(t *testing.T) {
	ctx := context.Background()
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	if _, err := sqldb.ExecContext(ctx, `CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	}
	if err := db.SyncAndWait(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.ExecContext(ctx, `INSERT INTO t VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	remote, err := db.Replica.MaxLTXFileInfo(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.MaxTXID <= remote.MaxTXID {
		t.Fatal("the fixture must snapshot ahead of level zero")
	}

	db.Replica.SetPos(ltx.Pos{})
	if _, err := sqldb.ExecContext(ctx, `INSERT INTO t VALUES (2)`); err != nil {
		t.Fatal(err)
	}
	if err := db.SyncAndWait(ctx); err != nil {
		t.Fatal(err)
	}
	if err := litestream.NewCompactor(db.Replica.Client, nil).VerifyLevelConsistency(ctx, 0); err != nil {
		t.Fatalf("a replica that lost its position under snapshot %s skipped level zero: %v", snapshot.MaxTXID, err)
	}
}
