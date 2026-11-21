package litestream

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/superfly/ltx"
)

type drainHeldClient struct {
	ReplicaClient
	store *Store
}

func (c *drainHeldClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	if level != 0 {
		select {
		case <-c.store.draining.Done():
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return c.ReplicaClient.WriteLTXFile(ctx, level, minTXID, maxTXID, r)
}

func TestStore_ADatabaseStoppedRightAfterASaltResetSnapshotRestoresEveryRowItSynced(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	app, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	app.SetMaxOpenConns(1)
	if _, err := app.Exec(`PRAGMA journal_mode = wal; CREATE TABLE t (id INT)`); err != nil {
		t.Fatal(err)
	}
	replica, err := NewReplicaClientFromURL("file://" + filepath.Join(dir, "replica"))
	if err != nil {
		t.Fatal(err)
	}
	insert := func(first, last int) {
		t.Helper()
		for id := first; id <= last; id++ {
			if _, err := app.Exec(`INSERT INTO t (id) VALUES (?)`, id); err != nil {
				t.Fatal(err)
			}
		}
	}
	open := func(logs io.Writer, held bool) *Store {
		t.Helper()
		store := NewStore(nil, DefaultCompactionLevels)
		store.Logger = slog.New(slog.NewJSONHandler(logs, nil))
		store.RetentionEnabled = false
		if err := store.Open(context.Background()); err != nil {
			t.Fatal(err)
		}
		db := NewDB(path)
		db.SyncDriven = true
		db.RetentionEnabled = false
		db.Replica = NewReplica(db)
		db.Replica.Client = replica
		if held {
			db.Replica.Client = &drainHeldClient{ReplicaClient: replica, store: store}
		}
		if _, err := store.RegisterDB(db); err != nil {
			t.Fatal(err)
		}
		return store
	}

	first := open(io.Discard, false)
	insert(1, 10)
	if _, err := first.SyncDB(context.Background(), path, true, false); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	insert(11, 20)
	if _, err := app.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	insert(21, 30)

	var logs bytes.Buffer
	second := open(&logs, true)
	if _, err := second.SyncDB(context.Background(), path, true, false); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "wal header salt reset, snapshotting") {
		t.Fatalf("the sync after the wal restart took no salt-reset snapshot:\n%s", logs.String())
	}
	if err := second.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	restored := filepath.Join(dir, "restored")
	if err := NewReplicaWithClient(nil, replica).Restore(context.Background(), RestoreOptions{OutputPath: restored}); err != nil {
		t.Fatalf("restore after a stop right after the salt-reset snapshot: %v", err)
	}
	check, err := sql.Open("sqlite", restored)
	if err != nil {
		t.Fatal(err)
	}
	defer check.Close()
	var rows int
	if err := check.QueryRow(`SELECT count(*) FROM t`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 30 {
		t.Fatalf("restored %d rows, want the 30 the last sync shipped", rows)
	}
}
