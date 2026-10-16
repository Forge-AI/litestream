package litestream_test

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

const handoverRows = 256

type handedOver struct {
	db     *litestream.DB
	client *file.ReplicaClient
	sqldb  *sql.DB
	txid   ltx.TXID
}

func handOver(t *testing.T, ctx context.Context, tail int, behind ltx.TXID) handedOver {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	client := file.NewReplicaClient(filepath.Join(dir, "replica"))

	before := openHandoverPublisher(t, path, client)
	sqldb := openServerSQLDB(t, ctx, path)
	if _, err := sqldb.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, body BLOB)`); err != nil {
		t.Fatal(err)
	}
	for first := 1; first <= handoverRows; first += handoverRows / 2 {
		insertHandoverRows(t, ctx, sqldb, first, handoverRows/2)
		if err := before.Sync(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := before.Close(ctx); err != nil {
		t.Fatal(err)
	}
	pos, err := before.Pos()
	if err != nil {
		t.Fatal(err)
	}
	testingutil.MustCloseSQLDB(t, sqldb)

	sqldb = openServerSQLDB(t, ctx, path)
	t.Cleanup(func() { _ = sqldb.Close() })
	if _, err := sqldb.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	insertHandoverRows(t, ctx, sqldb, handoverRows+1, tail)

	return handedOver{
		db:     openHandoverPublisherClaiming(t, path, client, pos.TXID-behind),
		client: client,
		sqldb:  sqldb,
		txid:   pos.TXID,
	}
}

func openServerSQLDB(t *testing.T, ctx context.Context, path string) *sql.DB {
	t.Helper()
	sqldb := testingutil.MustOpenSQLDB(t, path)
	sqldb.SetMaxOpenConns(1)
	if _, err := sqldb.ExecContext(ctx, `PRAGMA wal_autocheckpoint = 0`); err != nil {
		t.Fatal(err)
	}
	return sqldb
}

func openHandoverPublisher(t *testing.T, path string, client *file.ReplicaClient) *litestream.DB {
	t.Helper()
	db := testingutil.NewDB(t, path)
	db.MonitorInterval = 0
	db.ShutdownSyncTimeout = 0
	db.Replica = litestream.NewReplicaWithClient(db, client)
	db.Replica.MonitorEnabled = false
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	return db
}

func openHandoverPublisherClaiming(t *testing.T, path string, client *file.ReplicaClient, txid ltx.TXID) *litestream.DB {
	t.Helper()
	db := testingutil.NewDB(t, path)
	db.MonitorInterval = 0
	db.ShutdownSyncTimeout = 0
	db.Replica = litestream.NewReplicaWithClient(db, client)
	db.Replica.MonitorEnabled = false
	db.SetHandover(txid)
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	return db
}

func insertHandoverRows(t *testing.T, ctx context.Context, sqldb *sql.DB, first, n int) {
	t.Helper()
	for id := first; id < first+n; id++ {
		if _, err := sqldb.ExecContext(ctx, `INSERT INTO t (id, body) VALUES (?, randomblob(4000))`, id); err != nil {
			t.Fatal(err)
		}
	}
}

func shipFirstSync(t *testing.T, ctx context.Context, h handedOver) (pages int, commit uint32, snapshots []string) {
	t.Helper()
	var lane bytes.Buffer
	h.db.Logger = slog.New(slog.NewJSONHandler(&lane, nil))
	if err := h.db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.db.Replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	next := h.txid + 1
	f, err := os.Open(h.db.LTXPath(0, next, next))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	dec := ltx.NewDecoder(f)
	if err := dec.DecodeHeader(); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, dec.Header().PageSize)
	for {
		var hdr ltx.PageHeader
		if err := dec.DecodePage(&hdr, data); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		pages++
	}
	return pages, dec.Header().Commit, snapshotReasons(t, &lane)
}

func snapshotReasons(t *testing.T, lane *bytes.Buffer) []string {
	t.Helper()
	var reasons []string
	lines := bufio.NewScanner(lane)
	for lines.Scan() {
		var record struct {
			Level  string `json:"level"`
			Msg    string `json:"msg"`
			Reason string `json:"reason"`
		}
		if err := json.Unmarshal(lines.Bytes(), &record); err != nil {
			t.Fatal(err)
		}
		if record.Msg != "lane snapshot" {
			continue
		}
		if record.Level != slog.LevelWarn.String() || record.Reason == "" {
			t.Fatalf("a lane snapshot must warn with its reason: %s", lines.Bytes())
		}
		reasons = append(reasons, record.Reason)
	}
	if err := lines.Err(); err != nil {
		t.Fatal(err)
	}
	return reasons
}

func requireRestoreMatches(t *testing.T, ctx context.Context, h handedOver) {
	t.Helper()
	restored := filepath.Join(t.TempDir(), "restored.db")
	opt := litestream.NewRestoreOptions()
	opt.OutputPath = restored
	if err := litestream.NewReplicaWithClient(nil, h.client).Restore(ctx, opt); err != nil {
		t.Fatal(err)
	}
	r, err := sql.Open("sqlite", restored)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	digest := `SELECT count(*), coalesce(sum(id), 0), coalesce(sum(length(body)), 0), hex(coalesce(max(body), x'')) FROM t`
	var wantN, wantIDs, wantBytes int64
	var wantLast string
	if err := h.sqldb.QueryRowContext(ctx, digest).Scan(&wantN, &wantIDs, &wantBytes, &wantLast); err != nil {
		t.Fatal(err)
	}
	var gotN, gotIDs, gotBytes int64
	var gotLast string
	if err := r.QueryRowContext(ctx, digest).Scan(&gotN, &gotIDs, &gotBytes, &gotLast); err != nil {
		t.Fatal(err)
	}
	if gotN != wantN || gotIDs != wantIDs || gotBytes != wantBytes || gotLast != wantLast {
		t.Fatalf("the replica restores rows=%d ids=%d bytes=%d, the primary holds rows=%d ids=%d bytes=%d", gotN, gotIDs, gotBytes, wantN, wantIDs, wantBytes)
	}
}

func TestDB_HandoverTXIDResumesTheTruncatedWAL(t *testing.T) {
	for _, tail := range []struct {
		name string
		rows int
	}{
		{"tail shorter than the handed-over wal", 4},
		{"tail longer than the handed-over wal", handoverRows * 2},
	} {
		t.Run(tail.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			h := handOver(t, ctx, tail.rows, 0)

			proof, err := h.db.ProveHandover(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !proof.Continued || proof.TXID != uint64(h.txid) {
				t.Fatalf("a claim at the replicated position %s must continue: %+v", h.txid, proof)
			}
			pages, commit, snapshots := shipFirstSync(t, ctx, h)
			if pages >= int(commit) {
				t.Fatalf("the first sync after the hand-over shipped %d of %d pages, a full snapshot", pages, commit)
			}
			if len(snapshots) != 0 {
				t.Fatalf("a continued hand-over logged lane snapshots %q", snapshots)
			}
			requireRestoreMatches(t, ctx, h)
		})
	}
}

func TestDB_HandoverTXIDBehindTheReplicaSnapshots(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h := handOver(t, ctx, 4, 1)

	proof, err := h.db.ProveHandover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if proof.Continued || proof.TXID != uint64(h.txid) {
		t.Fatalf("a claim behind the replicated position %s must not continue: %+v", h.txid, proof)
	}
	pages, commit, snapshots := shipFirstSync(t, ctx, h)
	if pages != int(commit) {
		t.Fatalf("a refused claim must snapshot; the first sync shipped %d of %d pages", pages, commit)
	}
	if len(snapshots) != 1 {
		t.Fatalf("a refused claim must log one lane snapshot with its reason, got %q", snapshots)
	}
	requireRestoreMatches(t, ctx, h)
}
