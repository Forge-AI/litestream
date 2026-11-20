package litestream_test

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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
	return handOverLanding(t, ctx, behind, func(sqldb *sql.DB, _ string) litestream.WALEnd {
		insertHandoverRows(t, ctx, sqldb, handoverRows+1, tail)
		return litestream.WALEnd{}
	})
}

func handOverLanding(t *testing.T, ctx context.Context, behind ltx.TXID, land func(sqldb *sql.DB, path string) litestream.WALEnd) handedOver {
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
	end := land(sqldb, path)

	return handedOver{
		db:     openHandoverPublisherClaiming(t, path, client, litestream.Handover{TXID: pos.TXID - behind, WAL: end}),
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

func openHandoverPublisherClaiming(t *testing.T, path string, client *file.ReplicaClient, claim litestream.Handover) *litestream.DB {
	t.Helper()
	db := testingutil.NewDB(t, path)
	db.MonitorInterval = 0
	db.ShutdownSyncTimeout = 0
	db.Replica = litestream.NewReplicaWithClient(db, client)
	db.Replica.MonitorEnabled = false
	db.SetHandover(claim)
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

func shipFirstSync(t *testing.T, ctx context.Context, h handedOver) (pages int, shipped ltx.Header) {
	t.Helper()
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
	return pages, dec.Header()
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
			pages, shipped := shipFirstSync(t, ctx, h)
			if pages >= int(shipped.Commit) {
				t.Fatalf("the first sync after the hand-over shipped %d of %d pages, a full snapshot", pages, shipped.Commit)
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
	pages, shipped := shipFirstSync(t, ctx, h)
	if pages != int(shipped.Commit) {
		t.Fatalf("a refused claim must snapshot; the first sync shipped %d of %d pages", pages, shipped.Commit)
	}
	requireRestoreMatches(t, ctx, h)
}

func TestDB_HandoverWALEndResumesPastTheFramesTheClaimCovers(t *testing.T) {
	for _, pinned := range []bool{true, false} {
		t.Run(fmt.Sprintf("pinned=%t", pinned), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var end litestream.WALEnd
			var restarted bool
			h := handOverLanding(t, ctx, 0, func(sqldb *sql.DB, path string) litestream.WALEnd {
				for _, landed := range []string{`INSERT INTO t (id, body) VALUES (0, x'00')`, `DELETE FROM t WHERE id = 0`} {
					if _, err := sqldb.ExecContext(ctx, landed); err != nil {
						t.Fatal(err)
					}
				}
				if pinned {
					reader := testingutil.MustOpenSQLDB(t, path)
					defer testingutil.MustCloseSQLDB(t, reader)
					tx, err := reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = tx.Rollback() }()
					var rows int
					if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM t`).Scan(&rows); err != nil {
						t.Fatal(err)
					}
				}
				var busy, frames, folded int
				if err := sqldb.QueryRowContext(ctx, `PRAGMA wal_checkpoint(PASSIVE)`).Scan(&busy, &frames, &folded); err != nil {
					t.Fatal(err)
				}
				if frames == 0 || folded != frames {
					t.Fatalf("the landing's PASSIVE fold must backfill every frame it leaves: %d of %d", folded, frames)
				}
				end = litestream.WALEnd{Salt: binary.BigEndian.Uint64(walSalt(t, path+"-wal")), Frames: uint32(frames)}
				insertHandoverRows(t, ctx, sqldb, handoverRows+1, 4)
				restarted = binary.BigEndian.Uint64(walSalt(t, path+"-wal")) != end.Salt
				if restarted == pinned {
					t.Fatalf("pinned=%t: the tail restarted the wal: %t", pinned, restarted)
				}
				return end
			})

			proof, err := h.db.ProveHandover(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !proof.Continued || proof.TXID != uint64(h.txid) {
				t.Fatalf("a claim at the replicated position %s must continue: %+v", h.txid, proof)
			}
			pages, shipped := shipFirstSync(t, ctx, h)
			resumed := int64(litestream.WALHeaderSize)
			if !restarted {
				resumed += int64(end.Frames) * int64(shipped.PageSize+litestream.WALFrameHeaderSize)
			}
			if shipped.WALOffset != resumed {
				t.Fatalf("the first sync after the hand-over read the wal from %d, past the claimed frames at %d", shipped.WALOffset, resumed)
			}
			if pages >= int(shipped.Commit) {
				t.Fatalf("the first sync after the hand-over shipped %d of %d pages, a full snapshot", pages, shipped.Commit)
			}
			requireRestoreMatches(t, ctx, h)
		})
	}
}
