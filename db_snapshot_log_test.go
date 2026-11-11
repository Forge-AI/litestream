package litestream_test

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/superfly/ltx"
	_ "modernc.org/sqlite"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
)

type snapshotLine struct {
	Msg       string          `json:"msg"`
	Level     string          `json:"level"`
	Reason    string          `json:"reason"`
	SizeBytes int64           `json:"size_bytes"`
	ElapsedMs *int64          `json:"elapsed_ms"`
	TXID      json.RawMessage `json:"txid"`
}

func snapshotTXIDs(lines []snapshotLine) []string {
	txIDs := make([]string, 0, len(lines))
	for _, line := range lines {
		txIDs = append(txIDs, string(line.TXID))
	}
	return txIDs
}

func snapshotLines(t *testing.T, logs *bytes.Buffer) map[string][]snapshotLine {
	t.Helper()
	lines := make(map[string][]snapshotLine)
	scanner := bufio.NewScanner(bytes.NewReader(logs.Bytes()))
	for scanner.Scan() {
		var line snapshotLine
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			t.Fatal(err)
		}
		lines[line.Msg] = append(lines[line.Msg], line)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return lines
}

type refusingUploads struct {
	litestream.ReplicaClient
	refuse bool
}

func (c *refusingUploads) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	if c.refuse {
		return nil, errors.New("the store refused the upload")
	}
	return c.ReplicaClient.WriteLTXFile(ctx, level, minTXID, maxTXID, r)
}

func openSnapshotFixture(t *testing.T, client litestream.ReplicaClient) (*litestream.DB, *bytes.Buffer) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fixture.db")
	writer, err := sql.Open("sqlite", path+"?_pragma=journal_mode(wal)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	if _, err := writer.Exec("CREATE TABLE fixture (value TEXT); INSERT INTO fixture VALUES ('kept')"); err != nil {
		t.Fatal(err)
	}
	logs := &bytes.Buffer{}
	db := litestream.NewDB(path)
	db.Logger = slog.New(slog.NewJSONHandler(logs, nil))
	db.MonitorInterval = 0
	db.Replica = litestream.NewReplicaWithClient(db, client)
	db.Replica.MonitorEnabled = false
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	return db, logs
}

func TestDB_FullSnapshotNamesItsSizeDurationAndUploadOnce(t *testing.T) {
	for _, laterSyncs := range []int{1, 100} {
		t.Run(fmt.Sprint(laterSyncs), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			db, logs := openSnapshotFixture(t, file.NewReplicaClient(t.TempDir()))

			if err := db.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			if err := db.Replica.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			for range laterSyncs {
				if err := db.Sync(ctx); err != nil {
					t.Fatal(err)
				}
				if err := db.Replica.Sync(ctx); err != nil {
					t.Fatal(err)
				}
			}

			lines := snapshotLines(t, logs)
			started, written, uploaded := lines["lane snapshot"], lines["full snapshot written"], lines["full snapshot uploaded"]
			if len(started) != 1 || len(written) != 1 || len(uploaded) != 1 {
				t.Fatalf("started=%d written=%d uploaded=%d after %d later syncs, want one of each:\n%s", len(started), len(written), len(uploaded), laterSyncs, logs.Bytes())
			}
			for _, line := range []snapshotLine{written[0], uploaded[0]} {
				if line.Level != slog.LevelWarn.String() || line.SizeBytes <= 0 || line.ElapsedMs == nil {
					t.Fatalf("%s must warn with its size and duration: %+v", line.Msg, line)
				}
			}
			if written[0].Reason == "" || written[0].Reason != started[0].Reason {
				t.Fatalf("written reason %q, started reason %q", written[0].Reason, started[0].Reason)
			}
			if written[0].SizeBytes != uploaded[0].SizeBytes {
				t.Fatalf("written %d bytes, uploaded %d", written[0].SizeBytes, uploaded[0].SizeBytes)
			}
		})
	}
}

func TestDB_RefusedFullSnapshotUploadSaysSoUntilItLands(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	client := &refusingUploads{ReplicaClient: file.NewReplicaClient(t.TempDir()), refuse: true}
	db, logs := openSnapshotFixture(t, client)

	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.Replica.Sync(ctx); err == nil {
		t.Fatal("the upload must fail")
	}
	lines := snapshotLines(t, logs)
	if len(lines["full snapshot upload failed"]) != 1 || len(lines["full snapshot uploaded"]) != 0 {
		t.Fatalf("a refused upload must say so and claim nothing:\n%s", logs.Bytes())
	}

	client.refuse = false
	if err := db.Replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if got := len(snapshotLines(t, logs)["full snapshot uploaded"]); got != 1 {
		t.Fatalf("uploaded lines = %d after the retry landed:\n%s", got, logs.Bytes())
	}
}

func TestDB_LevelSnapshotNamesItsSizeAndDuration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	db, logs := openSnapshotFixture(t, file.NewReplicaClient(t.TempDir()))
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	info, err := db.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}

	written := snapshotLines(t, logs)["level snapshot written"]
	if len(written) != 1 || written[0].SizeBytes != info.Size || written[0].ElapsedMs == nil {
		t.Fatalf("level snapshot lines %+v for a %d byte file", written, info.Size)
	}
}

type uploadingAtPublish struct {
	slog.Handler
	t      *testing.T
	upload func() error
}

func (h uploadingAtPublish) Enabled(context.Context, slog.Level) bool { return true }

func (h uploadingAtPublish) Handle(ctx context.Context, record slog.Record) error {
	if record.Message == "db sync" {
		landed := make(chan error, 1)
		go func() { landed <- h.upload() }()
		if err := <-landed; err != nil {
			h.t.Error(err)
		}
	}
	if !h.Handler.Enabled(ctx, record.Level) {
		return nil
	}
	return h.Handler.Handle(ctx, record)
}

func TestDB_FullSnapshotUploadedBeforeItsSyncReturnsNamesItsUploadOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	db, logs := openSnapshotFixture(t, file.NewReplicaClient(t.TempDir()))
	logger := db.Logger
	db.Logger = slog.New(uploadingAtPublish{Handler: logger.Handler(), t: t, upload: func() error { return db.Replica.Sync(ctx) }})

	err := db.Sync(ctx)
	db.Logger = logger
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	lines := snapshotLines(t, logs)
	written, uploaded := snapshotTXIDs(lines["full snapshot written"]), snapshotTXIDs(lines["full snapshot uploaded"])
	if len(written) != 1 || !slices.Equal(uploaded, written) {
		t.Fatalf("written txids %v, uploaded txids %v, want one upload line naming the written snapshot:\n%s", written, uploaded, logs.Bytes())
	}
}

func TestDB_TwoFullSnapshotsWrittenBeforeUploadsDrainEachNameTheirOwnUpload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	db, logs := openSnapshotFixture(t, file.NewReplicaClient(t.TempDir()))
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	litestream.ForgetLastWALPosition(t, db)
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	lines := snapshotLines(t, logs)
	started, written := snapshotTXIDs(lines["lane snapshot"]), snapshotTXIDs(lines["full snapshot written"])
	if len(written) != 2 || !slices.Equal(started, written) {
		t.Fatalf("started txids %v, written txids %v, want both syncs to each start and write a snapshot:\n%s", started, written, logs.Bytes())
	}

	if err := db.Replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if uploaded := snapshotTXIDs(snapshotLines(t, logs)["full snapshot uploaded"]); !slices.Equal(uploaded, written) {
		t.Fatalf("written txids %v, uploaded txids %v:\n%s", written, uploaded, logs.Bytes())
	}
}

func TestDB_LevelSnapshotThatCannotReadItsDatabaseSaysSo(t *testing.T) {
	logs := &bytes.Buffer{}
	db := litestream.NewDB(filepath.Join(t.TempDir(), "never-opened.db"))
	db.Logger = slog.New(slog.NewJSONHandler(logs, nil))

	if _, err := db.Snapshot(t.Context()); err == nil {
		t.Fatal("a database that never opened has no snapshot to read")
	}

	failed := snapshotLines(t, logs)["level snapshot failed"]
	if len(failed) != 1 || failed[0].ElapsedMs == nil {
		t.Fatalf("failure lines %+v, want one with its duration", failed)
	}
}
