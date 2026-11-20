package litestream_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
)

type headRecorder struct {
	*file.ReplicaClient
	mu       sync.Mutex
	writes   []string
	failHead error
}

func (c *headRecorder) note(write string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes = append(c.writes, write)
}

func (c *headRecorder) WriteHead(ctx context.Context, key string, text []byte) error {
	if c.failHead != nil {
		return c.failHead
	}
	c.note("head " + strings.SplitN(string(text), "-", 2)[0])
	return c.ReplicaClient.WriteHead(ctx, key, text)
}

func (c *headRecorder) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	c.note(fmt.Sprintf("file %d", maxTXID))
	return c.ReplicaClient.WriteLTXFile(ctx, level, minTXID, maxTXID, r)
}

func (c *headRecorder) taken() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	writes := c.writes
	c.writes = nil
	return writes
}

type headRig struct {
	db     *litestream.DB
	sqldb  *sql.DB
	client *headRecorder
	key    string
}

func newHeadRig(t *testing.T, ctx context.Context, dir string) *headRig {
	t.Helper()
	path := filepath.Join(dir, "db")
	client := &headRecorder{ReplicaClient: file.NewReplicaClient(filepath.Join(dir, "replica"))}
	db := litestream.NewDB(path)
	db.MonitorInterval = 0
	db.SyncDriven = true
	db.Replica = litestream.NewReplica(db)
	db.Replica.MonitorEnabled = false
	db.Replica.Client = client
	db.Replica.HeadKey = filepath.Join(dir, "replica", "head")
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	sqldb := openServerSQLDB(t, ctx, path)
	if _, err := sqldb.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	return &headRig{db: db, sqldb: sqldb, client: client, key: db.Replica.HeadKey}
}

func (r *headRig) commit(t *testing.T, ctx context.Context) {
	t.Helper()
	if _, err := r.sqldb.ExecContext(ctx, `INSERT INTO t DEFAULT VALUES`); err != nil {
		t.Fatal(err)
	}
	if err := r.db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
}

func (r *headRig) head(t *testing.T) string {
	t.Helper()
	text, err := os.ReadFile(r.key)
	if err != nil {
		t.Fatal(err)
	}
	return string(text)
}

func TestHead_AWriterWritesMovingBeforeItsFirstFileAndItsPositionAfterItRests(t *testing.T) {
	ctx := context.Background()
	rig := newHeadRig(t, ctx, t.TempDir())
	rig.commit(t, ctx)
	if err := rig.db.Replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	writes := rig.client.taken()
	if len(writes) < 2 || writes[0] != "head moving" || !strings.HasPrefix(writes[1], "file ") {
		t.Fatalf("writes = %v, want moving before the first file", writes)
	}
	litestream.SweepHeads(ctx, time.Now())
	if head := rig.head(t); head != litestream.HeadMoving {
		t.Fatalf("head = %q right after an upload, want moving", head)
	}
	litestream.SweepHeads(ctx, time.Now().Add(litestream.HeadRest))
	pos := rig.db.Replica.Pos().TXID
	want := fmt.Sprintf("%016x-", uint64(pos))
	if head := rig.head(t); !strings.HasPrefix(head, want) {
		t.Fatalf("head = %q after its rest, want the prefix %q", head, want)
	}
	litestream.SweepHeads(ctx, time.Now().Add(2*litestream.HeadRest))
	if writes := rig.client.taken(); len(writes) != 1 {
		t.Fatalf("writes after the rest = %v, want one rest write", writes)
	}
}

func TestHead_AChatUploadingAtTheSlowPaceStaysMovingAndWritesNoHeadBetweenFiles(t *testing.T) {
	ctx := context.Background()
	rig := newHeadRig(t, ctx, t.TempDir())
	for range 3 {
		rig.commit(t, ctx)
		if err := rig.db.Replica.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		litestream.SweepHeads(ctx, time.Now().Add(litestream.HeadRest/2))
	}
	var heads []string
	for _, write := range rig.client.taken() {
		if strings.HasPrefix(write, "head ") {
			heads = append(heads, write)
		}
	}
	if len(heads) != 1 || heads[0] != "head moving" {
		t.Fatalf("head writes across slow uploads = %v, want one moving", heads)
	}
}

func TestHead_AFailedMovingWriteUploadsNothing(t *testing.T) {
	ctx := context.Background()
	rig := newHeadRig(t, ctx, t.TempDir())
	rig.commit(t, ctx)
	rig.client.failHead = errors.New("the bucket refused the head")
	if err := rig.db.Replica.Sync(ctx); !errors.Is(err, rig.client.failHead) {
		t.Fatalf("sync = %v, want the head's failure", err)
	}
	if writes := rig.client.taken(); len(writes) != 0 {
		t.Fatalf("writes = %v, want no file after a failed moving write", writes)
	}
}

func TestHead_AWriterThatDiesMidBurstLeavesItSayingMoving(t *testing.T) {
	ctx := context.Background()
	rig := newHeadRig(t, ctx, t.TempDir())
	rig.commit(t, ctx)
	if err := rig.db.Replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	rig.db.Abandon()
	if head := rig.head(t); head != litestream.HeadMoving {
		t.Fatalf("head = %q after the writer died mid burst, want moving", head)
	}
}

func TestHead_AWriterReopeningAMovingChatRestsIt(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	rig := newHeadRig(t, ctx, dir)
	rig.commit(t, ctx)
	if err := rig.db.Replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	rig.db.Abandon()
	_ = rig.sqldb.Close()

	reopened := newHeadRig(t, ctx, dir)
	if err := reopened.db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := reopened.db.Replica.Sync(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if head := reopened.head(t); head == litestream.HeadMoving {
		t.Fatal("a caught up writer left a reopened chat moving")
	}
}

func TestHead_ACleanCloseRestsIt(t *testing.T) {
	ctx := context.Background()
	rig := newHeadRig(t, ctx, t.TempDir())
	rig.commit(t, ctx)
	if err := rig.db.Replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	_ = rig.sqldb.Close()
	if err := rig.db.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if head := rig.head(t); head == litestream.HeadMoving {
		t.Fatal("a clean close left the head moving")
	}
}
