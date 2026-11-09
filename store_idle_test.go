package litestream

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/superfly/ltx"
)

func TestStore_IdleDatabasesAfterABurstMakeNoCallsAndDoNoWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var records atomic.Int64
		store := NewStore(nil, CompactionLevels{
			{Level: 0},
			{Level: 1, Interval: time.Minute},
			{Level: 2, Interval: time.Hour},
			{Level: 3, Interval: 24 * time.Hour},
		})
		store.Logger = slog.New(countedHandler{&records})
		store.L0Retention = 15 * time.Minute
		store.RetentionEnabled = false
		if err := store.Open(context.Background()); err != nil {
			t.Fatal(err)
		}
		dir := t.TempDir()
		healthy := registerCounted(t, store, dir, "healthy")
		refused := registerCounted(t, store, dir, "refused")
		for i := range 30 {
			for _, db := range []*countedDB{healthy, refused} {
				db.write(t, store, i)
			}
			time.Sleep(10 * time.Second)
		}
		refused.client.refusing.Store(true)

		time.Sleep(48 * time.Hour)
		synctest.Wait()
		calls, logged := healthy.client.calls.Load()+refused.client.calls.Load(), records.Load()
		time.Sleep(72 * time.Hour)
		synctest.Wait()

		if idle := healthy.client.calls.Load() + refused.client.calls.Load() - calls; idle != 0 {
			t.Errorf("two idle databases, one refused, made %d replica calls over three idle days, want none", idle)
		}
		if idle := records.Load() - logged; idle != 0 {
			t.Errorf("the store logged %d records over three idle days, want none: an idle database wakes no monitor", idle)
		}
		for _, db := range []*countedDB{healthy, refused} {
			if err := db.app.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if err := store.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestStore_ADatabaseWrittenOnceADayNamesItsUploadsAndStartsLevelOne(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		store := NewStore(nil, CompactionLevels{
			{Level: 0},
			{Level: 1, Interval: time.Minute},
			{Level: 2, Interval: time.Hour},
			{Level: 3, Interval: 24 * time.Hour},
		})
		store.L0Retention = 15 * time.Minute
		store.RetentionEnabled = false
		if err := store.Open(ctx); err != nil {
			t.Fatal(err)
		}
		dir := t.TempDir()
		db := registerCounted(t, store, dir, "sparse")
		time.Sleep(time.Second)

		const days = 5
		for day := range days {
			db.write(t, store, day)
			synctest.Wait()
			if err := store.FindDB(db.path).EnforceL0RetentionByTime(ctx); err != nil {
				t.Fatal(err)
			}
			time.Sleep(24 * time.Hour)
		}

		named := namedFiles(t, store, db)
		for _, level := range []int{1, 2, 3, SnapshotLevel} {
			for _, info := range uploadedFiles(t, db, level) {
				name := compactedName{Level: level, Min: uint64(info.MinTXID), Max: uint64(info.MaxTXID)}
				if !slices.Contains(named, name) {
					t.Errorf("level %d file %v was uploaded and never named", level, name)
				}
			}
		}
		if len(uploadedFiles(t, db, 1)) == 0 {
			t.Error("level 1 never started, so level-zero retention can never run")
		}
		levelZero, err := os.ReadDir(store.FindDB(db.path).LTXLevelDir(0))
		if err != nil {
			t.Fatal(err)
		}
		if got := len(levelZero); got > 2 {
			t.Errorf("%d local level-zero files remain after %d daily writes, want retention to keep at most 2", got, days)
		}

		restored := filepath.Join(dir, "restored")
		opt := NewRestoreOptions()
		opt.OutputPath = restored
		if err := NewReplicaWithClient(nil, db.client).Restore(ctx, opt); err != nil {
			t.Fatal(err)
		}
		check, err := sql.Open("sqlite", restored)
		if err != nil {
			t.Fatal(err)
		}
		var rows int
		if err := check.QueryRow(`SELECT COUNT(*) FROM t`).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows != days {
			t.Errorf("a restore read %d rows, want %d", rows, days)
		}

		for _, closer := range []io.Closer{check, db.app} {
			if err := closer.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if err := store.Close(ctx); err != nil {
			t.Fatal(err)
		}
	})
}

type compactedName struct {
	Level int    `json:"level"`
	Min   uint64 `json:"min"`
	Max   uint64 `json:"max"`
}

func namedFiles(t *testing.T, store *Store, db *countedDB) []compactedName {
	t.Helper()
	log, err := os.ReadFile(filepath.Join(store.FindDB(db.path).MetaPath(), "compacted"))
	if err != nil {
		t.Fatal(err)
	}
	var names []compactedName
	for line := range strings.Lines(string(log)) {
		var name compactedName
		if err := json.Unmarshal([]byte(line), &name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	return names
}

func uploadedFiles(t *testing.T, db *countedDB, level int) []*ltx.FileInfo {
	t.Helper()
	infos, err := FindLTXFiles(context.Background(), db.client, level, false, func(*ltx.FileInfo) (bool, error) {
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return infos
}

func TestStore_AfterANetworkDropIdleDatabasesMakeNoCalls(t *testing.T) {
	total := map[int]int64{}
	for _, n := range []int{1, 1000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) { total[n] = dropAndIdle(t, n) })
		})
	}
	if total[1000] != 1000*total[1] {
		t.Errorf("1,000 databases made %d replica calls and one made %d, want exactly 1,000 times as many", total[1000], total[1])
	}
}

func dropAndIdle(t *testing.T, n int) int64 {
	store := NewStore(nil, CompactionLevels{
		{Level: 0},
		{Level: 1, Interval: time.Minute},
		{Level: 2, Interval: time.Hour},
		{Level: 3, Interval: 24 * time.Hour},
	})
	store.L0Retention = 15 * time.Minute
	store.RetentionEnabled = false
	if err := store.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	var down atomic.Bool
	dir := t.TempDir()
	dbs := make([]*countedDB, n)
	for i := range dbs {
		dbs[i] = registerCounted(t, store, dir, fmt.Sprint(i))
		dbs[i].client.down = &down
	}
	calls := func() (sum int64) {
		for _, db := range dbs {
			sum += db.client.calls.Load()
		}
		return sum
	}
	lists := func() (sum int64) {
		for _, db := range dbs {
			sum += db.client.lists.Load()
		}
		return sum
	}
	time.Sleep(time.Second)
	for _, db := range dbs {
		db.write(t, store, 0)
	}
	time.Sleep(time.Minute)
	for _, db := range dbs {
		db.write(t, store, 1)
	}

	down.Store(true)
	for _, db := range dbs {
		if _, err := db.app.Exec(`INSERT INTO t VALUES (2)`); err != nil {
			t.Fatal(err)
		}
		if _, err := store.SyncDB(context.Background(), db.path, true, false); err == nil {
			t.Fatal("a sync with the network down succeeded")
		}
	}
	time.Sleep(time.Second)
	down.Store(false)
	listed := lists()
	for _, db := range dbs {
		written := db.client.writes.Load()
		result, err := store.SyncDB(context.Background(), db.path, true, false)
		if err != nil {
			t.Fatal(err)
		}
		if db.client.writes.Load() == written || result.ReplicatedTXID != result.TXID {
			t.Fatalf("the retry after the drop replicated through txid %d of %d with %d uploads, want the write the drop held uploaded", result.ReplicatedTXID, result.TXID, db.client.writes.Load()-written)
		}
	}
	if relisted := lists() - listed; relisted != 0 {
		t.Errorf("the retry after the drop listed the replica %d times for %d databases, want none: the position survives the failure", relisted, n)
	}

	landed := calls()
	time.Sleep(2 * time.Hour)
	synctest.Wait()
	if idle := calls() - landed; idle != 0 {
		t.Errorf("%d databases made %d replica calls in the two idle hours after the drop healed, want none", n, idle)
	}
	for _, db := range dbs {
		if err := db.app.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	return calls()
}

func TestStore_EachSyncDrivenWriteUploadsOneLTXFileAndSyncsOnce(t *testing.T) {
	store := NewStore(nil, CompactionLevels{{Level: 0}})
	db := registerCounted(t, store, t.TempDir(), "drafts")
	t.Cleanup(func() {
		_ = db.app.Close()
		_ = store.UnregisterDB(context.Background(), db.path)
	})
	db.write(t, store, 0)
	registered := store.FindDB(db.path)
	uploaded, synced := db.client.writes.Load(), idleWork([]*DB{registered}).syncs
	const writes = 20
	for i := 1; i <= writes; i++ {
		db.write(t, store, i)
	}
	if uploads := db.client.writes.Load() - uploaded; uploads != writes {
		t.Errorf("%d writes, each followed by a sync, uploaded %d LTX files, want %d: one per write and none for checkpoint bookkeeping", writes, uploads, writes)
	}
	if syncs := idleWork([]*DB{registered}).syncs - synced; syncs != writes {
		t.Errorf("%d writes, each followed by a sync, ran %v syncs, want %d", writes, syncs, writes)
	}
}

func TestServer_ASyncWhoseUploadIsRefusedAnswersForbidden(t *testing.T) {
	store := NewStore(nil, CompactionLevels{{Level: 0}})
	db := registerCounted(t, store, t.TempDir(), "refused")
	t.Cleanup(func() {
		_ = db.app.Close()
		_ = store.UnregisterDB(context.Background(), db.path)
	})
	db.client.refusing.Store(true)
	if _, err := db.app.Exec(`INSERT INTO t VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	body := fmt.Sprintf(`{"path": %q, "wait": true}`, db.path)
	NewServer(store).handleSync(rec, httptest.NewRequest(http.MethodPost, "/sync", strings.NewReader(body)))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s, want 403: a refused upload is a refusal, not a server error", rec.Code, rec.Body)
	}
}

func TestStore_TwoWritersOnOneDatabaseShipOneNextLTXFile(t *testing.T) {
	ctx := context.Background()
	store := NewStore(nil, CompactionLevels{{Level: 0}})
	shared := t.TempDir()
	dirs := []string{t.TempDir(), t.TempDir()}
	for _, dir := range dirs {
		if err := os.Symlink(shared, filepath.Join(dir, "replica")); err != nil {
			t.Fatal(err)
		}
	}
	winner := registerCounted(t, store, dirs[0], "chat")
	loser := registerCounted(t, store, dirs[1], "chat")
	t.Cleanup(func() {
		for _, db := range []*countedDB{winner, loser} {
			_ = db.app.Close()
			_ = store.UnregisterDB(context.Background(), db.path)
		}
	})
	for _, db := range []*countedDB{winner, loser} {
		if _, err := store.SyncDB(ctx, db.path, true, false); err != nil {
			t.Fatal(err)
		}
	}
	if w, l := store.FindDB(winner.path).Replica.Pos().TXID, store.FindDB(loser.path).Replica.Pos().TXID; w != l {
		t.Fatalf("winner at %v, loser at %v: both writers must start from the same stored change", w, l)
	}

	winner.write(t, store, 1)
	if _, err := loser.app.Exec(`INSERT INTO t VALUES (2)`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SyncDB(ctx, loser.path, true, false); !errors.Is(err, ErrLTXFileTaken) {
		t.Fatalf("the second writer of the next ltx file synced with %v, want ErrLTXFileTaken", err)
	}

	next := store.FindDB(winner.path).Replica.Pos().TXID
	want, err := os.ReadFile(store.FindDB(winner.path).LTXPath(0, next, next))
	if err != nil {
		t.Fatal(err)
	}
	stored, err := winner.client.OpenLTXFile(ctx, 0, next, next, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(stored)
	_ = stored.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("the stored next ltx file is not the first writer's")
	}

	calls := loser.client.calls.Load()
	if _, err := loser.app.Exec(`INSERT INTO t VALUES (3)`); err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	body := fmt.Sprintf(`{"path": %q, "wait": true}`, loser.path)
	NewServer(store).handleSync(rec, httptest.NewRequest(http.MethodPost, "/sync", strings.NewReader(body)))
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("status=%d body=%s, want 412: a writer whose next ltx file was taken has stopped shipping", rec.Code, rec.Body)
	}
	if n := loser.client.calls.Load() - calls; n != 0 {
		t.Fatalf("the stopped writer made %d more calls to the replica, want 0", n)
	}
	winner.write(t, store, 4)
}

type countedDB struct {
	path   string
	app    *sql.DB
	client *countedClient
}

func registerCounted(t *testing.T, store *Store, dir, name string) *countedDB {
	t.Helper()
	path := filepath.Join(dir, name)
	create, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := create.Exec(`CREATE TABLE t (id INT)`); err != nil {
		t.Fatal(err)
	}
	if err := create.Close(); err != nil {
		t.Fatal(err)
	}
	inner, err := NewReplicaClientFromURL("file://" + filepath.Join(dir, "replica", name))
	if err != nil {
		t.Fatal(err)
	}
	client := &countedClient{ReplicaClient: inner}
	db := NewDB(path)
	db.SyncDriven = true
	db.RetentionEnabled = false
	db.Replica = NewReplica(db)
	db.Replica.Client = client
	if err := store.RegisterDB(db); err != nil {
		t.Fatal(err)
	}
	app, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	return &countedDB{path: path, app: app, client: client}
}

func (db *countedDB) write(t *testing.T, store *Store, i int) {
	t.Helper()
	if _, err := db.app.Exec(`INSERT INTO t VALUES (?)`, i); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SyncDB(context.Background(), db.path, true, false); err != nil {
		t.Fatal(err)
	}
}

type countedClient struct {
	ReplicaClient
	calls    atomic.Int64
	lists    atomic.Int64
	writes   atomic.Int64
	refusing atomic.Bool
	down     *atomic.Bool
}

func (c *countedClient) call() error {
	c.calls.Add(1)
	if c.refusing.Load() {
		return refusal{}
	}
	if c.down != nil && c.down.Load() {
		return errNetworkDown
	}
	return nil
}

var errNetworkDown = errors.New("dial tcp: connect: network is unreachable")

func (c *countedClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	c.lists.Add(1)
	if err := c.call(); err != nil {
		return nil, err
	}
	return c.ReplicaClient.LTXFiles(ctx, level, seek, useMetadata)
}

func (c *countedClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	if err := c.call(); err != nil {
		return nil, err
	}
	return c.ReplicaClient.OpenLTXFile(ctx, level, minTXID, maxTXID, offset, size)
}

func (c *countedClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	if err := c.call(); err != nil {
		return nil, err
	}
	info, err := c.ReplicaClient.WriteLTXFile(ctx, level, minTXID, maxTXID, r)
	if err == nil {
		c.writes.Add(1)
	}
	return info, err
}

func (c *countedClient) DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) error {
	if err := c.call(); err != nil {
		return err
	}
	return c.ReplicaClient.DeleteLTXFiles(ctx, a)
}

type refusal struct{}

func (refusal) Error() string { return "403 Forbidden" }

func (refusal) HTTPStatusCode() int { return http.StatusForbidden }

type countedHandler struct{ records *atomic.Int64 }

func (h countedHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h countedHandler) Handle(context.Context, slog.Record) error {
	h.records.Add(1)
	return nil
}

func (h countedHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h countedHandler) WithGroup(string) slog.Handler { return h }
