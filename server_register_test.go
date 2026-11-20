package litestream

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestServer_RegisterTakesTheDatabasesIntervals(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")
	sqldb, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`CREATE TABLE t (id INT)`); err != nil {
		t.Fatal(err)
	}
	if err := sqldb.Close(); err != nil {
		t.Fatal(err)
	}
	server := NewServer(NewStore(nil, CompactionLevels{{Level: 0}}))
	body := fmt.Sprintf(`{"path": %q, "replica_url": %q, "monitor_interval_ms": 5000, "sync_interval_ms": 500}`,
		dbPath, "file://"+filepath.Join(dir, "replica"))
	rec := httptest.NewRecorder()
	server.handleRegister(rec, httptest.NewRequest(http.MethodPost, "/register", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s, want 200", rec.Code, rec.Body)
	}
	db := server.store.FindDB(dbPath)
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	if db.MonitorInterval != 5*time.Second {
		t.Fatalf("monitor interval=%s, want the registered 5s", db.MonitorInterval)
	}
	if db.Replica.SyncInterval != 500*time.Millisecond {
		t.Fatalf("sync interval=%s, want the registered 500ms", db.Replica.SyncInterval)
	}
}

func TestServer_IdleRegisteredDatabasesDoNoWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := NewServer(NewStore(nil, CompactionLevels{{Level: 0}}))
		dir := t.TempDir()
		dbs := make([]*DB, 1000)
		for i := range dbs {
			dbs[i] = registerSynced(t, server, dir, i)
		}
		before := idleWork(dbs)
		time.Sleep(time.Minute)
		if after := idleWork(dbs); after != before {
			t.Fatalf("1,000 idle registered databases went from %+v to %+v over a minute, want no sync, monitor or checkpoint", before, after)
		}
		for _, db := range dbs {
			if err := db.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
	})
}

func TestServer_ARegisteredDatabaseCheckpointsOnceASyncLeavesItsWALPastTheThreshold(t *testing.T) {
	server := NewServer(NewStore(nil, CompactionLevels{{Level: 0}}))
	db := registerSynced(t, server, t.TempDir(), 0)
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	app, err := sql.Open("sqlite", db.Path())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = app.Close() })
	synced := func(row int) work {
		t.Helper()
		if _, err := app.Exec(`INSERT INTO t VALUES (?)`, row); err != nil {
			t.Fatal(err)
		}
		if _, err := server.store.SyncDB(context.Background(), db.Path(), true, false); err != nil {
			t.Fatal(err)
		}
		return idleWork([]*DB{db})
	}
	before := idleWork([]*DB{db})
	under := synced(1)
	if under.passive != before.passive || under.truncate != before.truncate {
		t.Fatalf("a write synced under the WAL threshold went from %+v to %+v, want no checkpoint", before, under)
	}
	db.MinCheckpointPageN = 1
	past := synced(2)
	if past.passive != under.passive+1 || past.truncate != under.truncate {
		t.Fatalf("a write synced past the WAL threshold went from %+v to %+v, want one PASSIVE checkpoint and no TRUNCATE", under, past)
	}
	if _, err := server.store.SyncDB(context.Background(), db.Path(), true, false); err != nil {
		t.Fatal(err)
	}
	if idle := idleWork([]*DB{db}); idle.passive != past.passive || idle.truncate != past.truncate {
		t.Fatalf("a sync with nothing new went from %+v to %+v, want no checkpoint", past, idle)
	}
}

func TestServer_ARegisterAfterAStopTakesTheReplicaAndRetentionItNames(t *testing.T) {
	dir := t.TempDir()
	server := NewServer(NewStore(nil, CompactionLevels{{Level: 0}}))
	stopped := registerSynced(t, server, dir, 0)
	path := stopped.Path()
	stop := httptest.NewRecorder()
	server.handleStop(stop, httptest.NewRequest(http.MethodPost, "/stop", strings.NewReader(fmt.Sprintf(`{"path": %q}`, path))))
	if stop.Code != http.StatusOK {
		t.Fatalf("stop status=%d body=%s, want 200", stop.Code, stop.Body)
	}
	if err := os.RemoveAll(stopped.MetaPath()); err != nil {
		t.Fatal(err)
	}
	moved := filepath.Join(dir, "moved")
	body := fmt.Sprintf(`{"path": %q, "replica_url": %q, "retention": false}`, path, "file://"+moved)
	rec := httptest.NewRecorder()
	server.handleRegisterDriven(rec, httptest.NewRequest(http.MethodPost, "/register-driven", strings.NewReader(body)))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"status":"registered"`) {
		t.Fatalf("status=%d body=%s, want a fresh registration", rec.Code, rec.Body)
	}
	db := server.store.FindDB(path)
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	if db.RetentionEnabled {
		t.Fatal("the database took the store's retention over the false it registered with")
	}
	app, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = app.Close() })
	if _, err := app.Exec(`INSERT INTO t VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := server.store.SyncDB(context.Background(), path, true, false); err != nil {
		t.Fatal(err)
	}
	if shipped, err := os.ReadDir(LTXLevelDir(moved, 0)); err != nil || len(shipped) == 0 {
		t.Fatalf("the moved replica holds %d files (%v), want the write shipped there", len(shipped), err)
	}
}

func registerSynced(t *testing.T, server *Server, dir string, i int) *DB {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("db%d", i))
	sqldb, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`CREATE TABLE t (id INT)`); err != nil {
		t.Fatal(err)
	}
	if err := sqldb.Close(); err != nil {
		t.Fatal(err)
	}
	body := fmt.Sprintf(`{"path": %q, "replica_url": %q}`, path, "file://"+filepath.Join(dir, "replica", fmt.Sprint(i)))
	rec := httptest.NewRecorder()
	server.handleRegisterDriven(rec, httptest.NewRequest(http.MethodPost, "/register-driven", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s, want 200", rec.Code, rec.Body)
	}
	if _, err := server.store.SyncDB(context.Background(), path, true, false); err != nil {
		t.Fatal(err)
	}
	return server.store.FindDB(path)
}

type work struct {
	syncs, passive, truncate float64
	replicated               time.Time
}

func idleWork(dbs []*DB) (w work) {
	for _, db := range dbs {
		w.syncs += testutil.ToFloat64(syncNCounterVec.WithLabelValues(db.Path()))
		w.passive += testutil.ToFloat64(checkpointNCounterVec.WithLabelValues(db.Path(), CheckpointModePassive))
		w.truncate += testutil.ToFloat64(checkpointNCounterVec.WithLabelValues(db.Path(), CheckpointModeTruncate))
		if at := db.LastSuccessfulSyncAt(); at.After(w.replicated) {
			w.replicated = at
		}
	}
	return w
}
