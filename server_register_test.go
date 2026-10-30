package litestream

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
