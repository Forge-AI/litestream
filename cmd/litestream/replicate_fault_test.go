package main_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	main "github.com/benbjohnson/litestream/cmd/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

type faultRequest struct {
	Lane    string `json:"lane"`
	Segment uint32 `json:"segment"`
}

func TestReplicateCommand_FaultsALazyCopyThroughItsFaultSocket(t *testing.T) {
	for _, via := range []string{"config", "register"} {
		t.Run(via, func(t *testing.T) {
			ctx := t.Context()
			dir := t.TempDir()
			path := filepath.Join(dir, "db")
			replica := filepath.Join(dir, "replica")
			sqldb := testingutil.MustOpenSQLDB(t, path)
			t.Cleanup(func() { _ = sqldb.Close() })
			if _, err := sqldb.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, body BLOB)`); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path+"-segments", make([]byte, 4), 0o600); err != nil {
				t.Fatal(err)
			}
			faulted := serveSegmentFaults(t, path)

			control := testSocketPath(t)
			dbs := fmt.Sprintf("dbs:\n  - path: %q\n    replica:\n      type: file\n      path: %q\n", path, replica)
			if via == "register" {
				dbs = "dbs: []\n"
			}
			config, err := main.ParseConfig(strings.NewReader(fmt.Sprintf(
				"fault-socket: %q\nsocket:\n  enabled: true\n  path: %q\n%s", faulted.socket, control, dbs)), false)
			if err != nil {
				t.Fatal(err)
			}
			cmd := main.NewReplicateCommand()
			cmd.Config = config
			if err := cmd.Run(ctx); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cmd.Close(context.Background()) })
			if via == "register" {
				if err := (&main.RegisterCommand{}).Run(ctx, []string{"-socket", control, "-replica", "file://" + replica, path}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := sqldb.ExecContext(ctx, `INSERT INTO t VALUES (1, randomblob(4000))`); err != nil {
				t.Fatal(err)
			}

			select {
			case req := <-faulted.requests:
				if req != (faultRequest{Lane: path, Segment: 0}) {
					t.Fatalf("faulted %+v, want segment 0 of %s", req, path)
				}
			case <-time.After(30 * time.Second):
				t.Fatalf("the %s database snapshotted its lazy copy without faulting segment 0 through %s", via, faulted.socket)
			}
		})
	}
}

type segmentFaultServer struct {
	socket   string
	requests chan faultRequest
}

func serveSegmentFaults(t *testing.T, path string) *segmentFaultServer {
	t.Helper()
	server := &segmentFaultServer{socket: testSocketPath(t), requests: make(chan faultRequest, 16)}
	listener, err := net.Listen("unix", server.socket)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req faultRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		segments, err := os.OpenFile(path+"-segments", os.O_WRONLY, 0)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		defer segments.Close()
		var state [4]byte
		binary.LittleEndian.PutUint32(state[:], 1)
		if _, err := segments.WriteAt(state[:], int64(req.Segment)*int64(len(state))); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		server.requests <- req
	})}
	go func() { _ = httpServer.Serve(listener) }()
	t.Cleanup(func() { _ = httpServer.Close() })
	return server
}
