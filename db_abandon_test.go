package litestream_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestDB_AbandonPublishesItsPositionWithoutSyncing(t *testing.T) {
	var logs bytes.Buffer
	db := testingutil.NewDB(t, filepath.Join(t.TempDir(), "db"))
	db.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	db.MonitorInterval = 0
	db.Replica = litestream.NewReplica(db)
	db.Replica.Client = testingutil.NewFileReplicaClient(t)
	db.Replica.MonitorEnabled = false
	require.NoError(t, db.Open())
	sqldb := testingutil.MustOpenSQLDB(t, db.Path())
	defer func() { require.NoError(t, sqldb.Close()) }()

	_, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INT)`)
	require.NoError(t, err)
	require.NoError(t, db.Sync(t.Context()))
	require.NoError(t, db.Replica.Sync(t.Context()))
	_, err = sqldb.ExecContext(t.Context(), `INSERT INTO t (id) VALUES (1)`)
	require.NoError(t, err)
	require.NoError(t, db.Sync(t.Context()))

	pos, err := db.Pos()
	require.NoError(t, err)
	replicated := db.Replica.Pos()
	require.Greater(t, pos.TXID, replicated.TXID, "the tail must be local only before the abandon")
	wal, err := os.ReadFile(db.WALPath())
	require.NoError(t, err)

	db.Abandon()

	require.Equal(t, replicated, db.Replica.Pos(), "an abandon ships nothing")
	remote, err := db.Replica.MaxLTXFileInfo(t.Context(), 0)
	require.NoError(t, err)
	require.Equal(t, replicated.TXID, remote.MaxTXID, "an abandon uploads nothing")

	var published struct {
		Msg            string `json:"msg"`
		TXID           uint64 `json:"txid"`
		ReplicatedTXID uint64 `json:"replicated_txid"`
		Unsynced       bool   `json:"unsynced"`
	}
	for line := range bytes.Lines(logs.Bytes()) {
		require.NoError(t, json.Unmarshal(line, &published))
		if published.Msg == "abandoned without a final sync" {
			break
		}
	}
	require.Equal(t, "abandoned without a final sync", published.Msg)
	require.Equal(t, uint64(pos.TXID), published.TXID)
	require.Equal(t, uint64(replicated.TXID), published.ReplicatedTXID)
	require.True(t, published.Unsynced)

	checkpointed := make(chan error, 1)
	go func() { checkpointed <- db.Checkpoint(t.Context()) }()
	select {
	case err := <-checkpointed:
		t.Fatalf("a checkpoint ran after the abandon: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	kept, err := os.ReadFile(db.WALPath())
	require.NoError(t, err)
	require.Equal(t, wal[:litestream.WALHeaderSize], kept[:litestream.WALHeaderSize], "the abandon keeps the WAL generation")
	require.GreaterOrEqual(t, len(kept), len(wal), "the abandon keeps the WAL tail")
}
