package litestream_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestDB_AbandonKeepsTheTailWithoutSyncing(t *testing.T) {
	db := testingutil.NewDB(t, filepath.Join(t.TempDir(), "db"))
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

	checkpointed := make(chan error, 1)
	go func() { checkpointed <- db.Checkpoint(t.Context(), litestream.CheckpointModeTruncate) }()
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
