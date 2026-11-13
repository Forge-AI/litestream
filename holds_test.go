package litestream_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

var forkPointFiles = []litestream.HeldFile{
	{Level: litestream.SnapshotLevel, Min: 1, Max: 47900},
	{Level: 2, Min: 47901, Max: 48100},
	{Level: 0, Min: 48101, Max: 48211},
}

func TestHold_RetentionKeepsEveryFileAForkPointReadsUntilTheChildsBaseIsGone(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	parent := file.NewReplicaClient(filepath.Join(dir, "parent"))
	child := file.NewReplicaClient(filepath.Join(dir, "child"))
	compactor := litestream.NewCompactor(parent, slog.Default())

	old := time.Now().Add(-2 * time.Hour)
	createTestLTXFileWithTimestamp(t, parent, litestream.SnapshotLevel, 1, 47900, old)
	createTestLTXFile(t, parent, litestream.SnapshotLevel, 1, 50000)
	createTestLTXFileWithTimestamp(t, parent, 2, 47901, 48100, old)
	createTestLTXFile(t, parent, 2, 48101, 50000)
	createTestLTXFile(t, parent, 1, 48101, 50000)
	createTestLTXFileWithTimestamp(t, parent, 0, 48101, 48211, old)
	createTestLTXFileWithTimestamp(t, parent, 0, 48212, 50000, old)
	createTestLTXFile(t, parent, 0, 50001, 50001)

	childBase := child.ObjectKey(litestream.BaseName)
	if err := child.WriteHead(ctx, childBase, []byte(`{"txid":48211}`)); err != nil {
		t.Fatal(err)
	}
	if err := compactor.Hold(ctx, "01M751F2G3FH88536MWK938F8E", litestream.Hold{ChildBase: childBase, Files: forkPointFiles}); err != nil {
		t.Fatal(err)
	}

	enforceRetention(t, compactor)
	for _, f := range forkPointFiles {
		if !hasLTXFile(t, parent, f) {
			t.Errorf("retention deleted held file %+v", f)
		}
	}
	if hasLTXFile(t, parent, litestream.HeldFile{Level: 0, Min: 48212, Max: 50000}) {
		t.Error("retention kept an unheld compacted L0 file")
	}

	if err := os.Remove(childBase); err != nil {
		t.Fatal(err)
	}
	enforceRetention(t, compactor)
	for _, f := range forkPointFiles {
		if hasLTXFile(t, parent, f) {
			t.Errorf("retention kept %+v after the child's base was gone", f)
		}
	}
	if holds := listHolds(t, parent); len(holds) != 0 {
		t.Errorf("holds=%v after the child's base was gone, want none", holds)
	}
}

func TestHold_RefusesWhatRetentionAlreadyDeleted(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	parent := file.NewReplicaClient(filepath.Join(dir, "parent"))
	compactor := litestream.NewCompactor(parent, slog.Default())
	createTestLTXFile(t, parent, 0, 1, 1)
	childBase := filepath.Join(dir, "child", litestream.BaseName)

	for name, hold := range map[string]litestream.Hold{
		"a deleted file":           {ChildBase: childBase, Files: []litestream.HeldFile{{Level: 0, Min: 1, Max: 1}, {Level: 0, Min: 2, Max: 2}}},
		"a missing own base":       {ChildBase: childBase, Base: true},
		"a file at an empty level": {ChildBase: childBase, Files: []litestream.HeldFile{{Level: 1, Min: 1, Max: 1}}},
	} {
		if err := compactor.Hold(ctx, "child", hold); !errors.Is(err, litestream.ErrHeldFileGone) {
			t.Errorf("hold of %s: err=%v, want ErrHeldFileGone", name, err)
		}
	}
	if holds := listHolds(t, parent); len(holds) != 0 {
		t.Errorf("refused holds were written: %v", holds)
	}
}

func TestHold_ABaseRecordAHoldNamesStaysUntilTheGrandchildsBaseIsGone(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	parent := file.NewReplicaClient(filepath.Join(dir, "parent"))
	compactor := litestream.NewCompactor(parent, slog.Default())
	ownBase := parent.ObjectKey(litestream.BaseName)
	grandchildBase := filepath.Join(dir, "grandchild", litestream.BaseName)
	for _, key := range []string{ownBase, grandchildBase} {
		if err := parent.WriteHead(ctx, key, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := compactor.Hold(ctx, "grandchild", litestream.Hold{ChildBase: grandchildBase, Base: true}); err != nil {
		t.Fatal(err)
	}

	if err := compactor.DropBase(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(ownBase); err != nil {
		t.Fatalf("a held base record was dropped: %v", err)
	}

	if err := os.Remove(grandchildBase); err != nil {
		t.Fatal(err)
	}
	if err := compactor.DropBase(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(ownBase); !os.IsNotExist(err) {
		t.Fatalf("the base record outlived its last hold: %v", err)
	}
}

func TestServer_HoldAnswersGoneForAFileRetentionDeleted(t *testing.T) {
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)
	createTestLTXFile(t, db.Replica.Client, 0, 1, 1)
	client := serveSyncs(t, db)
	childBase := filepath.Join(t.TempDir(), litestream.BaseName)

	for txid, want := range map[ltx.TXID]int{1: http.StatusOK, 2: http.StatusGone} {
		body, err := json.Marshal(litestream.HoldRequest{Path: db.Path(), Name: "child", Hold: litestream.Hold{
			ChildBase: childBase,
			Files:     []litestream.HeldFile{{Level: 0, Min: uint64(txid), Max: uint64(txid)}},
		}})
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Post("http://localhost/hold", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("hold of L0 %d: status=%d, want %d", txid, resp.StatusCode, want)
		}
	}
}

func enforceRetention(t *testing.T, compactor *litestream.Compactor) {
	t.Helper()
	ctx := context.Background()
	minSnapshotTXID, err := compactor.EnforceSnapshotRetention(ctx, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := compactor.EnforceRetentionByTXID(ctx, 2, minSnapshotTXID); err != nil {
		t.Fatal(err)
	}
	if err := compactor.EnforceL0Retention(ctx, 15*time.Minute); err != nil {
		t.Fatal(err)
	}
}

func hasLTXFile(t *testing.T, client litestream.ReplicaClient, f litestream.HeldFile) bool {
	t.Helper()
	itr, err := client.LTXFiles(context.Background(), f.Level, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	defer itr.Close()
	var files []litestream.HeldFile
	for itr.Next() {
		info := itr.Item()
		files = append(files, litestream.HeldFile{Level: info.Level, Min: uint64(info.MinTXID), Max: uint64(info.MaxTXID)})
	}
	if err := itr.Close(); err != nil {
		t.Fatal(err)
	}
	return slices.Contains(files, f)
}

func listHolds(t *testing.T, client *file.ReplicaClient) []string {
	t.Helper()
	holds, err := client.ListHeads(context.Background(), client.ObjectKey(litestream.HoldsDir))
	if err != nil {
		t.Fatal(err)
	}
	return holds
}
