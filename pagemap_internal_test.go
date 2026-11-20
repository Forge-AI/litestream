package litestream

import (
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/superfly/ltx"
)

const pageMapExample = "52554e53020000000010000058020000090000000000000002000000000000000901000000000000000500000000000000e093040000000000020000000000000000060000000000000009000000000000008813000000000000020000000000000001000000ff00e8036400000000000000010100002be0ab0064e8030000000000020000000140060064000000000000005802000000b00400a40600000000000054f0a6ab734304d1"

func TestPageMap_WritesTheSpecExampleByteForByte(t *testing.T) {
	type frame struct {
		pgno         uint32
		offset, size int64
	}
	var snapshot []frame
	for pgno := uint32(1); pgno <= 300; pgno++ {
		snapshot = append(snapshot, frame{pgno, 100 + int64(pgno-1)*1000, 1000})
	}
	frames := map[ltx.TXID][]frame{
		5: snapshot,
		9: {{2, 100, 700}, {3, 800, 900}, {600, 1700, 1200}},
	}
	plan := []*ltx.FileInfo{
		{Level: SnapshotLevel, MinTXID: 1, MaxTXID: 5, Size: 300000},
		{Level: 0, MinTXID: 6, MaxTXID: 9, Size: 5000},
	}

	f, err := os.Create(filepath.Join(t.TempDir(), PageMapName))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	size, err := encodePageMap(f, ltx.Header{PageSize: 4096, Commit: 600}, plan, func(w io.Writer, info *ltx.FileInfo) (uint64, error) {
		runs := &pageMapRuns{w: w}
		for _, fr := range frames[info.MaxTXID] {
			if err := runs.add(fr.pgno, fr.offset, fr.size); err != nil {
				return 0, err
			}
		}
		if err := runs.flush(); err != nil {
			return 0, err
		}
		return runs.records, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(b); got != pageMapExample || size != int64(len(b)) {
		t.Fatalf("encoded %d bytes, reported %d:\n%s\nwant 170:\n%s", len(b), size, got, pageMapExample)
	}

	last, err := readPageMap(f, size)
	if err != nil {
		t.Fatal(err)
	}
	if sections := last.sections; last.txID != 9 || last.pageSize != 4096 || last.commit != 600 || len(sections) != 2 || sections[0].at != 98 || sections[0].records != 2 || sections[1].at != 130 || sections[1].records != 2 || *sections[1].info != *plan[1] {
		t.Fatalf("read the example as txid %s, %d-byte pages and %d pages over %+v, want txid 9, 4096-byte pages and 600 pages with two sections of two runs at 98 and 130", last.txID, last.pageSize, last.commit, sections)
	}
}
