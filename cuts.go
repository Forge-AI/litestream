package litestream

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream/internal"
)

type walPosition struct {
	Salt1  uint32 `json:"salt1"`
	Salt2  uint32 `json:"salt2"`
	Offset int64  `json:"offset"`
}

func (p walPosition) sameWAL(other walPosition) bool {
	return p.Salt1 == other.Salt1 && p.Salt2 == other.Salt2
}

type landedCut struct {
	TXID uint64       `json:"txid"`
	File ltx.Checksum `json:"file"`
}

type missedCut struct {
	Missed bool `json:"missed"`
}

type pendingCut struct {
	name string
	at   walPosition
	atMS int64
}

type walSpan struct {
	start, end walPosition
	cuts       []pendingCut
	next       *syncInfo
}

func (db *DB) cutsPath() string {
	return filepath.Join(db.metaPath, "cuts")
}

func (db *DB) pendingCuts(start walPosition) ([]pendingCut, error) {
	cuts := db.secondCuts(start)
	dir := db.cutsPath()
	ents, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return cuts, nil
	} else if err != nil {
		return nil, fmt.Errorf("list cuts: %w", err)
	}
	for _, ent := range ents {
		if strings.HasSuffix(ent.Name(), ".tmp") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, ent.Name()))
		if errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, fmt.Errorf("read cut %s: %w", ent.Name(), err)
		}
		var at walPosition
		if err := json.Unmarshal(b, &at); err != nil {
			return nil, fmt.Errorf("decode cut %s: %w", ent.Name(), err)
		}
		if at.Offset != 0 {
			cuts = append(cuts, pendingCut{name: ent.Name(), at: at})
		}
	}
	return cuts, nil
}

func (db *DB) settleCuts(span walSpan, info syncInfo, prior ltx.Pos, file *ltx.Pos) error {
	var settled bool
	for _, cut := range span.cuts {
		if cut.name == "" {
			continue
		}
		verdict, err := db.cutVerdict(cut.at, span, info, prior, file)
		if err != nil {
			return err
		} else if verdict == nil {
			continue
		}
		if err := db.writeCut(cut.name, verdict); err != nil {
			return err
		}
		settled = true
	}
	if !settled {
		return nil
	}
	if err := internal.FsyncUploadDir(db.cutsPath()); err != nil {
		return fmt.Errorf("sync cuts dir: %w", err)
	}
	return nil
}

func (db *DB) cutVerdict(at walPosition, span walSpan, info syncInfo, prior ltx.Pos, file *ltx.Pos) (any, error) {
	switch {
	case file != nil && at == span.end:
		return db.landedCut(file.TXID)
	case at == info.ltxEnd || (!info.snapshotting && at == span.start):
		return db.landedCut(prior.TXID)
	case at.sameWAL(span.end) && at.Offset > span.end.Offset:
		return nil, nil
	case at.sameWAL(span.end):
		return missedCut{Missed: true}, nil
	}
	hdr, err := readWALHeader(db.WALPath())
	if err != nil {
		return nil, fmt.Errorf("read wal header for a cut: %w", err)
	}
	if at.sameWAL(walPosition{Salt1: binary.BigEndian.Uint32(hdr[16:]), Salt2: binary.BigEndian.Uint32(hdr[20:])}) {
		return nil, nil
	}
	return missedCut{Missed: true}, nil
}

func (db *DB) landedCut(txid ltx.TXID) (any, error) {
	dir := db.LTXLevelDir(0)
	ents, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return missedCut{Missed: true}, nil
	} else if err != nil {
		return nil, fmt.Errorf("list level-0 files for a cut: %w", err)
	}
	for _, ent := range ents {
		if _, maxTXID, err := ltx.ParseFilename(ent.Name()); err != nil || maxTXID != txid {
			continue
		}
		file, err := readLTXTrailer(filepath.Join(dir, ent.Name()))
		if err != nil {
			return nil, err
		}
		return landedCut{TXID: uint64(txid), File: file}, nil
	}
	return missedCut{Missed: true}, nil
}

func readLTXTrailer(path string) (ltx.Checksum, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open the cut's ltx file: %w", err)
	}
	defer func() { _ = f.Close() }()
	fi, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat the cut's ltx file: %w", err)
	}
	b := make([]byte, ltx.TrailerSize)
	if _, err := f.ReadAt(b, fi.Size()-ltx.TrailerSize); err != nil {
		return 0, fmt.Errorf("read the cut's ltx trailer: %w", err)
	}
	var trailer ltx.Trailer
	if err := trailer.UnmarshalBinary(b); err != nil {
		return 0, fmt.Errorf("decode the cut's ltx trailer: %w", err)
	}
	return trailer.FileChecksum, nil
}

func (db *DB) writeCut(name string, verdict any) error {
	b, err := json.Marshal(verdict)
	if err != nil {
		return err
	}
	path := filepath.Join(db.cutsPath(), name)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create cut %s: %w", name, err)
	}
	defer func() { _ = os.Remove(tmp) }()
	defer func() { _ = f.Close() }()
	if _, err := f.Write(b); err != nil {
		return fmt.Errorf("write cut %s: %w", name, err)
	}
	if err := internal.Fsync(f); err != nil {
		return fmt.Errorf("sync cut %s: %w", name, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close cut %s: %w", name, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename cut %s: %w", name, err)
	}
	return nil
}

// Cut names where the last write of one second ends: the WAL holds Frames
// frames under Salt once that write commits, and AtMS is the write's time, as
// the server writing the database recorded it. A database sent a cut ends each
// LTX file at one from then on, so no file holds the writes of two seconds and
// each file's timestamp falls in the second of its last write.
type Cut struct {
	Salt   uint64 `json:"salt"`
	Frames uint32 `json:"frames"`
	AtMS   int64  `json:"at_ms"`
}

type cuts struct {
	sync.Mutex
	on       bool
	pending  []Cut
	baseMS   int64
	stampMS  int64
	flushing int
}

// AddCuts queues cuts for the syncs that follow.
func (db *DB) AddCuts(add []Cut) {
	if len(add) == 0 {
		return
	}
	db.cuts.Lock()
	defer db.cuts.Unlock()
	db.cuts.on = true
	db.cuts.pending = append(db.cuts.pending, add...)
}

func (db *DB) secondCuts(start walPosition) []pendingCut {
	db.cuts.Lock()
	defer db.cuts.Unlock()
	db.passCuts(start)
	cuts := make([]pendingCut, 0, len(db.cuts.pending))
	for _, cut := range db.cuts.pending {
		cuts = append(cuts, pendingCut{at: db.cutPosition(cut), atMS: cut.AtMS})
	}
	return cuts
}

func (db *DB) stampSpan(span walSpan, snapshotting bool, now time.Time) (time.Time, bool) {
	db.cuts.Lock()
	defer db.cuts.Unlock()
	if !db.cuts.on {
		return now, true
	}
	at := now.UnixMilli()
	if snapshotting {
		return time.UnixMilli(max(at, db.cuts.stampMS)), true
	}
	var next, forked bool
	var nearest int64
	for _, cut := range span.cuts {
		switch {
		case cut.atMS == 0:
			forked = forked || cut.at == span.end
		case cut.at.sameWAL(span.end) && cut.at.Offset >= span.end.Offset && (!next || cut.at.Offset < nearest):
			next, nearest, at = true, cut.at.Offset, cut.atMS
		}
	}
	if !next && !forked && db.cuts.flushing == 0 && db.cuts.baseMS/1000 != at/1000 {
		return time.Time{}, false
	}
	return time.UnixMilli(max(at, db.cuts.stampMS)), true
}

func (db *DB) cutWritten(end walPosition, at time.Time) {
	db.cuts.Lock()
	defer db.cuts.Unlock()
	db.cuts.stampMS = max(db.cuts.stampMS, at.UnixMilli())
	db.passCuts(end)
}

func (db *DB) passCuts(through walPosition) {
	passed := -1
	for i, cut := range db.cuts.pending {
		if at := db.cutPosition(cut); at.sameWAL(through) && at.Offset <= through.Offset {
			passed = i
			db.cuts.baseMS = max(db.cuts.baseMS, cut.AtMS)
		}
	}
	db.cuts.pending = db.cuts.pending[passed+1:]
}

func (db *DB) cutPosition(cut Cut) walPosition {
	frameSize := int64(WALFrameHeaderSize + db.pageSize)
	return walPosition{Salt1: uint32(cut.Salt >> 32), Salt2: uint32(cut.Salt), Offset: WALHeaderSize + int64(cut.Frames)*frameSize}
}

func (db *DB) flushCuts() func() {
	db.cuts.Lock()
	db.cuts.flushing++
	db.cuts.Unlock()
	return func() {
		db.cuts.Lock()
		db.cuts.flushing--
		db.cuts.Unlock()
	}
}
