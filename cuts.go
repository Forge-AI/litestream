package litestream

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

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
	TXID     uint64       `json:"txid"`
	Checksum ltx.Checksum `json:"checksum"`
}

type missedCut struct {
	Missed bool `json:"missed"`
}

type pendingCut struct {
	name string
	at   walPosition
}

type walSpan struct {
	start, end walPosition
	cuts       []pendingCut
	next       *syncInfo
}

func (db *DB) cutsPath() string {
	return filepath.Join(db.metaPath, "cuts")
}

func (db *DB) pendingCuts() ([]pendingCut, error) {
	dir := db.cutsPath()
	ents, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("list cuts: %w", err)
	}
	var cuts []pendingCut
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
		return landedCut{TXID: uint64(file.TXID), Checksum: file.PostApplyChecksum}, nil
	case at == info.ltxEnd || (!info.snapshotting && at == span.start):
		return landedCut{TXID: uint64(prior.TXID), Checksum: prior.PostApplyChecksum}, nil
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
