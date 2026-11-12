package litestream

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"log/slog"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/superfly/ltx"
)

const (
	PageMapDirName     = "map"
	PageMapMagic       = "PMAP"
	PageMapVersion     = 1
	PageMapHeaderSize  = 32
	PageMapFileSize    = 25
	PageMapTrailerSize = 8
)

type PageMapClient interface {
	WritePageMap(ctx context.Context, txID ltx.TXID, data []byte) error
	ReadPageMap(ctx context.Context, txID ltx.TXID) ([]byte, error)
	PageMaps(ctx context.Context) ([]ltx.TXID, error)
	DeletePageMaps(ctx context.Context, txIDs []ltx.TXID) error
}

type PageMap struct {
	TXID     ltx.TXID
	PageSize uint32
	Files    []*ltx.FileInfo
	Pages    []PageMapEntry
}

type PageMapEntry struct {
	File   uint32
	Offset int64
	Size   int64
}

func PageMapDir(root string) string {
	return path.Join(root, PageMapDirName)
}

func PageMapPath(root string, txID ltx.TXID) string {
	return path.Join(PageMapDir(root), FormatPageMapFilename(txID))
}

func FormatPageMapFilename(txID ltx.TXID) string {
	return txID.String() + ".pmap"
}

func ParsePageMapFilename(name string) (ltx.TXID, error) {
	hex, ok := strings.CutSuffix(name, ".pmap")
	if !ok || len(hex) != 16 {
		return 0, fmt.Errorf("not a page map name: %q", name)
	}
	return ltx.ParseTXID(hex)
}

var pageMapCRC = crc64.MakeTable(crc64.ISO)

func (m *PageMap) Encode() []byte {
	b := make([]byte, 0, PageMapHeaderSize+len(m.Files)*PageMapFileSize+len(m.Pages)*7+PageMapTrailerSize)
	b = append(b, PageMapMagic...)
	b = binary.BigEndian.AppendUint32(b, PageMapVersion)
	b = binary.BigEndian.AppendUint32(b, m.PageSize)
	b = binary.BigEndian.AppendUint32(b, uint32(len(m.Pages)))
	b = binary.BigEndian.AppendUint64(b, uint64(m.TXID))
	b = binary.BigEndian.AppendUint32(b, uint32(len(m.Files)))
	b = binary.BigEndian.AppendUint32(b, 0)
	for _, f := range m.Files {
		b = append(b, byte(f.Level))
		b = binary.BigEndian.AppendUint64(b, uint64(f.MinTXID))
		b = binary.BigEndian.AppendUint64(b, uint64(f.MaxTXID))
		b = binary.BigEndian.AppendUint64(b, uint64(f.Size))
	}
	for _, e := range m.Pages {
		b = binary.AppendUvarint(b, uint64(e.File))
		b = binary.AppendUvarint(b, uint64(e.Offset))
		b = binary.AppendUvarint(b, uint64(e.Size))
	}
	return binary.BigEndian.AppendUint64(b, crc64.Checksum(b, pageMapCRC))
}

func DecodePageMap(b []byte) (*PageMap, error) {
	if len(b) < PageMapHeaderSize+PageMapTrailerSize {
		return nil, fmt.Errorf("page map of %d bytes is shorter than its header and trailer", len(b))
	}
	body := b[:len(b)-PageMapTrailerSize]
	if binary.BigEndian.Uint64(b[len(body):]) != crc64.Checksum(body, pageMapCRC) {
		return nil, errors.New("page map checksum mismatch")
	}
	if string(body[:4]) != PageMapMagic {
		return nil, fmt.Errorf("page map magic %q", body[:4])
	}
	if version := binary.BigEndian.Uint32(body[4:]); version != PageMapVersion {
		return nil, fmt.Errorf("page map version %d", version)
	}
	m := &PageMap{
		PageSize: binary.BigEndian.Uint32(body[8:]),
		TXID:     ltx.TXID(binary.BigEndian.Uint64(body[16:])),
		Files:    make([]*ltx.FileInfo, binary.BigEndian.Uint32(body[24:])),
		Pages:    make([]PageMapEntry, binary.BigEndian.Uint32(body[12:])),
	}
	rest := body[PageMapHeaderSize:]
	if len(rest) < len(m.Files)*PageMapFileSize {
		return nil, fmt.Errorf("page map file table of %d files is cut short", len(m.Files))
	}
	for i := range m.Files {
		f := rest[i*PageMapFileSize:]
		m.Files[i] = &ltx.FileInfo{
			Level:   int(f[0]),
			MinTXID: ltx.TXID(binary.BigEndian.Uint64(f[1:])),
			MaxTXID: ltx.TXID(binary.BigEndian.Uint64(f[9:])),
			Size:    int64(binary.BigEndian.Uint64(f[17:])),
		}
	}
	rest = rest[len(m.Files)*PageMapFileSize:]
	for i := range m.Pages {
		var v [3]uint64
		for j := range v {
			x, n := binary.Uvarint(rest)
			if n <= 0 {
				return nil, fmt.Errorf("page map entry of page %d is cut short", i+1)
			}
			v[j], rest = x, rest[n:]
		}
		if v[2] > 0 && v[0] >= uint64(len(m.Files)) {
			return nil, fmt.Errorf("page map entry of page %d names file %d of %d", i+1, v[0], len(m.Files))
		}
		m.Pages[i] = PageMapEntry{File: uint32(v[0]), Offset: int64(v[1]), Size: int64(v[2])}
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("page map has %d bytes past its last page", len(rest))
	}
	return m, nil
}

type pageMapFile struct {
	level    int
	min, max ltx.TXID
}

func pageMapFileOf(info *ltx.FileInfo) pageMapFile {
	return pageMapFile{info.Level, info.MinTXID, info.MaxTXID}
}

func rebasePageMap(prev *PageMap, plan []*ltx.FileInfo, hdr ltx.Header, index func(*ltx.FileInfo) (map[uint32]ltx.PageIndexElem, error)) (*PageMap, error) {
	at := make(map[pageMapFile]int, len(plan))
	for i, info := range plan {
		at[pageMapFileOf(info)] = i
	}
	next := &PageMap{TXID: plan[len(plan)-1].MaxTXID, PageSize: hdr.PageSize, Files: plan, Pages: make([]PageMapEntry, hdr.Commit)}
	known := make(map[pageMapFile]bool)
	dropped := make(map[uint32]bool)
	if prev != nil {
		for _, info := range prev.Files {
			known[pageMapFileOf(info)] = true
		}
		for i, e := range prev.Pages[:min(len(prev.Pages), len(next.Pages))] {
			if e.Size == 0 {
				continue
			}
			file, ok := at[pageMapFileOf(prev.Files[e.File])]
			if !ok {
				dropped[uint32(i)+1] = true
				continue
			}
			next.Pages[i] = PageMapEntry{File: uint32(file), Offset: e.Offset, Size: e.Size}
		}
	}
	for i, info := range plan {
		if known[pageMapFileOf(info)] {
			continue
		}
		elems, err := index(info)
		if err != nil {
			return nil, fmt.Errorf("read the page index of %s: %w", ltx.FormatFilename(info.MinTXID, info.MaxTXID), err)
		}
		for pgno, elem := range elems {
			if pgno > hdr.Commit {
				continue
			}
			e := &next.Pages[pgno-1]
			if e.Size > 0 && int(e.File) > i {
				continue
			}
			*e = PageMapEntry{File: uint32(i), Offset: elem.Offset, Size: elem.Size}
			delete(dropped, pgno)
		}
	}
	if len(dropped) > 0 {
		return rebasePageMap(nil, plan, hdr, index)
	}
	return next, nil
}

func planPageMap(known []*ltx.FileInfo, txID ltx.TXID) []*ltx.FileInfo {
	var plan []*ltx.FileInfo
	var at ltx.TXID
	for _, info := range known {
		if info.Level == SnapshotLevel && info.MaxTXID <= txID && info.MaxTXID > at {
			plan, at = []*ltx.FileInfo{info}, info.MaxTXID
		}
	}
	for at < txID {
		var next *ltx.FileInfo
		for _, info := range known {
			if info.Level == SnapshotLevel || info.MinTXID > at+1 || info.MaxTXID <= at || info.MaxTXID > txID {
				continue
			}
			if next == nil || restoreCandidateBetter(next, info) {
				next = info
			}
		}
		if next == nil {
			return nil
		}
		plan, at = append(plan, next), next.MaxTXID
	}
	return plan
}

type pageMapState struct {
	mu      sync.Mutex
	loaded  bool
	current *PageMap
	known   []*ltx.FileInfo
	written []ltx.TXID
}

func (db *DB) writePageMap(ctx context.Context, upload *ltx.FileInfo) error {
	client, ok := db.Replica.Client.(PageMapClient)
	if !ok {
		return nil
	}
	s := &db.pageMap
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.loaded {
		written, err := client.PageMaps(ctx)
		if err != nil {
			return fmt.Errorf("list page maps: %w", err)
		}
		if len(written) > 0 {
			newest := slices.Max(written)
			b, err := client.ReadPageMap(ctx, newest)
			if err != nil {
				return fmt.Errorf("read page map %s: %w", newest, err)
			}
			if s.current, err = DecodePageMap(b); err != nil {
				return fmt.Errorf("decode page map %s: %w", newest, err)
			}
			s.known = slices.Clone(s.current.Files)
		}
		s.written, s.loaded = written, true
	}
	s.known = append(s.known, upload)
	if s.current != nil && upload.MaxTXID <= s.current.TXID {
		return nil
	}

	plan := planPageMap(s.known, upload.MaxTXID)
	if plan == nil {
		var err error
		if plan, err = CalcRestorePlan(ctx, db.Replica.Client, upload.MaxTXID, time.Time{}, db.Logger); err != nil {
			return fmt.Errorf("plan the page map at %s: %w", upload.MaxTXID, err)
		}
	}
	hdr, err := FetchLTXHeader(ctx, db.Replica.Client, plan[len(plan)-1])
	if err != nil {
		return fmt.Errorf("read the header of the page map's last file: %w", err)
	}
	next, err := rebasePageMap(s.current, plan, hdr, func(info *ltx.FileInfo) (map[uint32]ltx.PageIndexElem, error) {
		return FetchPageIndex(ctx, db.Replica.Client, info)
	})
	if err != nil {
		return err
	}
	if err := client.WritePageMap(ctx, next.TXID, next.Encode()); err != nil {
		return fmt.Errorf("write page map %s: %w", next.TXID, err)
	}
	s.current, s.known = next, slices.Clone(next.Files)
	if !db.RetentionEnabled {
		return nil
	}

	superseded := slices.DeleteFunc(s.written, func(txID ltx.TXID) bool { return txID == next.TXID })
	if err := client.DeletePageMaps(ctx, superseded); err != nil {
		db.Logger.Warn("delete superseded page maps; the next page map retries", "count", len(superseded), slog.Any("error", err))
		s.written = append(superseded, next.TXID)
		return nil
	}
	s.written = []ltx.TXID{next.TXID}
	return nil
}
