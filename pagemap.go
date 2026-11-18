package litestream

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream/internal"
)

const (
	PageMapName        = "map.runs"
	PageMapMagic       = "RUNS"
	PageMapVersion     = 2
	PageMapHeaderSize  = 32
	PageMapFileSize    = 33
	PageMapRunSize     = 16
	PageMapTrailerSize = 8

	pageMapRunPages    = 256
	pageMapRunMaxSize  = 1<<24 - 1
	retiredPageMapName = "map.pmap"
)

type PageMapClient interface {
	HeadClient
	ObjectKey(name string) string
	DeleteHead(ctx context.Context, key string) error
}

var pageMapCRC = crc64.MakeTable(crc64.ISO)

type pageMapRuns struct {
	w            io.Writer
	first, pages uint32
	offset, size int64
	records      uint64
}

func (r *pageMapRuns) add(pgno uint32, offset, size int64) error {
	if r.pages > 0 && pgno == r.first+r.pages && offset == r.offset+r.size && (pgno-1)/pageMapRunPages == (r.first-1)/pageMapRunPages && r.size+size <= pageMapRunMaxSize {
		r.pages, r.size = r.pages+1, r.size+size
		return nil
	}
	if err := r.flush(); err != nil {
		return err
	}
	r.first, r.pages, r.offset, r.size = pgno, 1, offset, size
	return nil
}

func (r *pageMapRuns) flush() error {
	if r.pages == 0 {
		return nil
	}
	var b [PageMapRunSize]byte
	binary.LittleEndian.PutUint32(b[0:], r.first)
	binary.LittleEndian.PutUint32(b[4:], r.pages-1|uint32(r.size)<<8)
	binary.LittleEndian.PutUint64(b[8:], uint64(r.offset))
	r.records++
	_, err := r.w.Write(b[:])
	return err
}

func encodePageMap(f *os.File, hdr ltx.Header, plan []*ltx.FileInfo, section func(io.Writer, *ltx.FileInfo) (uint64, error)) (int64, error) {
	head := make([]byte, 0, PageMapHeaderSize+len(plan)*PageMapFileSize)
	head = append(head, PageMapMagic...)
	head = binary.LittleEndian.AppendUint32(head, PageMapVersion)
	head = binary.LittleEndian.AppendUint32(head, hdr.PageSize)
	head = binary.LittleEndian.AppendUint32(head, hdr.Commit)
	head = binary.LittleEndian.AppendUint64(head, uint64(plan[len(plan)-1].MaxTXID))
	head = binary.LittleEndian.AppendUint32(head, uint32(len(plan)))
	head = binary.LittleEndian.AppendUint32(head, 0)
	for _, info := range plan {
		head = append(head, byte(info.Level))
		head = binary.LittleEndian.AppendUint64(head, uint64(info.MinTXID))
		head = binary.LittleEndian.AppendUint64(head, uint64(info.MaxTXID))
		head = binary.LittleEndian.AppendUint64(head, uint64(info.Size))
		head = binary.LittleEndian.AppendUint64(head, 0)
	}
	w := bufio.NewWriter(f)
	if _, err := w.Write(head); err != nil {
		return 0, err
	}
	for i, info := range plan {
		records, err := section(w, info)
		if err != nil {
			return 0, err
		}
		binary.LittleEndian.PutUint64(head[PageMapHeaderSize+i*PageMapFileSize+25:], records)
	}
	if err := w.Flush(); err != nil {
		return 0, err
	}
	if _, err := f.WriteAt(head, 0); err != nil {
		return 0, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	crc := crc64.New(pageMapCRC)
	n, err := io.Copy(crc, f)
	if err != nil {
		return 0, err
	}
	if _, err := f.Write(binary.LittleEndian.AppendUint64(nil, crc.Sum64())); err != nil {
		return 0, err
	}
	return n + PageMapTrailerSize, nil
}

type pageMapSection struct {
	info    *ltx.FileInfo
	at      int64
	records uint64
}

func readPageMapTable(r io.ReaderAt, size int64) (ltx.TXID, []pageMapSection, error) {
	hdr := make([]byte, PageMapHeaderSize)
	if _, err := r.ReadAt(hdr, 0); err != nil {
		return 0, nil, fmt.Errorf("read the header: %w", err)
	}
	if string(hdr[:4]) != PageMapMagic {
		return 0, nil, fmt.Errorf("magic %q", hdr[:4])
	}
	if version := binary.LittleEndian.Uint32(hdr[4:]); version != PageMapVersion {
		return 0, nil, fmt.Errorf("version %d", version)
	}
	table := make([]byte, int(binary.LittleEndian.Uint32(hdr[24:]))*PageMapFileSize)
	if _, err := r.ReadAt(table, PageMapHeaderSize); err != nil {
		return 0, nil, fmt.Errorf("read the file table: %w", err)
	}
	sections := make([]pageMapSection, len(table)/PageMapFileSize)
	at := int64(PageMapHeaderSize + len(table))
	for i := range sections {
		e := table[i*PageMapFileSize:]
		sections[i] = pageMapSection{
			info: &ltx.FileInfo{
				Level:   int(e[0]),
				MinTXID: ltx.TXID(binary.LittleEndian.Uint64(e[1:])),
				MaxTXID: ltx.TXID(binary.LittleEndian.Uint64(e[9:])),
				Size:    int64(binary.LittleEndian.Uint64(e[17:])),
			},
			at:      at,
			records: binary.LittleEndian.Uint64(e[25:]),
		}
		at += int64(sections[i].records) * PageMapRunSize
	}
	if at+PageMapTrailerSize != size {
		return 0, nil, fmt.Errorf("%d bytes, want %d for its %d files", size, at+PageMapTrailerSize, len(sections))
	}
	return ltx.TXID(binary.LittleEndian.Uint64(hdr[16:])), sections, nil
}

func samePageMapFile(a, b *ltx.FileInfo) bool {
	return a.Level == b.Level && a.MinTXID == b.MinTXID && a.MaxTXID == b.MaxTXID
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
	mu       sync.Mutex
	unmapped bool
}

func (db *DB) writePageMap(ctx context.Context, upload *ltx.FileInfo) error {
	client, ok := db.Replica.Client.(PageMapClient)
	if !ok {
		return nil
	}
	s := &db.pageMap
	s.mu.Lock()
	defer s.mu.Unlock()

	path := filepath.Join(db.MetaPath(), PageMapName)
	last, err := s.open(ctx, client, path)
	if err != nil {
		return err
	}
	txID := upload.MaxTXID
	var sections []pageMapSection
	if last != nil {
		defer func() { _ = last.Close() }()
		fi, err := last.Stat()
		if err != nil {
			return fmt.Errorf("stat the last page map: %w", err)
		}
		lastTXID, lastSections, err := readPageMapTable(last, fi.Size())
		if err != nil {
			return fmt.Errorf("read the last page map: %w", err)
		}
		txID, sections = max(txID, lastTXID), lastSections
	}
	known := make([]*ltx.FileInfo, 0, len(sections)+1)
	for _, sec := range sections {
		if !samePageMapFile(sec.info, upload) {
			known = append(known, sec.info)
		}
	}

	plan := planPageMap(append(known, upload), txID)
	if plan == nil && last == nil && db.BaseTXID > 0 {
		return nil
	}
	if plan == nil {
		if plan, err = CalcRestorePlan(ctx, db.Replica.Client, txID, time.Time{}, db.Logger); err != nil {
			return fmt.Errorf("plan the page map at %s: %w", txID, err)
		}
	}
	hdr, err := FetchLTXHeader(ctx, db.Replica.Client, plan[len(plan)-1])
	if err != nil {
		return fmt.Errorf("read the header of the page map's last file: %w", err)
	}

	tmp, err := os.Create(path + ".tmp")
	if err != nil {
		return fmt.Errorf("create the page map: %w", err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	defer func() { _ = tmp.Close() }()
	size, err := encodePageMap(tmp, hdr, plan, func(w io.Writer, info *ltx.FileInfo) (uint64, error) {
		if i := slices.IndexFunc(sections, func(sec pageMapSection) bool { return samePageMapFile(sec.info, info) && sec.info.Size == info.Size }); i >= 0 && !samePageMapFile(info, upload) {
			_, err := io.Copy(w, io.NewSectionReader(last, sections[i].at, int64(sections[i].records)*PageMapRunSize))
			return sections[i].records, err
		}
		runs := &pageMapRuns{w: w}
		if err := scanPageIndex(ctx, db.Replica.Client, info, runs.add); err != nil {
			return 0, fmt.Errorf("read the page index of %s: %w", ltx.FormatFilename(info.MinTXID, info.MaxTXID), err)
		}
		if err := runs.flush(); err != nil {
			return 0, err
		}
		return runs.records, nil
	})
	if err != nil {
		return fmt.Errorf("encode the page map at %s: %w", plan[len(plan)-1].MaxTXID, err)
	}
	if err := client.WriteHeadFrom(ctx, client.ObjectKey(PageMapName), io.NewSectionReader(tmp, 0, size)); err != nil {
		return fmt.Errorf("write the page map at %s: %w", plan[len(plan)-1].MaxTXID, err)
	}
	if err := keepPageMap(tmp, path); err != nil {
		return err
	}
	if last != nil {
		return nil
	}
	s.unmapped = false
	if err := client.DeleteHead(ctx, client.ObjectKey(retiredPageMapName)); err != nil {
		return fmt.Errorf("delete the retired page map: %w", err)
	}
	return nil
}

func (s *pageMapState) open(ctx context.Context, client PageMapClient, path string) (*os.File, error) {
	f, err := os.Open(path)
	if err == nil {
		return f, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("open the last page map: %w", err)
	}
	if s.unmapped {
		return nil, nil
	}
	b, err := client.ReadHead(ctx, client.ObjectKey(PageMapName))
	if errors.Is(err, os.ErrNotExist) {
		s.unmapped = true
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("read the page map: %w", err)
	}
	body := len(b) - PageMapTrailerSize
	if body < 0 || binary.LittleEndian.Uint64(b[body:]) != crc64.Checksum(b[:body], pageMapCRC) {
		return nil, fmt.Errorf("the page map of %d bytes fails its checksum", len(b))
	}
	tmp, err := os.Create(path + ".tmp")
	if err != nil {
		return nil, fmt.Errorf("stage the page map: %w", err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	defer func() { _ = tmp.Close() }()
	if _, err := tmp.Write(b); err != nil {
		return nil, fmt.Errorf("stage the page map: %w", err)
	}
	if err := keepPageMap(tmp, path); err != nil {
		return nil, err
	}
	return os.Open(path)
}

func keepPageMap(f *os.File, path string) error {
	if err := internal.Fsync(f); err != nil {
		return fmt.Errorf("sync the page map: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close the page map: %w", err)
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return fmt.Errorf("keep the page map: %w", err)
	}
	return nil
}
