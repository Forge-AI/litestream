package litestream

import (
	"bufio"
	"bytes"
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

	PageMapUploadsPerPublish = 8

	pageMapRunPages    = 256
	pageMapRunMaxSize  = 1<<24 - 1
	pageMapPublished   = "map.runs.published"
	retiredPageMapName = "map.pmap"
)

var ErrHeadMoved = errors.New("the object is not at the version the write expects")

type PageMapClient interface {
	HeadClient
	ObjectKey(name string) string
	DeleteHead(ctx context.Context, key string) error
	ReadHeadVersion(ctx context.Context, key string) ([]byte, string, error)
	WriteHeadIf(ctx context.Context, key string, r *io.SectionReader, version string) (string, error)
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

type pageMapLast struct {
	f        *os.File
	txID     ltx.TXID
	pageSize uint32
	commit   uint32
	sections []pageMapSection
}

func (last pageMapLast) known(upload *ltx.FileInfo) ([]*ltx.FileInfo, ltx.TXID) {
	known := make([]*ltx.FileInfo, 0, len(last.sections)+1)
	for _, sec := range last.sections {
		if !samePageMapFile(sec.info, upload) {
			known = append(known, sec.info)
		}
	}
	return append(known, upload), max(upload.MaxTXID, last.txID)
}

func readPageMap(r io.ReaderAt, size int64) (pageMapLast, error) {
	if size < PageMapHeaderSize+PageMapTrailerSize {
		return pageMapLast{}, fmt.Errorf("%d bytes is shorter than a header and a trailer", size)
	}
	crc := crc64.New(pageMapCRC)
	if _, err := io.Copy(crc, io.NewSectionReader(r, 0, size-PageMapTrailerSize)); err != nil {
		return pageMapLast{}, fmt.Errorf("read the body: %w", err)
	}
	trailer := make([]byte, PageMapTrailerSize)
	if _, err := r.ReadAt(trailer, size-PageMapTrailerSize); err != nil {
		return pageMapLast{}, fmt.Errorf("read the trailer: %w", err)
	}
	if binary.LittleEndian.Uint64(trailer) != crc.Sum64() {
		return pageMapLast{}, fmt.Errorf("%d bytes fail their checksum", size)
	}
	hdr := make([]byte, PageMapHeaderSize)
	if _, err := r.ReadAt(hdr, 0); err != nil {
		return pageMapLast{}, fmt.Errorf("read the header: %w", err)
	}
	if string(hdr[:4]) != PageMapMagic {
		return pageMapLast{}, fmt.Errorf("magic %q", hdr[:4])
	}
	if version := binary.LittleEndian.Uint32(hdr[4:]); version != PageMapVersion {
		return pageMapLast{}, fmt.Errorf("version %d", version)
	}
	table := make([]byte, int(binary.LittleEndian.Uint32(hdr[24:]))*PageMapFileSize)
	if _, err := r.ReadAt(table, PageMapHeaderSize); err != nil {
		return pageMapLast{}, fmt.Errorf("read the file table: %w", err)
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
		return pageMapLast{}, fmt.Errorf("%d bytes, want %d for its %d files", size, at+PageMapTrailerSize, len(sections))
	}
	return pageMapLast{
		txID:     ltx.TXID(binary.LittleEndian.Uint64(hdr[16:])),
		pageSize: binary.LittleEndian.Uint32(hdr[8:]),
		commit:   binary.LittleEndian.Uint32(hdr[12:]),
		sections: sections,
	}, nil
}

func samePageMapFile(a, b *ltx.FileInfo) bool {
	return a.Level == b.Level && a.MinTXID == b.MinTXID && a.MaxTXID == b.MaxTXID
}

func planPageMap(known []*ltx.FileInfo, base, txID ltx.TXID) []*ltx.FileInfo {
	var plan []*ltx.FileInfo
	at := base
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
	retired  bool
}

func (db *DB) writePageMap(ctx context.Context, upload *ltx.FileInfo, local *os.File, always bool) error {
	client, ok := db.Replica.Client.(PageMapClient)
	if !ok {
		return nil
	}
	s := &db.pageMap
	s.mu.Lock()
	defer s.mu.Unlock()

	path := filepath.Join(db.MetaPath(), PageMapName)
	last, err := openLocalPageMap(path)
	if err != nil {
		return err
	}
	known, txID := last.known(upload)
	plan := planPageMap(known, db.BaseTXID, txID)
	if plan == nil && last.f == nil && !s.unmapped {
		if last, err = s.fetch(ctx, client, db.MetaPath()); err != nil {
			return err
		}
		known, txID = last.known(upload)
		plan = planPageMap(known, db.BaseTXID, txID)
	}
	defer func() { _ = last.f.Close() }()
	if plan == nil {
		for level := 0; level <= SnapshotLevel; level++ {
			itr, err := db.Replica.Client.LTXFiles(ctx, level, 0, false)
			if err != nil {
				return fmt.Errorf("list level %d for the page map at %s: %w", level, txID, err)
			}
			listed, err := ltx.SliceFileIterator(itr)
			if err != nil {
				return fmt.Errorf("list level %d for the page map at %s: %w", level, txID, err)
			}
			known = append(known, listed...)
		}
		if plan = planPageMap(known, db.BaseTXID, txID); plan == nil {
			return fmt.Errorf("no chain of the replica's files reaches %s from %s", txID, db.BaseTXID)
		}
	}

	newest := plan[len(plan)-1]
	hdr := ltx.Header{PageSize: last.pageSize, Commit: last.commit}
	switch {
	case local != nil && samePageMapFile(newest, upload):
		if hdr, _, err = ltx.PeekHeader(io.NewSectionReader(local, 0, ltx.HeaderSize)); err != nil {
			return fmt.Errorf("read the header of the uploaded file: %w", err)
		}
	case last.f == nil || newest.MaxTXID != last.txID:
		if hdr, err = FetchLTXHeader(ctx, db.Replica.Client, newest); err != nil {
			return fmt.Errorf("read the header of the page map's last file: %w", err)
		}
	}

	tmp, err := os.CreateTemp(db.MetaPath(), PageMapName+".*.tmp")
	if err != nil {
		return fmt.Errorf("create the page map: %w", err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	defer func() { _ = tmp.Close() }()
	size, err := encodePageMap(tmp, hdr, plan, func(w io.Writer, info *ltx.FileInfo) (uint64, error) {
		if i := slices.IndexFunc(last.sections, func(sec pageMapSection) bool { return samePageMapFile(sec.info, info) && sec.info.Size == info.Size }); i >= 0 && !samePageMapFile(info, upload) {
			sec := last.sections[i]
			_, err := io.Copy(w, io.NewSectionReader(last.f, sec.at, int64(sec.records)*PageMapRunSize))
			return sec.records, err
		}
		runs := &pageMapRuns{w: w}
		var err error
		if local != nil && samePageMapFile(info, upload) {
			err = scanLocalPageIndex(local, info.Size, runs.add)
		} else {
			err = scanPageIndex(ctx, db.Replica.Client, info, runs.add)
		}
		if err != nil {
			return 0, fmt.Errorf("read the page index of %s: %w", ltx.FormatFilename(info.MinTXID, info.MaxTXID), err)
		}
		if err := runs.flush(); err != nil {
			return 0, err
		}
		return runs.records, nil
	})
	if err != nil {
		return fmt.Errorf("encode the page map at %s: %w", newest.MaxTXID, err)
	}
	_ = last.f.Close()

	published, version, err := readPageMapPublished(db.MetaPath())
	if err != nil {
		return err
	}
	if published <= newest.MaxTXID && (always || newest.MaxTXID >= published+PageMapUploadsPerPublish) {
		err = s.publish(ctx, client, db.MetaPath(), io.NewSectionReader(tmp, 0, size), newest.MaxTXID, version)
	}
	if err := keepPageMap(tmp, path); err != nil {
		return err
	}
	s.unmapped = false
	return err
}

func (s *pageMapState) publish(ctx context.Context, client PageMapClient, dir string, r *io.SectionReader, txID ltx.TXID, version string) error {
	// TODO: delete this once no map.pmap writer can run.
	if !s.retired {
		if err := client.DeleteHead(ctx, client.ObjectKey(retiredPageMapName)); err != nil {
			return fmt.Errorf("delete the retired page map: %w", err)
		}
		s.retired = true
	}
	key := client.ObjectKey(PageMapName)
	written, err := client.WriteHeadIf(ctx, key, io.NewSectionReader(r, 0, r.Size()), version)
	if errors.Is(err, ErrHeadMoved) {
		b, current, readErr := client.ReadHeadVersion(ctx, key)
		switch {
		case errors.Is(readErr, os.ErrNotExist):
			current = ""
		case readErr != nil:
			return fmt.Errorf("read the page map that moved: %w", readErr)
		default:
			if theirs, err := readPageMap(bytes.NewReader(b), int64(len(b))); err == nil && theirs.txID > txID {
				return writePageMapPublished(dir, theirs.txID, current)
			}
		}
		written, err = client.WriteHeadIf(ctx, key, io.NewSectionReader(r, 0, r.Size()), current)
	}
	if err != nil {
		return fmt.Errorf("write the page map at %s: %w", txID, err)
	}
	return writePageMapPublished(dir, txID, written)
}

func (s *pageMapState) fetch(ctx context.Context, client PageMapClient, dir string) (pageMapLast, error) {
	b, version, err := client.ReadHeadVersion(ctx, client.ObjectKey(PageMapName))
	if errors.Is(err, os.ErrNotExist) {
		s.unmapped = true
		return pageMapLast{}, nil
	} else if err != nil {
		return pageMapLast{}, fmt.Errorf("read the page map: %w", err)
	}
	last, err := readPageMap(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		return pageMapLast{}, fmt.Errorf("the bucket's page map: %w", err)
	}
	path := filepath.Join(dir, PageMapName)
	if err := keepPageMapBytes(dir, path, b); err != nil {
		return pageMapLast{}, err
	}
	if err := writePageMapPublished(dir, last.txID, version); err != nil {
		return pageMapLast{}, err
	}
	last.f, err = os.Open(path)
	return last, err
}

func openLocalPageMap(path string) (pageMapLast, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return pageMapLast{}, nil
	} else if err != nil {
		return pageMapLast{}, fmt.Errorf("open the last page map: %w", err)
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return pageMapLast{}, err
	}
	last, err := readPageMap(f, fi.Size())
	if err == nil {
		last.f = f
		return last, nil
	}
	_ = f.Close()
	if err := os.Remove(path); err != nil {
		return pageMapLast{}, fmt.Errorf("remove the invalid local page map: %w", err)
	}
	return pageMapLast{}, nil
}

func scanLocalPageIndex(f *os.File, size int64, fn func(pgno uint32, offset, size int64) error) error {
	b := make([]byte, 8)
	if _, err := f.ReadAt(b, size-ltx.TrailerSize-8); err != nil {
		return fmt.Errorf("read the page index size: %w", err)
	}
	n := int64(binary.BigEndian.Uint64(b))
	return readPageIndex(bufio.NewReader(io.NewSectionReader(f, size-ltx.TrailerSize-8-n, n)), fn)
}

func readPageMapPublished(dir string) (ltx.TXID, string, error) {
	b, err := os.ReadFile(filepath.Join(dir, pageMapPublished))
	if errors.Is(err, os.ErrNotExist) {
		return 0, "", nil
	} else if err != nil {
		return 0, "", fmt.Errorf("read the published page map's version: %w", err)
	}
	return ltx.TXID(binary.LittleEndian.Uint64(b)), string(b[8:]), nil
}

func writePageMapPublished(dir string, txID ltx.TXID, version string) error {
	return keepPageMapBytes(dir, filepath.Join(dir, pageMapPublished), append(binary.LittleEndian.AppendUint64(nil, uint64(txID)), version...))
}

func keepPageMapBytes(dir, path string, b []byte) error {
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".*.tmp")
	if err != nil {
		return fmt.Errorf("stage %s: %w", filepath.Base(path), err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	defer func() { _ = tmp.Close() }()
	if _, err := tmp.Write(b); err != nil {
		return fmt.Errorf("stage %s: %w", filepath.Base(path), err)
	}
	return keepPageMap(tmp, path)
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
