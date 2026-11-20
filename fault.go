package litestream

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
)

const (
	segmentPages   = 256
	segmentPresent = 1
)

// segmentFaults fetches a lazy copy's absent segments through the server's
// fault socket before the database file is read raw, since an absent
// segment's pages are holes on disk until it lands.
type segmentFaults struct {
	lane     string
	segments *os.File
	client   *http.Client
	present  uint32
	checked  bool
}

func (db *DB) segmentFaults() (*segmentFaults, error) {
	if db.FaultSocket == "" {
		return nil, nil
	}
	segments, err := os.Open(db.path + "-segments")
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("open segment map: %w", err)
	}
	socket := db.FaultSocket
	return &segmentFaults{
		lane:     db.path,
		segments: segments,
		client: &http.Client{Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", socket)
			},
		}},
	}, nil
}

func (f *segmentFaults) close() {
	if f != nil {
		_ = f.segments.Close()
	}
}

func (f *segmentFaults) ensure(ctx context.Context, pgno uint32) error {
	if f == nil {
		return nil
	}
	segment := (pgno - 1) / segmentPages
	if f.checked && f.present == segment {
		return nil
	}
	present, err := f.landed(segment)
	if err != nil {
		return err
	}
	if !present {
		if err := f.fault(ctx, segment); err != nil {
			return err
		}
		if present, err = f.landed(segment); err != nil {
			return err
		} else if !present {
			return fmt.Errorf("segment %d is still absent after its fault", segment)
		}
	}
	f.present, f.checked = segment, true
	return nil
}

func (f *segmentFaults) landed(segment uint32) (bool, error) {
	var state [4]byte
	_, err := f.segments.ReadAt(state[:], int64(segment)*int64(len(state)))
	if errors.Is(err, io.EOF) {
		return true, nil
	} else if err != nil {
		return false, fmt.Errorf("read segment %d state: %w", segment, err)
	}
	return binary.LittleEndian.Uint32(state[:])&segmentPresent != 0, nil
}

func (f *segmentFaults) fault(ctx context.Context, segment uint32) error {
	body, err := json.Marshal(struct {
		Lane    string `json:"lane"`
		Segment uint32 `json:"segment"`
	}{f.lane, segment})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://faults/fault", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := f.client.Do(req)
	if err != nil {
		return fmt.Errorf("fault segment %d: %w", segment, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		reason, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("fault segment %d: %s: %s", segment, resp.Status, bytes.TrimSpace(reason))
	}
	return nil
}
