package litestream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"slices"
	"sync"

	"github.com/superfly/ltx"
)

// HoldsDir is the folder of a replica that holds one object per child
// database whose base record reads this replica's files.
const HoldsDir = "holds"

// BaseName is a child replica's base record: the parent files its chain reads
// through the fork's TXID.
const BaseName = "base"

var ErrHeldFileGone = errors.New("a file the hold names is gone")

// HeadClient reads and writes small objects at full keys.
type HeadClient interface {
	WriteHead(ctx context.Context, key string, text []byte) error
	ReadHead(ctx context.Context, key string) ([]byte, error)
}

// HoldClient is a replica client that keeps holds beside its LTX files.
type HoldClient interface {
	HeadClient
	ObjectKey(name string) string
	ListHeads(ctx context.Context, dir string) ([]string, error)
	DeleteHead(ctx context.Context, key string) error
}

// Hold keeps Files, and this replica's own base record when Base is set, until
// the child's base record at ChildBase is gone.
type Hold struct {
	ChildBase string     `json:"child_base"`
	Files     []HeldFile `json:"files"`
	Base      bool       `json:"base,omitempty"`
}

type HeldFile struct {
	Level int    `json:"level"`
	Min   uint64 `json:"min"`
	Max   uint64 `json:"max"`
}

func (h Hold) names(info *ltx.FileInfo) bool {
	for _, f := range h.Files {
		if f.Level == info.Level && ltx.TXID(f.Min) == info.MinTXID && ltx.TXID(f.Max) == info.MaxTXID {
			return true
		}
	}
	return false
}

type holdState struct {
	mu     sync.Mutex
	loaded bool
	byKey  map[string]Hold
}

// Hold records hold under name and keeps what it names from every later
// retention pass. It refuses with ErrHeldFileGone when retention already
// deleted any of it.
func (c *Compactor) Hold(ctx context.Context, name string, hold Hold) error {
	if name == "" || path.Base(name) != name {
		return fmt.Errorf("hold name %q is not one path segment", name)
	}
	client, ok := c.client.(HoldClient)
	if !ok {
		return fmt.Errorf("a %s replica keeps no holds", c.client.Type())
	}
	c.holds.mu.Lock()
	defer c.holds.mu.Unlock()
	if err := c.loadHolds(ctx, client); err != nil {
		return err
	}
	if err := c.present(ctx, client, hold); err != nil {
		return err
	}
	b, err := json.Marshal(hold)
	if err != nil {
		return err
	}
	key := client.ObjectKey(path.Join(HoldsDir, name))
	if err := client.WriteHead(ctx, key, b); err != nil {
		return fmt.Errorf("write hold %s: %w", key, err)
	}
	c.holds.byKey[key] = hold
	return nil
}

// DropBase deletes this replica's base record unless a live hold names it.
func (c *Compactor) DropBase(ctx context.Context) error {
	client, ok := c.client.(HoldClient)
	if !ok {
		return nil
	}
	c.holds.mu.Lock()
	defer c.holds.mu.Unlock()
	held, err := c.liveHolds(ctx, client, func(h Hold) bool { return h.Base })
	if err != nil || len(held) > 0 {
		return err
	}
	if err := client.DeleteHead(ctx, client.ObjectKey(BaseName)); err != nil {
		return fmt.Errorf("delete the base record: %w", err)
	}
	return nil
}

func (c *Compactor) deleteUnheld(ctx context.Context, client ReplicaClient, files []*ltx.FileInfo) error {
	holdClient, ok := client.(HoldClient)
	if !ok || len(files) == 0 {
		return client.DeleteLTXFiles(ctx, files)
	}
	c.holds.mu.Lock()
	defer c.holds.mu.Unlock()
	held, err := c.liveHolds(ctx, holdClient, func(h Hold) bool { return slices.ContainsFunc(files, h.names) })
	if err != nil {
		return err
	}
	var unheld []*ltx.FileInfo
	for _, info := range files {
		if !slices.ContainsFunc(held, func(h Hold) bool { return h.names(info) }) {
			unheld = append(unheld, info)
		}
	}
	if kept := len(files) - len(unheld); kept > 0 {
		c.logger.Info("retention kept held files", "kept", kept)
	}
	return client.DeleteLTXFiles(ctx, unheld)
}

func (c *Compactor) liveHolds(ctx context.Context, client HoldClient, match func(Hold) bool) ([]Hold, error) {
	if err := c.loadHolds(ctx, client); err != nil {
		return nil, err
	}
	var live []Hold
	for key, hold := range c.holds.byKey {
		if !match(hold) {
			continue
		}
		_, err := client.ReadHead(ctx, hold.ChildBase)
		if err == nil {
			live = append(live, hold)
			continue
		}
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("read the base record %s hold %s keeps files for: %w", hold.ChildBase, key, err)
		}
		if err := client.DeleteHead(ctx, key); err != nil {
			return nil, fmt.Errorf("delete hold %s: %w", key, err)
		}
		delete(c.holds.byKey, key)
		c.logger.Info("dropped a hold whose child's base record is gone", "hold", key, "child_base", hold.ChildBase)
	}
	return live, nil
}

func (c *Compactor) loadHolds(ctx context.Context, client HoldClient) error {
	if c.holds.loaded {
		return nil
	}
	keys, err := client.ListHeads(ctx, client.ObjectKey(HoldsDir))
	if err != nil {
		return fmt.Errorf("list holds: %w", err)
	}
	byKey := make(map[string]Hold, len(keys))
	for _, key := range keys {
		b, err := client.ReadHead(ctx, key)
		if err != nil {
			return fmt.Errorf("read hold %s: %w", key, err)
		}
		var hold Hold
		if err := json.Unmarshal(b, &hold); err != nil {
			return fmt.Errorf("decode hold %s: %w", key, err)
		}
		byKey[key] = hold
	}
	c.holds.byKey, c.holds.loaded = byKey, true
	return nil
}

func (c *Compactor) forgetHolds() {
	c.holds.mu.Lock()
	defer c.holds.mu.Unlock()
	c.holds.byKey, c.holds.loaded = nil, false
}

func (c *Compactor) present(ctx context.Context, client HoldClient, hold Hold) error {
	if hold.Base {
		if _, err := client.ReadHead(ctx, client.ObjectKey(BaseName)); errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: the base record", ErrHeldFileGone)
		} else if err != nil {
			return fmt.Errorf("read the base record: %w", err)
		}
	}
	seeks := map[int]uint64{}
	for _, f := range hold.Files {
		if seek, ok := seeks[f.Level]; !ok || f.Min < seek {
			seeks[f.Level] = f.Min
		}
	}
	listed := map[HeldFile]bool{}
	for level, seek := range seeks {
		itr, err := c.client.LTXFiles(ctx, level, ltx.TXID(seek), false)
		if err != nil {
			return fmt.Errorf("list level %d: %w", level, err)
		}
		for itr.Next() {
			info := itr.Item()
			listed[HeldFile{Level: level, Min: uint64(info.MinTXID), Max: uint64(info.MaxTXID)}] = true
		}
		if err := itr.Close(); err != nil {
			return fmt.Errorf("list level %d: %w", level, err)
		}
	}
	for _, f := range hold.Files {
		if !listed[f] {
			return fmt.Errorf("%w: level %d %s-%s", ErrHeldFileGone, f.Level, ltx.TXID(f.Min), ltx.TXID(f.Max))
		}
	}
	return nil
}
