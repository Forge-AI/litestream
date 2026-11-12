package file

import (
	"context"
	"os"
	"path/filepath"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
)

func (c *ReplicaClient) WritePageMap(ctx context.Context, txID ltx.TXID, data []byte) error {
	return c.WriteHead(ctx, filepath.FromSlash(litestream.PageMapPath(c.path, txID)), data)
}

func (c *ReplicaClient) ReadPageMap(ctx context.Context, txID ltx.TXID) ([]byte, error) {
	return c.ReadHead(ctx, filepath.FromSlash(litestream.PageMapPath(c.path, txID)))
}

func (c *ReplicaClient) PageMaps(_ context.Context) ([]ltx.TXID, error) {
	entries, err := os.ReadDir(filepath.FromSlash(litestream.PageMapDir(c.path)))
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	var txIDs []ltx.TXID
	for _, entry := range entries {
		if txID, err := litestream.ParsePageMapFilename(entry.Name()); err == nil {
			txIDs = append(txIDs, txID)
		}
	}
	return txIDs, nil
}

func (c *ReplicaClient) DeletePageMaps(_ context.Context, txIDs []ltx.TXID) error {
	for _, txID := range txIDs {
		if err := os.Remove(filepath.FromSlash(litestream.PageMapPath(c.path, txID))); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
