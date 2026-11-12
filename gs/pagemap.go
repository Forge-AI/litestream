package gs

import (
	"context"
	"errors"
	"fmt"
	"path"

	"cloud.google.com/go/storage"
	"github.com/superfly/ltx"
	"google.golang.org/api/iterator"

	"github.com/benbjohnson/litestream"
)

func (c *ReplicaClient) WritePageMap(ctx context.Context, txID ltx.TXID, data []byte) error {
	return c.WriteHead(ctx, litestream.PageMapPath(c.Path, txID), data)
}

func (c *ReplicaClient) ReadPageMap(ctx context.Context, txID ltx.TXID) ([]byte, error) {
	return c.ReadHead(ctx, litestream.PageMapPath(c.Path, txID))
}

func (c *ReplicaClient) PageMaps(ctx context.Context) ([]ltx.TXID, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}
	var txIDs []ltx.TXID
	for it := c.bkt.Objects(ctx, &storage.Query{Prefix: litestream.PageMapDir(c.Path) + "/"}); ; {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return txIDs, nil
		} else if err != nil {
			return nil, fmt.Errorf("gs: list page maps: %w", err)
		}
		if txID, err := litestream.ParsePageMapFilename(path.Base(attrs.Name)); err == nil {
			txIDs = append(txIDs, txID)
		}
	}
}

func (c *ReplicaClient) DeletePageMaps(ctx context.Context, txIDs []ltx.TXID) error {
	if err := c.Init(ctx); err != nil {
		return err
	}
	for _, txID := range txIDs {
		key := litestream.PageMapPath(c.Path, txID)
		if err := c.bkt.Object(key).Delete(ctx); err != nil && !isNotExists(err) {
			return fmt.Errorf("gs: delete page map %q: %w", key, err)
		}
	}
	return nil
}
