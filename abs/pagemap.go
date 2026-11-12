package abs

import (
	"context"
	"fmt"
	"path"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/superfly/ltx"

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
	prefix := litestream.PageMapDir(c.Path) + "/"
	var txIDs []ltx.TXID
	pager := c.client.NewListBlobsFlatPager(c.Bucket, &azblob.ListBlobsFlatOptions{Prefix: &prefix})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("abs: list page maps: %w", err)
		}
		for _, item := range page.Segment.BlobItems {
			if txID, err := litestream.ParsePageMapFilename(path.Base(*item.Name)); err == nil {
				txIDs = append(txIDs, txID)
			}
		}
	}
	return txIDs, nil
}

func (c *ReplicaClient) DeletePageMaps(ctx context.Context, txIDs []ltx.TXID) error {
	if err := c.Init(ctx); err != nil {
		return err
	}
	for _, txID := range txIDs {
		key := litestream.PageMapPath(c.Path, txID)
		if _, err := c.client.DeleteBlob(ctx, c.Bucket, key, nil); err != nil && !isNotExists(err) {
			return fmt.Errorf("abs: delete page map %q: %w", key, err)
		}
	}
	return nil
}
