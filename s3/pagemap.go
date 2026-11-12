package s3

import (
	"context"
	"fmt"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
)

func (c *ReplicaClient) pageMapDir() string {
	return c.Path + "/" + litestream.PageMapDirName + "/"
}

func (c *ReplicaClient) WritePageMap(ctx context.Context, txID ltx.TXID, data []byte) error {
	return c.WriteHead(ctx, c.pageMapDir()+litestream.FormatPageMapFilename(txID), data)
}

func (c *ReplicaClient) ReadPageMap(ctx context.Context, txID ltx.TXID) ([]byte, error) {
	return c.ReadHead(ctx, c.pageMapDir()+litestream.FormatPageMapFilename(txID))
}

func (c *ReplicaClient) PageMaps(ctx context.Context) ([]ltx.TXID, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}
	var txIDs []ltx.TXID
	paginator := s3.NewListObjectsV2Paginator(c.s3, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.Bucket),
		Prefix: aws.String(c.pageMapDir()),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3: list page maps: %w", err)
		}
		for _, obj := range page.Contents {
			if txID, err := litestream.ParsePageMapFilename(path.Base(aws.ToString(obj.Key))); err == nil {
				txIDs = append(txIDs, txID)
			}
		}
	}
	return txIDs, nil
}

func (c *ReplicaClient) DeletePageMaps(ctx context.Context, txIDs []ltx.TXID) error {
	if len(txIDs) == 0 {
		return nil
	}
	if err := c.Init(ctx); err != nil {
		return err
	}
	objIDs := make([]types.ObjectIdentifier, len(txIDs))
	for i, txID := range txIDs {
		objIDs[i] = types.ObjectIdentifier{Key: aws.String(c.pageMapDir() + litestream.FormatPageMapFilename(txID))}
	}
	for len(objIDs) > 0 {
		n := min(len(objIDs), MaxKeys)
		out, err := c.s3.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(c.Bucket),
			Delete: &types.Delete{Objects: objIDs[:n], Quiet: aws.Bool(true)},
		})
		if err != nil {
			return fmt.Errorf("s3: delete %d page maps: %w", n, err)
		} else if err := deleteOutputError(out); err != nil {
			return err
		}
		objIDs = objIDs[n:]
	}
	return nil
}
