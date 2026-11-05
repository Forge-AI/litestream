//go:build !unix

package main

import "context"

func exitWithOwner(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
