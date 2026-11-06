//go:build !unix

package main

import "context"

func exitWithOwner(ctx context.Context) (context.Context, <-chan struct{}, error) {
	return ctx, nil, nil
}
