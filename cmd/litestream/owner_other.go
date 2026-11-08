//go:build !linux && !darwin

package main

import (
	"context"
	"fmt"
	"os"
)

const ownerFDEnv = "SANDSQL_OWNER_FD"

func exitWithOwner(ctx context.Context) (context.Context, <-chan struct{}, error) {
	if _, ok := os.LookupEnv(ownerFDEnv); ok {
		return nil, nil, fmt.Errorf("%s is not supported on this platform", ownerFDEnv)
	}
	return ctx, nil, nil
}
