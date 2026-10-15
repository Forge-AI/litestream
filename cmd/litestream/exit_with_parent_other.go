//go:build !linux && !darwin

package main

import (
	"context"
	"fmt"
	"os"
)

const exitWithParentEnv = "SANDSQL_EXIT_WITH_PARENT"

func exitWithParent(ctx context.Context) (context.Context, error) {
	if _, ok := os.LookupEnv(exitWithParentEnv); ok {
		return nil, fmt.Errorf("%s is not supported on this platform", exitWithParentEnv)
	}
	return ctx, nil
}
