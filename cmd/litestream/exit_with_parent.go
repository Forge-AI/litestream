//go:build linux || darwin

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"golang.org/x/sys/unix"
)

const (
	exitWithParentEnv   = "SANDSQL_EXIT_WITH_PARENT"
	exitWithParentGrace = 5 * time.Second
)

func exitWithParent(ctx context.Context) (context.Context, error) {
	exited, err := parentExit()
	if err != nil || exited == nil {
		return ctx, err
	}
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		<-exited
		cancel()
		time.Sleep(exitWithParentGrace)
		os.Exit(1)
	}()
	return ctx, nil
}

func parentExit() (<-chan struct{}, error) {
	raw, ok := os.LookupEnv(exitWithParentEnv)
	if !ok {
		return nil, nil
	}
	pid, err := strconv.Atoi(raw)
	if err != nil || pid <= 0 {
		return nil, fmt.Errorf("%s names a pid, not %q", exitWithParentEnv, raw)
	}
	exited := make(chan struct{})
	if os.Getppid() != pid {
		close(exited)
		return exited, nil
	}
	handle, err := openProcessExit(pid)
	if errors.Is(err, unix.ESRCH) {
		close(exited)
		return exited, nil
	}
	if err != nil {
		return nil, fmt.Errorf("watch process %d for exit: %w", pid, err)
	}
	if os.Getppid() != pid {
		handle.close()
		close(exited)
		return exited, nil
	}
	go func() {
		handle.wait()
		handle.close()
		close(exited)
	}()
	return exited, nil
}
