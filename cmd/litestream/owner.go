//go:build linux || darwin

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"golang.org/x/sys/unix"
)

const (
	ownerFDEnv     = "SANDSQL_OWNER_FD"
	ownerGoneGrace = 5 * time.Second
)

func exitWithOwner(ctx context.Context) (context.Context, <-chan struct{}, error) {
	gone, err := ownerGone()
	if err != nil || gone == nil {
		return ctx, nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		<-gone
		cancel()
		time.Sleep(ownerGoneGrace)
		os.Exit(1)
	}()
	return ctx, gone, nil
}

func ownerGone() (<-chan struct{}, error) {
	raw, ok := os.LookupEnv(ownerFDEnv)
	if !ok {
		return nil, nil
	}
	os.Unsetenv(ownerFDEnv)
	fd, err := strconv.Atoi(raw)
	if err != nil || fd <= 2 {
		return nil, fmt.Errorf("%s names a file descriptor, not %q", ownerFDEnv, raw)
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFD, unix.FD_CLOEXEC); err != nil {
		return nil, fmt.Errorf("%s names file descriptor %d, which this process did not inherit: %w", ownerFDEnv, fd, err)
	}
	owner := os.NewFile(uintptr(fd), "owner")
	gone := make(chan struct{})
	go func() {
		io.Copy(io.Discard, owner)
		owner.Close()
		close(gone)
	}()
	return gone, nil
}
