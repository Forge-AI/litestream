//go:build linux || darwin

package main

import (
	"os"
	"strconv"
	"syscall"
	"testing"
	"time"
)

func ownedPipe(t *testing.T) *os.File {
	t.Helper()
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	fd, err := syscall.Dup(int(read.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	read.Close()
	t.Setenv(ownerFDEnv, strconv.Itoa(fd))
	t.Cleanup(func() { write.Close() })
	return write
}

func TestOwnerGone_WaitsWhileTheOwnerHoldsThePipe(t *testing.T) {
	ownedPipe(t)
	gone, err := ownerGone()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-gone:
		t.Fatal("reported the owner gone while it holds the pipe")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestOwnerGone_ReportsWhenTheOwnerClosesThePipe(t *testing.T) {
	owner := ownedPipe(t)
	gone, err := ownerGone()
	if err != nil {
		t.Fatal(err)
	}

	owner.Close()

	select {
	case <-gone:
	case <-time.After(5 * time.Second):
		t.Fatal("a closed owner pipe was not reported")
	}
}

func TestOwnerGone_RefusesAValueThatNamesNoDescriptor(t *testing.T) {
	t.Setenv(ownerFDEnv, "owner")
	if _, err := ownerGone(); err == nil {
		t.Fatal("a value naming no descriptor was accepted")
	}
}
