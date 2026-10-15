//go:build linux || darwin

package main

import (
	"os"
	"strconv"
	"testing"
	"time"
)

func TestParentExit_WaitsWhileTheParentRuns(t *testing.T) {
	t.Setenv(exitWithParentEnv, strconv.Itoa(os.Getppid()))
	exited, err := parentExit()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-exited:
		t.Fatal("reported the parent gone while it still runs")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestParentExit_ReportsAtOnceWhenTheNamedProcessIsNotTheParent(t *testing.T) {
	t.Setenv(exitWithParentEnv, strconv.Itoa(os.Getpid()))
	exited, err := parentExit()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("a pid that is no longer the parent was watched as if it were")
	}
}

func TestParentExit_RefusesAValueThatNamesNoPid(t *testing.T) {
	t.Setenv(exitWithParentEnv, "parent")
	if _, err := parentExit(); err == nil {
		t.Fatal("a value naming no pid was accepted")
	}
}
