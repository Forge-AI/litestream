package main

import (
	"errors"

	"golang.org/x/sys/unix"
)

type processExitHandle struct{ fd int }

func openProcessExit(pid int) (processExitHandle, error) {
	fd, err := unix.PidfdOpen(pid, 0)
	return processExitHandle{fd: fd}, err
}

func (h processExitHandle) wait() {
	for {
		_, err := unix.Poll([]unix.PollFd{{Fd: int32(h.fd), Events: unix.POLLIN}}, -1)
		if !errors.Is(err, unix.EINTR) {
			return
		}
	}
}

func (h processExitHandle) close() { unix.Close(h.fd) }
