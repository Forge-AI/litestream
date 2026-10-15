package main

import (
	"errors"

	"golang.org/x/sys/unix"
)

type processExitHandle struct{ queue int }

func openProcessExit(pid int) (processExitHandle, error) {
	queue, err := unix.Kqueue()
	if err != nil {
		return processExitHandle{}, err
	}
	exit := unix.Kevent_t{
		Ident:  uint64(pid),
		Filter: unix.EVFILT_PROC,
		Flags:  unix.EV_ADD | unix.EV_ONESHOT,
		Fflags: unix.NOTE_EXIT,
	}
	if _, err := unix.Kevent(queue, []unix.Kevent_t{exit}, nil, nil); err != nil {
		unix.Close(queue)
		return processExitHandle{}, err
	}
	return processExitHandle{queue: queue}, nil
}

func (h processExitHandle) wait() {
	events := make([]unix.Kevent_t, 1)
	for {
		_, err := unix.Kevent(h.queue, nil, events, nil)
		if !errors.Is(err, unix.EINTR) {
			return
		}
	}
}

func (h processExitHandle) close() { unix.Close(h.queue) }
