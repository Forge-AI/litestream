package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
)

const listenFDEnv = "SANDSQL_LISTEN_FD"

func inheritedListener() (net.Listener, error) {
	raw, ok := os.LookupEnv(listenFDEnv)
	if !ok {
		return nil, nil
	}
	os.Unsetenv(listenFDEnv)
	fd, err := strconv.Atoi(raw)
	if err != nil || fd <= 2 {
		return nil, fmt.Errorf("%s names a file descriptor, not %q", listenFDEnv, raw)
	}
	socket := os.NewFile(uintptr(fd), "control socket")
	defer socket.Close()
	listener, err := net.FileListener(socket)
	if err != nil {
		return nil, fmt.Errorf("%s names file descriptor %d, which is no listening socket this process inherited: %w", listenFDEnv, fd, err)
	}
	return listener, nil
}
