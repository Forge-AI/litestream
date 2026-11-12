package litestream

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSegmentFaults_AnAbsentSegmentIsFaultedOnceBeforeItsPagesAreRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	states := make([]byte, 8)
	binary.LittleEndian.PutUint32(states[0:], segmentPresent)
	require.NoError(t, os.WriteFile(path+"-segments", states, 0o600))

	socket := filepath.Join(dir, "fault.sock")
	listener, err := net.Listen("unix", socket)
	require.NoError(t, err)
	var faulted []uint32
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Lane    string `json:"lane"`
			Segment uint32 `json:"segment"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		require.Equal(t, path, req.Lane)
		faulted = append(faulted, req.Segment)
		f, err := os.OpenFile(path+"-segments", os.O_WRONLY, 0)
		require.NoError(t, err)
		var state [4]byte
		binary.LittleEndian.PutUint32(state[:], segmentPresent)
		_, err = f.WriteAt(state[:], int64(req.Segment)*4)
		require.NoError(t, err)
		require.NoError(t, f.Close())
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })

	db := &DB{path: path, FaultSocket: socket}
	faults, err := db.segmentFaults()
	require.NoError(t, err)
	defer faults.close()
	for _, pgno := range []uint32{1, segmentPages, segmentPages + 1, 2 * segmentPages, 3*segmentPages + 1} {
		require.NoError(t, faults.ensure(context.Background(), pgno))
	}
	require.Equal(t, []uint32{1}, faulted, "only the absent segment is faulted, and pages past the map are present")
}

func TestSegmentFaults_AWholeCopyHasNoFaults(t *testing.T) {
	db := &DB{path: filepath.Join(t.TempDir(), "db"), FaultSocket: "unused.sock"}
	faults, err := db.segmentFaults()
	require.NoError(t, err)
	require.Nil(t, faults)
	require.NoError(t, faults.ensure(context.Background(), 1))
}

func TestSegmentFaults_AFailedFaultFailsTheRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	require.NoError(t, os.WriteFile(path+"-segments", make([]byte, 4), 0o600))
	socket := filepath.Join(dir, "fault.sock")
	listener, err := net.Listen("unix", socket)
	require.NoError(t, err)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "no lane here follows it", http.StatusServiceUnavailable)
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })

	faults, err := (&DB{path: path, FaultSocket: socket}).segmentFaults()
	require.NoError(t, err)
	defer faults.close()
	require.ErrorContains(t, faults.ensure(context.Background(), 1), "no lane here follows it")
}
