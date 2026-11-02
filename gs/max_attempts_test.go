package gs

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/superfly/ltx"
)

func TestReplicaClient_MaxAttemptsBoundsTheCallsToABucketThatAnswersEveryCallUnavailable(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)
	t.Setenv("STORAGE_EMULATOR_HOST", strings.TrimPrefix(server.URL, "http://"))
	client := NewReplicaClient()
	client.Bucket, client.Path = "test-bucket", "replica"
	client.MaxAttempts = 3

	itr, err := client.LTXFiles(t.Context(), 0, 0, false)
	if err == nil {
		_, err = ltx.SliceFileIterator(itr)
		_ = itr.Close()
	}

	if err == nil {
		t.Fatal("listing a bucket that answers every call unavailable succeeded")
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("the client made %d calls, want its 3 attempts", got)
	}
}
