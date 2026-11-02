package s3

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/superfly/ltx"
)

func TestReplicaClient_MaxAttemptsBoundsTheCallsToABucketThatDropsEveryConnection(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		conn, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = conn.Close()
	}))
	t.Cleanup(server.Close)
	client := NewReplicaClient()
	client.Bucket, client.Path, client.Region = "test-bucket", "replica", "us-east-1"
	client.Endpoint, client.ForcePathStyle = server.URL, true
	client.AccessKeyID, client.SecretAccessKey = "test-key", "test-secret"
	client.MaxAttempts = 3

	itr, err := client.LTXFiles(t.Context(), 0, 0, false)
	if err == nil {
		_, err = ltx.SliceFileIterator(itr)
		_ = itr.Close()
	}

	if err == nil {
		t.Fatal("listing a bucket that drops every connection succeeded")
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("the client made %d calls, want its 3 attempts", got)
	}
}
