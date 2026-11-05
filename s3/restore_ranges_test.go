package s3

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/superfly/ltx"

	litestream "github.com/benbjohnson/litestream"
)

func TestReplica_Restore_S3Streams(t *testing.T) {
	const pageSize = 4096
	const pageCount = 1024
	const cutoff = 1 << 20
	want := make([]byte, pageSize*pageCount)
	_, _ = rand.New(rand.NewSource(1)).Read(want)
	var encoded bytes.Buffer
	enc, err := ltx.NewEncoder(&encoded)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.EncodeHeader(ltx.Header{
		Version: ltx.Version, Flags: ltx.HeaderFlagNoChecksum, PageSize: pageSize, Commit: pageCount,
		MinTXID: 1, MaxTXID: 1, Timestamp: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatal(err)
	}
	for pgno := uint32(1); pgno <= pageCount; pgno++ {
		page := want[(pgno-1)*pageSize : pgno*pageSize]
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, page); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	data := encoded.Bytes()
	const key = "replica/0009/0000000000000001-0000000000000001.ltx"
	for _, mode := range []string{"healthy", "dropped", "stripped", "denied"} {
		t.Run(mode, func(t *testing.T) {
			var mu sync.Mutex
			var offsets []int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("list-type") == "2" {
					w.Header().Set("Content-Type", "application/xml")
					_, _ = fmt.Fprintf(w, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>false</IsTruncated><Contents><Key>%s</Key><Size>%d</Size><LastModified>2026-10-14T00:00:00Z</LastModified></Contents></ListBucketResult>`, key, len(data))
					return
				}
				if mode == "denied" {
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(http.StatusForbidden)
					_, _ = fmt.Fprint(w, `<Error><Code>AccessDenied</Code><Message>source provision read denied</Message></Error>`)
					return
				}
				if strings.Contains(r.Header.Get("Authorization"), ";range;") {
					t.Error("Range is bound into the request signature")
				}
				var start int64
				if value := r.Header.Get("Range"); value != "" {
					if _, err := fmt.Sscanf(value, "bytes=%d-", &start); err != nil {
						t.Error(err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
				}
				mu.Lock()
				offsets = append(offsets, start)
				first := len(offsets) == 1
				mu.Unlock()
				if mode == "stripped" {
					r.Header.Del("Range")
					start = 0
				}
				if start < 0 || start >= int64(len(data)) {
					w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
					return
				}
				w.Header().Set("Content-Length", fmt.Sprint(int64(len(data))-start))
				if r.Header.Get("Range") != "" {
					w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(data)-1, len(data)))
					w.WriteHeader(http.StatusPartialContent)
				}
				end := int64(len(data))
				if mode == "dropped" || mode == "stripped" && first {
					end = min(end, start+cutoff)
				}
				_, _ = w.Write(data[start:end])
			}))
			t.Cleanup(server.Close)
			client := NewReplicaClient()
			client.Bucket, client.Path, client.Region = "test-bucket", "replica", "us-east-1"
			client.Endpoint, client.ForcePathStyle = server.URL, true
			client.AccessKeyID, client.SecretAccessKey = "test-key", "test-secret"
			output := filepath.Join(t.TempDir(), "restored.db")
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			err := litestream.NewReplicaWithClient(nil, client).Restore(ctx, litestream.RestoreOptions{OutputPath: output, TXID: 1})
			if mode == "denied" {
				if err == nil || !strings.Contains(err.Error(), "max retries exceeded") || !strings.Contains(err.Error(), "AccessDenied") || !strings.Contains(err.Error(), "source provision read denied") {
					t.Fatalf("restore error lost cause: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(output)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatal("restored database differs from source pages")
			}
			mu.Lock()
			defer mu.Unlock()
			switch mode {
			case "healthy":
				if len(offsets) != 1 || offsets[0] != 0 {
					t.Fatalf("healthy restore issued GETs at %v, want one full-object GET", offsets)
				}
			case "stripped":
				if len(offsets) != 2 || offsets[0] != 0 || offsets[1] != cutoff {
					t.Fatalf("stripped range did not resume at the last delivered byte: %v", offsets)
				}
			case "dropped":
				if len(offsets) < 5 {
					t.Fatalf("only %d responses crossed the proxy cutoff", len(offsets))
				}
				for i, offset := range offsets {
					if offset != int64(i)*cutoff {
						t.Fatalf("restore resumed at %v, want consecutive delivered offsets", offsets)
					}
				}
			}
		})
	}
}
