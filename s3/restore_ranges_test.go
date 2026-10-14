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

	litestream "github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
	"github.com/superfly/ltx"
)

func TestReplica_Restore_BoundedS3Ranges(t *testing.T) {
	const pageSize = 4096
	const pageCount = 1024
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
	for _, mode := range []string{"dropped", "stripped", "denied"} {
		denied := mode == "denied"
		t.Run(mode, func(t *testing.T) {
			var mu sync.Mutex
			var ranges [][2]int64
			dropped := make(map[int64]bool)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("list-type") == "2" {
					w.Header().Set("Content-Type", "application/xml")
					_, _ = fmt.Fprintf(w, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>false</IsTruncated><Contents><Key>%s</Key><Size>%d</Size><LastModified>2026-10-14T00:00:00Z</LastModified></Contents></ListBucketResult>`, key, len(data))
					return
				}
				if denied {
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(http.StatusForbidden)
					_, _ = fmt.Fprint(w, `<Error><Code>AccessDenied</Code><Message>source provision read denied</Message></Error>`)
					return
				}
				var start, end int64
				if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil || end-start+1 > internal.LTXReadChunkSize {
					w.Header().Set("Content-Length", fmt.Sprint(len(data)))
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write(data[:64*1024])
					return
				}
				if start < 0 || end >= int64(len(data)) || end < start {
					w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
					return
				}
				mu.Lock()
				ranges = append(ranges, [2]int64{start, end})
				chunk := start / internal.LTXReadChunkSize
				drop := !dropped[chunk] && end-start+1 > 64*1024
				if drop {
					dropped[chunk] = true
				}
				mu.Unlock()
				if mode == "stripped" {
					if strings.Contains(r.Header.Get("Authorization"), ";range;") {
						w.WriteHeader(http.StatusForbidden)
						_, _ = fmt.Fprint(w, `<Error><Code>SignatureDoesNotMatch</Code></Error>`)
						return
					}
					r.Header.Del("Range")
					w.Header().Set("Content-Length", fmt.Sprint(len(data)))
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write(data)
					return
				}
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
				w.Header().Set("Content-Length", fmt.Sprint(end-start+1))
				w.WriteHeader(http.StatusPartialContent)
				if drop {
					_, _ = w.Write(data[start : start+64*1024])
					return
				}
				_, _ = w.Write(data[start : end+1])
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
			if denied {
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
			if mode == "stripped" {
				if len(ranges) < 4 || ranges[1][0] != internal.LTXReadChunkSize {
					t.Fatalf("full responses did not preserve ranged offsets: %v", ranges)
				}
				return
			}
			if len(dropped) < 4 {
				t.Fatalf("only %d chunks dropped", len(dropped))
			}
			if len(ranges) < 2 || ranges[0] != [2]int64{0, internal.LTXReadChunkSize - 1} || ranges[1] != [2]int64{64 * 1024, internal.LTXReadChunkSize - 1} {
				t.Fatalf("restore did not resume at last delivered byte: %v", ranges)
			}
		})
	}
}
