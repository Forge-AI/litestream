package s3

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/superfly/ltx"
)

func TestReplicaClient_LTXFilesListsFromSeek(t *testing.T) {
	for _, tc := range []struct {
		name       string
		seek       ltx.TXID
		startAfter string
	}{
		{name: "no seek", seek: 0, startAfter: ""},
		{name: "seek", seek: 5, startAfter: "replica/0000/0000000000000004-ffffffffffffffff.ltx"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.URL.Query().Get("start-after"); got != tc.startAfter {
					t.Errorf("start-after %q, want %q", got, tc.startAfter)
				}
				w.Header().Set("Content-Type", "application/xml")
				_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>test-bucket</Name><Prefix>replica/0000/</Prefix><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>replica/0000/0000000000000005-0000000000000007.ltx</Key><Size>10</Size><LastModified>2026-10-15T00:00:00.000Z</LastModified></Contents></ListBucketResult>`)
			}))
			t.Cleanup(server.Close)
			client := NewReplicaClient()
			client.Bucket, client.Path, client.Region = "test-bucket", "replica", "us-east-1"
			client.Endpoint, client.ForcePathStyle = server.URL, true
			client.AccessKeyID, client.SecretAccessKey = "test-key", "test-secret"
			itr, err := client.LTXFiles(t.Context(), 0, tc.seek, false)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = itr.Close() })
			files, err := ltx.SliceFileIterator(itr)
			if err != nil {
				t.Fatal(err)
			}
			if len(files) != 1 || files[0].MinTXID != 5 || files[0].MaxTXID != 7 {
				t.Fatalf("listed %v, want the one file 5-7", files)
			}
		})
	}
}
