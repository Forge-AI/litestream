package s3

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReplicaClient_OpenLTXFileProxyRange(t *testing.T) {
	const data = "0123456789abcdef"
	for _, tc := range []struct {
		name         string
		offset       int64
		size         int64
		status       int
		contentRange string
		body         string
		want         string
		wantError    bool
	}{
		{name: "whole", status: http.StatusOK, body: data, want: data},
		{name: "stripped", offset: 4, size: 5, status: http.StatusOK, body: data, want: "45678"},
		{name: "partial", offset: 4, size: 5, status: http.StatusPartialContent, contentRange: "bytes 4-8/16", body: "45678", want: "45678"},
		{name: "wrong range", offset: 4, size: 5, status: http.StatusPartialContent, contentRange: "bytes 0-4/16", body: "01234", wantError: true},
		{name: "missing range", offset: 4, size: 5, status: http.StatusPartialContent, body: "45678", wantError: true},
		{name: "short full body", offset: 20, size: 5, status: http.StatusOK, body: data, wantError: true},
		{name: "short partial body", offset: 4, size: 5, status: http.StatusPartialContent, contentRange: "bytes 4-8/16", body: "456", wantError: true},
		{name: "range on full response", offset: 4, size: 5, status: http.StatusOK, contentRange: "bytes 4-8/16", body: "45678", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.Contains(r.Header.Get("Authorization"), ";range;") {
					t.Error("Range is bound into the request signature")
				}
				if tc.offset == 0 && tc.size == 0 && r.Header.Get("Range") != "" {
					t.Error("whole-object read sent Range")
				}
				r.Header.Del("Range")
				if tc.contentRange != "" {
					w.Header().Set("Content-Range", tc.contentRange)
				}
				w.Header().Set("Content-Length", fmt.Sprint(len(tc.body)))
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			t.Cleanup(server.Close)
			client := NewReplicaClient()
			client.Bucket, client.Path, client.Region = "test-bucket", "replica", "us-east-1"
			client.Endpoint, client.ForcePathStyle = server.URL, true
			client.AccessKeyID, client.SecretAccessKey = "test-key", "test-secret"
			rc, err := client.OpenLTXFile(t.Context(), 9, 1, 1, tc.offset, tc.size)
			if tc.wantError {
				if err == nil {
					_ = rc.Close()
					t.Fatal("accepted an invalid range response")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = rc.Close() })
			got, err := io.ReadAll(rc)
			if err != nil || string(got) != tc.want {
				t.Fatalf("read %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}
