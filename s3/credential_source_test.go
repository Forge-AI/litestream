package s3

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/superfly/ltx"
)

func TestReplicaClient_SignsWithItsDatabasesCredentialSource(t *testing.T) {
	credentials := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "database-token" {
			t.Errorf("the credential request carried %q, want the database's token", got)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"AccessKeyId":"DATABASEKEY","SecretAccessKey":"database-secret","Token":"database-session","Expiration":"2099-01-01T00:00:00Z"}`)
	}))
	t.Cleanup(credentials.Close)
	storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); !strings.Contains(got, "Credential=DATABASEKEY/") {
			t.Errorf("the S3 request was signed as %q, want the database's key", got)
		}
		if got := r.Header.Get("X-Amz-Security-Token"); got != "database-session" {
			t.Errorf("the S3 request carried session %q, want the database's", got)
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>test-bucket</Name><Prefix>replica/0000/</Prefix><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>`)
	}))
	t.Cleanup(storage.Close)
	t.Setenv("AWS_ACCESS_KEY_ID", "PROCESSKEY")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "process-secret")
	client, err := NewReplicaClientFromURL("s3", "test-bucket", "replica", url.Values{
		"endpoint":          {storage.URL},
		"region":            {"us-east-1"},
		"force-path-style":  {"true"},
		"credentials-url":   {credentials.URL},
		"credentials-token": {"database-token"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	itr, err := client.LTXFiles(t.Context(), 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = itr.Close() })
	files, err := ltx.SliceFileIterator(itr)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Fatalf("listed %v, want an empty level", files)
	}
}

func TestReplicaClient_ReadsAHeadItsOwnKeyIsRefusedWithItsDatabasesAncestorsKey(t *testing.T) {
	credentials := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := "DATABASEKEY"
		if r.URL.Path == "/ancestors" {
			key = "ANCESTORSKEY"
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"AccessKeyId":%q,"SecretAccessKey":"secret","Token":"session","Expiration":"2099-01-01T00:00:00Z"}`, key)
	}))
	t.Cleanup(credentials.Close)
	storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Authorization"), "Credential=ANCESTORSKEY/") {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusForbidden)
			_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>`)
			return
		}
		_, _ = fmt.Fprint(w, `{"prefix":"teams/acme/sessions/parent"}`)
	}))
	t.Cleanup(storage.Close)
	client, err := NewReplicaClientFromURL("s3", "test-bucket", "teams/acme/sessions/parent", url.Values{
		"endpoint":          {storage.URL},
		"region":            {"us-east-1"},
		"force-path-style":  {"true"},
		"credentials-url":   {credentials.URL},
		"credentials-token": {"database-token"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	base, err := client.(*ReplicaClient).ReadHead(t.Context(), "teams/acme/sessions/child/base")
	if err != nil {
		t.Fatal(err)
	}
	if string(base) != `{"prefix":"teams/acme/sessions/parent"}` {
		t.Fatalf("read %q, want the child's base record", base)
	}
}
