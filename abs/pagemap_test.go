package abs

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/benbjohnson/litestream"
)

func TestPageMap_ABSWritesAHeadOnlyWhenItIsAbsentOrAtTheETagTheWriteNames(t *testing.T) {
	var (
		mu         sync.Mutex
		stored     []byte
		etag       string
		conditions []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("ETag", etag)
			w.Header().Set("Content-Length", strconv.Itoa(len(stored)))
			_, _ = w.Write(stored)
		case http.MethodPut:
			match, none := r.Header.Get("If-Match"), r.Header.Get("If-None-Match")
			conditions = append(conditions, match+"|"+none)
			switch {
			case none == "*" && etag != "":
				w.Header().Set("x-ms-error-code", "BlobAlreadyExists")
				w.WriteHeader(http.StatusConflict)
				return
			case match != "" && match != etag:
				w.Header().Set("x-ms-error-code", "ConditionNotMet")
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			stored, _ = io.ReadAll(r.Body)
			etag = fmt.Sprintf(`"v%d"`, len(conditions))
			w.Header().Set("ETag", etag)
			w.WriteHeader(http.StatusCreated)
		}
	}))
	t.Cleanup(server.Close)
	client := NewReplicaClient()
	client.Bucket, client.Path = "test-container", "replica"
	client.Endpoint, client.SASToken = server.URL, "sv=2024-01-01"
	ctx := t.Context()
	key := client.ObjectKey(litestream.PageMapName)
	write := func(text, version string) (string, error) {
		return client.WriteHeadIf(ctx, key, io.NewSectionReader(strings.NewReader(text), 0, int64(len(text))), version)
	}

	first, err := write("first", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := write("again", ""); !errors.Is(err, litestream.ErrHeadMoved) {
		t.Fatalf("a create over an existing head returned %v, want %v", err, litestream.ErrHeadMoved)
	}
	second, err := write("second", first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := write("stale", first); !errors.Is(err, litestream.ErrHeadMoved) {
		t.Fatalf("a write at a replaced ETag returned %v, want %v", err, litestream.ErrHeadMoved)
	}
	if b, version, err := client.ReadHeadVersion(ctx, key); err != nil || string(b) != "second" || version != second {
		t.Fatalf("read %q at %q (%v), want %q at %q", b, version, err, "second", second)
	}
	if want := []string{"|*", "|*", first + "|", first + "|"}; !slices.Equal(conditions, want) {
		t.Fatalf("the puts named the conditions %q, want %q", conditions, want)
	}
}
