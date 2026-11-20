package gs

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

func TestPageMap_GCSWritesAHeadOnlyWhenItIsAbsentOrAtTheGenerationTheWriteNames(t *testing.T) {
	var (
		mu            sync.Mutex
		generation    int64
		preconditions []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		mu.Lock()
		defer mu.Unlock()
		match := r.URL.Query().Get("ifGenerationMatch")
		preconditions = append(preconditions, match)
		w.Header().Set("Content-Type", "application/json")
		if match != strconv.FormatInt(generation, 10) {
			w.WriteHeader(http.StatusPreconditionFailed)
			_, _ = io.WriteString(w, `{"error":{"code":412,"message":"conditionNotMet"}}`)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		generation++
		_, _ = fmt.Fprintf(w, `{"bucket":"test-bucket","name":"replica/map.runs","generation":"%d"}`, generation)
	}))
	t.Cleanup(server.Close)
	t.Setenv("STORAGE_EMULATOR_HOST", strings.TrimPrefix(server.URL, "http://"))
	client := NewReplicaClient()
	client.Bucket, client.Path = "test-bucket", "replica"
	ctx := t.Context()
	key := client.ObjectKey(litestream.PageMapName)
	write := func(text, version string) (string, error) {
		return client.WriteHeadIf(ctx, key, io.NewSectionReader(strings.NewReader(text), 0, int64(len(text))), version)
	}

	first, err := write("first", "")
	if err != nil || first != "1" {
		t.Fatalf("a create wrote generation %q (%v), want 1", first, err)
	}
	if _, err := write("again", ""); !errors.Is(err, litestream.ErrHeadMoved) {
		t.Fatalf("a create over an existing head returned %v, want %v", err, litestream.ErrHeadMoved)
	}
	if second, err := write("second", first); err != nil || second != "2" {
		t.Fatalf("a write at generation 1 wrote generation %q (%v), want 2", second, err)
	}
	if _, err := write("stale", first); !errors.Is(err, litestream.ErrHeadMoved) {
		t.Fatalf("a write at a replaced generation returned %v, want %v", err, litestream.ErrHeadMoved)
	}
	if want := []string{"0", "0", "1", "1"}; !slices.Equal(preconditions, want) {
		t.Fatalf("the uploads named the generations %q, want %q", preconditions, want)
	}
}
