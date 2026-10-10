package s3

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestStallBoundTransport_FailsABodyThatStopsSending(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("partial"))
		w.(http.Flusher).Flush()
		<-release
	}))
	defer server.Close()
	defer close(release)

	client := &http.Client{Transport: stallBoundTransport{RoundTripper: http.DefaultTransport, idle: 100 * time.Millisecond}}
	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if !errors.Is(err, errBodyStalled) {
		t.Fatalf("read %q, err %v; want a stall error", body, err)
	}
	if string(body) != "partial" {
		t.Fatalf("read %q before the stall, want the bytes the server sent", body)
	}
}

func TestStallBoundTransport_PassesASlowBodyThatKeepsSending(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for range 5 {
			w.Write([]byte("x"))
			w.(http.Flusher).Flush()
			time.Sleep(50 * time.Millisecond)
		}
	}))
	defer server.Close()

	client := &http.Client{Transport: stallBoundTransport{RoundTripper: http.DefaultTransport, idle: 100 * time.Millisecond}}
	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || string(body) != "xxxxx" {
		t.Fatalf("read %q, err %v", body, err)
	}
}
