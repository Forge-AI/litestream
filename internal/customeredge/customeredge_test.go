package customeredge

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestAdmissionPacesWithoutBankingAnIdleBurst(t *testing.T) {
	b := &budget{interval: 500 * time.Millisecond}
	now := time.Unix(100, 0)
	if got := b.admit(now); got != 0 {
		t.Fatalf("first admission: %s", got)
	}
	if got := b.admit(now); got != 500*time.Millisecond {
		t.Fatalf("queued admission: %s", got)
	}
	if got := b.admit(now.Add(time.Hour)); got != 0 {
		t.Fatalf("idle admission: %s", got)
	}
	if got := b.admit(now.Add(time.Hour)); got != 500*time.Millisecond {
		t.Fatalf("idle burst: %s", got)
	}
}

func TestQueuedAdmissionRechecksRetryAfter(t *testing.T) {
	b := &budget{interval: 500 * time.Millisecond}
	now := time.Unix(100, 0)
	b.admit(now)
	b.respond(now.Add(100*time.Millisecond), 403, "10")
	if got := b.admit(now.Add(500 * time.Millisecond)); got != 9600*time.Millisecond {
		t.Fatalf("refusal cooldown: %s", got)
	}
	b.respond(now.Add(time.Second), 200, "")
	if got := b.admit(now.Add(500 * time.Millisecond)); got != 9600*time.Millisecond {
		t.Fatalf("in-flight success erased cooldown: %s", got)
	}
}

func TestRefusalBackoffAndHTTPDate(t *testing.T) {
	b := &budget{interval: 500 * time.Millisecond}
	now := time.Unix(100, 0)
	b.respond(now, 403, "invalid")
	if got := b.admit(now); got != time.Second {
		t.Fatalf("first backoff: %s", got)
	}
	b.respond(now.Add(time.Second), 403, "")
	if got := b.admit(now.Add(time.Second)); got != 2*time.Second {
		t.Fatalf("second backoff: %s", got)
	}
	b.respond(now, 403, now.Add(30*time.Second).UTC().Format(http.TimeFormat))
	if got := b.admit(now); got != 30*time.Second {
		t.Fatalf("HTTP date cooldown: %s", got)
	}
}

func TestCanceledWaitDoesNotConsumeAdmission(t *testing.T) {
	b := &budget{interval: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.wait(ctx); err != context.Canceled {
		t.Fatalf("wait: %v", err)
	}
	if !b.next.IsZero() {
		t.Fatal("canceled request consumed admission")
	}
}

func TestACooldownPastTheDeadlineRefusesAtOnce(t *testing.T) {
	b := &budget{interval: time.Millisecond}
	b.respond(time.Now(), 403, "60")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	began := time.Now()
	var refused *Refused
	if err := b.wait(ctx); !errors.As(err, &refused) {
		t.Fatalf("wait: %v", err)
	}
	if took := time.Since(began); took > time.Second {
		t.Fatalf("the refusal waited %s", took)
	}
	if !b.next.IsZero() {
		t.Fatal("a refused request consumed admission")
	}
}

func TestACooldownWithinTheDeadlineIsWaitedOut(t *testing.T) {
	b := &budget{interval: time.Millisecond}
	b.respond(time.Now(), 403, "")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := b.wait(ctx); err != nil {
		t.Fatalf("wait: %v", err)
	}
}

type answering struct {
	status int
	calls  int
}

func (a *answering) Do(req *http.Request) (*http.Response, error) {
	a.calls++
	return &http.Response{StatusCode: a.status, Header: http.Header{}, Request: req}, nil
}

func TestARefusedEdgeFailsTheNextRequestAsNonRetriable(t *testing.T) {
	edge := &answering{status: http.StatusForbidden}
	client := Client{HTTP: edge, Name: "test"}
	first, err := http.NewRequest(http.MethodPut, "https://refusing.edge.test/blob", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(first)
	if err != nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("first request: %v %v", response, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	second, err := http.NewRequestWithContext(ctx, http.MethodPut, "https://refusing.edge.test/blob", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Do(second)
	var refused *Refused
	if !errors.As(err, &refused) || refused.Host != "refusing.edge.test" {
		t.Fatalf("second request: %v", err)
	}
	var nonRetriable interface{ NonRetriable() }
	if !errors.As(err, &nonRetriable) {
		t.Fatal("a refusal reads as retriable")
	}
	if edge.calls != 1 {
		t.Fatalf("the edge saw %d requests during its cooldown", edge.calls)
	}
}

func TestAgentFamilyIsTheLeadingProductName(t *testing.T) {
	for userAgent, want := range map[string]string{
		"sand/0.162.1117 azsdk-go-azblob/v1.6.0 (go1.25; linux)": "sand",
		"azsdk-go-azblob/v1.6.0 (go1.25; linux)":                 "azsdk-go-azblob",
		"Go-http-client/1.1":                                     "go-http-client",
		"  ":                                                     "none",
	} {
		if got := agentFamily(userAgent); got != want {
			t.Errorf("agentFamily(%q) = %q, want %q", userAgent, got, want)
		}
	}
}
