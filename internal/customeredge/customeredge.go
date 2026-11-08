package customeredge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type budget struct {
	mu           sync.Mutex
	interval     time.Duration
	next         time.Time
	blockedUntil time.Time
	refusals     int
}

func (b *budget) admit(now time.Time) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	wait := max(b.next.Sub(now), b.blockedUntil.Sub(now))
	if wait > 0 {
		return wait
	}
	b.next = now.Add(b.interval)
	return 0
}

func (b *budget) respond(now time.Time, status int, retryAfter string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if status != http.StatusForbidden {
		if status >= 200 && status < 300 {
			b.refusals = 0
		}
		return
	}
	backoff := time.Second * time.Duration(1<<min(b.refusals, 6))
	b.refusals++
	until := now.Add(backoff)
	if seconds, err := strconv.ParseUint(retryAfter, 10, 32); err == nil {
		until = later(until, now.Add(time.Duration(seconds)*time.Second))
	} else if date, err := http.ParseTime(retryAfter); err == nil {
		until = later(until, date)
	}
	b.blockedUntil = later(b.blockedUntil, until)
}

func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func (b *budget) cooling(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.blockedUntil.After(now)
}

type Refused struct {
	Host  string
	Until time.Time
}

func (e *Refused) Error() string {
	return fmt.Sprintf("customer edge %s answered 403 and cools down until %s, past this request's deadline", e.Host, e.Until.UTC().Format(time.RFC3339))
}

func (e *Refused) NonRetriable() {}

func (b *budget) wait(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		now := time.Now()
		delay := b.admit(now)
		if delay == 0 {
			return nil
		}
		if deadline, ok := ctx.Deadline(); ok && now.Add(delay).After(deadline) && b.cooling(now) {
			return &Refused{Until: now.Add(delay)}
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

var shared = struct {
	sync.Mutex
	rates   map[string]float64
	budgets map[string]*budget
}{budgets: make(map[string]*budget)}

func hostBudget(host string) *budget {
	shared.Lock()
	defer shared.Unlock()
	if shared.rates == nil {
		shared.rates = make(map[string]float64)
		if raw := os.Getenv("CUSTOMER_EDGE_REQUESTS_PER_SECOND"); raw != "" {
			if err := json.Unmarshal([]byte(raw), &shared.rates); err != nil {
				panic(fmt.Sprintf("CUSTOMER_EDGE_REQUESTS_PER_SECOND: %v", err))
			}
			for host, rate := range shared.rates {
				if rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) {
					panic(fmt.Sprintf("CUSTOMER_EDGE_REQUESTS_PER_SECOND: invalid rate for %s", host))
				}
			}
		}
	}
	if cached := shared.budgets[host]; cached != nil {
		return cached
	}
	rate := 5.0
	if configured, ok := shared.rates[host]; ok {
		rate = configured
	}
	b := &budget{interval: time.Duration(float64(time.Second) / rate)}
	shared.budgets[host] = b
	return b
}

type Doer interface {
	Do(*http.Request) (*http.Response, error)
}

type Client struct {
	HTTP Doer
	Name string
}

func (c Client) Do(req *http.Request) (*http.Response, error) {
	host := strings.ToLower(req.URL.Hostname())
	if strings.HasSuffix(host, ".blob.core.windows.net") || host == "127.0.0.1" || host == "localhost" {
		return c.HTTP.Do(req)
	}
	b := hostBudget(host)
	err := b.wait(req.Context())
	var refused *Refused
	if errors.As(err, &refused) {
		refused.Host = host
	} else if err != nil {
		return nil, err
	}
	var response *http.Response
	if refused == nil {
		response, err = c.HTTP.Do(req)
	}
	status, retryAfter := 0, ""
	if response != nil {
		status, retryAfter = response.StatusCode, response.Header.Get("Retry-After")
		b.respond(time.Now(), status, retryAfter)
	}
	outcome, cause := "other", "none"
	switch {
	case refused != nil:
		outcome, cause = "403", "cooldown"
	case err != nil:
		cause = "transport"
	case status >= 200 && status < 300:
		outcome = "2xx"
	case status == http.StatusForbidden:
		outcome, cause = "403", "forbidden"
	case status >= 500:
		cause = "server_error"
	case status >= 400:
		cause = "client_error"
	}
	sent := req
	if response != nil {
		sent = response.Request
	}
	agent := agentFamily(sent.Header.Get("User-Agent"))
	requests().Add(req.Context(), 1, metric.WithAttributes(attribute.String("edge_host", host), attribute.String("outcome", outcome)))
	if err != nil || status >= 400 {
		slog.WarnContext(req.Context(), "customer edge request failed", "stage", "customer_edge_request", "edge_host", host, "client", c.Name, "user_agent", agent, "cause", cause, "method", req.Method, "status", status)
	}
	return response, err
}

func agentFamily(userAgent string) string {
	product, _, _ := strings.Cut(strings.TrimSpace(userAgent), " ")
	product, _, _ = strings.Cut(product, "/")
	if product == "" {
		return "none"
	}
	return strings.ToLower(product)
}

var requests = sync.OnceValue(func() metric.Int64Counter {
	requests, err := otel.Meter("customeredge").Int64Counter("customer.edge.request.count")
	if err != nil {
		panic(err)
	}
	return requests
})
