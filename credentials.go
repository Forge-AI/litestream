package litestream

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"
)

// Transport carries every replica client's requests, so the databases one
// process replicates share one pool of connections to each store.
var Transport = &http.Transport{
	Proxy: http.ProxyFromEnvironment,
	DialContext: (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext,
	ForceAttemptHTTP2:     true,
	MaxIdleConns:          100,
	MaxIdleConnsPerHost:   100,
	IdleConnTimeout:       90 * time.Second,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 1 * time.Second,
}

type CredentialSource struct {
	URL   string
	Token string
}

func CredentialSourceFromQuery(query url.Values) CredentialSource {
	return CredentialSource{URL: query.Get("credentials-url"), Token: query.Get("credentials-token")}
}

func (s CredentialSource) Fetch(ctx context.Context, into any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.URL, nil)
	if err != nil {
		return fmt.Errorf("build the credential request: %w", err)
	}
	req.Header.Set("Authorization", s.Token)
	resp, err := (&http.Client{Transport: Transport}).Do(req)
	if err != nil {
		return fmt.Errorf("fetch the database's credentials: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch the database's credentials: %s", resp.Status)
	}
	if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
		return fmt.Errorf("decode the database's credentials: %w", err)
	}
	return nil
}
