package s3

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

func TestReplicaClient_SignsWithRotatingSecret(t *testing.T) {
	client := NewReplicaClient()
	client.Bucket = "runs"
	client.Endpoint = "https://r2.example"
	client.Region = "wnam"
	client.AccessKeyID = "test-key"
	client.SecretAccessKey = "first-secret"
	if err := client.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, time.November, 2, 11, 40, 0, 0, time.UTC)
	authorization := func(signer v4.HTTPSigner, credentials aws.Credentials) string {
		t.Helper()
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://r2.example/runs?list-type=2&prefix=ltx/", nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := signer.SignHTTP(context.Background(), credentials, req, "UNSIGNED-PAYLOAD", "s3", "wnam", at); err != nil {
			t.Fatal(err)
		}
		return req.Header.Get("Authorization")
	}
	signer := client.s3.Options().HTTPSignerV4
	for _, secret := range []string{"first-secret", "second-secret", "first-secret"} {
		credentials := aws.Credentials{
			AccessKeyID:     "test-key",
			SecretAccessKey: secret,
			SessionToken:    "token-" + secret,
		}
		if authorization(signer, credentials) != authorization(v4.NewSigner(), credentials) {
			t.Fatal("credential rotation signed with a stale derived key")
		}
	}
}
