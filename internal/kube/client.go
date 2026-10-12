// Package kube is a deliberately tiny Kubernetes API client.
//
// It does exactly four things: list Deployments by label, read one, patch
// annotations, and patch the scale subresource. That's all the reaper needs,
// so there's no client-go, no informers, no codegen. Just net/http + JSON.
package kube

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	saDir     = "/var/run/secrets/kubernetes.io/serviceaccount"
	tokenFile = saDir + "/token"
	caFile    = saDir + "/ca.crt"
)

// Deployment is the subset of apps/v1 Deployment we care about.
type Deployment struct {
	Metadata struct {
		Name            string            `json:"name"`
		Namespace       string            `json:"namespace"`
		Labels          map[string]string `json:"labels,omitempty"`
		Annotations     map[string]string `json:"annotations,omitempty"`
		ResourceVersion string            `json:"resourceVersion,omitempty"`
	} `json:"metadata"`
	Spec struct {
		Replicas *int32 `json:"replicas,omitempty"`
	} `json:"spec"`
}

// Replicas returns spec.replicas, defaulting to 1 like the API server does.
func (d Deployment) Replicas() int32 {
	if d.Spec.Replicas == nil {
		return 1
	}
	return *d.Spec.Replicas
}

// Key is namespace/name, handy for logs and metrics.
func (d Deployment) Key() string { return d.Metadata.Namespace + "/" + d.Metadata.Name }

type deploymentList struct {
	Items    []Deployment `json:"items"`
	Metadata struct {
		Continue string `json:"continue"`
	} `json:"metadata"`
}

// APIError is a non-2xx response from the API server.
type APIError struct {
	Status int
	Body   string
}

func (e *APIError) Error() string { return fmt.Sprintf("kube api: %d: %s", e.Status, e.Body) }

// Client talks to one API server.
type Client struct {
	base  string
	http  *http.Client
	token func() (string, error)
}

// New builds a client for an explicit base URL with no auth. Use it with
// `kubectl proxy` for local runs, or in tests with httptest.
func New(base string, hc *http.Client) *Client {
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}
	return &Client{base: strings.TrimRight(base, "/"), http: hc, token: func() (string, error) { return "", nil }}
}

// InCluster builds a client from the pod's service account.
func InCluster() (*Client, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, fmt.Errorf("not running in a cluster (KUBERNETES_SERVICE_HOST/PORT unset)")
	}
	ca, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read ca: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("no certs in %s", caFile)
	}
	hc := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
		},
	}
	c := New("https://"+net.JoinHostPort(host, port), hc)
	// Bound SA tokens rotate. Read the file every time instead of caching it.
	c.token = func() (string, error) {
		b, err := os.ReadFile(tokenFile)
		return strings.TrimSpace(string(b)), err
	}
	return c, nil
}

func (c *Client) do(ctx context.Context, method, path, contentType string, body []byte, out any) error {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rdr)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	tok, err := c.token()
	if err != nil {
		return fmt.Errorf("read token: %w", err)
	}
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return &APIError{Status: resp.StatusCode, Body: strings.TrimSpace(string(data))}
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}

// ListDeployments returns every Deployment matching labelSelector in all
// namespaces, following pagination.
func (c *Client) ListDeployments(ctx context.Context, labelSelector string) ([]Deployment, error) {
	var all []Deployment
	cont := ""
	for {
		q := url.Values{}
		q.Set("labelSelector", labelSelector)
		q.Set("limit", "500")
		if cont != "" {
			q.Set("continue", cont)
		}
		var page deploymentList
		if err := c.do(ctx, http.MethodGet, "/apis/apps/v1/deployments?"+q.Encode(), "", nil, &page); err != nil {
			return nil, err
		}
		all = append(all, page.Items...)
		if page.Metadata.Continue == "" {
			return all, nil
		}
		cont = page.Metadata.Continue
	}
}

func deployPath(ns, name string) string {
	return "/apis/apps/v1/namespaces/" + url.PathEscape(ns) + "/deployments/" + url.PathEscape(name)
}

// PatchAnnotations sets annotations; a nil value deletes the key
// (JSON merge patch semantics).
func (c *Client) PatchAnnotations(ctx context.Context, ns, name string, ann map[string]*string) error {
	body, err := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": ann}})
	if err != nil {
		return err
	}
	return c.do(ctx, http.MethodPatch, deployPath(ns, name), "application/merge-patch+json", body, nil)
}

// ScaleAndAnnotate sets replicas and annotations in one patch on the
// Deployment itself, guarded by resourceVersion. If someone changed the
// Deployment since we read it (a human scaled it, a rollout started), the
// API server returns 409 and we skip this round instead of stomping on them.
func (c *Client) ScaleAndAnnotate(ctx context.Context, d Deployment, replicas int32, ann map[string]*string) error {
	patch := map[string]any{
		"metadata": map[string]any{
			"resourceVersion": d.Metadata.ResourceVersion,
			"annotations":     ann,
		},
		"spec": map[string]any{"replicas": replicas},
	}
	body, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	return c.do(ctx, http.MethodPatch, deployPath(d.Metadata.Namespace, d.Metadata.Name), "application/merge-patch+json", body, nil)
}

// IsConflict reports a 409 from the API server.
func IsConflict(err error) bool {
	if e, ok := err.(*APIError); ok {
		return e.Status == http.StatusConflict
	}
	return false
}
