// Package prom runs instant queries against the Prometheus HTTP API.
package prom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ErrNoData means the query ran fine but returned nothing. The reaper treats
// this as "not idle": a broken query or a missing scrape should never be the
// reason something gets scaled to zero.
var ErrNoData = errors.New("query returned no data")

type Client struct {
	base string
	http *http.Client
}

func New(base string, hc *http.Client) *Client {
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}
	return &Client{base: strings.TrimRight(base, "/"), http: hc}
}

type response struct {
	Status    string `json:"status"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
	Data      struct {
		ResultType string          `json:"resultType"`
		Result     json.RawMessage `json:"result"`
	} `json:"data"`
}

type sample struct {
	Value [2]any `json:"value"` // [unix_ts, "value"]
}

// Scalar runs an instant query and returns a single number.
//
// Vector results must have exactly one series. If your query can return
// several, wrap it in sum() so the answer is unambiguous.
func (c *Client) Scalar(ctx context.Context, query string) (float64, error) {
	q := url.Values{}
	q.Set("query", query)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/api/v1/query?"+q.Encode(), nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return 0, err
	}

	var r response
	if err := json.Unmarshal(body, &r); err != nil {
		return 0, fmt.Errorf("prometheus: %d: bad json: %w", resp.StatusCode, err)
	}
	if r.Status != "success" {
		return 0, fmt.Errorf("prometheus: %s: %s", r.ErrorType, r.Error)
	}

	switch r.Data.ResultType {
	case "scalar":
		var v [2]any
		if err := json.Unmarshal(r.Data.Result, &v); err != nil {
			return 0, err
		}
		return parseValue(v[1])
	case "vector":
		var vec []sample
		if err := json.Unmarshal(r.Data.Result, &vec); err != nil {
			return 0, err
		}
		switch len(vec) {
		case 0:
			return 0, ErrNoData
		case 1:
			return parseValue(vec[0].Value[1])
		default:
			return 0, fmt.Errorf("query returned %d series, want 1 (wrap it in sum())", len(vec))
		}
	default:
		return 0, fmt.Errorf("unsupported result type %q", r.Data.ResultType)
	}
}

func parseValue(v any) (float64, error) {
	s, ok := v.(string)
	if !ok {
		return 0, fmt.Errorf("value is %T, want string", v)
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}
	// NaN from e.g. 0/0 is not evidence of idleness either.
	if f != f {
		return 0, ErrNoData
	}
	return f, nil
}
