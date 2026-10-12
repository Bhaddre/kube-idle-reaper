package prom

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func serve(t *testing.T, body string) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query" || r.URL.Query().Get("query") == "" {
			t.Errorf("bad request %s", r.URL)
		}
		io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return New(srv.URL, nil)
}

func TestScalar(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		want    float64
		wantErr string
		noData  bool
	}{
		{"one series", `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"2.5"]}]}}`, 2.5, "", false},
		{"scalar", `{"status":"success","data":{"resultType":"scalar","result":[1,"7"]}}`, 7, "", false},
		{"empty vector", `{"status":"success","data":{"resultType":"vector","result":[]}}`, 0, "", true},
		{"NaN", `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"NaN"]}]}}`, 0, "", true},
		{"two series", `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"1"]},{"value":[1,"2"]}]}}`, 0, "wrap it in sum()", false},
		{"prom error", `{"status":"error","errorType":"bad_data","error":"parse error"}`, 0, "parse error", false},
		{"matrix", `{"status":"success","data":{"resultType":"matrix","result":[]}}`, 0, "unsupported", false},
		{"garbage", `<html>`, 0, "bad json", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := serve(t, tc.body).Scalar(context.Background(), "up")
			switch {
			case tc.noData:
				if !errors.Is(err, ErrNoData) {
					t.Fatalf("want ErrNoData, got %v", err)
				}
			case tc.wantErr != "":
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
				}
			default:
				if err != nil || got != tc.want {
					t.Fatalf("got %v, %v want %v", got, err, tc.want)
				}
			}
		})
	}
}
