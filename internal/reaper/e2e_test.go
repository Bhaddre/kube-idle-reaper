package reaper

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Bhaddre/kube-idle-reaper/internal/kube"
	"github.com/Bhaddre/kube-idle-reaper/internal/prom"
)

// fakeAPIServer is just enough of the Kubernetes API to exercise the real
// kube client: list deployments, and JSON merge patch with resourceVersion
// checks, the same way the real API server behaves.
type fakeAPIServer struct {
	mu   sync.Mutex
	deps map[string]map[string]any // "ns/name" -> object
	rv   int
}

func (s *fakeAPIServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if r.Method == http.MethodGet && r.URL.Path == "/apis/apps/v1/deployments" {
		var items []any
		for _, d := range s.deps {
			items = append(items, d)
		}
		json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{}, "items": items})
		return
	}

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	// apis apps v1 namespaces {ns} deployments {name}
	if r.Method != http.MethodPatch || len(parts) != 7 {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if r.Header.Get("Content-Type") != "application/merge-patch+json" {
		http.Error(w, "unsupported patch type", http.StatusUnsupportedMediaType)
		return
	}
	obj, ok := s.deps[parts[4]+"/"+parts[6]]
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	var patch map[string]any
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	meta := obj["metadata"].(map[string]any)
	if pm, ok := patch["metadata"].(map[string]any); ok {
		if want, ok := pm["resourceVersion"].(string); ok && want != meta["resourceVersion"] {
			http.Error(w, "the object has been modified", http.StatusConflict)
			return
		}
		delete(pm, "resourceVersion")
	}
	mergePatch(obj, patch)
	s.rv++
	meta["resourceVersion"] = strconv.Itoa(s.rv)
	json.NewEncoder(w).Encode(obj)
}

// mergePatch implements RFC 7386.
func mergePatch(target, patch map[string]any) {
	for k, v := range patch {
		if v == nil {
			delete(target, k)
			continue
		}
		if pm, ok := v.(map[string]any); ok {
			tm, ok := target[k].(map[string]any)
			if !ok {
				tm = map[string]any{}
				target[k] = tm
			}
			mergePatch(tm, pm)
			continue
		}
		target[k] = v
	}
}

func (s *fakeAPIServer) add(ns, name string, replicas int, ann map[string]any) {
	s.rv++
	s.deps[ns+"/"+name] = map[string]any{
		"metadata": map[string]any{
			"name": name, "namespace": ns, "resourceVersion": strconv.Itoa(s.rv),
			"labels":      map[string]any{LabelEnabled: "true"},
			"annotations": ann,
		},
		"spec": map[string]any{"replicas": float64(replicas)},
	}
}

func (s *fakeAPIServer) get(key string) (replicas int, ann map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.deps[key]
	ann, _ = d["metadata"].(map[string]any)["annotations"].(map[string]any)
	return int(d["spec"].(map[string]any)["replicas"].(float64)), ann
}

func TestEndToEndWithRealClients(t *testing.T) {
	api := &fakeAPIServer{deps: map[string]map[string]any{}}
	api.add("ml", "notebook", 2, map[string]any{AnnQuery: `sum(rate(http_requests_total{app="notebook"}[1h]))`, AnnGrace: "2h"})
	api.add("ml", "busy", 1, map[string]any{AnnQuery: `sum(rate(http_requests_total{app="busy"}[1h]))`, AnnGrace: "2h"})
	apiSrv := httptest.NewServer(api)
	defer apiSrv.Close()

	promSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v := "0"
		if strings.Contains(r.URL.Query().Get("query"), `app="busy"`) {
			v = "12.5"
		}
		fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,%q]}]}}`, v)
	}))
	defer promSrv.Close()

	now := t0
	r := New(kube.New(apiSrv.URL, nil), prom.New(promSrv.URL, nil), Config{
		DefaultGrace: 48 * time.Hour, MaxScalePerCycle: 5,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	r.SetClock(func() time.Time { return now })

	run := func() map[string]Outcome {
		out, err := r.Cycle(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return out
	}

	if out := run(); out["ml/notebook"] != IdleStarted || out["ml/busy"] != Active {
		t.Fatalf("cycle 1: %v", out)
	}
	now = now.Add(3 * time.Hour)
	if out := run(); out["ml/notebook"] != Scaled || out["ml/busy"] != Active {
		t.Fatalf("cycle 2: %v", out)
	}

	replicas, ann := api.get("ml/notebook")
	if replicas != 0 {
		t.Fatalf("notebook replicas = %d", replicas)
	}
	if ann[AnnScaledFrom] != "2" {
		t.Fatalf("scaled-from = %v", ann[AnnScaledFrom])
	}
	if _, ok := ann[AnnIdleSince]; ok {
		t.Fatal("idle-since should be deleted by the merge patch")
	}
	if replicas, _ := api.get("ml/busy"); replicas != 1 {
		t.Fatalf("busy replicas = %d", replicas)
	}
}

func TestEndToEndConflictWhenSomeoneScalesFirst(t *testing.T) {
	api := &fakeAPIServer{deps: map[string]map[string]any{}}
	api.add("ml", "svc", 2, map[string]any{AnnQuery: "q", AnnIdleSince: t0.Add(-49 * time.Hour).Format(time.RFC3339)})
	apiSrv := httptest.NewServer(api)
	defer apiSrv.Close()

	promSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// a human bumps the deployment between our list and our patch
		api.mu.Lock()
		meta := api.deps["ml/svc"]["metadata"].(map[string]any)
		meta["resourceVersion"] = "999"
		api.mu.Unlock()
		io.WriteString(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"0"]}]}}`)
	}))
	defer promSrv.Close()

	now := t0
	r := New(kube.New(apiSrv.URL, nil), prom.New(promSrv.URL, nil), Config{DefaultGrace: 48 * time.Hour, MaxScalePerCycle: 5},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	r.SetClock(func() time.Time { return now })

	out, err := r.Cycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out["ml/svc"] != Conflict {
		t.Fatalf("got %s, want conflict", out["ml/svc"])
	}
	if replicas, _ := api.get("ml/svc"); replicas != 2 {
		t.Fatalf("replicas = %d, should be untouched", replicas)
	}
}
