package kube

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestListDeploymentsFollowsPagination(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/apis/apps/v1/deployments" {
			t.Errorf("path %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("labelSelector"); got != "idle-reaper.io/enabled=true" {
			t.Errorf("selector %q", got)
		}
		if r.Header.Get("Authorization") != "" {
			t.Error("New() client must not send auth")
		}
		switch r.URL.Query().Get("continue") {
		case "":
			io.WriteString(w, `{"metadata":{"continue":"page2"},"items":[{"metadata":{"name":"a","namespace":"x"},"spec":{"replicas":2}}]}`)
		case "page2":
			io.WriteString(w, `{"metadata":{},"items":[{"metadata":{"name":"b","namespace":"y"},"spec":{}}]}`)
		default:
			t.Errorf("unexpected continue token")
		}
	}))
	defer srv.Close()

	deps, err := New(srv.URL, nil).ListDeployments(context.Background(), "idle-reaper.io/enabled=true")
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || len(deps) != 2 {
		t.Fatalf("calls=%d deps=%d", calls, len(deps))
	}
	if deps[0].Key() != "x/a" || deps[0].Replicas() != 2 {
		t.Errorf("first: %s %d", deps[0].Key(), deps[0].Replicas())
	}
	if deps[1].Replicas() != 1 {
		t.Errorf("nil replicas should default to 1, got %d", deps[1].Replicas())
	}
}

func TestPatchesUseMergePatchAndCarryResourceVersion(t *testing.T) {
	var gotCT string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch || r.URL.Path != "/apis/apps/v1/namespaces/ns/deployments/web" {
			t.Errorf("%s %s", r.Method, r.URL.Path)
		}
		gotCT = r.Header.Get("Content-Type")
		json.NewDecoder(r.Body).Decode(&gotBody)
		io.WriteString(w, `{}`)
	}))
	defer srv.Close()

	var d Deployment
	d.Metadata.Namespace, d.Metadata.Name, d.Metadata.ResourceVersion = "ns", "web", "42"
	v := "3"
	if err := New(srv.URL, nil).ScaleAndAnnotate(context.Background(), d, 0, map[string]*string{"k": &v, "gone": nil}); err != nil {
		t.Fatal(err)
	}
	if gotCT != "application/merge-patch+json" {
		t.Errorf("content-type %q", gotCT)
	}
	meta := gotBody["metadata"].(map[string]any)
	if meta["resourceVersion"] != "42" {
		t.Errorf("resourceVersion not sent: %v", meta)
	}
	ann := meta["annotations"].(map[string]any)
	if ann["k"] != "3" {
		t.Errorf("annotation value: %v", ann)
	}
	if v, ok := ann["gone"]; !ok || v != nil {
		t.Errorf("nil annotation must be sent as JSON null to delete it: %v", ann)
	}
	if gotBody["spec"].(map[string]any)["replicas"].(float64) != 0 {
		t.Errorf("replicas: %v", gotBody["spec"])
	}
}

func TestConflictIsDetectable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"reason":"Conflict"}`, http.StatusConflict)
	}))
	defer srv.Close()

	var d Deployment
	d.Metadata.Namespace, d.Metadata.Name = "ns", "web"
	err := New(srv.URL, nil).ScaleAndAnnotate(context.Background(), d, 0, nil)
	if !IsConflict(err) {
		t.Fatalf("want conflict, got %v", err)
	}
	if IsConflict(&APIError{Status: 500}) {
		t.Fatal("500 is not a conflict")
	}
}

func TestInClusterFailsOutsideCluster(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	if _, err := InCluster(); err == nil {
		t.Fatal("want error outside a cluster")
	}
}
