package reaper

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/Bhaddre/kube-idle-reaper/internal/kube"
	"github.com/Bhaddre/kube-idle-reaper/internal/prom"
)

// --- fakes -------------------------------------------------------------------

type fakeKube struct {
	deps      map[string]*kube.Deployment
	conflict  map[string]bool
	listErr   error
	patches   int
	scaleCall int
}

func newFakeKube(deps ...kube.Deployment) *fakeKube {
	f := &fakeKube{deps: map[string]*kube.Deployment{}, conflict: map[string]bool{}}
	for i := range deps {
		d := deps[i]
		f.deps[d.Key()] = &d
	}
	return f
}

func (f *fakeKube) ListDeployments(_ context.Context, sel string) ([]kube.Deployment, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	if sel != LabelEnabled+"=true" {
		return nil, fmt.Errorf("unexpected selector %q", sel)
	}
	var out []kube.Deployment
	for _, d := range f.deps {
		cp := *d
		cp.Metadata.Annotations = map[string]string{}
		for k, v := range d.Metadata.Annotations {
			cp.Metadata.Annotations[k] = v
		}
		out = append(out, cp)
	}
	return out, nil
}

func apply(d *kube.Deployment, ann map[string]*string) {
	if d.Metadata.Annotations == nil {
		d.Metadata.Annotations = map[string]string{}
	}
	for k, v := range ann {
		if v == nil {
			delete(d.Metadata.Annotations, k)
		} else {
			d.Metadata.Annotations[k] = *v
		}
	}
}

func (f *fakeKube) PatchAnnotations(_ context.Context, ns, name string, ann map[string]*string) error {
	f.patches++
	apply(f.deps[ns+"/"+name], ann)
	return nil
}

func (f *fakeKube) ScaleAndAnnotate(_ context.Context, d kube.Deployment, replicas int32, ann map[string]*string) error {
	f.scaleCall++
	if f.conflict[d.Key()] {
		return &kube.APIError{Status: 409, Body: "conflict"}
	}
	cur := f.deps[d.Key()]
	cur.Spec.Replicas = &replicas
	apply(cur, ann)
	return nil
}

type fakeProm map[string]struct {
	v   float64
	err error
}

func (p fakeProm) Scalar(_ context.Context, q string) (float64, error) {
	r, ok := p[q]
	if !ok {
		return 0, prom.ErrNoData
	}
	return r.v, r.err
}

// --- helpers -----------------------------------------------------------------

var t0 = time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)

func dep(ns, name string, replicas int32, ann map[string]string) kube.Deployment {
	var d kube.Deployment
	d.Metadata.Namespace, d.Metadata.Name = ns, name
	d.Metadata.Labels = map[string]string{LabelEnabled: "true"}
	d.Metadata.Annotations = ann
	d.Spec.Replicas = &replicas
	return d
}

func cfg() Config {
	return Config{DefaultGrace: 48 * time.Hour, MaxScalePerCycle: 5, ExcludeNamespaces: map[string]bool{"kube-system": true}}
}

func newReaper(k Kube, p Prom, c Config, now *time.Time) *Reaper {
	r := New(k, p, c, slog.New(slog.NewTextHandler(io.Discard, nil)))
	r.SetClock(func() time.Time { return *now })
	return r
}

func cycle(t *testing.T, r *Reaper) map[string]Outcome {
	t.Helper()
	out, err := r.Cycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// --- tests -------------------------------------------------------------------

func TestFullLifecycle(t *testing.T) {
	now := t0
	k := newFakeKube(dep("team-a", "batch", 3, map[string]string{AnnQuery: "q"}))
	p := fakeProm{"q": {v: 0}}
	r := newReaper(k, p, cfg(), &now)

	if got := cycle(t, r)["team-a/batch"]; got != IdleStarted {
		t.Fatalf("first idle cycle: got %s", got)
	}
	if k.deps["team-a/batch"].Metadata.Annotations[AnnIdleSince] != t0.Format(time.RFC3339) {
		t.Fatal("idle-since not set")
	}

	now = t0.Add(47 * time.Hour)
	if got := cycle(t, r)["team-a/batch"]; got != IdleWaiting {
		t.Fatalf("inside grace: got %s", got)
	}

	now = t0.Add(48 * time.Hour)
	if got := cycle(t, r)["team-a/batch"]; got != Scaled {
		t.Fatalf("past grace: got %s", got)
	}
	d := k.deps["team-a/batch"]
	if d.Replicas() != 0 {
		t.Fatalf("replicas = %d, want 0", d.Replicas())
	}
	if d.Metadata.Annotations[AnnScaledFrom] != "3" {
		t.Fatalf("scaled-from = %q, want 3", d.Metadata.Annotations[AnnScaledFrom])
	}
	if _, ok := d.Metadata.Annotations[AnnIdleSince]; ok {
		t.Fatal("idle-since should be cleared after scaling")
	}

	// next cycle leaves it alone
	if got := cycle(t, r)["team-a/batch"]; got != SkippedZero {
		t.Fatalf("after scale: got %s", got)
	}
}

func TestActivityResetsTheClock(t *testing.T) {
	now := t0
	k := newFakeKube(dep("a", "svc", 2, map[string]string{AnnQuery: "q", AnnIdleSince: t0.Add(-40 * time.Hour).Format(time.RFC3339)}))
	p := fakeProm{"q": {v: 3.5}}
	r := newReaper(k, p, cfg(), &now)

	if got := cycle(t, r)["a/svc"]; got != Active {
		t.Fatalf("got %s", got)
	}
	if _, ok := k.deps["a/svc"].Metadata.Annotations[AnnIdleSince]; ok {
		t.Fatal("activity should clear idle-since")
	}
}

func TestNoDataIsNeverIdle(t *testing.T) {
	now := t0
	old := t0.Add(-100 * time.Hour).Format(time.RFC3339)
	k := newFakeKube(
		dep("a", "nodata", 1, map[string]string{AnnQuery: "missing", AnnIdleSince: old}),
		dep("a", "promdown", 1, map[string]string{AnnQuery: "boom", AnnIdleSince: old}),
	)
	p := fakeProm{"boom": {err: errors.New("connection refused")}}
	r := newReaper(k, p, cfg(), &now)

	out := cycle(t, r)
	for _, key := range []string{"a/nodata", "a/promdown"} {
		if out[key] != QueryFailed {
			t.Errorf("%s: got %s, want %s", key, out[key], QueryFailed)
		}
		if k.deps[key].Replicas() != 1 {
			t.Errorf("%s was scaled on missing data", key)
		}
		if _, ok := k.deps[key].Metadata.Annotations[AnnIdleSince]; ok {
			t.Errorf("%s: idle clock should reset on missing data", key)
		}
	}
}

func TestThresholdAndPerDeploymentGrace(t *testing.T) {
	now := t0
	k := newFakeKube(dep("a", "svc", 1, map[string]string{
		AnnQuery:     "q",
		AnnThreshold: "0.5",
		AnnGrace:     "1h",
		AnnIdleSince: t0.Add(-61 * time.Minute).Format(time.RFC3339),
	}))
	p := fakeProm{"q": {v: 0.4}} // under threshold counts as idle
	r := newReaper(k, p, cfg(), &now)

	if got := cycle(t, r)["a/svc"]; got != Scaled {
		t.Fatalf("got %s", got)
	}
}

func TestDryRunAnnotatesButDoesNotScale(t *testing.T) {
	now := t0
	k := newFakeKube(dep("a", "svc", 4, map[string]string{AnnQuery: "q", AnnIdleSince: t0.Add(-49 * time.Hour).Format(time.RFC3339)}))
	c := cfg()
	c.DryRun = true
	r := newReaper(k, fakeProm{"q": {v: 0}}, c, &now)

	if got := cycle(t, r)["a/svc"]; got != DryRun {
		t.Fatalf("got %s", got)
	}
	if k.scaleCall != 0 || k.deps["a/svc"].Replicas() != 4 {
		t.Fatal("dry run must not scale")
	}
	if k.deps["a/svc"].Metadata.Annotations[AnnWouldScale] == "" {
		t.Fatal("dry run should leave a would-scale-at annotation")
	}

	// second dry-run cycle doesn't re-patch the same annotation
	before := k.patches
	cycle(t, r)
	if k.patches != before {
		t.Fatal("dry run re-patched an unchanged annotation")
	}
}

func TestCapLimitsBlastRadius(t *testing.T) {
	now := t0
	old := t0.Add(-100 * time.Hour).Format(time.RFC3339)
	var deps []kube.Deployment
	for i := 0; i < 5; i++ {
		deps = append(deps, dep("a", fmt.Sprintf("svc-%d", i), 1, map[string]string{AnnQuery: "q", AnnIdleSince: old}))
	}
	k := newFakeKube(deps...)
	c := cfg()
	c.MaxScalePerCycle = 2
	r := newReaper(k, fakeProm{"q": {v: 0}}, c, &now)

	out := cycle(t, r)
	counts := map[Outcome]int{}
	for _, o := range out {
		counts[o]++
	}
	if counts[Scaled] != 2 || counts[CapReached] != 3 {
		t.Fatalf("got %v, want 2 scaled and 3 capped", counts)
	}
}

func TestConflictSkips(t *testing.T) {
	now := t0
	k := newFakeKube(dep("a", "svc", 2, map[string]string{AnnQuery: "q", AnnIdleSince: t0.Add(-49 * time.Hour).Format(time.RFC3339)}))
	k.conflict["a/svc"] = true
	r := newReaper(k, fakeProm{"q": {v: 0}}, cfg(), &now)

	if got := cycle(t, r)["a/svc"]; got != Conflict {
		t.Fatalf("got %s", got)
	}
	if k.deps["a/svc"].Replicas() != 2 {
		t.Fatal("should not scale on conflict")
	}
}

func TestSkipsAndConfigErrors(t *testing.T) {
	now := t0
	k := newFakeKube(
		dep("kube-system", "dns", 2, map[string]string{AnnQuery: "q"}),
		dep("a", "noquery", 1, nil),
		dep("a", "badthreshold", 1, map[string]string{AnnQuery: "q", AnnThreshold: "lots"}),
		dep("a", "badgrace", 1, map[string]string{AnnQuery: "q", AnnGrace: "-5m"}),
	)
	r := newReaper(k, fakeProm{"q": {v: 0}}, cfg(), &now)

	want := map[string]Outcome{
		"kube-system/dns": SkippedNS,
		"a/noquery":       ConfigError,
		"a/badthreshold":  ConfigError,
		"a/badgrace":      ConfigError,
	}
	out := cycle(t, r)
	for key, w := range want {
		if out[key] != w {
			t.Errorf("%s: got %s, want %s", key, out[key], w)
		}
	}
	if k.patches != 0 || k.scaleCall != 0 {
		t.Fatal("skipped deployments must not be touched")
	}
}

func TestListErrorFailsCycle(t *testing.T) {
	now := t0
	k := newFakeKube()
	k.listErr = errors.New("forbidden")
	r := newReaper(k, fakeProm{}, cfg(), &now)
	if _, err := r.Cycle(context.Background()); err == nil {
		t.Fatal("want error")
	}
	if r.Healthy(time.Hour) {
		t.Fatal("failed cycle should not count as healthy")
	}
}

func TestMetricsAndHealth(t *testing.T) {
	now := t0
	k := newFakeKube(dep("a", "svc", 1, map[string]string{AnnQuery: "q"}))
	r := newReaper(k, fakeProm{"q": {v: 1}}, cfg(), &now)

	if r.Healthy(time.Hour) {
		t.Fatal("healthy before any cycle")
	}
	cycle(t, r)
	if !r.Healthy(time.Minute) {
		t.Fatal("should be healthy right after a cycle")
	}
	now = t0.Add(2 * time.Hour)
	if r.Healthy(time.Hour) {
		t.Fatal("should go unhealthy when cycles stop")
	}

	m := r.Metrics()
	for _, want := range []string{
		`idle_reaper_decisions_total{outcome="active"} 1`,
		"idle_reaper_dry_run 0",
		"# TYPE idle_reaper_last_cycle_timestamp_seconds gauge",
	} {
		if !strings.Contains(m, want) {
			t.Errorf("metrics missing %q:\n%s", want, m)
		}
	}
}
