// Package reaper decides when an opted-in Deployment has been idle long
// enough to scale to zero, and does it.
//
// All state lives in annotations on the Deployment itself, so the reaper is
// stateless: restart it, run it somewhere else, nothing is lost. And anyone
// can see what it thinks with `kubectl get deploy -o yaml`.
package reaper

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Bhaddre/kube-idle-reaper/internal/kube"
)

const (
	prefix = "idle-reaper.io/"

	// LabelEnabled opts a Deployment in. Label, not annotation, so the list
	// call can filter server side.
	LabelEnabled = prefix + "enabled"

	// Config, set by the owner of the Deployment.
	AnnQuery     = prefix + "query"     // PromQL returning one number. required
	AnnThreshold = prefix + "threshold" // idle when value <= threshold. default 0
	AnnGrace     = prefix + "grace"     // Go duration, e.g. 48h. default from flag

	// State, written by the reaper.
	AnnIdleSince  = prefix + "idle-since"     // RFC3339, first time we saw it idle
	AnnScaledFrom = prefix + "scaled-from"    // replica count before we scaled to 0
	AnnScaledAt   = prefix + "scaled-at"      // RFC3339
	AnnWouldScale = prefix + "would-scale-at" // dry run only
)

// Outcome is what happened to one Deployment in one cycle.
type Outcome string

const (
	Active      Outcome = "active"
	IdleStarted Outcome = "idle_started"
	IdleWaiting Outcome = "idle_waiting"
	Scaled      Outcome = "scaled"
	DryRun      Outcome = "dry_run"
	CapReached  Outcome = "cap_reached"
	SkippedZero Outcome = "skipped_zero"
	SkippedNS   Outcome = "skipped_namespace"
	ConfigError Outcome = "config_error"
	QueryFailed Outcome = "query_failed"
	Conflict    Outcome = "conflict"
	APIError    Outcome = "api_error"
)

// Kube is what the reaper needs from the API server.
type Kube interface {
	ListDeployments(ctx context.Context, labelSelector string) ([]kube.Deployment, error)
	PatchAnnotations(ctx context.Context, ns, name string, ann map[string]*string) error
	ScaleAndAnnotate(ctx context.Context, d kube.Deployment, replicas int32, ann map[string]*string) error
}

// Prom is what the reaper needs from Prometheus.
type Prom interface {
	Scalar(ctx context.Context, query string) (float64, error)
}

type Config struct {
	DefaultGrace      time.Duration
	DryRun            bool
	ExcludeNamespaces map[string]bool
	// MaxScalePerCycle caps blast radius. If a query breaks in a way that
	// reads as 0 everywhere (say `or vector(0)` on a renamed metric), this
	// stops the reaper from taking out the whole cluster in one pass.
	MaxScalePerCycle int
}

type Reaper struct {
	kube Kube
	prom Prom
	cfg  Config
	now  func() time.Time
	log  *slog.Logger

	mu        sync.Mutex
	counts    map[Outcome]uint64
	lastCycle time.Time
}

func New(k Kube, p Prom, cfg Config, log *slog.Logger) *Reaper {
	if log == nil {
		log = slog.Default()
	}
	return &Reaper{kube: k, prom: p, cfg: cfg, now: time.Now, log: log, counts: map[Outcome]uint64{}}
}

// SetClock is for tests.
func (r *Reaper) SetClock(now func() time.Time) { r.now = now }

func strp(s string) *string { return &s }

// Cycle runs one pass over every opted-in Deployment.
func (r *Reaper) Cycle(ctx context.Context) (map[string]Outcome, error) {
	deps, err := r.kube.ListDeployments(ctx, LabelEnabled+"=true")
	if err != nil {
		return nil, fmt.Errorf("list deployments: %w", err)
	}
	sort.Slice(deps, func(i, j int) bool { return deps[i].Key() < deps[j].Key() })

	out := make(map[string]Outcome, len(deps))
	scaled := 0
	for _, d := range deps {
		o := r.reconcile(ctx, d, &scaled)
		out[d.Key()] = o
		r.record(o)
	}
	r.mu.Lock()
	r.lastCycle = r.now()
	r.mu.Unlock()
	return out, nil
}

func (r *Reaper) reconcile(ctx context.Context, d kube.Deployment, scaled *int) Outcome {
	log := r.log.With("deployment", d.Key())
	ann := d.Metadata.Annotations
	now := r.now().UTC()

	if r.cfg.ExcludeNamespaces[d.Metadata.Namespace] {
		return SkippedNS
	}
	if d.Replicas() == 0 {
		// Already off, by us or by a human. Waking it up isn't our job.
		return SkippedZero
	}

	query := strings.TrimSpace(ann[AnnQuery])
	if query == "" {
		log.Warn("opted in but has no query annotation", "annotation", AnnQuery)
		return ConfigError
	}
	threshold := 0.0
	if s := ann[AnnThreshold]; s != "" {
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			log.Warn("bad threshold", "value", s, "err", err)
			return ConfigError
		}
		threshold = v
	}
	grace := r.cfg.DefaultGrace
	if s := ann[AnnGrace]; s != "" {
		v, err := time.ParseDuration(s)
		if err != nil || v <= 0 {
			log.Warn("bad grace", "value", s, "err", err)
			return ConfigError
		}
		grace = v
	}

	value, err := r.prom.Scalar(ctx, query)
	if err != nil {
		// No evidence is not evidence of idleness. Reset the clock so a
		// flaky scrape can't add up to a scale-down.
		log.Info("query failed, treating as active", "err", err)
		r.clearIdle(ctx, d, log)
		return QueryFailed
	}

	if value > threshold {
		r.clearIdle(ctx, d, log)
		return Active
	}

	since, err := time.Parse(time.RFC3339, ann[AnnIdleSince])
	if err != nil {
		// first time we've seen it idle (or someone mangled the annotation)
		if err := r.kube.PatchAnnotations(ctx, d.Metadata.Namespace, d.Metadata.Name,
			map[string]*string{AnnIdleSince: strp(now.Format(time.RFC3339))}); err != nil {
			log.Error("set idle-since", "err", err)
			return APIError
		}
		log.Info("idle, starting grace period", "value", value, "grace", grace.String())
		return IdleStarted
	}

	if now.Sub(since) < grace {
		return IdleWaiting
	}

	if *scaled >= r.cfg.MaxScalePerCycle {
		log.Warn("idle past grace but hit max scale-downs for this cycle", "max", r.cfg.MaxScalePerCycle)
		return CapReached
	}

	if r.cfg.DryRun {
		if ann[AnnWouldScale] == "" {
			if err := r.kube.PatchAnnotations(ctx, d.Metadata.Namespace, d.Metadata.Name,
				map[string]*string{AnnWouldScale: strp(now.Format(time.RFC3339))}); err != nil {
				log.Error("set would-scale-at", "err", err)
				return APIError
			}
		}
		log.Info("dry run: would scale to 0", "from", d.Replicas(), "idle_since", since)
		*scaled++
		return DryRun
	}

	err = r.kube.ScaleAndAnnotate(ctx, d, 0, map[string]*string{
		AnnScaledFrom: strp(strconv.Itoa(int(d.Replicas()))),
		AnnScaledAt:   strp(now.Format(time.RFC3339)),
		AnnIdleSince:  nil,
		AnnWouldScale: nil,
	})
	if kube.IsConflict(err) {
		log.Info("deployment changed under us, skipping this cycle")
		return Conflict
	}
	if err != nil {
		log.Error("scale to zero", "err", err)
		return APIError
	}
	log.Info("scaled to 0", "from", d.Replicas(), "idle_since", since)
	*scaled++
	return Scaled
}

func (r *Reaper) clearIdle(ctx context.Context, d kube.Deployment, log *slog.Logger) {
	ann := d.Metadata.Annotations
	if ann[AnnIdleSince] == "" && ann[AnnWouldScale] == "" {
		return
	}
	err := r.kube.PatchAnnotations(ctx, d.Metadata.Namespace, d.Metadata.Name,
		map[string]*string{AnnIdleSince: nil, AnnWouldScale: nil})
	if err != nil {
		log.Error("clear idle-since", "err", err)
	}
}

func (r *Reaper) record(o Outcome) {
	r.mu.Lock()
	r.counts[o]++
	r.mu.Unlock()
}

// Healthy reports whether a cycle finished within maxAge.
func (r *Reaper) Healthy(maxAge time.Duration) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.lastCycle.IsZero() && r.now().Sub(r.lastCycle) <= maxAge
}

// Metrics renders Prometheus text format. Small enough to not need a library.
func (r *Reaper) Metrics() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	keys := make([]string, 0, len(r.counts))
	for k := range r.counts {
		keys = append(keys, string(k))
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString("# HELP idle_reaper_decisions_total Per-deployment outcomes, one per cycle.\n")
	b.WriteString("# TYPE idle_reaper_decisions_total counter\n")
	for _, k := range keys {
		fmt.Fprintf(&b, "idle_reaper_decisions_total{outcome=%q} %d\n", k, r.counts[Outcome(k)])
	}
	b.WriteString("# HELP idle_reaper_last_cycle_timestamp_seconds Unix time the last cycle finished.\n")
	b.WriteString("# TYPE idle_reaper_last_cycle_timestamp_seconds gauge\n")
	ts := 0.0
	if !r.lastCycle.IsZero() {
		ts = float64(r.lastCycle.UnixNano()) / 1e9
	}
	fmt.Fprintf(&b, "idle_reaper_last_cycle_timestamp_seconds %g\n", ts)
	b.WriteString("# HELP idle_reaper_dry_run 1 if the reaper is in dry-run mode.\n")
	b.WriteString("# TYPE idle_reaper_dry_run gauge\n")
	dry := 0
	if r.cfg.DryRun {
		dry = 1
	}
	fmt.Fprintf(&b, "idle_reaper_dry_run %d\n", dry)
	return b.String()
}
