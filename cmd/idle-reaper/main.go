// idle-reaper scales opted-in Deployments to zero after they've been idle
// for a grace period, judged by a PromQL query you choose per Deployment.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Bhaddre/kube-idle-reaper/internal/kube"
	"github.com/Bhaddre/kube-idle-reaper/internal/prom"
	"github.com/Bhaddre/kube-idle-reaper/internal/reaper"
)

func main() {
	var (
		apiserver = flag.String("apiserver", "", "API server URL with no auth, e.g. http://127.0.0.1:8001 from `kubectl proxy`. Empty = in-cluster service account.")
		promURL   = flag.String("prometheus", "", "Prometheus base URL (required)")
		interval  = flag.Duration("interval", 5*time.Minute, "time between cycles")
		grace     = flag.Duration("default-grace", 48*time.Hour, "idle time before scaling to 0, unless the Deployment sets "+reaper.AnnGrace)
		dryRun    = flag.Bool("dry-run", true, "only annotate what would be scaled. turn off on purpose")
		exclude   = flag.String("exclude-namespaces", "kube-system,kube-public,kube-node-lease", "comma separated namespaces to never touch")
		maxScale  = flag.Int("max-scale-per-cycle", 5, "cap on scale-downs per cycle")
		listen    = flag.String("listen", ":8080", "address for /metrics and /healthz")
	)
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log, *apiserver, *promURL, *interval, *grace, *dryRun, *exclude, *maxScale, *listen); err != nil {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, apiserver, promURL string, interval, grace time.Duration, dryRun bool,
	exclude string, maxScale int, listen string) error {
	if promURL == "" {
		return errors.New("--prometheus is required")
	}
	if interval <= 0 || grace <= 0 || maxScale < 1 {
		return errors.New("--interval and --default-grace must be > 0, --max-scale-per-cycle >= 1")
	}

	var kc *kube.Client
	if apiserver != "" {
		kc = kube.New(apiserver, nil)
	} else {
		var err error
		if kc, err = kube.InCluster(); err != nil {
			return fmt.Errorf("in-cluster config: %w (use --apiserver for local runs)", err)
		}
	}

	ex := map[string]bool{}
	for _, ns := range strings.Split(exclude, ",") {
		if ns = strings.TrimSpace(ns); ns != "" {
			ex[ns] = true
		}
	}

	r := reaper.New(kc, prom.New(promURL, nil), reaper.Config{
		DefaultGrace:      grace,
		DryRun:            dryRun,
		ExcludeNamespaces: ex,
		MaxScalePerCycle:  maxScale,
	}, log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, r.Metrics())
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		// unhealthy if we've missed a few cycles. lets the kubelet restart a
		// reaper that's wedged instead of silently doing nothing.
		if !r.Healthy(3*interval + time.Minute) {
			http.Error(w, "no recent cycle", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ok")
	})
	srv := &http.Server{Addr: listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server", "err", err)
			stop()
		}
	}()

	log.Info("starting", "dry_run", dryRun, "interval", interval.String(), "default_grace", grace.String(), "max_scale_per_cycle", maxScale)
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		cctx, cancel := context.WithTimeout(ctx, interval)
		res, err := r.Cycle(cctx)
		cancel()
		if err != nil {
			log.Error("cycle failed", "err", err)
		} else {
			log.Info("cycle done", "deployments", len(res))
		}
		select {
		case <-ctx.Done():
			shutCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
			defer c()
			return srv.Shutdown(shutCtx)
		case <-tick.C:
		}
	}
}
