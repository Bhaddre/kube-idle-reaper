# kube-idle-reaper

A small Kubernetes controller that scales opted-in Deployments to zero after they've been idle for a grace period. You decide what "idle" means, per Deployment, with a PromQL query.

The binary and annotations are called `idle-reaper`.

Built for the stuff that quietly burns money: the GPU notebook someone forgot about, the demo environment from last quarter, the feature-branch preview nobody's opened in a week.

```
$ kubectl get deploy jupyter-gpu -n ml-sandbox -o jsonpath='{.metadata.annotations}' | jq
{
  "idle-reaper.io/query": "sum(rate(nginx_ingress_controller_requests{exported_service=\"jupyter-gpu\"}[1h]))",
  "idle-reaper.io/grace": "12h",
  "idle-reaper.io/scaled-from": "1",
  "idle-reaper.io/scaled-at": "2026-10-11T04:12:09Z"
}
```

## how it works

Every cycle (default 5 min) the reaper lists Deployments labeled `idle-reaper.io/enabled=true` and runs each one's query.

```
            value > threshold                    value <= threshold
 ┌────────┐ ─────────────────► clear idle-since  ┌──────────────┐
 │ active │                                      │ idle-since=T │──── now - T >= grace ───► scale to 0
 └────────┘ ◄──────────────── any activity ───── └──────────────┘                          (record scaled-from)
```

- **Opt-in only.** No label, no touching it.
- **State lives in annotations** on the Deployment. The reaper itself is stateless. Restart it, move it, nothing's lost, and `kubectl get -o yaml` shows exactly what it thinks.
- **Scale to zero, never delete.** Bringing something back is `kubectl scale --replicas=$(…scaled-from)`. No YAML archaeology.

## the safety rules

These are the parts I'd want a reviewer to push on.

1. **No data is never idle.** If the query errors, returns nothing, or returns NaN, the Deployment is treated as active and its idle clock resets. A broken scrape or a renamed metric must never be the reason something gets turned off.
2. **Dry run is the default.** In dry run the reaper only writes `idle-reaper.io/would-scale-at`. You flip `--dry-run=false` on purpose, after the annotations look right.
3. **Blast-radius cap.** `--max-scale-per-cycle` (default 5) limits scale-downs per cycle. If someone writes `... or vector(0)` and then the metric gets renamed, every Deployment reads as idle at once. The cap turns that from an outage into a slow trickle someone notices.
4. **Optimistic concurrency.** The scale-down is a single merge patch carrying the `resourceVersion` the reaper read. If a human scaled it or a rollout started in between, the API server returns 409 and the reaper backs off until the next cycle.
5. **Least privilege.** RBAC is `list` and `patch` on `deployments`. Nothing else.

## why no client-go

It's on purpose. The reaper needs four API calls: list Deployments, patch annotations, patch replicas, and read the service-account token. `net/http` and `encoding/json` cover that in ~200 lines (`internal/kube`).

What that buys:
- zero dependencies, so no client-go/apimachinery version churn and nothing for Dependabot to nag about
- a single static binary on distroless
- tests that run against fake API servers in plain `httptest`, including real merge-patch and resourceVersion-conflict behavior

What it gives up: informers and watch caches. For a controller that wakes up every few minutes and reads a few hundred objects, that's the right trade. If this had to react in seconds or watch thousands of objects, I'd use controller-runtime.

## install

```bash
# build and push the image (or use your own registry)
docker build -t <registry>/kube-idle-reaper:v0.1.0 .
docker push <registry>/kube-idle-reaper:v0.1.0

# set the image and your Prometheus URL in deploy/deployment.yaml, then
kubectl apply -f deploy/rbac.yaml -f deploy/deployment.yaml
```

It starts in dry run.

## opt a workload in

```yaml
metadata:
  labels:
    idle-reaper.io/enabled: "true"
  annotations:
    idle-reaper.io/query: 'sum(rate(nginx_ingress_controller_requests{exported_service="jupyter-gpu"}[1h]))'
    idle-reaper.io/threshold: "0"   # optional, default 0. idle when value <= threshold
    idle-reaper.io/grace: "12h"     # optional, default --default-grace (48h)
```

Full example: [`deploy/example-opt-in.yaml`](deploy/example-opt-in.yaml).

Query tips:
- It has to return **one** number. Wrap it in `sum()`.
- Use a window at least as long as your traffic gaps. `rate(...[1h])` won't call something idle just because nobody hit it for 10 minutes.
- Don't add `or vector(0)`. Missing data should stay missing (see rule 1).
- Good activity signals: ingress request rate, queue consumer lag, GPU utilization (`DCGM_FI_DEV_GPU_UTIL`) for notebooks.

## annotations

| annotation | set by | meaning |
|---|---|---|
| `idle-reaper.io/query` | you | PromQL, one number |
| `idle-reaper.io/threshold` | you | idle when `value <= threshold` (default `0`) |
| `idle-reaper.io/grace` | you | Go duration, e.g. `12h` (default `--default-grace`) |
| `idle-reaper.io/idle-since` | reaper | first time it was seen idle |
| `idle-reaper.io/would-scale-at` | reaper | dry run only: when it would have been scaled |
| `idle-reaper.io/scaled-from` | reaper | replica count before scale-down |
| `idle-reaper.io/scaled-at` | reaper | when it was scaled |

## flags

| flag | default | |
|---|---|---|
| `--prometheus` | (required) | Prometheus base URL |
| `--apiserver` | in-cluster | API server URL with no auth, for `kubectl proxy` |
| `--interval` | `5m` | time between cycles |
| `--default-grace` | `48h` | idle time before scale-down |
| `--dry-run` | `true` | only annotate |
| `--max-scale-per-cycle` | `5` | blast-radius cap |
| `--exclude-namespaces` | `kube-system,kube-public,kube-node-lease` | never touched, even if labeled |
| `--listen` | `:8080` | `/metrics` and `/healthz` |

## observability

`/metrics`:

```
idle_reaper_decisions_total{outcome="scaled"} 3
idle_reaper_decisions_total{outcome="query_failed"} 1
idle_reaper_last_cycle_timestamp_seconds 1.79e+09
idle_reaper_dry_run 0
```

Alert on `query_failed` and `config_error` going up (someone's query is broken, so their workload is now never reaped) and on `cap_reached` (something's marking a lot of things idle at once, so go look).

`/healthz` goes unhealthy if no cycle has finished in 3 intervals, so a wedged reaper gets restarted instead of silently doing nothing.

## run locally

```bash
kubectl proxy &
go run ./cmd/idle-reaper --apiserver=http://127.0.0.1:8001 --prometheus=http://localhost:9090 --interval=30s
```

## development

```bash
make lint test build    # gofmt, vet, race-enabled tests, static binary
make manifests          # kubeconform on deploy/
```

The tests cover the full lifecycle, the safety rules above, and an end-to-end run of the real HTTP clients against a fake API server that implements JSON merge patch and resourceVersion conflicts.

## not in scope

- **Scale up on demand.** Waking something on the first request needs a proxy in the request path (KEDA's HTTP add-on, Knative). This only handles the "turn it off" half.
- **Leader election.** Run one replica (`strategy: Recreate`). Two wouldn't corrupt state, since every write is a merge patch and scale-downs are resourceVersion-guarded, but each would get its own blast-radius budget.
- **StatefulSets.** Easy to add, but scaling a database to zero deserves more thought than one annotation.

## license

MIT
