# KubeTimer — Prometheus Metrics & Grafana Dashboard Setup

This guide covers deploying the KubeTimer operator with Prometheus metrics
instrumentation, ServiceMonitor auto-discovery, and a Grafana dashboard — all
integrated with **kube-prometheus-stack**.

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Running Kubernetes cluster | Minikube, kind, EKS, GKE, etc. |
| kube-prometheus-stack installed | Helm release name assumed to be `observability` (adjust `release` label in `deploy/servicemonitor.yaml` if yours differs) |
| `kubectl` configured | Pointing at the target cluster |
| Operator image pushed | e.g. `docker push ryanjorgeac/kubetimer:v0.2.2-metrics` |

> **Find your Helm release name:**
> ```bash
> kubectl --namespace monitoring get pods -l "app.kubernetes.io/managed-by=Helm" \
>   -o jsonpath='{.items[0].metadata.labels.release}'
> ```

---

## Architecture

```
Operator pod (:9091/metrics)
    ↑ scraped by
Service (kubetimer-operator-metrics:9091)
    ↑ discovered by
ServiceMonitor (release: observability)
    ↑ watched by
Prometheus Operator → configures Prometheus to scrape
    ↑ queried by
Grafana (dashboard auto-loaded from ConfigMap)
```

---

## Step 1 — Deploy the Operator

Update the image tag in `deploy/deployment.yaml` to match your push:

```yaml
image: ryanjorgeac/kubetimer:v0.2.2-metrics
```

Then apply the core manifests:

```bash
kubectl apply -f deploy/namespace.yaml
kubectl apply -f deploy/rbac.yaml
kubectl apply -f deploy/deployment.yaml
```

The pod now exposes two ports:

| Port | Purpose |
|---|---|
| `8080` | Kopf liveness probe (`/healthz`) |
| `9091` | Prometheus metrics (`/metrics`) |

### Verify the pod is running

```bash
kubectl get pods -n kubetimer-system
```

---

## Step 2 — Create the Metrics Service

```bash
kubectl apply -f deploy/service.yaml
```

This creates a **ClusterIP Service** named `kubetimer-operator-metrics` that
routes port `9091` to the operator pod. Prometheus discovers endpoints through
Services, so this is required for ServiceMonitor scraping.

### Verify

```bash
kubectl get svc -n kubetimer-system
# Expected: kubetimer-operator-metrics   ClusterIP   ...   9091/TCP
```

---

## Step 3 — Create the ServiceMonitor

```bash
kubectl apply -f deploy/servicemonitor.yaml
```

This tells the **Prometheus Operator** to auto-discover and scrape the KubeTimer
metrics endpoint every **15 seconds**. No manual Prometheus configuration editing
needed — the Prometheus Operator watches for `ServiceMonitor` custom resources
and reloads Prometheus automatically.

> **Important:** The `release: observability` label in `servicemonitor.yaml` must
> match your kube-prometheus-stack Helm release name. If yours is different, edit
> the label before applying.

### Verify Prometheus is scraping

```bash
# Port-forward to Prometheus
kubectl port-forward -n monitoring svc/observability-kube-prometheus-prometheus 9090:9090
```

Then open [http://localhost:9091/targets](http://localhost:9091/targets) and look
for `serviceMonitor/kubetimer-system/kubetimer-operator` with state **UP**.

Or run a quick PromQL query at
[http://localhost:9091/graph](http://localhost:9091/graph):

```promql
kubetimer_operator_info
```

---

## Step 4 — Deploy the Grafana Dashboard

```bash
kubectl apply -f deploy/grafana-dashboard-configmap.yaml
```

This creates a **ConfigMap** in the `monitoring` namespace with the label
`grafana_dashboard: "1"`. Grafana's sidecar container (bundled with
kube-prometheus-stack) watches for ConfigMaps with this label, reads the JSON
from the `kubetimer.json` data key, and **auto-provisions** it as a dashboard.
No manual import needed.

### Access Grafana

```bash
# Port-forward to Grafana
kubectl port-forward -n monitoring svc/observability-grafana 3000:80
```

Open [http://localhost:3000](http://localhost:3000):

- **Default credentials:** `admin` / `prom-operator`
  (or check your Helm values: `kubectl get secret -n monitoring observability-grafana -o jsonpath='{.data.admin-password}' | base64 -d`)
- Navigate to **Dashboards → Browse** and search for **"KubeTimer Operator"**

---

## Dashboard Panels

The auto-provisioned Grafana dashboard includes these panels:

### Deletion Latency

| Panel | Type | What it shows |
|---|---|---|
| **Deletion Latency Percentiles** | Time series | p50 (median), p95, p99 delete times |
| **Slowest Deletion** | Stat | Maximum observed delete duration |
| **Fastest Deletion** | Stat | Minimum observed delete duration |
| **Deletion Latency by Source** | Time series | p50/p95 broken down by `event_handler`, `scheduler`, `reconciler` |

### Deletion Throughput

| Panel | Type | What it shows |
|---|---|---|
| **Deletions/sec by Source** | Stacked bars | Deletion rate from each source |
| **Deletions/sec by Outcome** | Stacked bars | `deleted`, `dry_run`, `error`, `already_gone` |

### Concurrent Events & Handler Performance

| Panel | Type | What it shows |
|---|---|---|
| **Concurrent Events** | Time series | In-flight `create`, `update`, `delete` handlers |
| **Event Handler Duration** | Time series | p50/p95 handler latency by event type |

### Concurrency vs Resource Usage

| Panel | Type | What it shows |
|---|---|---|
| **Concurrent Events vs CPU** | Dual-axis | Overlay of concurrent handler count and container CPU |
| **Concurrent Events vs Memory** | Dual-axis | Overlay of concurrent handler count and container memory |

### Scheduling

| Panel | Type | What it shows |
|---|---|---|
| **Jobs Scheduled & Cancelled Rate** | Time series | APScheduler job churn |

### Startup & Reconciliation

| Panel | Type | What it shows |
|---|---|---|
| **Startup Duration** | Stat | Total operator startup time |
| **Reconciliation Duration** | Stat | Time for startup reconciliation pass |
| **Reconciled Deployments** | Bar gauge | Breakdown by `expired`, `scheduled`, `error` |

---

## Useful PromQL Queries

These can be run directly in the Prometheus UI or added as Grafana panels.

### Median delete time (last 5 min)

```promql
histogram_quantile(0.5, sum(rate(kubetimer_delete_duration_seconds_bucket[5m])) by (le))
```

### Slowest delete time (last 5 min)

```promql
histogram_quantile(1, sum(rate(kubetimer_delete_duration_seconds_bucket[5m])) by (le))
```

### Fastest delete time (last 5 min)

```promql
histogram_quantile(0, sum(rate(kubetimer_delete_duration_seconds_bucket[5m])) by (le))
```

### Delete rate by source

```promql
sum(rate(kubetimer_deployments_deleted_total[5m])) by (source)
```

### Current concurrent create events

```promql
kubetimer_concurrent_events{event_type="create"}
```

### Handler p95 latency by event type

```promql
histogram_quantile(0.95, sum(rate(kubetimer_event_handler_duration_seconds_bucket[5m])) by (le, event_type))
```

---

## Environment Variables

Metrics-specific settings (set in `deploy/deployment.yaml`):

| Variable | Default | Purpose |
|---|---|---|
| `KUBETIMER_METRICS_ENABLED` | `true` | Enable/disable the Prometheus HTTP endpoint |
| `KUBETIMER_METRICS_PORT` | `9091` | Port for the `/metrics` endpoint (must differ from 8080) |

---

## Validation Checklist

| Check | Command |
|---|---|
| Pod running | `kubectl get pods -n kubetimer-system` |
| Metrics endpoint | `kubectl port-forward -n kubetimer-system deploy/kubetimer-operator 9091:9091` then `curl localhost:9091/metrics` |
| Service exists | `kubectl get svc -n kubetimer-system` |
| ServiceMonitor exists | `kubectl get servicemonitor -n kubetimer-system` |
| Prometheus scraping | Prometheus UI → Status → Targets |
| Dashboard loaded | Grafana → Dashboards → search "KubeTimer" |

---

## Troubleshooting

### ServiceMonitor not appearing in Prometheus targets

1. Check the `release` label matches your Helm release:
   ```bash
   kubectl get servicemonitor -n kubetimer-system kubetimer-operator -o yaml | grep release
   ```
2. Verify Prometheus Operator is watching `kubetimer-system`:
   ```bash
   kubectl get prometheus -n monitoring -o yaml | grep -A5 serviceMonitorNamespaceSelector
   ```
   If it has a selector, ensure `kubetimer-system` matches.

### Grafana dashboard not appearing

1. Check the ConfigMap exists in the right namespace:
   ```bash
   kubectl get configmap kubetimer-grafana-dashboard -n monitoring
   ```
2. Verify the sidecar label:
   ```bash
   kubectl get configmap kubetimer-grafana-dashboard -n monitoring --show-labels
   # Should include: grafana_dashboard=1
   ```
3. Restart the Grafana pod to force a sidecar rescan:
   ```bash
   kubectl rollout restart deployment -n monitoring observability-grafana
   ```

### Metrics endpoint returns empty

1. Check `KUBETIMER_METRICS_ENABLED` is `"true"` in the Deployment env.
2. Check operator logs for `prometheus_metrics_server_started`:
   ```bash
   kubectl logs -n kubetimer-system deploy/kubetimer-operator | grep prometheus
   ```
