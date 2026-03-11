"""
Prometheus metric definitions for KubeTimer operator.
"""

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    Info,
    start_http_server,
)

from kubetimer.utils.logs import get_logger

logger = get_logger(__name__)

REGISTRY = CollectorRegistry()

# ---------------------------------------------------------------------------
# Delete latency — one observation per K8s DELETE API call.
#
# Labels:
#   source:    "event_handler" | "scheduler" | "reconciler"
#   namespace: K8s namespace of the deleted Deployment
#
# PromQL examples:
#   Median:  histogram_quantile(0.5,  rate(kubetimer_delete_duration_seconds_bucket[5m]))
#   Max:     histogram_quantile(1.0,  rate(kubetimer_delete_duration_seconds_bucket[5m]))
#   Min:     histogram_quantile(0.0,  rate(kubetimer_delete_duration_seconds_bucket[5m]))
# ---------------------------------------------------------------------------
DELETE_DURATION = Histogram(
    "kubetimer_delete_duration_seconds",
    "Time spent on each K8s Deployment DELETE API call",
    labelnames=["source", "namespace"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
    registry=REGISTRY,
)

DEPLOYMENTS_DELETED = Counter(
    "kubetimer_deployments_deleted_total",
    "Total Deployment deletions attempted by KubeTimer",
    labelnames=["source", "namespace", "outcome"],
    registry=REGISTRY,
)

CONCURRENT_EVENTS = Gauge(
    "kubetimer_concurrent_events",
    "Number of Kopf event handlers executing concurrently",
    labelnames=["event_type"],
    registry=REGISTRY,
)

EVENT_HANDLER_DURATION = Histogram(
    "kubetimer_event_handler_duration_seconds",
    "Time spent inside each Kopf event handler",
    labelnames=["event_type"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
    registry=REGISTRY,
)

STARTUP_DURATION = Histogram(
    "kubetimer_startup_duration_seconds",
    "Total time for operator startup (config + reconciliation)",
    buckets=(0.5, 1, 2.5, 5, 10, 30, 60, 120, 300),
    registry=REGISTRY,
)

RECONCILE_DURATION = Histogram(
    "kubetimer_reconcile_duration_seconds",
    "Time spent on startup reconciliation of existing Deployments",
    buckets=(0.5, 1, 2.5, 5, 10, 30, 60, 120, 300),
    registry=REGISTRY,
)

RECONCILE_DEPLOYMENTS = Gauge(
    "kubetimer_reconcile_deployments_total",
    "Number of Deployments found during startup reconciliation",
    labelnames=["status"],
    registry=REGISTRY,
)

JOBS_SCHEDULED = Counter(
    "kubetimer_jobs_scheduled_total",
    "Total APScheduler deletion jobs scheduled",
    registry=REGISTRY,
)

JOBS_CANCELLED = Counter(
    "kubetimer_jobs_cancelled_total",
    "Total APScheduler deletion jobs cancelled",
    registry=REGISTRY,
)

OPERATOR_INFO = Info(
    "kubetimer_operator",
    "KubeTimer operator build and configuration metadata",
    registry=REGISTRY,
)


def start_metrics_server(port: int) -> None:
    start_http_server(port, registry=REGISTRY)
    logger.info("prometheus_metrics_server_started", port=port, path="/metrics")
