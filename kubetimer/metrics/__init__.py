"""Prometheus metrics for KubeTimer operator.

Exports:
- Metric instruments (counters, histograms, gauges)
- Context managers for tracking duration and concurrency
- Registry and server startup helper
"""

from kubetimer.metrics.instruments import (
    CONCURRENT_EVENTS,
    DELETE_DURATION,
    DEPLOYMENTS_DELETED,
    EVENT_HANDLER_DURATION,
    JOBS_CANCELLED,
    JOBS_SCHEDULED,
    OPERATOR_INFO,
    RECONCILE_DEPLOYMENTS,
    RECONCILE_DURATION,
    REGISTRY,
    STARTUP_DURATION,
    start_metrics_server,
)
from kubetimer.metrics.decorators import (
    track_concurrency,
    track_duration,
)

__all__ = [
    "CONCURRENT_EVENTS",
    "DELETE_DURATION",
    "DEPLOYMENTS_DELETED",
    "EVENT_HANDLER_DURATION",
    "JOBS_CANCELLED",
    "JOBS_SCHEDULED",
    "OPERATOR_INFO",
    "RECONCILE_DEPLOYMENTS",
    "RECONCILE_DURATION",
    "REGISTRY",
    "STARTUP_DURATION",
    "start_metrics_server",
    "track_concurrency",
    "track_duration",
]
