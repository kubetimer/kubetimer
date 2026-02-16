"""
Handlers for KubeTimer operator.

Exports:
- Event handler functions for APScheduler-based deletion scheduling
- Startup reconciliation for existing Deployments
- Registry functions for memo configuration
"""

from kubetimer.handlers.deployment import (
    on_deployment_created_with_ttl,
    on_ttl_annotation_changed,
    on_deployment_deleted_with_ttl,
    reconcile_existing_deployments,
)

from kubetimer.handlers.registry import (
    configure_memo,
)

__all__ = [
    "on_deployment_created_with_ttl",
    "on_ttl_annotation_changed",
    "on_deployment_deleted_with_ttl",
    "reconcile_existing_deployments",
    "configure_memo",
]
