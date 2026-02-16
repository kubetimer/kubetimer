"""Bulk deletion of expired Deployments.

Used by the reconcile orchestrator at startup to delete Deployments
whose TTL has already passed.
"""

from kubetimer.reconcile.fetcher import (
    delete_namespaced_deployment,
)
from kubetimer.utils.logs import get_logger

logger = get_logger(__name__)


def _delete_one(
    dep_info: dict[str, str],
    dry_run: bool,
) -> str:
    """Delete a single expired Deployment.

    Returns one of: deleted, dry_run, error.
    """
    ns = dep_info["namespace"]
    name = dep_info["name"]
    ttl_value = dep_info["ttl_value"]

    if dry_run:
        logger.info(
            "reconcile_deployment_dry_run_delete",
            namespace=ns, name=name, ttl=ttl_value,
        )
        return "dry_run"

    try:
        delete_namespaced_deployment(ns, name)
        logger.info(
            "reconcile_deployment_deleted",
            namespace=ns, name=name, ttl=ttl_value,
        )
        return "deleted"
    except Exception as e:
        logger.error(
            "reconcile_deployment_delete_failed",
            namespace=ns, name=name, ttl=ttl_value, error=str(e),
        )
        return "error"


def bulk_delete_expired(
    expired_deployments: list[dict[str, str]],
    dry_run: bool,
) -> tuple[int, int]:
    """Delete all expired Deployments sequentially.

    Returns (expired_deleted_count, error_count).
    """
    logger.info(
        "reconcile_deleting_expired",
        count=len(expired_deployments), dry_run=dry_run,
    )

    expired_count = 0
    error_count = 0

    for dep in expired_deployments:
        result = _delete_one(dep, dry_run)
        if result in ("deleted", "dry_run"):
            expired_count += 1
        elif result == "error":
            error_count += 1

    return expired_count, error_count
