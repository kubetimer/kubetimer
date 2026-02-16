"""Bulk deletion of expired Deployments.

Used by the reconcile orchestrator at startup to delete Deployments
whose TTL has already passed.  Deletions run concurrently via
asyncio.to_thread so each blocking K8s HTTP call gets its own thread.
"""

import asyncio

from kubetimer.reconcile.fetcher import async_delete_namespaced_deployment
from kubetimer.utils.logs import get_logger

logger = get_logger(__name__)


async def _delete_one(
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
        await async_delete_namespaced_deployment(ns, name)
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


async def bulk_delete_expired(
    expired_deployments: list[dict[str, str]],
    dry_run: bool,
) -> tuple[int, int]:
    """Delete all expired Deployments concurrently.

    Each K8s API call is offloaded to a thread via asyncio.to_thread,
    so all deletions execute in parallel rather than sequentially.

    Returns (expired_deleted_count, error_count).
    """
    logger.info(
        "reconcile_deleting_expired",
        count=len(expired_deployments), dry_run=dry_run,
    )

    results = await asyncio.gather(
        *[_delete_one(dep, dry_run) for dep in expired_deployments],
        return_exceptions=True,
    )

    expired_count = 0
    error_count = 0
    for r in results:
        if isinstance(r, Exception):
            error_count += 1
        elif r in ("deleted", "dry_run"):
            expired_count += 1
        elif r == "error":
            error_count += 1

    return expired_count, error_count
