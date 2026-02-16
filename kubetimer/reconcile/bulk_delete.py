"""Rate-limited bulk deletion of expired Deployments.

Used by the reconcile orchestrator at startup to delete Deployments
whose TTL has already passed, with a semaphore for API rate-limiting.
"""

import asyncio

from kubetimer.reconcile.fetcher import (
    delete_namespaced_deployment,
)
from kubetimer.utils.logs import get_logger

logger = get_logger(__name__)


async def _delete_one(
    dep_info: dict[str, str],
    semaphore: asyncio.Semaphore,
    scheduler,
    annotation_key: str,
    timezone_str: str,
    dry_run: bool,
) -> str:
    """Delete a single expired Deployment, guarded by a semaphore.
    Returns one of: deleted, dry_run, skipped, rescheduled, error.
    """
    async with semaphore:
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

    
async def bulk_delete_expired(
    expired_deployments: list[dict[str, str]],
    scheduler,
    annotation_key: str,
    timezone_str: str,
    dry_run: bool,
    max_concurrent_deletions: int,
) -> tuple[int, int]:
    """Delete all expired Deployments concurrently, rate-limited by a semaphore.

    Returns (expired_deleted_count, error_count).
    """
    logger.info(
        "reconcile_deleting_expired",
        count=len(expired_deployments), dry_run=dry_run,
    )

    semaphore = asyncio.Semaphore(max_concurrent_deletions)

    results = await asyncio.gather(
        *[
            _delete_one(
                dep, semaphore, scheduler,
                annotation_key, timezone_str, dry_run,
            )
            for dep in expired_deployments
        ],
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
