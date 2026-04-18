"""Bulk deletion of expired Deployments.

Used by the reconcile orchestrator at startup to delete Deployments
whose TTL has already passed.  Deletions run concurrently via
asyncio.to_thread so each blocking K8s HTTP call gets its own thread.
"""

import asyncio

from kubernetes.client.exceptions import ApiException

from kubetimer.reconcile.fetcher import async_delete_namespaced_deployment
from kubetimer.reconcile.models import TtlDeployment
from kubetimer.utils.logs import get_logger

logger = get_logger(__name__)


async def _delete_one(
    dep_info: TtlDeployment,
    dry_run: bool,
    semaphore: asyncio.Semaphore,
) -> str:
    """Delete a single expired Deployment.

    Returns one of: deleted, dry_run, error.
    Acquires the semaphore to bound concurrency.
    """
    async with semaphore:
        ns = dep_info.namespace
        name = dep_info.name
        ttl_value = dep_info.ttl_value

        if dry_run:
            logger.info(
                "reconcile_deployment_dry_run_delete",
                namespace=ns,
                name=name,
                ttl=ttl_value,
            )
            return "dry_run"

        try:
            await async_delete_namespaced_deployment(ns, name)
            logger.info(
                "reconcile_deployment_deleted",
                namespace=ns,
                name=name,
                ttl=ttl_value,
            )
            return "deleted"
        except ApiException as e:
            if e.status == 404:
                logger.info(
                    "reconcile_deployment_already_gone",
                    namespace=ns,
                    name=name,
                    message="Deployment was already deleted (likely by event handler)",
                )
                return "deleted"
            logger.error(
                "reconcile_deployment_delete_failed",
                namespace=ns,
                name=name,
                ttl=ttl_value,
                error=str(e),
            )
            return "error"
        except Exception as e:
            logger.error(
                "reconcile_deployment_delete_failed",
                namespace=ns,
                name=name,
                ttl=ttl_value,
                error=str(e),
            )
            return "error"


async def bulk_delete_expired(
    expired_deployments: list[TtlDeployment],
    dry_run: bool,
    max_concurrent_deletes: int = 25,
) -> tuple[int, int]:
    """Delete all expired Deployments with bounded concurrency.

    Each K8s API call is offloaded to a thread via asyncio.to_thread.
    An asyncio.Semaphore limits parallel deletions to
    ``max_concurrent_deletes`` to avoid connection-pool exhaustion.

    Returns (expired_deleted_count, error_count).
    """
    logger.info(
        "reconcile_deleting_expired",
        count=len(expired_deployments),
        dry_run=dry_run,
        max_concurrent_deletes=max_concurrent_deletes,
    )

    semaphore = asyncio.Semaphore(max_concurrent_deletes)

    results = await asyncio.gather(
        *[_delete_one(dep, dry_run, semaphore) for dep in expired_deployments],
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
