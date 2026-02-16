"""Reconcile orchestrator — recovers state at operator startup.

Fetches all TTL-annotated Deployments via the K8s list API, schedules
future deletions, and bulk-deletes already-expired ones.
"""

import kopf

from kubetimer.reconcile.bulk_delete import bulk_delete_expired
from kubetimer.reconcile.fetcher import list_deployments_all_namespaces
from kubetimer.scheduler.jobs import schedule_deletion_job
from kubetimer.utils.logs import get_logger
from kubetimer.utils.namespace import should_scan_namespace
from kubetimer.utils.time_utils import is_ttl_expired, parse_ttl

logger = get_logger(__name__)


def _fetch_ttl_deployments(
    annotation_key: str,
    include_ns: list[str],
    exclude_ns: list[str],
) -> list[dict[str, str]]:
    """List all Deployments cluster-wide and return those with the TTL annotation.

    Returns:
        List of dicts with keys: name, namespace, uid, ttl_value.
        Empty list on API error.
    """
    try:
        all_deployments = list_deployments_all_namespaces()
    except Exception as e:
        logger.error("reconcile_list_failed", error=str(e))
        return []

    deployments: list[dict[str, str]] = []
    for dep in all_deployments.items:
        annotations = dep.metadata.annotations or {}
        ttl_value = annotations.get(annotation_key)
        if not ttl_value:
            continue

        ns = dep.metadata.namespace
        if not should_scan_namespace(ns, include_ns, exclude_ns):
            continue

        deployments.append({
            "name": dep.metadata.name,
            "namespace": ns,
            "uid": dep.metadata.uid,
            "ttl_value": ttl_value,
        })

    return deployments


def _triage_deployments(
    deployments: list[dict[str, str]],
    scheduler,
    annotation_key: str,
    timezone_str: str,
    dry_run: bool,
) -> tuple[list[dict[str, str]], int, int]:
    """Classify deployments into expired (immediate delete) vs future (schedule).

    Returns:
        (expired_deployments, scheduled_count, error_count)
    """
    expired: list[dict[str, str]] = []
    scheduled_count = 0
    error_count = 0

    for dep in deployments:
        try:
            ttl_datetime = parse_ttl(dep["ttl_value"])
        except ValueError as e:
            logger.error(
                "reconcile_invalid_ttl",
                namespace=dep["namespace"], name=dep["name"],
                ttl=dep["ttl_value"], error=str(e),
            )
            error_count += 1
            continue

        if is_ttl_expired(ttl_datetime, timezone_str):
            expired.append(dep)
        else:
            if schedule_deletion_job(
                scheduler, dep["namespace"], dep["name"], dep["uid"],
                ttl_datetime, annotation_key, timezone_str, dry_run,
            ):
                scheduled_count += 1
            else:
                error_count += 1

    return expired, scheduled_count, error_count


async def reconcile_existing_deployments(
    memo: kopf.Memo,
    **_,
) -> None:
    """Reconcile all existing Deployments at operator startup.

    Three phases:
    1. Fetch  — list all TTL-annotated Deployments from the K8s API
    2. Triage — classify into expired vs future, schedule future ones
    3. Delete — rate-limited bulk deletion of expired Deployments
    """
    if not hasattr(memo, "scheduler") or not memo.scheduler.running:
        logger.error("reconcile_skipped_no_scheduler")
        return

    scheduler = memo.scheduler
    annotation_key = memo.annotation_key
    timezone_str = memo.timezone
    dry_run = memo.dry_run
    max_concurrent_deletions = memo.max_concurrent_deletions

    deployments = _fetch_ttl_deployments(
        annotation_key, memo.namespace_include, memo.namespace_exclude,
    )
    if not deployments:
        logger.info("reconcile_no_ttl_deployments_found")
        return

    logger.info(
        "reconcile_starting",
        total_with_ttl=len(deployments),
        max_concurrent_deletions=max_concurrent_deletions,
    )

    expired, scheduled_count, error_count = _triage_deployments(
        deployments, scheduler, annotation_key, timezone_str, dry_run,
    )

    expired_count = 0
    if expired:
        expired_count, delete_errors = await bulk_delete_expired(
            expired, scheduler,
            annotation_key, timezone_str, dry_run, max_concurrent_deletions,
        )
        error_count += delete_errors

    logger.info(
        "reconcile_complete",
        scheduled=scheduled_count,
        expired_deleted=expired_count,
        errors=error_count,
    )
