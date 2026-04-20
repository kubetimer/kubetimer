"""Reconcile orchestrator — recovers state at operator startup.

Fetches all TTL-annotated Deployments via the K8s list API, schedules
future deletions, and bulk-deletes already-expired ones.
"""

import asyncio
from datetime import datetime, timezone
from time import time
import kopf

from kubetimer.reconcile.bulk_delete import bulk_delete_expired
from kubetimer.reconcile.fetcher import list_deployments_all_namespaces_paginated
from kubetimer.reconcile.models import TtlDeployment
from kubetimer.scheduler.jobs import schedule_deletion_job
from kubetimer.utils.logs import get_logger
from kubetimer.utils.namespace import should_scan_namespace
from kubetimer.utils.time_utils import (
    is_ttl_expired,
    parse_expires_at,
    parse_ttl_duration,
)

logger = get_logger(__name__)


def _fetch_ttl_deployments(
    annotation_key: str,
    expires_at_key: str,
    include_ns: list[str] | frozenset[str],
    exclude_ns: list[str] | frozenset[str],
) -> list[TtlDeployment]:
    """List all Deployments cluster-wide (paginated)
    and return those with the TTL annotation.

    Returns:
        List of TtlDeployment instances.
        Empty list on API error.
    """
    deployments: list[TtlDeployment] = []
    try:
        for dep in list_deployments_all_namespaces_paginated():
            annotations = dep.metadata.annotations or {}
            ttl_value = annotations.get(annotation_key)
            if not ttl_value:
                continue

            ns = dep.metadata.namespace
            if not should_scan_namespace(ns, include_ns, exclude_ns):
                continue

            deployments.append(
                TtlDeployment(
                    name=dep.metadata.name,
                    namespace=ns,
                    uid=dep.metadata.uid,
                    ttl_value=ttl_value,
                    creation_timestamp=dep.metadata.creation_timestamp,
                    expires_at=annotations.get(expires_at_key),
                )
            )
    except Exception as e:
        logger.error("reconcile_list_failed", error=str(e))

    return deployments


def _triage_deployments(
    deployments: list[TtlDeployment],
    scheduler,
    annotation_key: str,
    timezone_str: str,
    dry_run: bool,
    expires_at_key: str | None = None,
    reconciling_uids: set[str] | None = None,
) -> tuple[list[TtlDeployment], int, int]:
    """Classify deployments into expired (immediate delete) vs future (schedule).

    Returns:
        (expired_deployments, scheduled_count, error_count)
    """
    expired: list[TtlDeployment] = []
    scheduled_count = 0
    error_count = 0

    for dep in deployments:
        expires_at_dt = None

        if dep.expires_at:
            try:
                expires_at_dt = parse_expires_at(dep.expires_at)
            except ValueError:
                logger.warning(
                    "reconcile_invalid_expires_at",
                    namespace=dep.namespace,
                    name=dep.name,
                    expires_at=dep.expires_at,
                )

        if expires_at_dt is None:
            try:
                duration = parse_ttl_duration(dep.ttl_value)
                creation = datetime.fromisoformat(
                    dep.creation_timestamp.isoformat()
                    if hasattr(dep.creation_timestamp, "isoformat")
                    else str(dep.creation_timestamp)
                )
                if creation.tzinfo is None:
                    creation = creation.replace(tzinfo=timezone.utc)
                expires_at_dt = creation + duration
            except (ValueError, TypeError) as e:
                logger.error(
                    "reconcile_invalid_ttl",
                    namespace=dep.namespace,
                    name=dep.name,
                    ttl=dep.ttl_value,
                    error=str(e),
                )
                error_count += 1
                continue

        if is_ttl_expired(expires_at_dt, timezone_str):
            expired.append(dep)
        else:
            if schedule_deletion_job(
                scheduler,
                dep.namespace,
                dep.name,
                dep.uid,
                expires_at_dt,
                annotation_key,
                timezone_str,
                dry_run,
                expires_at_key=expires_at_key,
                reconciling_uids=reconciling_uids,
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
    3. Delete — concurrent bulk deletion of expired Deployments
    """
    starttime = time()
    if not hasattr(memo, "scheduler") or not memo.scheduler.running:
        logger.error("reconcile_skipped_no_scheduler")
        return

    scheduler = memo.scheduler
    annotation_key = memo.annotation_key
    expires_at_key = memo.expires_at_key
    timezone_str = memo.timezone
    dry_run = memo.dry_run

    deployments = await asyncio.to_thread(
        _fetch_ttl_deployments,
        annotation_key,
        expires_at_key,
        memo.namespace_include,
        memo.namespace_exclude,
    )
    if not deployments:
        logger.info("reconcile_no_ttl_deployments_found")
        return

    logger.info(
        "reconcile_starting",
        total_with_ttl=len(deployments),
    )

    reconciling_uids: set[str] = getattr(memo, "reconciling_uids", set())
    reconciling_uids.update(dep.uid for dep in deployments)

    expired, scheduled_count, error_count = _triage_deployments(
        deployments,
        scheduler,
        annotation_key,
        timezone_str,
        dry_run,
        expires_at_key=expires_at_key,
        reconciling_uids=reconciling_uids,
    )

    expired_count = 0
    if expired:
        expired_count, delete_errors = await bulk_delete_expired(
            expired,
            dry_run,
            max_concurrent_deletes=getattr(memo, "max_concurrent_deletes", 25),
        )
        error_count += delete_errors

    for dep in expired:
        reconciling_uids.discard(dep.uid)

    logger.info(
        "reconcile_complete",
        scheduled=scheduled_count,
        expired_deleted=expired_count,
        errors=error_count,
        duration_seconds=f"{time() - starttime:.9f}",
    )
