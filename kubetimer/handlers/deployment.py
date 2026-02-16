"""
Deployment handler for KubeTimer operator.

Contains Kopf handlers for managing Deployment lifecycle based on TTL.

This module includes both:
1. Old timer-based scanning (DEPRECATED - will be removed)
2. New APScheduler-based event-driven deletion scheduling
"""

import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import kopf
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.jobstores.base import JobLookupError
from kubernetes import client

from kubetimer.config.k8s import apps_v1_client
from kubetimer.utils.logs import get_logger
from kubetimer.utils.time_utils import is_ttl_expired, parse_ttl

logger = get_logger(__name__)


def deployment_indexer(
    name: str,
    namespace: str,
    meta: kopf.Meta,
    memo: kopf.Memo,
    **_
) -> Optional[Dict[str, Any]]:
    annotations = meta.get('annotations', {})
    date = annotations.get(memo.annotation_key, '')

    if not date:
        return None
    
    return {
        name: {
            'namespace': namespace,
            memo.annotation_key: date
        }
    }


def should_scan_namespace(
    namespace: str,
    include_namespaces: List[str],
    exclude_namespaces: List[str]
) -> bool:
    if namespace in exclude_namespaces:
        return False

    if not include_namespaces:
        return True

    return namespace in include_namespaces


def deployment_handler(
    apps_v1: client.AppsV1Api,
    deployment_index: kopf.Index,
    include_namespaces: List[str],
    exclude_namespaces: List[str],
    annotation_key: str,
    dry_run: bool,
    timezone_str: str = "UTC"
) -> int:

    logger.debug(
        "scanning_deployments_from_index",
        total_indexed=len(deployment_index),
        include_namespaces=include_namespaces or "all",
        exclude_namespaces=exclude_namespaces
    )

    deleted_count = 0
    scanned_count = 0


    deployments_snapshot = []
    for name, store in deployment_index.items():
        for value in store:
            deployments_snapshot.append({
                'name': name,
                'namespace': value['namespace'],
                annotation_key: value.get(annotation_key)
            })

    logger.debug("deployment_snapshot", count=len(deployments_snapshot))

    for deployment_info in deployments_snapshot:
        name = deployment_info['name']
        ns = deployment_info['namespace']

        logger.debug("checking_deployment", deployment=name, namespace=ns)

        if not should_scan_namespace(ns, include_namespaces, exclude_namespaces):
            continue
        
        scanned_count += 1

        ttl_value = deployment_info.get(annotation_key)

        try:
            ttl_datetime = parse_ttl(ttl_value)

            if is_ttl_expired(ttl_datetime, timezone_str):
                logger.debug(
                    "deployment_expired",
                    name=name,
                    namespace=ns,
                    ttl=ttl_value,
                    dry_run=dry_run
                )
                
                if not dry_run:
                    apps_v1.delete_namespaced_deployment(
                        name=name,
                        namespace=ns,
                        body=client.V1DeleteOptions()
                    )
                    logger.info("deployment_deleted", name=name, namespace=ns)
                
                deleted_count += 1
        
        except ValueError as e:
            logger.error(
                "invalid_ttl_format",
                name=name,
                namespace=ns,
                ttl=ttl_value,
                error=str(e)
            )
        except client.ApiException as e:
            if e.status == 404:
                logger.error(
                    "deployment_was_already_deleted",
                    name=name,
                    namespace=ns)
            else:
                logger.error(
                    "api_exception",
                    name=name,
                    namespace=ns,
                    error=str(e)
                )
    
    logger.info(
        "deployment_scan_complete",
        scanned_deployments=scanned_count,
        deleted_count=deleted_count,
        dry_run=dry_run
    )
    return deleted_count


# ============================================================================
# APScheduler-Based Event-Driven Deletion Functions (NEW)
# ============================================================================

def _make_job_id(namespace: str, name: str, uid: str) -> str:
    """
    Create a unique job ID for APScheduler.
    
    Why include UID?
    - Deployments can be deleted and recreated with the same name
    - UID is unique per Kubernetes object instance
    - Prevents job ID collisions when Deployment is recreated
    
    Format: namespace/name/uid
    Example: default/nginx/abc123-def456-ghi789
    """
    return f"{namespace}/{name}/{uid}"


async def delete_deployment_job(
    namespace: str,
    name: str,
    uid: str,
    annotation_key: str,
    timezone_str: str,
    dry_run: bool,
    max_concurrent_deletions: int
) -> None:
    """
    Execute the actual deletion of a Deployment (called by APScheduler).
    
    This is the job function that APScheduler executes at TTL expiry time.
    
    Why re-verify the TTL?
    - The TTL annotation might have changed since job was scheduled
    - Deployment might have been deleted/recreated with same name
    - Always fetch fresh state before destructive operations
    
    Why async?
    - Integrates with AsyncIOScheduler
    - Allows concurrent deletions with rate limiting (semaphore)
    - Non-blocking for other operator operations
    
    Args:
        namespace: Kubernetes namespace
        name: Deployment name
        uid: Deployment UID (for verification)
        annotation_key: TTL annotation key
        timezone_str: Timezone for TTL comparison
        dry_run: If True, log instead of delete
        max_concurrent_deletions: Semaphore limit for rate limiting
    """
    job_id = _make_job_id(namespace, name, uid)
    
    try:
        # Create K8s API client
        apps_v1 = apps_v1_client()
        
        # Fetch current Deployment state
        try:
            deployment = apps_v1.read_namespaced_deployment(name=name, namespace=namespace)
        except client.ApiException as e:
            if e.status == 404:
                logger.info(
                    "deployment_already_deleted",
                    job_id=job_id,
                    namespace=namespace,
                    name=name
                )
                return
            else:
                raise
        
        # Verify UID matches (not a recreated Deployment)
        if deployment.metadata.uid != uid:
            logger.warning(
                "deployment_uid_mismatch",
                job_id=job_id,
                expected_uid=uid,
                actual_uid=deployment.metadata.uid,
                message="Deployment was recreated, skipping deletion"
            )
            return
        
        # Re-verify TTL annotation still exists and is expired
        annotations = deployment.metadata.annotations or {}
        ttl_value = annotations.get(annotation_key)
        
        if not ttl_value:
            logger.info(
                "ttl_annotation_removed",
                job_id=job_id,
                namespace=namespace,
                name=name,
                message="TTL annotation was removed, skipping deletion"
            )
            return
        
        # Parse and check expiry
        try:
            ttl_datetime = parse_ttl(ttl_value)
            if not is_ttl_expired(ttl_datetime, timezone_str):
                logger.warning(
                    "ttl_not_expired_at_execution",
                    job_id=job_id,
                    namespace=namespace,
                    name=name,
                    ttl=ttl_value,
                    message="TTL was updated, not expired anymore"
                )
                return
        except ValueError as e:
            logger.error(
                "invalid_ttl_at_execution",
                job_id=job_id,
                namespace=namespace,
                name=name,
                ttl=ttl_value,
                error=str(e)
            )
            return
        
        # Execute deletion (or log if dry_run)
        if dry_run:
            logger.info(
                "dry_run_deletion",
                job_id=job_id,
                namespace=namespace,
                name=name,
                ttl=ttl_value
            )
        else:
            apps_v1.delete_namespaced_deployment(
                name=name,
                namespace=namespace,
                body=client.V1DeleteOptions()
            )
            logger.info(
                "deployment_deleted_by_scheduler",
                job_id=job_id,
                namespace=namespace,
                name=name,
                ttl=ttl_value
            )
    
    except Exception as e:
        logger.error(
            "deletion_job_failed",
            job_id=job_id,
            namespace=namespace,
            name=name,
            error=str(e),
            error_type=type(e).__name__
        )


def schedule_deletion_job(
    scheduler: AsyncIOScheduler,
    namespace: str,
    name: str,
    uid: str,
    ttl_datetime: datetime,
    annotation_key: str,
    timezone_str: str,
    dry_run: bool,
    max_concurrent_deletions: int
) -> bool:
    """
    Schedule a deletion job at TTL expiry time.

    Args:
        scheduler: APScheduler instance from memo
        namespace: Kubernetes namespace
        name: Deployment name  
        uid: Deployment UID (for uniqueness)
        ttl_datetime: When to execute deletion
        annotation_key: TTL annotation key
        timezone_str: Timezone for TTL comparison
        dry_run: If True, log instead of delete
        max_concurrent_deletions: Rate limiting parameter
    
    Returns:
        True if job was scheduled, False if already expired or error
    """
    job_id = _make_job_id(namespace, name, uid)

    now = datetime.now(ttl_datetime.tzinfo)
    if ttl_datetime <= now:
        logger.debug(
            "ttl_already_expired",
            job_id=job_id,
            namespace=namespace,
            name=name,
            ttl=ttl_datetime.isoformat(),
            message="Will be deleted in startup cleanup, not scheduling"
        )
        return False
    
    try:
        # Schedule the job with DateTrigger
        scheduler.add_job(
            delete_deployment_job,
            trigger=DateTrigger(run_date=ttl_datetime),
            id=job_id,
            name=f"Delete {namespace}/{name}",
            replace_existing=True,  # Handle reschedule case
            kwargs={
                'namespace': namespace,
                'name': name,
                'uid': uid,
                'annotation_key': annotation_key,
                'timezone_str': timezone_str,
                'dry_run': dry_run,
                'max_concurrent_deletions': max_concurrent_deletions
            }
        )
        
        logger.info(
            "deletion_job_scheduled",
            job_id=job_id,
            namespace=namespace,
            name=name,
            run_date=ttl_datetime.isoformat(),
            seconds_until_execution=(ttl_datetime - now).total_seconds()
        )
        return True
    
    except Exception as e:
        logger.error(
            "failed_to_schedule_job",
            job_id=job_id,
            namespace=namespace,
            name=name,
            error=str(e),
            error_type=type(e).__name__
        )
        return False


def reschedule_deletion_job(
    scheduler: AsyncIOScheduler,
    namespace: str,
    name: str,
    uid: str,
    new_ttl_datetime: datetime,
    annotation_key: str,
    timezone_str: str,
    dry_run: bool,
    max_concurrent_deletions: int
) -> bool:
    """
    Reschedule a deletion job when TTL annotation changes.
    
    Why reschedule vs cancel+schedule?
    - APScheduler's replace_existing=True handles this efficiently
    - Just call schedule_deletion_job again with new TTL
    - Simpler API, less code
    
    This is essentially an alias for schedule_deletion_job
    with explicit semantics for the caller.
    """
    logger.debug(
        "rescheduling_deletion_job",
        job_id=_make_job_id(namespace, name, uid),
        namespace=namespace,
        name=name,
        new_ttl=new_ttl_datetime.isoformat()
    )
    
    return schedule_deletion_job(
        scheduler=scheduler,
        namespace=namespace,
        name=name,
        uid=uid,
        ttl_datetime=new_ttl_datetime,
        annotation_key=annotation_key,
        timezone_str=timezone_str,
        dry_run=dry_run,
        max_concurrent_deletions=max_concurrent_deletions
    )


def cancel_deletion_job(
    scheduler: AsyncIOScheduler,
    namespace: str,
    name: str,
    uid: str
) -> bool:
    """
    Cancel a scheduled deletion job.
    
    When to call this?
    - TTL annotation is removed from Deployment
    - Deployment is deleted (before TTL expires)
    - Resource should no longer be managed by KubeTimer
    
    Why handle JobLookupError?
    - Job might have already executed and been removed
    - Job might never have been scheduled (e.g., TTL was invalid)
    - This is not an error condition, just log it
    
    Returns:
        True if job was cancelled, False if job didn't exist
    """
    job_id = _make_job_id(namespace, name, uid)
    
    try:
        scheduler.remove_job(job_id)
        logger.info(
            "deletion_job_cancelled",
            job_id=job_id,
            namespace=namespace,
            name=name
        )
        return True
    
    except JobLookupError:
        logger.debug(
            "job_not_found_for_cancellation",
            job_id=job_id,
            namespace=namespace,
            name=name,
            message="Job may have already executed or was never scheduled"
        )
        return False
    
    except Exception as e:
        logger.error(
            "failed_to_cancel_job",
            job_id=job_id,
            namespace=namespace,
            name=name,
            error=str(e),
            error_type=type(e).__name__
        )
        return False

# ============================================================================
# Refactored Event Handlers (Kopf-Optimized with Filtering)
# ============================================================================

def on_deployment_created_with_ttl(
    namespace: str,
    name: str,
    uid: str,
    annotations: Dict[str, str],
    memo: kopf.Memo,
    **kwargs
) -> None:

    logger.info(
        "handling_deployment_creation",
        namespace=namespace,
        name=name,
        uid=uid
    )

    if not hasattr(memo, 'scheduler'):
        logger.error("scheduler_not_in_memo", namespace=namespace, name=name, handler="on_create")
        return
    
    scheduler = memo.scheduler
    annotation_key = memo.annotation_key
    timezone_str = memo.timezone
    dry_run = memo.dry_run
    max_concurrent_deletions = memo.max_concurrent_deletions
    
    if not should_scan_namespace(namespace, memo.namespace_include, memo.namespace_exclude):
        logger.debug("namespace_filtered_on_create", namespace=namespace, name=name)
        return
    
    ttl_value = annotations.get(annotation_key)
    if not ttl_value:
        return
    
    try:
        ttl_datetime = parse_ttl(ttl_value)
    except ValueError as e:
        logger.error("invalid_ttl_on_create", namespace=namespace, name=name, ttl=ttl_value, error=str(e))
        return
    
    if is_ttl_expired(ttl_datetime, timezone_str):
        logger.info("ttl_already_expired_on_create. Deleting...", namespace=namespace, name=name, ttl=ttl_value)
        now = datetime.now(ttl_datetime.tzinfo) + timedelta(seconds=2)
        schedule_deletion_job(scheduler, namespace, name, uid, now, annotation_key, timezone_str, dry_run, max_concurrent_deletions)
        return
    
    logger.debug("scheduling_newly_created_deployment", namespace=namespace, name=name, ttl=ttl_value)
    schedule_deletion_job(scheduler, namespace, name, uid, ttl_datetime, annotation_key, timezone_str, dry_run, max_concurrent_deletions)


def on_ttl_annotation_changed(
    namespace: str,
    name: str,
    uid: str,
    old: Optional[str],
    new: Optional[str],
    memo: kopf.Memo,
    **_
) -> None:
    """
    Handle changes to the TTL annotation field.

    Args:
        old: Previous TTL value (None if annotation didn't exist)
        new: New TTL value (None if annotation was removed)
    """

    logger.info(
        "handling_ttl_annotation_change",
        namespace=namespace,
        name=name,
        uid=uid
    )

    if not hasattr(memo, 'scheduler'):
        logger.error("scheduler_not_in_memo", namespace=namespace, name=name, handler="on_field")
        return
    
    scheduler = memo.scheduler
    annotation_key = memo.annotation_key
    timezone_str = memo.timezone
    dry_run = memo.dry_run
    max_concurrent_deletions = memo.max_concurrent_deletions
    
    if not should_scan_namespace(namespace, memo.namespace_include, memo.namespace_exclude):
        logger.debug("namespace_filtered_on_ttl_change", namespace=namespace, name=name)
        cancel_deletion_job(scheduler, namespace, name, uid)
        return

    if new is None:
        logger.info("ttl_annotation_removed", namespace=namespace, name=name, old_ttl=old)
        cancel_deletion_job(scheduler, namespace, name, uid)
        return

    try:
        ttl_datetime = parse_ttl(new)
    except ValueError as e:
        logger.error("invalid_ttl_on_change", namespace=namespace, name=name, new_ttl=new, error=str(e))
        cancel_deletion_job(scheduler, namespace, name, uid)
        return
    
    if is_ttl_expired(ttl_datetime, timezone_str):
        logger.info("ttl_changed_to_expired. Deleting...", namespace=namespace, name=name, new_ttl=new)
        now = datetime.now(ttl_datetime.tzinfo) + timedelta(seconds=2)
        schedule_deletion_job(scheduler, namespace, name, uid, now, annotation_key, timezone_str, dry_run, max_concurrent_deletions)
        return
    
    logger.info("rescheduling_due_to_ttl_change", namespace=namespace, name=name, old_ttl=old, new_ttl=new)
    reschedule_deletion_job(scheduler, namespace, name, uid, ttl_datetime, annotation_key, timezone_str, dry_run, max_concurrent_deletions)


def on_deployment_deleted_with_ttl(
    namespace: str,
    name: str,
    uid: str,
    memo: kopf.Memo,
    **_
) -> None:
    """
    Handle deletion of Deployments that had a TTL annotation.
    Cancels any scheduled deletion jobs since the resource is already gone.
    """
    logger.info(
        "handling_deployment_deletion",
        namespace=namespace,
        name=name,
        uid=uid
    )
    if not hasattr(memo, 'scheduler'):
        return
    
    logger.info("deployment_deleted_cancelling_job", namespace=namespace, name=name, uid=uid)
    cancel_deletion_job(memo.scheduler, namespace, name, uid)


def _fetch_ttl_deployments(
    annotation_key: str,
    include_ns: List[str],
    exclude_ns: List[str],
) -> List[Dict[str, str]]:
    """
    List all Deployments cluster-wide and return those that have the TTL
    annotation and pass namespace filtering.

    Returns:
        List of dicts with keys: name, namespace, uid, ttl_value.
        Empty list on API error (error is logged internally).
    """
    apps_v1 = apps_v1_client()
    try:
        all_deployments = apps_v1.list_deployment_for_all_namespaces(
            field_selector=f'metadata.annotations.{annotation_key}'
        )
    except client.ApiException as e:
        logger.error("reconcile_list_failed", error=str(e))
        return []

    deployments: List[Dict[str, str]] = []
    for dep in all_deployments.items:
        annotations = dep.metadata.annotations or {}
        ttl_value = annotations.get(annotation_key)
        if not ttl_value:
            continue

        ns = dep.metadata.namespace
        name = dep.metadata.name
        uid = dep.metadata.uid

        if not should_scan_namespace(ns, include_ns, exclude_ns):
            continue

        deployments.append({
            'name': name,
            'namespace': ns,
            'uid': uid,
            'ttl_value': ttl_value,
        })

    return deployments


def _triage_deployments(
    deployments: List[Dict[str, str]],
    scheduler: AsyncIOScheduler,
    annotation_key: str,
    timezone_str: str,
    dry_run: bool,
    max_concurrent_deletions: int,
) -> tuple[List[Dict[str, str]], int, int]:
    """
    Classify deployments into *expired* (need immediate deletion) and
    *future* (schedule an APScheduler job).

    Returns:
        (expired_deployments, scheduled_count, error_count)
    """
    expired: List[Dict[str, str]] = []
    scheduled_count = 0
    error_count = 0

    for dep in deployments:
        name = dep['name']
        ns = dep['namespace']
        uid = dep['uid']
        ttl_value = dep['ttl_value']

        try:
            ttl_datetime = parse_ttl(ttl_value)
        except ValueError as e:
            logger.error(
                "reconcile_invalid_ttl",
                namespace=ns,
                name=name,
                ttl=ttl_value,
                error=str(e),
            )
            error_count += 1
            continue

        if is_ttl_expired(ttl_datetime, timezone_str):
            expired.append(dep)
        else:
            scheduled = schedule_deletion_job(
                scheduler, ns, name, uid, ttl_datetime,
                annotation_key, timezone_str, dry_run, max_concurrent_deletions,
            )
            if scheduled:
                scheduled_count += 1
            else:
                error_count += 1

    return expired, scheduled_count, error_count


async def _delete_expired_deployment(
    dep_info: Dict[str, str],
    semaphore: asyncio.Semaphore,
    scheduler: AsyncIOScheduler,
    annotation_key: str,
    timezone_str: str,
    dry_run: bool,
    max_concurrent_deletions: int,
) -> str:
    """
    Delete a single expired Deployment, guarded by a semaphore.

    Why a top-level function instead of a closure?
    - Closures capture mutable locals → subtle bugs if the enclosing
      scope changes between scheduling and execution
    - Module-level functions are directly importable for unit tests
    - Explicit parameters make dependencies visible

    Returns:
        One of "deleted", "dry_run", "skipped", "rescheduled", "error".
    """
    async with semaphore:
        ns = dep_info['namespace']
        dep_name = dep_info['name']
        dep_uid = dep_info['uid']

        try:
            apps_v1 = apps_v1_client()

            try:
                deployment = apps_v1.read_namespaced_deployment(
                    name=dep_name, namespace=ns
                )
            except client.ApiException as e:
                if e.status == 404:
                    logger.debug(
                        "reconcile_already_deleted",
                        namespace=ns,
                        name=dep_name,
                    )
                    return "skipped"
                raise

            if deployment.metadata.uid != dep_uid:
                logger.debug(
                    "reconcile_uid_mismatch",
                    namespace=ns,
                    name=dep_name,
                    expected_uid=dep_uid,
                    actual_uid=deployment.metadata.uid,
                )
                return "skipped"

            annotations = deployment.metadata.annotations or {}
            current_ttl = annotations.get(annotation_key)
            if not current_ttl:
                return "skipped"

            try:
                current_dt = parse_ttl(current_ttl)
                if not is_ttl_expired(current_dt, timezone_str):
                    schedule_deletion_job(
                        scheduler, ns, dep_name, dep_uid, current_dt,
                        annotation_key, timezone_str, dry_run,
                        max_concurrent_deletions,
                    )
                    return "rescheduled"
            except ValueError:
                return "error"

            if dry_run:
                logger.info(
                    "reconcile_dry_run_delete",
                    namespace=ns,
                    name=dep_name,
                    ttl=current_ttl,
                )
                return "dry_run"

            apps_v1.delete_namespaced_deployment(
                name=dep_name,
                namespace=ns,
                body=client.V1DeleteOptions(),
            )
            logger.info(
                "reconcile_deployment_deleted",
                namespace=ns,
                name=dep_name,
                ttl=current_ttl,
            )
            return "deleted"

        except Exception as e:
            logger.error(
                "reconcile_delete_failed",
                namespace=ns,
                name=dep_name,
                error=str(e),
            )
            return "error"


async def _bulk_delete_expired(
    expired_deployments: List[Dict[str, str]],
    scheduler: AsyncIOScheduler,
    annotation_key: str,
    timezone_str: str,
    dry_run: bool,
    max_concurrent_deletions: int,
) -> tuple[int, int]:
    """
    Delete all expired Deployments concurrently, rate-limited by a semaphore.

    - Encapsulates the gather + result-counting pattern
    - The semaphore is created here (single owner, clear lifetime)
    - Caller only cares about aggregate counts, not individual results

    Returns:
        (expired_deleted_count, error_count)
    """
    logger.info(
        "reconcile_deleting_expired",
        count=len(expired_deployments),
        dry_run=dry_run,
    )

    semaphore = asyncio.Semaphore(max_concurrent_deletions)

    results = await asyncio.gather(
        *[
            _delete_expired_deployment(
                dep, semaphore, scheduler,
                annotation_key, timezone_str, dry_run,
                max_concurrent_deletions,
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


# ============================================================================
# Reconcile Orchestrator
# ============================================================================

async def reconcile_existing_deployments(
    memo: kopf.Memo,
    **_
) -> None:
    """
    Reconcile all existing Deployments at operator startup.

    Orchestrates three phases:
    1. Fetch — list all TTL-annotated Deployments from the K8s API
    2. Triage — classify into expired vs future, schedule future ones
    3. Delete — rate-limited bulk deletion of expired Deployments
    """
    if not hasattr(memo, 'scheduler') or not memo.scheduler.running:
        logger.error("reconcile_skipped_no_scheduler")
        return

    scheduler = memo.scheduler
    annotation_key = memo.annotation_key
    timezone_str = memo.timezone
    dry_run = memo.dry_run
    max_concurrent_deletions = memo.max_concurrent_deletions

    # Phase 1 — Fetch
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

    # Phase 2 — Triage
    expired, scheduled_count, error_count = _triage_deployments(
        deployments, scheduler,
        annotation_key, timezone_str, dry_run, max_concurrent_deletions,
    )

    # Phase 3 — Delete expired
    expired_count = 0
    if expired:
        expired_count, delete_errors = await _bulk_delete_expired(
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
