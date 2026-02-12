"""
Deployment handler for KubeTimer operator.

Contains Kopf handlers for managing Deployment lifecycle based on TTL.

This module includes both:
1. Old timer-based scanning (DEPRECATED - will be removed)
2. New APScheduler-based event-driven deletion scheduling
"""

import asyncio
from datetime import datetime
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
    
    Why DateTrigger?
    - Executes job exactly once at specified datetime
    - More efficient than checking periodically
    - APScheduler's heap ensures O(log n) scheduling
    
    Why return bool?
    - Allows caller to know if scheduling succeeded
    - Useful for logging/metrics
    
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
    
    # Check if TTL is already expired (handle startup backlog)
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


def deployment_event_handler(
    event: Dict[str, Any],
    namespace: str,
    name: str,
    uid: str,
    meta: kopf.Meta,
    memo: kopf.Memo,
    **_
) -> None:
    """
    Handle Deployment events to schedule/reschedule/cancel deletion jobs.
    
    This is the core of the event-driven architecture. Called by Kopf whenever:
    - A Deployment is created/updated/deleted
    - Annotations change on an existing Deployment
    
    Event-driven vs Timer-based:
    - OLD: Poll every 60s, check all Deployments, delete expired ones
    - NEW: React to changes immediately, schedule individual deletion jobs
    
    Why this is better:
    - Zero CPU when idle (no polling loop)
    - Immediate response (schedule within milliseconds)
    - Precise deletions (at exact TTL expiry time)
    - Scalable (thousands of Deployments = thousands of lightweight jobs)
    
    Args:
        event: Kopf event dict with 'type' (ADDED/MODIFIED/DELETED) and 'object'
        namespace: Deployment namespace
        name: Deployment name
        uid: Deployment UID (unique per instance)
        meta: Deployment metadata
        memo: Kopf memo with scheduler and config
    """
    event_type = event.get('type')
    
    # Get scheduler from memo
    if not hasattr(memo, 'scheduler'):
        logger.error(
            "scheduler_not_in_memo",
            namespace=namespace,
            name=name,
            message="APScheduler not initialized, cannot handle event"
        )
        return
    
    scheduler = memo.scheduler
    annotation_key = memo.annotation_key
    timezone_str = memo.timezone
    dry_run = memo.dry_run
    max_concurrent_deletions = memo.max_concurrent_deletions

    include_namespaces = memo.namespace_include
    exclude_namespaces = memo.namespace_exclude
    
    if not should_scan_namespace(namespace, include_namespaces, exclude_namespaces):
        logger.debug(
            "namespace_filtered",
            namespace=namespace,
            name=name,
            event_type=event_type,
            message="Namespace excluded by config"
        )
        cancel_deletion_job(scheduler, namespace, name, uid)
        return

    if event_type == 'DELETED':
        logger.debug(
            "deployment_deleted_event",
            namespace=namespace,
            name=name,
            uid=uid
        )
        cancel_deletion_job(scheduler, namespace, name, uid)
        return

    annotations = meta.get('annotations', {})
    ttl_value = annotations.get(annotation_key)

    if not ttl_value:
        logger.debug(
            "no_ttl_annotation",
            namespace=namespace,
            name=name,
            event_type=event_type
        )
        cancel_deletion_job(scheduler, namespace, name, uid)
        return
    
    try:
        ttl_datetime = parse_ttl(ttl_value)
    except ValueError as e:
        logger.error(
            "invalid_ttl_in_event",
            namespace=namespace,
            name=name,
            ttl=ttl_value,
            error=str(e),
            event_type=event_type
        )
        cancel_deletion_job(scheduler, namespace, name, uid)
        return

    if is_ttl_expired(ttl_datetime, timezone_str):
        logger.info(
            "ttl_already_expired_in_event",
            namespace=namespace,
            name=name,
            ttl=ttl_value,
            event_type=event_type,
            message="Will be deleted immediately, not scheduling job"
        )
        # Don't schedule - will be handled by startup cleanup (Step 7)
        # Or we could delete immediately here, but startup cleanup is safer
        return
    
    # Schedule or reschedule the deletion job
    if event_type == 'ADDED':
        logger.debug(
            "scheduling_new_deployment",
            namespace=namespace,
            name=name,
            ttl=ttl_value
        )
        schedule_deletion_job(
            scheduler=scheduler,
            namespace=namespace,
            name=name,
            uid=uid,
            ttl_datetime=ttl_datetime,
            annotation_key=annotation_key,
            timezone_str=timezone_str,
            dry_run=dry_run,
            max_concurrent_deletions=max_concurrent_deletions
        )
    
    elif event_type == 'MODIFIED':
        logger.debug(
            "rescheduling_modified_deployment",
            namespace=namespace,
            name=name,
            ttl=ttl_value
        )
        # reschedule_deletion_job uses replace_existing=True internally
        reschedule_deletion_job(
            scheduler=scheduler,
            namespace=namespace,
            name=name,
            uid=uid,
            new_ttl_datetime=ttl_datetime,
            annotation_key=annotation_key,
            timezone_str=timezone_str,
            dry_run=dry_run,
            max_concurrent_deletions=max_concurrent_deletions
        )


# ============================================================================
# Refactored Event Handlers (Kopf-Optimized with Filtering)
# ============================================================================

def on_deployment_created_with_ttl(
    namespace: str,
    name: str,
    uid: str,
    annotations: Dict[str, str],
    memo: kopf.Memo,
    **_
) -> None:
    """
    Handle new Deployments created with a TTL annotation.
    
    This handler is called ONLY when:
    1. A new Deployment is created (kopf.on.create)
    2. It has the TTL annotation (Kopf filter)
    
    Why separate creation handler?
    - Kopf filter ensures we only see relevant Deployments
    - No manual annotation checking needed
    - More efficient than generic event handler
    """
    logger.debug(
        "handling_deployment_creation",
        _,
    )
    if not hasattr(memo, 'scheduler'):
        logger.error("scheduler_not_in_memo", namespace=namespace, name=name, handler="on_create")
        return
    
    scheduler = memo.scheduler
    annotation_key = memo.annotation_key
    timezone_str = memo.timezone
    dry_run = memo.dry_run
    max_concurrent_deletions = memo.max_concurrent_deletions
    
    # Namespace filtering
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
        logger.info("ttl_already_expired_on_create", namespace=namespace, name=name, ttl=ttl_value)
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
    
    This handler is called when the TTL annotation:
    - Is added to an existing Deployment (old=None, new=value)
    - Is modified (old=value1, new=value2)
    - Is removed (old=value, new=None)
    
    Why use kopf.on.field()?
    - Kopf only calls this when the SPECIFIC field changes
    - Not called for status updates, replica changes, etc.
    - Most efficient approach for annotation watching
    
    For your thesis:
    - Explain Kopf's field-level watching (patch-based)
    - Discuss event reduction: 1000 status updates = 0 handler calls
    - Compare to polling: must check annotation on every iteration
    
    Args:
        old: Previous TTL value (None if annotation didn't exist)
        new: New TTL value (None if annotation was removed)
    """
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
    
    # TTL annotation removed
    if new is None:
        logger.info("ttl_annotation_removed", namespace=namespace, name=name, old_ttl=old)
        cancel_deletion_job(scheduler, namespace, name, uid)
        return
    
    # TTL annotation added or modified
    try:
        ttl_datetime = parse_ttl(new)
    except ValueError as e:
        logger.error("invalid_ttl_on_change", namespace=namespace, name=name, new_ttl=new, error=str(e))
        cancel_deletion_job(scheduler, namespace, name, uid)
        return
    
    if is_ttl_expired(ttl_datetime, timezone_str):
        logger.info("ttl_changed_to_expired", namespace=namespace, name=name, new_ttl=new)
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
    
    Why handle deletions?
    - User might manually delete the Deployment before TTL expires
    - Prevents orphaned jobs in APScheduler
    - Clean up is good practice
    
    For your thesis:
    - Discuss garbage collection in distributed systems
    - Explain why orphaned jobs matter (memory leaks in scheduler)
    """
    if not hasattr(memo, 'scheduler'):
        return
    
    logger.info("deployment_deleted_cancelling_job", namespace=namespace, name=name, uid=uid)
    cancel_deletion_job(memo.scheduler, namespace, name, uid)
