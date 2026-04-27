import asyncio
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.jobstores.base import JobLookupError

from kubetimer.reconcile.fetcher import (
    async_delete_namespaced_deployment,
    async_hibernate_deployment,
    get_namespaced_deployment,
)
from kubetimer.utils.actions import (
    ACTION_DELETE,
    ACTION_HIBERNATE,
    ActionLiteral,
    resolve_action,
)
from kubetimer.utils.logs import get_logger
from kubetimer.utils.time_utils import (
    is_ttl_expired,
    parse_expires_at,
    parse_ttl_duration,
)

logger = get_logger(__name__)


def _make_job_id(namespace: str, name: str, uid: str) -> str:
    """
    Create a unique job ID for APScheduler.
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
    expires_at_key: str | None = None,
    action_key: str | None = None,
    default_action: ActionLiteral = ACTION_HIBERNATE,
    original_replicas_key: str | None = None,
    reconciling_uids: set[str] | None = None,
) -> None:
    """
    Execute the TTL-expiry action for a Deployment (called by APScheduler).

    Re-verifies UID + expiry + action before doing anything destructive — the
    annotation might have changed, or the Deployment might have been recreated
    since the job was scheduled.
    """
    job_id = _make_job_id(namespace, name, uid)

    try:
        deployment = await asyncio.to_thread(
            get_namespaced_deployment,
            namespace,
            name,
        )
        if not deployment:
            logger.info(
                "deployment_already_deleted_at_execution",
                job_id=job_id,
                namespace=namespace,
                name=name,
                message="Deployment was already deleted before job execution",
            )
            return

        if deployment.metadata.uid != uid:
            logger.warning(
                "deployment_uid_mismatch",
                job_id=job_id,
                expected_uid=uid,
                actual_uid=deployment.metadata.uid,
                message="Deployment was recreated, skipping deletion",
            )
            return

        annotations = deployment.metadata.annotations or {}
        expires_at_dt = None

        if expires_at_key:
            expires_at_value = annotations.get(expires_at_key)
            if expires_at_value:
                try:
                    expires_at_dt = parse_expires_at(expires_at_value)
                except ValueError as e:
                    logger.warning(
                        "invalid_expires_at_annotation",
                        job_id=job_id,
                        namespace=namespace,
                        name=name,
                        expires_at=expires_at_value,
                        error=str(e),
                    )

        if not expires_at_dt:
            ttl_value = annotations.get(annotation_key)
            if not ttl_value:
                logger.info(
                    "ttl_annotation_removed",
                    job_id=job_id,
                    namespace=namespace,
                    name=name,
                    message="TTL annotation was removed, skipping deletion",
                )
                return
            try:
                duration = parse_ttl_duration(ttl_value)
                creation = datetime.fromisoformat(
                    deployment.metadata.creation_timestamp.isoformat()
                    if hasattr(deployment.metadata.creation_timestamp, "isoformat")
                    else str(deployment.metadata.creation_timestamp)
                )
                if creation.tzinfo is None:
                    creation = creation.replace(tzinfo=timezone.utc)
                expires_at_dt = creation + duration
            except (ValueError, TypeError) as e:
                logger.error(
                    "invalid_ttl_at_execution",
                    job_id=job_id,
                    namespace=namespace,
                    name=name,
                    ttl=ttl_value,
                    error=str(e),
                )
                return

        if not is_ttl_expired(expires_at_dt, timezone_str):
            logger.warning(
                "ttl_not_expired_at_execution",
                job_id=job_id,
                namespace=namespace,
                name=name,
                expires_at=expires_at_dt.isoformat(),
                message="TTL was updated, not expired anymore",
            )
            return

        try:
            action = resolve_action(annotations, action_key or "", default_action)
        except ValueError as e:
            logger.error(
                "invalid_action_at_execution",
                job_id=job_id,
                namespace=namespace,
                name=name,
                action_key=action_key,
                error=str(e),
                message="Refusing to act; fix the action annotation.",
            )
            return

        if action == ACTION_DELETE:
            if dry_run:
                logger.info(
                    "dry_run_deletion",
                    job_id=job_id,
                    namespace=namespace,
                    name=name,
                    expires_at=expires_at_dt.isoformat(),
                )
            else:
                await async_delete_namespaced_deployment(namespace, name)
                logger.info(
                    "deployment_deleted_by_scheduler",
                    job_id=job_id,
                    namespace=namespace,
                    name=name,
                    expires_at=expires_at_dt.isoformat(),
                )
            return

        replicas = deployment.spec.replicas if deployment.spec else None
        if not replicas or replicas == 0:
            logger.info(
                "deployment_already_hibernated",
                job_id=job_id,
                namespace=namespace,
                name=name,
                replicas=replicas,
            )
            return

        if dry_run:
            logger.info(
                "dry_run_hibernation",
                job_id=job_id,
                namespace=namespace,
                name=name,
                original_replicas=replicas,
                expires_at=expires_at_dt.isoformat(),
            )
        else:
            await async_hibernate_deployment(
                namespace,
                name,
                replicas,
                original_replicas_key or "",
            )
            logger.info(
                "deployment_hibernated_by_scheduler",
                job_id=job_id,
                namespace=namespace,
                name=name,
                original_replicas=replicas,
                expires_at=expires_at_dt.isoformat(),
            )

    except Exception as e:
        logger.error(
            "deletion_job_failed",
            job_id=job_id,
            namespace=namespace,
            name=name,
            error=str(e),
            error_type=type(e).__name__,
        )
    finally:
        if reconciling_uids is not None:
            reconciling_uids.discard(uid)


def schedule_deletion_job(
    scheduler: AsyncIOScheduler,
    namespace: str,
    name: str,
    uid: str,
    expires_at: datetime,
    annotation_key: str,
    timezone_str: str,
    dry_run: bool,
    expires_at_key: str | None = None,
    action_key: str | None = None,
    default_action: ActionLiteral = ACTION_HIBERNATE,
    original_replicas_key: str | None = None,
    reconciling_uids: set[str] | None = None,
) -> bool:
    """
    Schedule a deletion job at TTL expiry time.

    Uses DateTrigger for exact, one-shot execution.  If expires_at is
    in the past, APScheduler fires the job immediately — this is the
    correct behavior for both startup reconciliation and deployments
    created with an already-expired TTL.

    If *reconciling_uids* is provided (during startup reconciliation),
    the reference is forwarded to ``delete_deployment_job`` so the UID
    is removed from the set once the job completes.

    Returns:
        True if job was scheduled, False on error.
    """
    job_id = _make_job_id(namespace, name, uid)
    now = datetime.now(expires_at.tzinfo)

    try:
        job_kwargs: dict = {
            "namespace": namespace,
            "name": name,
            "uid": uid,
            "annotation_key": annotation_key,
            "timezone_str": timezone_str,
            "dry_run": dry_run,
        }
        if expires_at_key is not None:
            job_kwargs["expires_at_key"] = expires_at_key
        if action_key is not None:
            job_kwargs["action_key"] = action_key
        if original_replicas_key is not None:
            job_kwargs["original_replicas_key"] = original_replicas_key
        job_kwargs["default_action"] = default_action
        if reconciling_uids is not None:
            job_kwargs["reconciling_uids"] = reconciling_uids

        scheduler.add_job(
            delete_deployment_job,
            trigger=DateTrigger(run_date=expires_at),
            id=job_id,
            name=f"Delete {namespace}/{name}",
            replace_existing=True,
            kwargs=job_kwargs,
        )

        logger.info(
            "deletion_job_scheduled",
            job_id=job_id,
            namespace=namespace,
            name=name,
            run_date=expires_at.isoformat(),
            seconds_until_execution=(expires_at - now).total_seconds(),
        )
        return True

    except Exception as e:
        logger.error(
            "failed_to_schedule_job",
            job_id=job_id,
            namespace=namespace,
            name=name,
            error=str(e),
            error_type=type(e).__name__,
        )
        return False


def cancel_deletion_job(
    scheduler: AsyncIOScheduler,
    namespace: str,
    name: str,
    uid: str,
) -> bool:
    """Cancel a scheduled deletion job.  Handles JobLookupError gracefully."""
    job_id = _make_job_id(namespace, name, uid)

    try:
        scheduler.remove_job(job_id)
        logger.info(
            "deletion_job_cancelled", job_id=job_id, namespace=namespace, name=name
        )
        return True

    except JobLookupError:
        logger.debug(
            "job_not_found_for_cancellation",
            job_id=job_id,
            namespace=namespace,
            name=name,
            message="Job may have already executed or was never scheduled",
        )
        return False

    except Exception as e:
        logger.error(
            "failed_to_cancel_job",
            job_id=job_id,
            namespace=namespace,
            name=name,
            error=str(e),
            error_type=type(e).__name__,
        )
        return False
