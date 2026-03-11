import asyncio
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.jobstores.base import JobLookupError

from kubetimer.metrics import (
    DELETE_DURATION,
    DEPLOYMENTS_DELETED,
    JOBS_CANCELLED,
    JOBS_SCHEDULED,
    track_duration,
)
from kubetimer.reconcile.fetcher import (
    async_delete_namespaced_deployment,
    get_namespaced_deployment,
)
from kubetimer.utils.logs import get_logger
from kubetimer.utils.time_utils import is_ttl_expired, parse_ttl

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
    reconciling_uids: set[str] | None = None,
) -> None:
    """
    Execute the actual deletion of a Deployment (called by APScheduler).

    Re-verifies UID + TTL expiry before deleting — the annotation might
    have changed, or the Deployment might have been recreated since the
    job was scheduled.
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
            ttl_datetime = parse_ttl(ttl_value)
            if not is_ttl_expired(ttl_datetime, timezone_str):
                logger.warning(
                    "ttl_not_expired_at_execution",
                    job_id=job_id,
                    namespace=namespace,
                    name=name,
                    ttl=ttl_value,
                    message="TTL was updated, not expired anymore",
                )
                return
        except ValueError as e:
            logger.error(
                "invalid_ttl_at_execution",
                job_id=job_id,
                namespace=namespace,
                name=name,
                ttl=ttl_value,
                error=str(e),
            )
            return

        if dry_run:
            logger.info(
                "dry_run_deletion",
                job_id=job_id,
                namespace=namespace,
                name=name,
                ttl=ttl_value,
            )
            DEPLOYMENTS_DELETED.labels(
                source="scheduler", namespace=namespace, outcome="dry_run"
            ).inc()
        else:
            async with track_duration(
                DELETE_DURATION, source="scheduler", namespace=namespace
            ):
                await async_delete_namespaced_deployment(namespace, name)
            logger.info(
                "deployment_deleted_by_scheduler",
                job_id=job_id,
                namespace=namespace,
                name=name,
                ttl=ttl_value,
            )
            DEPLOYMENTS_DELETED.labels(
                source="scheduler", namespace=namespace, outcome="deleted"
            ).inc()

    except Exception as e:
        logger.error(
            "deletion_job_failed",
            job_id=job_id,
            namespace=namespace,
            name=name,
            error=str(e),
            error_type=type(e).__name__,
        )
        DEPLOYMENTS_DELETED.labels(
            source="scheduler", namespace=namespace, outcome="error"
        ).inc()
    finally:
        if reconciling_uids is not None:
            reconciling_uids.discard(uid)


def schedule_deletion_job(
    scheduler: AsyncIOScheduler,
    namespace: str,
    name: str,
    uid: str,
    ttl_datetime: datetime,
    annotation_key: str,
    timezone_str: str,
    dry_run: bool,
    reconciling_uids: set[str] | None = None,
) -> bool:
    """
    Schedule a deletion job at TTL expiry time.

    Uses DateTrigger for exact, one-shot execution.  If ttl_datetime is
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
    now = datetime.now(ttl_datetime.tzinfo)

    try:
        job_kwargs: dict = {
            "namespace": namespace,
            "name": name,
            "uid": uid,
            "annotation_key": annotation_key,
            "timezone_str": timezone_str,
            "dry_run": dry_run,
        }
        if reconciling_uids is not None:
            job_kwargs["reconciling_uids"] = reconciling_uids

        scheduler.add_job(
            delete_deployment_job,
            trigger=DateTrigger(run_date=ttl_datetime),
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
            run_date=ttl_datetime.isoformat(),
            seconds_until_execution=(ttl_datetime - now).total_seconds(),
        )
        JOBS_SCHEDULED.inc()
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
        JOBS_CANCELLED.inc()
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
