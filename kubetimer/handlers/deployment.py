"""Kopf event handlers for Deployment TTL lifecycle.

Thin adapter layer: unpacks Kopf arguments, validates, and delegates
to scheduler.jobs for scheduling/cancelling APScheduler jobs.
"""

from typing import Dict, Optional

import kopf

from kubetimer.reconcile.fetcher import async_delete_namespaced_deployment
from kubetimer.scheduler.jobs import cancel_deletion_job, schedule_deletion_job
from kubetimer.utils.logs import get_logger
from kubetimer.utils.namespace import should_scan_namespace
from kubetimer.utils.time_utils import is_ttl_expired, parse_ttl

logger = get_logger(__name__)


async def on_deployment_created_with_ttl(
    namespace: str,
    name: str,
    uid: str,
    annotations: Dict[str, str],
    memo: kopf.Memo,
    **kwargs,
) -> None:
    """Handle creation of a Deployment that already carries a TTL annotation."""
    logger.info("handling_deployment_creation", namespace=namespace, name=name, uid=uid)

    reconciling_uids: set = getattr(memo, "reconciling_uids", set())
    if uid in reconciling_uids:
        logger.debug(
            "skipping_create_during_reconciliation",
            namespace=namespace,
            name=name,
            uid=uid,
        )
        return

    if not should_scan_namespace(
        namespace, memo.namespace_include, memo.namespace_exclude
    ):
        logger.debug("namespace_filtered_on_create", namespace=namespace, name=name)
        return

    ttl_value = annotations.get(memo.annotation_key)
    if not ttl_value:
        return

    try:
        ttl_datetime = parse_ttl(ttl_value)
    except ValueError as e:
        logger.error(
            "invalid_ttl_on_create",
            namespace=namespace,
            name=name,
            ttl=ttl_value,
            error=str(e),
        )
        return

    if is_ttl_expired(ttl_datetime, memo.timezone):
        logger.info(
            "ttl_already_expired_on_create",
            namespace=namespace,
            name=name,
            ttl=ttl_value,
        )
        await async_delete_namespaced_deployment(namespace, name)
        return

    else:
        logger.info(
            "scheduling_due_to_ttl_on_create",
            namespace=namespace,
            name=name,
            ttl=ttl_value,
        )
        schedule_deletion_job(
            memo.scheduler,
            namespace,
            name,
            uid,
            ttl_datetime,
            memo.annotation_key,
            memo.timezone,
            memo.dry_run,
        )


def on_ttl_annotation_changed(
    namespace: str,
    name: str,
    uid: str,
    old: Optional[str],
    new: Optional[str],
    memo: kopf.Memo,
    **_,
) -> None:
    """Handle changes to the TTL annotation field."""
    logger.info(
        "handling_ttl_annotation_change", namespace=namespace, name=name, uid=uid
    )

    reconciling_uids: set = getattr(memo, "reconciling_uids", set())
    if uid in reconciling_uids:
        logger.debug(
            "skipping_update_during_reconciliation",
            namespace=namespace,
            name=name,
            uid=uid,
        )
        return

    if not should_scan_namespace(
        namespace, memo.namespace_include, memo.namespace_exclude
    ):
        logger.debug("namespace_filtered_on_ttl_change", namespace=namespace, name=name)
        cancel_deletion_job(memo.scheduler, namespace, name, uid)
        return

    if new is None:
        logger.info(
            "ttl_annotation_removed", namespace=namespace, name=name, old_ttl=old
        )
        cancel_deletion_job(memo.scheduler, namespace, name, uid)
        return

    try:
        ttl_datetime = parse_ttl(new)
    except ValueError as e:
        logger.error(
            "invalid_ttl_on_change",
            namespace=namespace,
            name=name,
            new_ttl=new,
            error=str(e),
        )
        cancel_deletion_job(memo.scheduler, namespace, name, uid)
        return

    logger.info(
        "rescheduling_due_to_ttl_change",
        namespace=namespace,
        name=name,
        old_ttl=old,
        new_ttl=new,
    )
    schedule_deletion_job(
        memo.scheduler,
        namespace,
        name,
        uid,
        ttl_datetime,
        memo.annotation_key,
        memo.timezone,
        memo.dry_run,
    )


def on_deployment_deleted_with_ttl(
    namespace: str, name: str, uid: str, memo: kopf.Memo, **_
) -> None:
    """
    Handle deletion of Deployments that had a TTL annotation.
    Cancels any scheduled deletion jobs since the resource is already gone.
    """
    logger.info("handling_deployment_deletion", namespace=namespace, name=name, uid=uid)
    if not hasattr(memo, "scheduler"):
        return

    logger.info(
        "deployment_deleted_cancelling_job", namespace=namespace, name=name, uid=uid
    )
    cancel_deletion_job(memo.scheduler, namespace, name, uid)
