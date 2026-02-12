"""
Timer handlers for KubeTimer operator.

These handlers are triggered periodically by Kopf timers to scan
for resources with expired TTL.

NOTE: This file will be removed in the event-driven refactoring.
The timer-based polling approach will be replaced with APScheduler
that schedules deletion jobs based on TTL expiry times.
"""

from datetime import datetime
import kopf

from kubetimer.config.k8s import apps_v1_client
from kubetimer.handlers.deployment import (
    deployment_handler,
)
from kubetimer.utils.logs import get_logger

klogger = get_logger(__name__)


def check_ttl_timer_handler(
    name,
    memo: kopf.Memo,
    deployment_indexer: kopf.Index,
    logger: kopf.Logger,
    **_
):
    """
    DEPRECATED: Timer-based TTL checking (will be replaced by APScheduler).
    
    This handler runs periodically to scan all indexed deployments
    and delete those with expired TTLs.
    
    Why this approach has issues:
    - Wastes CPU scanning resources that aren't expired yet
    - Fixed interval means delayed deletions (up to check_interval seconds)
    - Doesn't scale to zero when idle
    
    The event-driven approach will solve this by scheduling individual
    deletion jobs at exact TTL expiry times.
    """
    if name != 'kubetimerconfig':
        klogger.warning("ignoring_non_default_config", config=name)
        return  

    starttime = datetime.now()
    enabled_resources = memo.enabled_resources
    annotation_key = memo.annotation_key
    dry_run = memo.dry_run
    timezone_str = memo.timezone
    
    # Updated: Use new memo structure with separate lists
    include_namespaces = memo.namespace_include
    exclude_namespaces = memo.namespace_exclude
    
    klogger.info(
        "starting_scan",
        config=name
    )
    
    apps_v1 = apps_v1_client()

    if 'deployments' in enabled_resources:
        deleted_count = deployment_handler(
            apps_v1=apps_v1,
            deployment_index=deployment_indexer,
            include_namespaces=include_namespaces,
            exclude_namespaces=exclude_namespaces,
            annotation_key=annotation_key,
            dry_run=dry_run,
            timezone_str=timezone_str
        )
        klogger.info("deployments_processed", deleted=deleted_count)
    
    # TODO: Add pods processing
    # if 'pods' in enabled_resources:
    #     deleted_count = scan_pods_from_index(...)
    #     logger.info("pods_processed", deleted=deleted_count)

    completiontime = (datetime.now() - starttime).total_seconds()
    klogger.info(
        "scan_completed",
        config=name,
        execution_time=completiontime
    )
    logger.info(f"ttl_check_completed: {name} in {completiontime:.2f}s")
