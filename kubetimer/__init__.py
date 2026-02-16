"""
KubeTimer - Kubernetes Resource TTL Operator.

A Kubernetes operator that manages the lifecycle of resources based on
TTL (Time-To-Live) annotations. Resources with expired TTL are automatically
deleted from the cluster.

Usage:
    kopf run kubetimer/main.py --standalone

Environment Variables:
    KUBETIMER_LOG_LEVEL: Kubetimer logging level (default: INFO)
    KUBETIMER_KOPF_LOG_LEVEL: Kopf logging level (default: WARNING)
    KUBETIMER_LOG_FORMAT: Log format, json or text (default: text)
    KUBETIMER_CHECK_INTERVAL: Check interval in seconds (default: 60)
"""

__version__ = "0.1.0"
__author__ = "Ryan Carvalho"

import asyncio
import kopf
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import uvloop

from kubetimer.config import Settings, get_settings
from kubetimer.config.k8s import load_k8s_config
from kubetimer.handlers.deployment import on_deployment_created_with_ttl, on_deployment_deleted_with_ttl, on_ttl_annotation_changed
from kubetimer.handlers.registry import configure_memo
from kubetimer.reconcile.orchestrator import reconcile_existing_deployments
from kubetimer.utils.logs import get_logger, map_log_level, setup_logging


logger = get_logger(__name__)
kubetimer_settings = get_settings()
loop = uvloop.new_event_loop()
asyncio.set_event_loop(loop)

async def startup_handler(settings: kopf.OperatorSettings, memo: kopf.Memo, **_):

    logger.info("kubetimer_operator_starting_up")
    settings.execution.max_workers = 20
    settings.posting.level = map_log_level(kubetimer_settings.kopf_log_level)

    try:
        load_k8s_config()
        configure_memo(memo, kubetimer_settings)
        
        logger.info(
            "startup_config_loaded",
            enabled_resources=kubetimer_settings.get_enabled_resources_list(),
            dry_run=kubetimer_settings.dry_run,
            timezone=kubetimer_settings.timezone
        )

        scheduler = AsyncIOScheduler(event_loop=loop)
        scheduler.start()
        memo.scheduler = scheduler
        logger.info(
            "apscheduler_started",
            jobstore="memory",
            executor="default"
        )

        reconcile_existing_deployments(memo=memo)
            
    except Exception as e:
        logger.error("startup_config_load_failed", error=str(e))
        raise


def shutdown_handler(memo: kopf.Memo, **_):
    """
    Gracefully shutdown operator on termination.
    - Allows in-flight deletion jobs to complete
    - Prevents orphaned jobs (jobs scheduled but not executed)
    - Ensures clean state for Kubernetes pod lifecycle
    """
    logger.info("kubetimer_operator_shutting_down")

    if hasattr(memo, 'scheduler') and memo.scheduler.running:
        try:
            logger.info("shutting_down_apscheduler")
            memo.scheduler.shutdown(wait=True)
            logger.info(
                "apscheduler_shutdown_complete",
                message="All executing jobs completed"
            )
        except Exception as e:
            logger.error("apscheduler_shutdown_failed", error=str(e))
    else:
        logger.warning("apscheduler_not_running_during_shutdown")


def register_all_handlers():
    """
    Register all Kopf handlers imperatively.
    
    Handler lifecycle:
    1. startup_handler - Initialize config and start APScheduler
    2. index handlers - Build resource indexes from K8s watches
    3. event handlers - Schedule/reschedule/cancel deletion jobs
    4. shutdown_handler - Gracefully stop APScheduler on termination
    """
    logger.info("registering_kopf_handlers")
    
    kopf.on.startup()(startup_handler)
    kopf.on.cleanup()(shutdown_handler)

    # Health probe for K8s liveness checks
    kopf.on.probe(id="health")(lambda **_: True)

    kopf.on.create(
        'apps', 'v1', 'deployments',
        annotations={kubetimer_settings.annotation_key: kopf.PRESENT}
    )(on_deployment_created_with_ttl)
    
    kopf.on.field(
        'apps', 'v1', 'deployments',
        field=f'metadata.annotations.{kubetimer_settings.annotation_key.replace(".", "\\.")}',
    )(on_ttl_annotation_changed)
    
    kopf.on.delete(
        'apps', 'v1', 'deployments',
        annotations={kubetimer_settings.annotation_key: kopf.PRESENT}
    )(on_deployment_deleted_with_ttl)

    logger.info(
        "starting_kubetimer",
        version="0.1.0",
        log_level=kubetimer_settings.log_level,
        enabled_resources=kubetimer_settings.get_enabled_resources_list(),
        dry_run=kubetimer_settings.dry_run,
        event_loop_policy="uvloop"
    )

__all__ = [
    "Settings",
    "get_settings",
    "setup_logging",
    "get_logger",
    "__version__",
]
