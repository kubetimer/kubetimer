"""
KubeTimer Operator - Main entry point.
Manages Kubernetes resources based on TTL annotations.

This module will be refactored to use APScheduler for event-driven
deletion scheduling instead of the current timer-based polling.
"""

import asyncio
import kopf
import uvloop
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from kubetimer.config.k8s import load_k8s_config
from kubetimer.config.settings import get_settings
from kubetimer.handlers import (
    on_deployment_created_with_ttl,
    on_ttl_annotation_changed,
    on_deployment_deleted_with_ttl,
    reconcile_existing_deployments,
    configure_memo,
)
from kubetimer.utils.logs import map_log_level, setup_logging

kubetimer_settings = get_settings()
logger = setup_logging()

loop = uvloop.new_event_loop()
asyncio.set_event_loop(loop)


async def startup_handler(settings: kopf.OperatorSettings, memo: kopf.Memo, **_):
    """
    Initialize operator on startup.
    
    Execution order guarantee:
    - Kopf completes ALL startup activities before starting watches
    - This means reconcile runs BEFORE on_deployment_created_with_ttl fires
    - No race conditions: jobs are scheduled here, then on.create handles new ones
    """
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

        await reconcile_existing_deployments(memo=memo)
            
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


register_all_handlers()

def main():
    """
    Main entry point for KubeTimer operator.
    """    
    logger.info(
        "starting_kubetimer",
        version="0.1.0",
        log_level=kubetimer_settings.log_level,
        enabled_resources=kubetimer_settings.get_enabled_resources_list(),
        dry_run=kubetimer_settings.dry_run,
        event_loop_policy="uvloop"
    )

    kopf.run(
        standalone=True,
        clusterwide=True,
        liveness_endpoint="http://0.0.0.0:8080/healthz",
        loop=loop
    )


if __name__ == "__main__":
    main()
