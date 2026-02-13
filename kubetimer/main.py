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
    deployment_indexer,
    on_deployment_created_with_ttl,
    on_ttl_annotation_changed,
    on_deployment_deleted_with_ttl,
    init_memo,
    configure_memo,
    register_all_indexes,
)
from kubetimer.utils.logs import map_log_level, setup_logging

kubetimer_settings = get_settings()
logger = setup_logging()

loop = uvloop.new_event_loop()
asyncio.set_event_loop(loop)


def startup_handler(settings: kopf.OperatorSettings, memo: kopf.Memo, **_):
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
            
    except Exception as e:
        logger.error("startup_config_load_failed", error=str(e))
        raise


_registration_memo = kopf.Memo()
init_memo(_registration_memo)


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

    register_all_indexes(
        memo=_registration_memo,
        deployment_index_fn=deployment_indexer
    )

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

    # Should already look for expired deployments


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
