"""
KubeTimer Operator - Main entry point.
Manages Kubernetes resources based on TTL annotations.

This module will be refactored to use APScheduler for event-driven
deletion scheduling instead of the current timer-based polling.
"""

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
    check_ttl_timer_handler,
    init_memo,
    configure_memo,
    register_all_indexes,
)
from kubetimer.utils.logs import map_log_level, setup_logging

kubetimer_settings = get_settings()
logger = setup_logging()
scheduler = AsyncIOScheduler()  # Will be started in Step 4


def startup_handler(settings: kopf.OperatorSettings, memo: kopf.Memo, **_):
    """
    Initialize operator on startup.
    
    Why this order matters:
    1. Load K8s config first (needed for API access)
    2. Configure memo from Settings (environment variables)
    3. Set Kopf execution settings
    4. Start APScheduler for event-driven deletion scheduling
    5. Store scheduler in memo for handler access
    
    APScheduler Integration:
    - Uses AsyncIOScheduler (integrates with asyncio/uvloop)
    - Jobs stored in memory (MemoryJobStore by default)
    - Priority queue (heap) efficiently handles thousands of jobs
    - On restart, jobs are rebuilt from Kopf index (see Step 7)
    """
    logger.info("kubetimer_operator_starting_up")
    settings.execution.max_workers = 20
    settings.posting.level = map_log_level(kubetimer_settings.kopf_log_level)

    try:
        load_k8s_config()
        # Configure memo from environment variables (Settings)
        configure_memo(memo, kubetimer_settings)
        
        logger.info(
            "startup_config_loaded",
            enabled_resources=kubetimer_settings.get_enabled_resources_list(),
            dry_run=kubetimer_settings.dry_run,
            timezone=kubetimer_settings.timezone
        )
        
        if not scheduler.running:
            logger.info("starting_apscheduler")
            scheduler.start()
            memo.scheduler = scheduler
            logger.info(
                "apscheduler_started",
                jobstore="memory",
                executor="default"
            )
        else:
            logger.warning("apscheduler_already_running")
            
    except Exception as e:
        logger.error("startup_config_load_failed", error=str(e))
        raise


_registration_memo = kopf.Memo()
init_memo(_registration_memo)


def shutdown_handler(memo: kopf.Memo, **_):
    """
    Gracefully shutdown operator on termination.
    
    Why graceful shutdown matters:
    - Allows in-flight deletion jobs to complete
    - Prevents orphaned jobs (jobs scheduled but not executed)
    - Ensures clean state for Kubernetes pod lifecycle
    
    APScheduler shutdown behavior:
    - wait=True blocks until all currently executing jobs finish
    - Scheduled but not yet executing jobs are cancelled
    - This is acceptable because jobs will be rescheduled on restart
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
    
    Why imperative registration?
    - Allows passing pre-initialized memo to handlers
    - Gives us control over registration order
    - Makes dependencies explicit
    
    Handler lifecycle:
    1. startup_handler - Initialize config and start APScheduler
    2. index handlers - Build resource indexes from K8s watches
    3. event handlers - Schedule/reschedule/cancel deletion jobs
    4. shutdown_handler - Gracefully stop APScheduler on termination
    
    Event-driven architecture:
    - No more timer-based polling!
    - Deployment events trigger immediate job scheduling
    - Zero CPU usage when idle (no resources to watch)
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
    
    # TEMPORARY: Keep timer for backward compatibility during transition
    # Will be removed in final implementation (Step 7)
    @kopf.timer('', 'v1', 'namespaces', interval=60.0, idle=10.0)
    def temp_timer_handler(memo: kopf.Memo, deployment_indexer: kopf.Index, logger: kopf.Logger, **_):
        check_ttl_timer_handler(
            name='kubetimerconfig',
            memo=memo,
            deployment_indexer=deployment_indexer,
            logger=logger
        )


register_all_handlers()

def main():
    logger.info(
        "starting_kubetimer",
        version="0.1.0",
        log_level=kubetimer_settings.log_level,
        enabled_resources=kubetimer_settings.get_enabled_resources_list(),
        dry_run=kubetimer_settings.dry_run
    )

    kopf.run(
        standalone=True,
        clusterwide=True,
        loop=uvloop.EventLoopPolicy().new_event_loop(),
        liveness_endpoint="http://0.0.0.0:8080/healthz",
    )


if __name__ == "__main__":
    main()
