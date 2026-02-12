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
    
    Note: With Settings (env vars), configuration is loaded instantly
    without K8s API calls, making startup faster than CRD-based config.
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
    except Exception as e:
        logger.error("startup_config_load_failed", error=str(e))
        raise


_registration_memo = kopf.Memo()
init_memo(_registration_memo)


def register_all_handlers():
    """
    Register all Kopf handlers imperatively.
    
    Why imperative registration?
    - Allows passing pre-initialized memo to handlers
    - Gives us control over registration order
    - Makes dependencies explicit
    
    Note: CRD watch handlers removed - no runtime config changes.
    Timer handler is TEMPORARY and will be replaced by APScheduler.
    """
    logger.info("registering_kopf_handlers")
    
    kopf.on.startup()(startup_handler)

    register_all_indexes(
        memo=_registration_memo,
        deployment_index_fn=deployment_indexer
    )

    # Health probe for K8s liveness checks
    kopf.on.probe(id="health")(lambda **_: True)

    # TEMPORARY: Timer-based scanning (will be replaced by APScheduler)
    # Using a dummy timer on cluster-scoped operator itself
    @kopf.timer('', 'v1', 'namespaces', interval=60.0, idle=10.0)
    def temp_timer_handler(memo: kopf.Memo, deployment_indexer: kopf.Index, logger: kopf.Logger, **_):
        check_ttl_timer_handler(
            name='kubetimerconfig',  # Dummy name for compatibility
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
