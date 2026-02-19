"""
KubeTimer - Kubernetes Resource TTL Operator.

A Kubernetes operator that manages the lifecycle of resources based on
TTL (Time-To-Live) annotations. Resources with expired TTL are deleted
from the cluster.
"""

__version__ = "0.2.0"
__author__ = "Ryan Carvalho"

import asyncio
from concurrent.futures import ThreadPoolExecutor
import kopf
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from kubetimer.config import Settings, get_settings
from kubetimer.config.k8s import (
    close_k8s_clients,
    load_k8s_config,
)
from kubetimer.handlers.deployment import (
    on_deployment_created_with_ttl,
    on_deployment_deleted_with_ttl,
    on_ttl_annotation_changed,
)
from kubetimer.handlers.registry import configure_memo
from kubetimer.reconcile.orchestrator import reconcile_existing_deployments
from kubetimer.utils.logs import get_logger, map_log_level, setup_logging

logger = get_logger(__name__)
kubetimer_settings = get_settings()


async def startup_handler(settings: kopf.OperatorSettings, memo: kopf.Memo, **_):

    logger.info("kubetimer_operator_starting_up")
    settings.execution.max_workers = 20
    settings.posting.level = map_log_level(kubetimer_settings.kopf_log_level)
    settings.scanning.disabled = True

    try:
        pool_size = kubetimer_settings.connection_pool_size
        load_k8s_config(pool_size=pool_size)

        loop = asyncio.get_event_loop()

        executor = ThreadPoolExecutor(max_workers=pool_size)
        loop.set_default_executor(executor)
        memo.executor = executor
        logger.debug("thread_pool_configured", max_workers=pool_size)

        configure_memo(memo, kubetimer_settings)

        logger.info(
            "startup_config_loaded",
            dry_run=kubetimer_settings.dry_run,
            timezone=kubetimer_settings.timezone,
        )

        scheduler = AsyncIOScheduler(
            job_defaults={
                "misfire_grace_time": 60,
                "max_instances": 1,
                "coalesce": True,
            },
        )
        scheduler.start()
        memo.scheduler = scheduler
        logger.info(
            "apscheduler_started",
            jobstore="memory",
            executor="AsyncIOExecutor",
            misfire_grace_time=60,
            max_instances=1,
            coalesce=True,
        )

        memo.reconciling_uids = set()
        memo.reconciliation_done = False

        await reconcile_existing_deployments(memo=memo)

        # is this flag needed? --- it is set to True at the end of this startup_handler, but it is not currently used anywhere else in the code.  It could be used by event handlers to check if startup reconciliation is still in progress, and if so, they could skip scheduling new jobs for Deployments that are being reconciled.  This would prevent potential conflicts between the startup reconciliation process and any new events that come in during that time.  However, since the reconciling_uids set is already being used to track which Deployments are being reconciled, it may not be strictly necessary to have this additional flag.  It could be useful for clarity and to avoid edge cases where a Deployment's UID is not in the reconciling_uids set but startup reconciliation is still ongoing.  Overall, it seems like a reasonable safety check to have, but it may not be strictly required given the current logic of the code.
        memo.reconciliation_done = True

        logger.info(
            "reconciliation_complete_handlers_unblocked",
            remaining_reconciling_uids=len(memo.reconciling_uids),
        )

    except Exception as e:
        logger.error("startup_config_load_failed", error=str(e))
        raise


async def shutdown_handler(memo: kopf.Memo, **_):
    """
    Gracefully shutdown operator on termination.
    """
    logger.info("kubetimer_operator_shutting_down")

    try:
        close_k8s_clients()
    except Exception as e:
        logger.error("k8s_client_close_failed", error=str(e))

    if hasattr(memo, "scheduler") and memo.scheduler.running:
        try:
            logger.info("shutting_down_apscheduler")
            memo.scheduler.shutdown(wait=True)
            logger.info(
                "apscheduler_shutdown_complete", message="All executing jobs completed"
            )
        except Exception as e:
            logger.error("apscheduler_shutdown_failed", error=str(e))
    else:
        logger.warning("apscheduler_not_running_during_shutdown")

    if hasattr(memo, "executor"):
        try:
            logger.info("shutting_down_thread_pool_executor")
            memo.executor.shutdown(wait=True)
            logger.info("thread_pool_executor_shutdown_complete")
        except Exception as e:
            logger.error(
                "thread_pool_executor_shutdown_failed",
                error=str(e),
            )


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
        "apps",
        "v1",
        "deployments",
        annotations={kubetimer_settings.annotation_key: kopf.PRESENT},
    )(
        on_deployment_created_with_ttl  # type: ignore[arg-type]
    )

    annotation_selector = kubetimer_settings.annotation_key.replace(".", "\\.")
    kopf.on.field(
        "apps",
        "v1",
        "deployments",
        field=f"metadata.annotations.{annotation_selector}",
    )(
        on_ttl_annotation_changed  # type: ignore[arg-type]
    )

    kopf.on.delete(
        "apps",
        "v1",
        "deployments",
        annotations={kubetimer_settings.annotation_key: kopf.PRESENT},
    )(
        on_deployment_deleted_with_ttl  # type: ignore[arg-type]
    )

    logger.info(
        "starting_kubetimer",
        version=__version__,
        log_level=kubetimer_settings.log_level,
        dry_run=kubetimer_settings.dry_run,
        event_loop_policy="uvloop",
    )


__all__ = [
    "Settings",
    "get_settings",
    "setup_logging",
    "get_logger",
    "__version__",
]
