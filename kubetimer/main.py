"""
KubeTimer Operator - Main entry point.
Manages Kubernetes resources based on TTL annotations.
"""

import kopf
import uvloop

from kubetimer.config.k8s import get_kubetimerconfig, load_k8s_config
from kubetimer.config.settings import get_settings
from kubetimer.handlers import (
    deployment_indexer,
    check_ttl_timer_handler,
    config_changed_handler,
    init_memo,
    configure_memo,
    register_all_indexes,
)
from kubetimer.utils.logs import map_log_level, setup_logging

kubetimer_settings = get_settings()
logger = setup_logging()


def startup_handler(settings: kopf.OperatorSettings, memo: kopf.Memo, **_):
    logger.info("kubetimer_operator_starting_up")
    settings.execution.max_workers = 20
    settings.posting.level = map_log_level(kubetimer_settings.kopf_log_level)

    try:
        load_k8s_config()
        config = get_kubetimerconfig()
        configure_memo(memo, config)
        
        logger.info("startup_config_loaded")
    except Exception:
        logger.error("startup_config_load_failed")
        raise


_registration_memo = kopf.Memo()
init_memo(_registration_memo)


def register_all_handlers():
    logger.info("registering_kopf_handlers")
    
    kopf.on.startup()(startup_handler)

    register_all_indexes(
        memo=_registration_memo,
        deployment_index_fn=deployment_indexer
    )

    kopf.on.create('kubetimer.io', 'v1', 'kubetimerconfigs')(config_changed_handler)
    kopf.on.update('kubetimer.io', 'v1', 'kubetimerconfigs')(config_changed_handler)
    kopf.on.probe(id="health")(lambda **_: True)

    kopf.timer(
        'kubetimer.io', 'v1', 'kubetimerconfigs',
        interval=float(kubetimer_settings.check_interval),
        idle=10.0
    )(check_ttl_timer_handler)


register_all_handlers()

def main():
    logger.info(
        "starting_kubetimer",
        version="0.1.0",
        log_level=kubetimer_settings.log_level,
        check_interval=kubetimer_settings.check_interval
    )

    kopf.run(
        standalone=True,
        clusterwide=True,
        loop=uvloop.EventLoopPolicy().new_event_loop(),
        liveness_endpoint="http://0.0.0.0:8080/healthz",
    )


if __name__ == "__main__":
    main()
