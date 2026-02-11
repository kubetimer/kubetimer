"""
Timer handlers for KubeTimer operator.

These handlers are triggered periodically by Kopf timers to scan
for resources with expired TTL.
"""

from datetime import datetime
import kopf

from kubetimer.config.k8s import KubeTimerConfig, apps_v1_client
from kubetimer.handlers.deployment import (
    deployment_handler,
)
from kubetimer.handlers.registry import configure_memo
from kubetimer.utils.logs import get_logger

klogger = get_logger(__name__)


def check_ttl_timer_handler(
    name,
    memo: kopf.Memo,
    deployment_indexer: kopf.Index,
    logger: kopf.Logger,
    **_
):  
    if name != 'kubetimerconfig':
        klogger.warning("ignoring_non_default_config", config=name)
        return  

    starttime = datetime.now()
    enabled_resources = memo.enabled_resources
    annotation_key = memo.annotation_key
    dry_run = memo.dry_run
    timezone_str = memo.timezone
    
    namespaces_config = memo.namespaces
    include_namespaces = namespaces_config.get('include', [])
    exclude_namespaces = namespaces_config.get(
        'exclude', 
        ['kube-system', 'kube-public', 'kube-node-lease']
    )
    
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


def config_changed_handler(spec, name, memo: kopf.Memo, **_):
    if name != memo.get('config_name'):
        klogger.warning("ignoring_non_default_config_change", config=name)
        return

    kubetimerconfig = KubeTimerConfig(
        name=name,
        enabled_resources=spec.get('enabledResources', ['deployments']),
        annotation_key=spec.get('annotationKey', 'kubetimer.io/ttl'),
        dry_run=spec.get('dryRun', False),
        timezone=spec.get('timezone', 'UTC'),
        namespaces=spec.get('namespaces', {}),
    )
    
    configure_memo(memo, kubetimerconfig)

    memo.enabled_resources = set(kubetimerconfig.enabled_resources)
    memo.annotation_key = kubetimerconfig.annotation_key
    memo.dry_run = kubetimerconfig.dry_run
    memo.timezone = kubetimerconfig.timezone
    memo.namespaces = kubetimerconfig.namespaces
    memo.config_loaded = True

    klogger.info(
        "config_updated",
        config=name,
        enabled_resources=spec.get('enabledResources', ['deployments']),
        annotation_key=spec.get('annotationKey', 'kubetimer.io/ttl'),
        check_interval=spec.get('checkIntervalSeconds', 60)
    )
