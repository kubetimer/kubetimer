from typing import List, Set

import kopf

from kubetimer.config.k8s import KubeTimerConfig
from kubetimer.utils.logs import get_logger

logger = get_logger(__name__)


def init_memo(memo: kopf.Memo) -> None:
    if not hasattr(memo, 'registered_indexes'):
        memo.registered_indexes = set()
    if not hasattr(memo, 'enabled_resources'):
        memo.enabled_resources = set()
    if not hasattr(memo, 'config_loaded'):
        memo.config_loaded = False


def configure_memo(memo: kopf.Memo, config: KubeTimerConfig) -> None:
    memo.enabled_resources = set(config.enabled_resources)
    memo.annotation_key = config.annotation_key
    memo.dry_run = config.dry_run
    memo.timezone = config.timezone
    memo.namespaces = config.namespaces
    memo.config_loaded = True
    logger.info(
        "memo_configured",
        enabled_resources=list(memo.enabled_resources),
        include_namespaces=memo.namespaces.get('include', []),
        exclude_namespaces=memo.namespaces.get('exclude', []),
        timezone=memo.timezone,
        dry_run=memo.dry_run
    )


def is_index_registered_in_memo(memo: kopf.Memo, resource_type: str) -> bool:
    registered = getattr(memo, 'registered_indexes', set())
    return resource_type in registered



def register_all_indexes(
    memo: kopf.Memo,
    deployment_index_fn=None
) -> List[str]:

    registered = []
    
    if deployment_index_fn:
        kopf.index('apps', 'v1', 'deployments')(deployment_index_fn)
        memo.registered_indexes.add('deployments')
        registered.append('deployments')
        logger.debug("registered_deployment_index")

    # TODO: Add other resource indexes when implemented
    
    logger.info("all_indexes_registered", registered=registered)
    return registered


def get_registered_indexes_from_memo(memo: kopf.Memo) -> Set[str]:
    """Get the set of registered indexes from memo."""
    return getattr(memo, 'registered_indexes', set()).copy()
