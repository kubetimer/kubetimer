"""
Handler registry and memo configuration for KubeTimer operator.

This module manages:
- Kopf memo initialization and configuration
- Resource index registration
- Shared state between handlers

Why memo pattern?
- Kopf handlers are called independently by the framework
- Memo provides a shared, mutable object to pass state between handlers
- Allows configuration to be loaded once and accessed everywhere
"""
from typing import List, Set

import kopf

from kubetimer.config.settings import Settings
from kubetimer.utils.logs import get_logger

logger = get_logger(__name__)


def init_memo(memo: kopf.Memo) -> None:
    """
    Initialize the memo object with default values.
    
    Why: Ensures memo has expected attributes before first use.
    This prevents AttributeError if handlers try to access memo
    before configuration is loaded.
    """
    if not hasattr(memo, 'registered_indexes'):
        memo.registered_indexes = set()
    if not hasattr(memo, 'enabled_resources'):
        memo.enabled_resources = set()
    if not hasattr(memo, 'config_loaded'):
        memo.config_loaded = False


def configure_memo(memo: kopf.Memo, settings: Settings) -> None:
    """
    Configure memo from Settings (environment variables).
    
    Why: Centralizes configuration parsing and stores it in memo
    for all handlers to access. This is called once at startup.
    
    Args:
        memo: Kopf memo object to store configuration
        settings: Parsed settings from environment variables
    
    Note: With event-driven architecture, configuration is static
    after startup (no runtime changes like with CRD).
    """
    memo.enabled_resources = set(settings.get_enabled_resources_list())
    memo.annotation_key = settings.annotation_key
    memo.dry_run = settings.dry_run
    memo.timezone = settings.timezone
    memo.namespace_include = settings.get_namespace_include_list()
    memo.namespace_exclude = settings.get_namespace_exclude_list()
    memo.max_concurrent_deletions = settings.max_concurrent_deletions
    memo.config_loaded = True
    
    logger.info(
        "memo_configured",
        enabled_resources=list(memo.enabled_resources),
        include_namespaces=memo.namespace_include or "all",
        exclude_namespaces=memo.namespace_exclude,
        timezone=memo.timezone,
        dry_run=memo.dry_run,
        max_concurrent_deletions=memo.max_concurrent_deletions
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
