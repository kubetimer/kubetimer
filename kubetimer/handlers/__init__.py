"""
Handlers for KubeTimer operator.

Exports:
- Index handler functions for each resource type
- Timer handlers for TTL scanning (DEPRECATED - will be replaced by APScheduler)
- Registry functions for handler registration
"""

from kubetimer.handlers.deployment import (
    deployment_indexer,
    deployment_handler,
)

from kubetimer.handlers.timer import (
    check_ttl_timer_handler,
)

from kubetimer.handlers.registry import (
    init_memo,
    configure_memo,
    register_all_indexes,
    is_index_registered_in_memo,
    get_registered_indexes_from_memo,
)

__all__ = [
    "deployment_indexer",
    "deployment_handler",
    "check_ttl_timer_handler",
    "init_memo",
    "configure_memo",
    "register_all_indexes",
    "is_index_registered_in_memo",
    "get_registered_indexes_from_memo",
]
