import kopf

from kubetimer.config.settings import Settings
from kubetimer.utils.actions import (
    ACTION_ANNOTATION_SUFFIX,
    ORIGINAL_REPLICAS_SUFFIX,
)
from kubetimer.utils.logs import get_logger

logger = get_logger(__name__)


def configure_memo(memo: kopf.Memo, settings: Settings) -> None:
    """
    Configure memo from Settings (environment variables).

    Args:
        memo: Kopf memo object to store configuration
        settings: Parsed settings from environment variables
    """
    prefix = settings.annotation_key.rsplit("/", 1)[0]
    memo.annotation_key = settings.annotation_key
    memo.expires_at_key = f"{prefix}/expires-at"
    memo.action_key = f"{prefix}/{ACTION_ANNOTATION_SUFFIX}"
    memo.original_replicas_key = f"{prefix}/{ORIGINAL_REPLICAS_SUFFIX}"
    memo.default_action = settings.default_action
    memo.dry_run = settings.dry_run
    memo.timezone = settings.timezone
    memo.namespace_include = frozenset(settings.get_namespace_include_list())
    memo.namespace_exclude = frozenset(settings.get_namespace_exclude_list())
    memo.max_concurrent_deletes = settings.max_concurrent_deletes
    memo.config_loaded = True

    logger.info(
        "memo_configured",
        include_namespaces=sorted(memo.namespace_include) or "all",
        exclude_namespaces=sorted(memo.namespace_exclude),
        timezone=memo.timezone,
        dry_run=memo.dry_run,
        default_action=memo.default_action,
        action_key=memo.action_key,
        original_replicas_key=memo.original_replicas_key,
        max_concurrent_deletes=memo.max_concurrent_deletes,
        connection_pool_size=settings.connection_pool_size,
    )
