import kopf

from kubetimer.config.settings import Settings
from kubetimer.utils.logs import get_logger

logger = get_logger(__name__)


def configure_memo(memo: kopf.Memo, settings: Settings) -> None:
    """
    Configure memo from Settings (environment variables).

    Args:
        memo: Kopf memo object to store configuration
        settings: Parsed settings from environment variables
    """
    memo.annotation_key = settings.annotation_key
    memo.dry_run = settings.dry_run
    memo.timezone = settings.timezone
    memo.namespace_include = settings.get_namespace_include_list()
    memo.namespace_exclude = settings.get_namespace_exclude_list()
    memo.config_loaded = True
    
    logger.info(
        "memo_configured",
        include_namespaces=memo.namespace_include or "all",
        exclude_namespaces=memo.namespace_exclude,
        timezone=memo.timezone,
        dry_run=memo.dry_run,
    )
