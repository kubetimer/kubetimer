"""
Settings module for KubeTimer operator.

This module uses Pydantic Settings to manage configuration from environment variables.
All settings can be overridden via KUBETIMER_* environment variables.

Example:
    KUBETIMER_LOG_LEVEL=DEBUG
    KUBETIMER_NAMESPACE_EXCLUDE=kube-system,kube-public
"""

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    KubeTimer operator settings - Single source of truth for all configuration.
    
    All settings can be configured via environment variables with KUBETIMER_ prefix.
    
    Attributes:
        log_level: Logging level for the operator
        log_format: Output format (json for production, text for development)
        kopf_log_level: Logging level for Kopf framework
        annotation_key: Annotation key to look for TTL values on resources
        namespace_include: Comma-separated list of namespaces to include (empty = all)
        namespace_exclude: Comma-separated list of namespaces to exclude
        timezone: IANA timezone string for TTL comparison (e.g., America/New_York)
        dry_run: If true, log deletions without actually deleting
    """
    
    model_config = SettingsConfigDict(
        env_prefix="KUBETIMER_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )
    
    # Logging configuration
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO",
        description="Logging level for the operator"
    )
    
    log_format: Literal["json", "text"] = Field(
        default="text",
        description="Log output format (json for production, text for development)"
    )

    kopf_log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="WARNING",
        description="Logging level for Kopf library"
    )

    annotation_key: str = Field(
        default="kubetimer.io/ttl",
        description="Annotation key to look for TTL values"
    )

    namespace_include: str = Field(
        default="",
        description="Comma-separated list of namespaces to include (empty = all namespaces)"
    )
    
    namespace_exclude: str = Field(
        default="kube-system,kube-public,kube-node-lease",
        description="Comma-separated list of namespaces to exclude"
    )

    # TTL and deletion behavior
    timezone: str = Field(
        default="UTC",
        description="IANA timezone string for TTL comparison (e.g., 'America/New_York', 'Europe/London')"
    )
    
    dry_run: bool = Field(
        default=False,
        description="If true, log deletions without actually deleting resources"
    )

    def get_namespace_include_list(self) -> list[str]:
        """
        Parse comma-separated namespace_include into a list.
        """
        if not self.namespace_include.strip():
            return []
        return [ns.strip() for ns in self.namespace_include.split(',') if ns.strip()]
    
    def get_namespace_exclude_list(self) -> list[str]:
        """
        Parse comma-separated namespace_exclude into a list.
        """
        return [ns.strip() for ns in self.namespace_exclude.split(',') if ns.strip()]


@lru_cache
def get_settings() -> Settings:
    """
    Get cached settings instance.
    
    Uses @lru_cache so settings are loaded once and reused.
    This is efficient and ensures consistent configuration.
    
    Returns:
        Settings: The configured settings instance
    """
    return Settings()
