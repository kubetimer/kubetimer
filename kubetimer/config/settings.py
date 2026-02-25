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

from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _validate_prefix(prefix: str) -> bool:
    return (len(prefix) > 0 and len(prefix) <= 253
        and prefix[0].isalnum()
        and prefix[-1].isalnum() 
        and all(
        c.isalnum() or c in "-." for c in prefix.split(".")
    ))


def _validate_name(name: str) -> bool:
    return (
        len(name) > 0 and len(name) <= 63
        and name[0].isalnum()
        and name[-1].isalnum()
        and all(c.isalnum() or c in "-._" for c in name)
    )


class Settings(BaseSettings):
    """
    KubeTimer operator settings - Single source of truth for all configuration.
    """

    model_config = SettingsConfigDict(
        env_prefix="KUBETIMER_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO", description="Logging level for the operator"
    )

    log_format: Literal["json", "text"] = Field(
        default="text",
        description="Log output format (json for production, text for development)",
    )

    kopf_log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="WARNING", description="Logging level for Kopf library"
    )

    annotation_key: str = Field(
        default="kubetimer.io/ttl", description="Annotation key to look for TTL values"
    )

    namespace_include: str = Field(
        default="",
        description="Comma-separated list of namespaces "
        "to include (empty = all namespaces)",
    )

    namespace_exclude: str = Field(
        default="kube-system,kube-public,kube-node-lease",
        description="Comma-separated list of namespaces to exclude",
    )

    timezone: str = Field(
        default="UTC",
        description=(
            "IANA timezone string for TTL comparison"
            " (e.g., 'America/New_York', 'Europe/London')"
        ),
    )

    dry_run: bool = Field(
        default=False,
        description="If true, log deletions without actually deleting resources",
    )

    max_concurrent_deletes: int = Field(
        default=25,
        ge=1,
        le=200,
        description=(
            "Maximum number of concurrent K8s delete calls during "
            "startup reconciliation and bulk deletes"
        ),
    )

    list_page_size: int = Field(
        default=1000,
        ge=50,
        le=5000,
        description=(
            "Page size for paginated K8s list API calls " "(limit parameter per page)"
        ),
    )

    connection_pool_size: int = Field(
        default=50,
        ge=4,
        le=200,
        description=(
            "urllib3 connection pool max size and ThreadPoolExecutor "
            "max_workers for offloaded K8s API calls"
        ),
    )

    api_timeout_connect: int = Field(
        default=5,
        ge=1,
        le=30,
        description="Connect timeout in seconds for K8s API calls",
    )

    api_timeout_read: int = Field(
        default=30,
        ge=5,
        le=120,
        description="Read timeout in seconds for K8s API calls",
    )

    @property
    def api_timeout(self) -> tuple[int, int]:
        """Return (connect, read) timeout tuple for _request_timeout."""
        return (self.api_timeout_connect, self.api_timeout_read)

    def get_namespace_include_list(self) -> list[str]:
        if not self.namespace_include.strip():
            return []
        return [ns.strip() for ns in self.namespace_include.split(",") if ns.strip()]

    def get_namespace_exclude_list(self) -> list[str]:
        return [ns.strip() for ns in self.namespace_exclude.split(",") if ns.strip()]

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, v: str) -> str:
        if v == "UTC":
            return v
        try:
            ZoneInfo(v)
        except ZoneInfoNotFoundError, KeyError:
            raise ValueError(
                f"Invalid timezone: {v!r}. "
                "Use IANA format like 'America/New_York' or 'Europe/London'."
            )
        return v

    @field_validator("annotation_key")
    @classmethod
    def validate_annotation_key(cls, v: str) -> str:
        if not v:
            raise ValueError("Annotation key cannot be empty.")

        splitted = v.split("/", 1)
        if len(splitted) == 1:
            name = splitted[0]
            if not _validate_name(name):
                raise ValueError(
                    f"Invalid annotation key: {v!r}. "
                    "Must be a valid Kubernetes annotation"
                    " key (DNS subdomain (optional)"
                    " + '/' + name)."
                )
            return v
        elif len(splitted) == 2:
            prefix, name = splitted
            if not prefix or not _validate_prefix(prefix) or not _validate_name(name):
                raise ValueError(
                    f"Invalid annotation key prefix:"
                    f" {prefix!r}. Must be a valid DNS"
                    " subdomain (e.g., 'example.com')."
                )
        return v


@lru_cache
def get_settings() -> Settings:
    """
    Get cached settings instance.

    Returns:
        Settings: The configured settings instance
    """
    return Settings()
