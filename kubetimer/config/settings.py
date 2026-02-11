"""
Settings module for KubeTimer operator.

This module uses Pydantic Settings to manage configuration from environment variables.
All settings can be overridden via KUBETIMER_* environment variables.
"""

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    KubeTimer operator settings.
    
    All settings can be configured via environment variables with KUBETIMER_ prefix.
    Example: KUBETIMER_LOG_LEVEL=DEBUG
    
    Attributes:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_format: Output format (json for production, text for development)
        check_interval: Default check interval in seconds
    """
    
    model_config = SettingsConfigDict(
        env_prefix="KUBETIMER_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )
    
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO",
        description="Logging level for the operator"
    )
    
    log_format: Literal["json", "text"] = Field(
        default="text",
        description="Log output format (json for production, text for development)"
    )
    
    check_interval: int = Field(
        default=60,
        ge=5,
        le=3600,
        description="Default check interval in seconds"
    )

    kopf_log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="WARNING",
        description="Logging level for Kopf library"
    )


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
