"""
KubeTimer - Kubernetes Resource TTL Operator.

A Kubernetes operator that manages the lifecycle of resources based on
TTL (Time-To-Live) annotations. Resources with expired TTL are automatically
deleted from the cluster.

Usage:
    kopf run kubetimer/main.py --standalone

Environment Variables:
    KUBETIMER_LOG_LEVEL: Kubetimer logging level (default: INFO)
    KUBETIMER_KOPF_LOG_LEVEL: Kopf logging level (default: WARNING)
    KUBETIMER_LOG_FORMAT: Log format, json or text (default: text)
    KUBETIMER_CHECK_INTERVAL: Check interval in seconds (default: 60)
"""

__version__ = "0.1.0"
__author__ = "Ryan Carvalho"

from kubetimer.config import Settings, get_settings
from kubetimer.utils.logs import get_logger, setup_logging

__all__ = [
    "Settings",
    "get_settings",
    "setup_logging",
    "get_logger",
    "__version__",
]
