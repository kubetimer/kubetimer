"""
Kubernetes client configuration and factories.

This module provides K8s client initialization and API client factories.
Configuration is now sourced from environment variables via Settings.
"""

from kubernetes import client, config

from kubetimer.utils.logs import setup_logging

logger = setup_logging()

_connection_pool_maxsize: int = 0


def load_k8s_config():
    """
    Load Kubernetes configuration.

    Tries in-cluster config first (for production), falls back to
    local kubeconfig (for development).

    Reads the effective connection_pool_maxsize from the client
    Configuration so other modules can align to it.
    """
    global _connection_pool_maxsize

    try:
        config.load_incluster_config()
        logger.info("loaded_incluster_config")
    except config.ConfigException:
        config.load_kube_config()
        logger.info("loaded_local_kube_config")

    k8s_config = client.Configuration.get_default_copy()
    _connection_pool_maxsize = k8s_config.connection_pool_maxsize
    logger.debug("k8s_connection_pool_maxsize", pool_size=_connection_pool_maxsize)


def get_connection_pool_maxsize() -> int:
    """Return the K8s client's connection_pool_maxsize.

    Available after load_k8s_config() has been called.
    """
    return _connection_pool_maxsize


_apps_v1: client.AppsV1Api | None = None


def apps_v1_client() -> client.AppsV1Api:
    """
    Return a cached AppsV1Api client for managing Deployments.

    A single instance is reused so urllib3 connection pooling
    stays effective across concurrent calls.
    """
    global _apps_v1
    if _apps_v1 is None:
        _apps_v1 = client.AppsV1Api()
    return _apps_v1
