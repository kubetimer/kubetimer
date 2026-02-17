"""
Kubernetes client configuration and factories.

This module provides K8s client initialization and API client factories.
Configuration is now sourced from environment variables via Settings.
"""

from functools import lru_cache

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


def get_connection_pool_maxsize() -> int | None:
    """Return the K8s client's connection_pool_maxsize.

    Available after load_k8s_config() has been called.
    """
    if _connection_pool_maxsize == 0:
        logger.warning(
            "connection_pool_maxsize_not_set",
            message="load_k8s_config() must be called before getting pool size",
        )
        return None
    return _connection_pool_maxsize


@lru_cache(maxsize=1)
def apps_v1_client() -> client.AppsV1Api:
    """
    Return a cached AppsV1Api client for managing Deployments.

    A single instance is reused so urllib3 connection pooling
    stays effective across concurrent calls. The @lru_cache
    decorator provides thread-safe, lazy initialization.
    """
    return client.AppsV1Api()


def close_k8s_clients() -> None:
    """
    Close the cached K8s API client and its urllib3 connection pool.
    """
    cached_client = apps_v1_client.cache_info()
    if cached_client.currsize > 0:
        try:
            apps_v1_client().api_client.close()
        except Exception:
            pass
        apps_v1_client.cache_clear()
        logger.info("k8s_api_client_closed")
