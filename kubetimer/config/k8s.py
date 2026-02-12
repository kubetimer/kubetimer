"""
Kubernetes client configuration and factories.

This module provides K8s client initialization and API client factories.
Configuration is now sourced from environment variables via Settings.
"""
from kubernetes import client, config

from kubetimer.utils.logs import setup_logging

logger = setup_logging()


def load_k8s_config():
    """
    Load Kubernetes configuration.
    
    Tries in-cluster config first (for production), falls back to
    local kubeconfig (for development).
    """
    try:
        config.load_incluster_config()
        logger.info("loaded_incluster_config")
    except config.ConfigException:
        config.load_kube_config()
        logger.info("loaded_local_kube_config")


def apps_v1_client() -> client.AppsV1Api:
    """
    Create an AppsV1Api client for managing Deployments.
    """
    return client.AppsV1Api()
