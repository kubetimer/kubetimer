"""K8s API wrappers for reading and deleting Deployments.

Thin layer over the kubernetes client that handles ApiException
and returns None / logs errors so callers don't need try/except.
"""

from kubernetes.client.exceptions import ApiException
from kubernetes.client import V1DeleteOptions

from kubetimer.config.k8s import apps_v1_client
from kubetimer.utils.logs import get_logger

logger = get_logger(__name__)


def get_namespaced_deployment(namespace: str, name: str):
    apps_v1 = apps_v1_client()
    try:
        return apps_v1.read_namespaced_deployment(name=name, namespace=namespace)
    except ApiException as e:
        logger.error(
            "error_fetching_deployment",
            namespace=namespace, name=name, error=str(e),
        )
        return None


def delete_namespaced_deployment(namespace: str, name: str):
    apps_v1 = apps_v1_client()
    apps_v1.delete_namespaced_deployment(
        name=name, namespace=namespace,
        body=V1DeleteOptions(),
    )


def list_deployments_all_namespaces(**kwargs):
    apps_v1 = apps_v1_client()
    return apps_v1.list_deployment_for_all_namespaces(**kwargs)