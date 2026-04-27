"""K8s API wrappers for reading and deleting Deployments.

Thin layer over the kubernetes client that handles ApiException
and returns None / logs errors so callers don't need try/except.
Sync functions are used by the scheduler jobs (which run in APScheduler's
thread pool). Async variants use asyncio.to_thread for concurrent I/O
during startup reconciliation.

All API calls include ``_request_timeout`` to avoid blocking a thread
indefinitely when the API server is slow or unresponsive.
"""

import asyncio
from typing import Generator

from kubernetes.client.exceptions import ApiException
from kubernetes.client import V1DeleteOptions, V1Deployment, V1DeploymentList

from kubetimer.config.k8s import apps_v1_client
from kubetimer.config.settings import get_settings
from kubetimer.utils.logs import get_logger

logger = get_logger(__name__)

_settings = get_settings()
_TIMEOUT = _settings.api_timeout


def get_namespaced_deployment(
    namespace: str,
    name: str,
) -> V1Deployment | None:
    apps_v1 = apps_v1_client()
    try:
        return apps_v1.read_namespaced_deployment(
            name=name,
            namespace=namespace,
            _request_timeout=_TIMEOUT,
        )
    except ApiException as e:
        logger.error(
            "error_fetching_deployment",
            namespace=namespace,
            name=name,
            error=str(e),
        )
        return None


def delete_namespaced_deployment(namespace: str, name: str) -> None:
    apps_v1 = apps_v1_client()
    try:
        apps_v1.delete_namespaced_deployment(
            name=name,
            namespace=namespace,
            body=V1DeleteOptions(
                propagation_policy="Background",
                grace_period_seconds=0,
            ),
            _request_timeout=_TIMEOUT,
        )
    except ApiException as e:
        logger.error(
            "error_deleting_deployment",
            namespace=namespace,
            name=name,
            error=str(e),
        )
        return None


async def async_delete_namespaced_deployment(namespace: str, name: str):
    """Delete a Deployment in a thread so the event loop stays free."""
    await asyncio.to_thread(delete_namespaced_deployment, namespace, name)


def patch_deployment_annotations(namespace: str, name: str, annotations: dict) -> bool:
    """PATCH a Deployment's annotations via strategic merge patch.

    Returns True on success, False on API error (logged).
    """
    apps_v1 = apps_v1_client()
    body = {"metadata": {"annotations": annotations}}
    try:
        apps_v1.patch_namespaced_deployment(
            name=name,
            namespace=namespace,
            body=body,
            _request_timeout=_TIMEOUT,
        )
        return True
    except ApiException as e:
        logger.error(
            "error_patching_deployment_annotations",
            namespace=namespace,
            name=name,
            error=str(e),
        )
        return False


async def async_patch_deployment_annotations(
    namespace: str, name: str, annotations: dict
) -> bool:
    """Async wrapper for patch_deployment_annotations."""
    return await asyncio.to_thread(
        patch_deployment_annotations, namespace, name, annotations
    )


def hibernate_deployment(
    namespace: str,
    name: str,
    original_replicas: int,
    original_replicas_key: str,
) -> bool:
    """Scale a Deployment to 0 replicas and stash the original count.

    Returns True on success, False on API error (logged).
    """
    apps_v1 = apps_v1_client()
    body = {
        "metadata": {"annotations": {original_replicas_key: str(original_replicas)}},
        "spec": {"replicas": 0},
    }
    try:
        apps_v1.patch_namespaced_deployment(
            name=name,
            namespace=namespace,
            body=body,
            _request_timeout=_TIMEOUT,
        )
        return True
    except ApiException as e:
        logger.error(
            "error_hibernating_deployment",
            namespace=namespace,
            name=name,
            original_replicas=original_replicas,
            error=str(e),
        )
        return False


async def async_hibernate_deployment(
    namespace: str,
    name: str,
    original_replicas: int,
    original_replicas_key: str,
) -> bool:
    return await asyncio.to_thread(
        hibernate_deployment,
        namespace,
        name,
        original_replicas,
        original_replicas_key,
    )


def list_deployments_all_namespaces_paginated(
    page_size: int | None = None,
    **kwargs,
) -> Generator[V1Deployment, None, None]:
    """Yield Deployments in pages using the K8s list/continue API.

    Each page fetches at most ``page_size`` items from the API server,
    avoiding a single huge JSON response on large clusters.
    """
    apps_v1 = apps_v1_client()
    limit = page_size or _settings.list_page_size
    _continue: str | None = None
    page = 0

    while True:
        page += 1
        list_kwargs: dict = {**kwargs, "limit": limit, "_request_timeout": _TIMEOUT}
        if _continue:
            list_kwargs["_continue"] = _continue

        try:
            result: V1DeploymentList = apps_v1.list_deployment_for_all_namespaces(
                **list_kwargs,
            )
        except ApiException as e:
            logger.error(
                "error_listing_deployments",
                page=page,
                error=str(e),
            )
            return

        yield from result.items

        _continue = result.metadata._continue
        if not _continue:
            logger.debug(
                "list_pagination_complete",
                total_pages=page,
                page_size=limit,
            )
            return
