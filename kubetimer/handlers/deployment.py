"""
Deployment handler for KubeTimer operator.

Contains Kopf handlers for managing Deployment lifecycle based on TTL.
"""

from typing import Any, Dict, List, Optional

import kopf
from kubernetes import client

from kubetimer.utils.logs import get_logger
from kubetimer.utils.time_utils import is_ttl_expired, parse_ttl

logger = get_logger(__name__)


def deployment_indexer(
    name: str,
    namespace: str,
    meta: kopf.Meta,
    memo: kopf.Memo,
    **_
) -> Optional[Dict[str, Any]]:
    annotations = meta.get('annotations', {})
    date = annotations.get(memo.annotation_key, '')

    if not date:
        return None
    
    return {
        name: {
            'namespace': namespace,
            memo.annotation_key: date
        }
    }


def should_scan_namespace(
    namespace: str,
    include_namespaces: List[str],
    exclude_namespaces: List[str]
) -> bool:
    if namespace in exclude_namespaces:
        return False

    if not include_namespaces:
        return True

    return namespace in include_namespaces


def deployment_handler(
    apps_v1: client.AppsV1Api,
    deployment_index: kopf.Index,
    include_namespaces: List[str],
    exclude_namespaces: List[str],
    annotation_key: str,
    dry_run: bool,
    timezone_str: str = "UTC"
) -> int:

    logger.debug(
        "scanning_deployments_from_index",
        total_indexed=len(deployment_index),
        include_namespaces=include_namespaces or "all",
        exclude_namespaces=exclude_namespaces
    )

    deleted_count = 0
    scanned_count = 0


    deployments_snapshot = []
    for name, store in deployment_index.items():
        for value in store:
            deployments_snapshot.append({
                'name': name,
                'namespace': value['namespace'],
                annotation_key: value.get(annotation_key)
            })

    logger.debug("deployment_snapshot", count=len(deployments_snapshot))

    for deployment_info in deployments_snapshot:
        name = deployment_info['name']
        ns = deployment_info['namespace']

        logger.debug("checking_deployment", deployment=name, namespace=ns)

        if not should_scan_namespace(ns, include_namespaces, exclude_namespaces):
            continue
        
        scanned_count += 1

        ttl_value = deployment_info.get(annotation_key)

        try:
            ttl_datetime = parse_ttl(ttl_value)

            if is_ttl_expired(ttl_datetime, timezone_str):
                logger.debug(
                    "deployment_expired",
                    name=name,
                    namespace=ns,
                    ttl=ttl_value,
                    dry_run=dry_run
                )
                
                if not dry_run:
                    apps_v1.delete_namespaced_deployment(
                        name=name,
                        namespace=ns,
                        body=client.V1DeleteOptions()
                    )
                    logger.info("deployment_deleted", name=name, namespace=ns)
                
                deleted_count += 1
        
        except ValueError as e:
            logger.error(
                "invalid_ttl_format",
                name=name,
                namespace=ns,
                ttl=ttl_value,
                error=str(e)
            )
        except client.ApiException as e:
            if e.status == 404:
                logger.error(
                    "deployment_was_already_deleted",
                    name=name,
                    namespace=ns)
            else:
                logger.error(
                    "api_exception",
                    name=name,
                    namespace=ns,
                    error=str(e)
                )
    
    logger.info(
        "deployment_scan_complete",
        scanned_deployments=scanned_count,
        deleted_count=deleted_count,
        dry_run=dry_run
    )
    return deleted_count
