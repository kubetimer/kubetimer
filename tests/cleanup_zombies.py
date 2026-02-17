#!/usr/bin/env python3
"""Remove all zombie deployments from the cluster.

Usage:
    python tests/cleanup_zombies.py
    python tests/cleanup_zombies.py --namespace staging
    python tests/cleanup_zombies.py --all-namespaces
"""

import argparse
import asyncio
import sys

from kubernetes import client, config

from kubetimer.reconcile.fetcher import async_delete_namespaced_deployment

LABEL_SELECTOR = "app=kubetimer-zombie"


async def cleanup(namespace: str | None, all_namespaces: bool):
    config.load_kube_config()
    api = client.AppsV1Api()

    if all_namespaces:
        deps = api.list_deployment_for_all_namespaces(
            label_selector=LABEL_SELECTOR,
        )
    else:
        ns = namespace or "default"
        deps = api.list_namespaced_deployment(
            ns, label_selector=LABEL_SELECTOR,
        )

    total = len(deps.items)
    if total == 0:
        print("No zombie deployments found.")
        return

    print(f"Found {total} zombie deployment(s). Deleting …")

    deleted = 0
    errors = 0
    for dep in deps.items:
        name = dep.metadata.name
        ns = dep.metadata.namespace
        try:
            await async_delete_namespaced_deployment(ns, name)
            deleted += 1
            print(f"  ✓ {ns}/{name}")
        except client.exceptions.ApiException as e:
            errors += 1
            print(
                f"  ✗ {ns}/{name} — {e.reason}",
                file=sys.stderr,
            )

    print(
        f"\nDone. Deleted {deleted}, "
        f"errors {errors}."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Remove all zombie deployments.",
    )
    parser.add_argument(
        "--namespace",
        type=str,
        default=None,
        help="Target namespace (default: 'default')",
    )
    parser.add_argument(
        "--all-namespaces",
        action="store_true",
        default=False,
        help="Delete zombies from all namespaces",
    )
    args = parser.parse_args()
    asyncio.run(cleanup(args.namespace, args.all_namespaces))
