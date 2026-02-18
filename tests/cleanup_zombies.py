#!/usr/bin/env python3
"""Remove all zombie deployments from the cluster.

Usage:
    python tests/cleanup_zombies.py
    python tests/cleanup_zombies.py --namespace staging
    python tests/cleanup_zombies.py --all-namespaces
    python tests/cleanup_zombies.py --force --concurrency 20
"""

import argparse
import asyncio
import sys

from kubernetes import client, config
from kubernetes.client import V1DeleteOptions

LABEL_SELECTOR = "app=kubetimer-zombie"

# Per-call timeout: (connect_timeout, read_timeout) in seconds
REQUEST_TIMEOUT = (5, 15)


def _delete_options() -> V1DeleteOptions:
    """Delete options that force immediate background garbage collection."""
    return V1DeleteOptions(
        propagation_policy="Background",
        grace_period_seconds=0,
    )


def _patch_remove_finalizers(
    api: client.AppsV1Api,
    namespace: str,
    name: str,
) -> None:
    """Remove all finalizers from a Deployment so it can be deleted."""
    try:
        api.patch_namespaced_deployment(
            name=name,
            namespace=namespace,
            body={"metadata": {"finalizers": None}},
            _request_timeout=REQUEST_TIMEOUT,
        )
    except client.exceptions.ApiException:
        pass  # best-effort; the delete call will report the real error


async def _delete_one(
    api: client.AppsV1Api,
    namespace: str,
    name: str,
    force: bool,
    semaphore: asyncio.Semaphore,
) -> tuple[str, str | None]:
    """Delete a single deployment, returning ("ok", None) or ("error", reason)."""
    async with semaphore:
        try:
            if force:
                await asyncio.to_thread(
                    _patch_remove_finalizers,
                    api,
                    namespace,
                    name,
                )

            await asyncio.to_thread(
                api.delete_namespaced_deployment,
                name=name,
                namespace=namespace,
                body=_delete_options(),
                _request_timeout=REQUEST_TIMEOUT,
            )
            return ("ok", None)

        except client.exceptions.ApiException as e:
            if e.status == 404:
                return ("ok", None)  # already gone
            return ("error", f"{e.status}: {e.reason}")
        except Exception as e:
            return ("error", str(e))


async def cleanup(
    namespace: str | None,
    all_namespaces: bool,
    force: bool,
    concurrency: int,
):
    config.load_kube_config()
    api = client.AppsV1Api()

    if all_namespaces:
        deps = api.list_deployment_for_all_namespaces(
            label_selector=LABEL_SELECTOR,
            _request_timeout=REQUEST_TIMEOUT,
        )
    else:
        ns = namespace or "default"
        deps = api.list_namespaced_deployment(
            ns,
            label_selector=LABEL_SELECTOR,
            _request_timeout=REQUEST_TIMEOUT,
        )

    total = len(deps.items)
    if total == 0:
        print("No zombie deployments found.")
        return

    mode = " (force)" if force else ""
    print(
        f"Found {total} zombie deployment(s). "
        f"Deleting with concurrency={concurrency}{mode} …"
    )

    semaphore = asyncio.Semaphore(concurrency)
    tasks = [
        _delete_one(
            api,
            dep.metadata.namespace,
            dep.metadata.name,
            force,
            semaphore,
        )
        for dep in deps.items
    ]
    results = await asyncio.gather(*tasks)

    deleted = 0
    errors = 0
    for dep, (status, reason) in zip(deps.items, results):
        ns_name = dep.metadata.namespace
        name = dep.metadata.name
        if status == "ok":
            deleted += 1
            print(f"  ✓ {ns_name}/{name}")
        else:
            errors += 1
            print(f"  ✗ {ns_name}/{name} — {reason}", file=sys.stderr)

    print(f"\nDone. Deleted {deleted}, errors {errors}.")


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
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Patch out finalizers before deleting (unstuck stuck deployments)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        help="Max concurrent delete operations (default: 10)",
    )
    args = parser.parse_args()
    asyncio.run(
        cleanup(args.namespace, args.all_namespaces, args.force, args.concurrency)
    )
