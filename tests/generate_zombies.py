#!/usr/bin/env python3
"""KubeTimer Load Generator — creates zombie deployments with random TTLs.

Usage:
    python tests/generate_zombies.py --duration 60
    python tests/generate_zombies.py --duration 300 --rate 5 --namespace test
    python tests/generate_zombies.py --duration 10 --rate 20 --past-ratio 0.8
    python tests/generate_zombies.py --with-pods --max-cluster-pods 450

The script continuously creates "zombie" Deployments annotated with
kubetimer.io/ttl set to random datetimes.

  --past-ratio       controls the mix of already-expired vs future TTLs.
                     0.5 (default) = 50 % expired, 50 % 1–120 min in the future.
  --rate             deployments created per second (default: 2).
  --namespace        target namespace (default: "default").
  --cleanup          delete all zombie deployments on exit.
  --with-pods        create deployments with 1 replica (actual pods) instead of 0.
  --max-cluster-pods hard cap on total pods in cluster (default: 450, for Minikube).
"""

import argparse
import random
import string
import sys
import time
from datetime import datetime, timedelta
from urllib3.exceptions import MaxRetryError
from zoneinfo import ZoneInfo

from kubernetes import client, config

# ── Constants ────────────────────────────────────────────────────────

ANNOTATION_KEY = "kubetimer.io/ttl"
LABEL_KEY = "app"
LABEL_VALUE = "kubetimer-zombie"


# ── Helpers ──────────────────────────────────────────────────────────
tz = ZoneInfo("America/Sao_Paulo")


def _random_suffix(length: int = 6) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def _random_ttl(past_ratio: float) -> str:
    """Return a random ISO 8601 TTL string in America/Sao_Paulo.

    With probability `past_ratio` the TTL is in the past.
    Otherwise it's in the future.
    """
    now = datetime.now(tz)

    randomnumber = random.random()

    if randomnumber < past_ratio:
        delta = timedelta(seconds=random.randint(10, 300))
        dt = now - delta
    else:
        delta = timedelta(seconds=random.randint(60, 300))
        dt = now + delta

    return dt.isoformat()


def _build_deployment(
    name: str, namespace: str, ttl: str, *, with_pods: bool = False,
) -> dict:
    container = {
        "name": "pause",
        "image": "registry.k8s.io/pause:3.10",
    }
    if with_pods:
        # Minimal resource requests so the scheduler can pack pods tightly
        container["resources"] = {
            "requests": {"cpu": "1m", "memory": "4Mi"},
            "limits":   {"cpu": "2m", "memory": "8Mi"},
        }

    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {LABEL_KEY: LABEL_VALUE},
            "annotations": {ANNOTATION_KEY: ttl},
        },
        "spec": {
            "replicas": 1 if with_pods else 0,
            "selector": {
                "matchLabels": {LABEL_KEY: LABEL_VALUE, "instance": name},
            },
            "template": {
                "metadata": {
                    "labels": {
                        LABEL_KEY: LABEL_VALUE,
                        "instance": name,
                    },
                },
                "spec": {
                    "terminationGracePeriodSeconds": 0,
                    "containers": [container],
                },
            },
        },
    }


def _cleanup_zombies(api: client.AppsV1Api, namespace: str):
    """Delete every Deployment with our zombie label."""
    print("\n  Cleaning up zombie deployments …")
    deps = api.list_namespaced_deployment(
        namespace,
        label_selector=f"{LABEL_KEY}={LABEL_VALUE}",
    )
    deleted = 0
    for dep in deps.items:
        try:
            api.delete_namespaced_deployment(
                name=dep.metadata.name,
                namespace=namespace,
            )
            deleted += 1
        except client.exceptions.ApiException:
            pass
    print(f"  Deleted {deleted} remaining zombie(s).")


# ── Main ─────────────────────────────────────────────────────────────


def _count_zombies(api: client.AppsV1Api, namespace: str) -> int:
    """Count existing zombie deployments in the namespace."""
    for attempt in range(3):
        try:
            deps = api.list_namespaced_deployment(
                namespace, label_selector=f"{LABEL_KEY}={LABEL_VALUE}",
            )
            return len(deps.items)
        except (client.exceptions.ApiException, MaxRetryError) as e:
            if attempt < 2:
                print(f"  ⚠ API error counting zombies (attempt {attempt+1}): {e!r:.80s}, retrying …")
                time.sleep(5 * (attempt + 1))
            else:
                raise
    return 0  # unreachable


def _count_cluster_pods() -> int:
    """Count total pods across all namespaces."""
    v1 = client.CoreV1Api()
    for attempt in range(3):
        try:
            pods = v1.list_pod_for_all_namespaces(limit=1000)
            return len(pods.items)
        except (client.exceptions.ApiException, MaxRetryError) as e:
            if attempt < 2:
                print(f"  ⚠ API error counting pods (attempt {attempt+1}): {e!r:.80s}, retrying …")
                time.sleep(5 * (attempt + 1))
            else:
                raise
    return 0  # unreachable


def run(
    duration: int,
    rate: float,
    namespace: str,
    past_ratio: float,
    cleanup: bool,
    count: int | None = None,
    max_existing: int | None = None,
    *,
    with_pods: bool = False,
    max_cluster_pods: int = 450,
):
    config.load_kube_config()
    api = client.AppsV1Api()

    interval = 1.0 / rate
    created = 0
    errors = 0
    past_count = 0
    future_count = 0
    throttled = 0

    mode = "with-pods (replicas=1)" if with_pods else "zero-replica"
    count_msg = f", max={count}" if count is not None else ""
    cap_msg = f", cap={max_existing}" if max_existing is not None else ""
    pod_msg = f", max_cluster_pods={max_cluster_pods}" if with_pods else ""
    print(
        f"Generating {mode} zombie deployments in {namespace!r} "
        f"for {duration}s at ~{rate}/s  "
        f"(past_ratio={past_ratio:.0%}{count_msg}{cap_msg}{pod_msg}) …\n"
    )

    start = time.monotonic()

    # How often to check the cap (every N creations)
    _CAP_CHECK_INTERVAL = 25
    # For pod mode, check more frequently since pods take time to terminate
    _POD_CHECK_INTERVAL = 10 if with_pods else _CAP_CHECK_INTERVAL

    try:
        while (time.monotonic() - start) < duration and (
            count is None or created < count
        ):
            check_interval = _POD_CHECK_INTERVAL if with_pods else _CAP_CHECK_INTERVAL

            # Backpressure: deployment cap
            if max_existing is not None and created % check_interval == 0:
                while (time.monotonic() - start) < duration:
                    existing = _count_zombies(api, namespace)
                    if existing < max_existing:
                        break
                    throttled += 1
                    elapsed = time.monotonic() - start
                    print(
                        f"  [{elapsed:>7.1f}s] "
                        f"THROTTLED  {existing}/{max_existing} deployments exist, "
                        f"waiting for operator to delete …"
                    )
                    time.sleep(2)
                else:
                    break  # duration exceeded while waiting

            # Backpressure: cluster pod cap (only in pod mode)
            if with_pods and created % check_interval == 0:
                while (time.monotonic() - start) < duration:
                    total_pods = _count_cluster_pods()
                    if total_pods < max_cluster_pods:
                        break
                    throttled += 1
                    elapsed = time.monotonic() - start
                    print(
                        f"  [{elapsed:>7.1f}s] "
                        f"POD CAP  {total_pods}/{max_cluster_pods} cluster pods, "
                        f"waiting for pods to terminate …"
                    )
                    time.sleep(3)
                else:
                    break

            name = f"zombie-{_random_suffix()}"
            ttl = _random_ttl(past_ratio)
            body = _build_deployment(name, namespace, ttl, with_pods=with_pods)

            try:
                api.create_namespaced_deployment(namespace=namespace, body=body)
                created += 1

                # Track past vs future
                ttl_dt = datetime.fromisoformat(ttl).replace(tzinfo=tz)
                if ttl_dt <= datetime.now(tz):
                    past_count += 1
                else:
                    future_count += 1

                elapsed = time.monotonic() - start
                print(
                    f"  [{elapsed:>7.1f}s] "
                    f"CREATED {name}  ttl={ttl}  "
                    f"(total={created})"
                )

            except client.exceptions.ApiException as e:
                if e.status == 409:
                    pass  # name collision — very unlikely
                else:
                    errors += 1
                    print(
                        f"  ERROR  {e.status}: {e.reason}",
                        file=sys.stderr,
                    )
            except MaxRetryError as e:
                errors += 1
                print(
                    f"  ⚠ CONNECTION ERROR: {e!r:.120s}",
                    file=sys.stderr,
                )
                # Back off significantly — API server may be recovering
                print("  Backing off 15s to let API server recover …")
                time.sleep(15)

            time.sleep(interval)

    except KeyboardInterrupt:
        pass
    finally:
        wall = time.monotonic() - start
        _print_summary(wall, created, past_count, future_count, errors, throttled, count)
        if cleanup:
            _cleanup_zombies(api, namespace)


def _print_summary(
    wall: float,
    created: int,
    past_count: int,
    future_count: int,
    errors: int,
    throttled: int = 0,
    count: int | None = None,
):
    print("\n" + "=" * 60)
    print("  KUBETIMER LOAD GENERATOR SUMMARY")
    print("=" * 60)
    print(f"\n  Wall clock         : {wall:.1f}s")
    target = f" / {count}" if count is not None else ""
    print(f"  Deployments created: {created}{target}")
    print(f"    Already expired  : {past_count}")
    print(f"    Future TTL       : {future_count}")
    if wall > 0:
        print(f"  Effective rate     : {created / wall:.2f}/s")
    print(f"  Errors             : {errors}")
    if throttled > 0:
        print(f"  Throttle events    : {throttled} (cap reached, waited for operator)")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Generate zombie deployments with random "
            "TTL annotations for KubeTimer testing."
        ),
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="How long to generate, in seconds (default: 60)",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=2.0,
        help="Deployments per second (default: 2)",
    )
    parser.add_argument(
        "--namespace",
        type=str,
        default="default",
        help="Target namespace (default: 'default')",
    )
    parser.add_argument(
        "--past-ratio",
        type=float,
        default=0.5,
        help=("Fraction of TTLs set in the past, " "0.0–1.0 (default: 0.5)"),
    )
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="Maximum number of deployments to create (default: unlimited)",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        default=False,
        help="Delete all zombie deployments on exit",
    )
    parser.add_argument(
        "--max-existing",
        type=int,
        default=None,
        help=(
            "Maximum number of zombie deployments allowed to exist "
            "simultaneously. When the cap is reached, creation pauses "
            "until the operator deletes some. (default: unlimited)"
        ),
    )
    parser.add_argument(
        "--with-pods",
        action="store_true",
        default=False,
        help=(
            "Create deployments with 1 replica (actual running pods) "
            "instead of 0 replicas. Pods use the pause image (~1 MB). "
            "Combine with --max-cluster-pods to respect Minikube limits."
        ),
    )
    parser.add_argument(
        "--max-cluster-pods",
        type=int,
        default=450,
        help=(
            "Hard cap on total pods in the cluster (all namespaces). "
            "Only enforced when --with-pods is set. Minikube limit is 500, "
            "default 450 leaves headroom for system pods. (default: 450)"
        ),
    )
    args = parser.parse_args()
    run(
        args.duration,
        args.rate,
        args.namespace,
        args.past_ratio,
        args.cleanup,
        args.count,
        args.max_existing,
        with_pods=args.with_pods,
        max_cluster_pods=args.max_cluster_pods,
    )
