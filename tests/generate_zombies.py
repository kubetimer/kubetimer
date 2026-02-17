#!/usr/bin/env python3
"""KubeTimer Load Generator — creates zombie deployments with random TTLs.

Usage:
    python tests/generate_zombies.py --duration 60
    python tests/generate_zombies.py --duration 300 --rate 5 --namespace test
    python tests/generate_zombies.py --duration 10 --rate 20 --past-ratio 0.8

The script continuously creates zero-replica "zombie" Deployments
annotated with kubetimer.io/ttl set to random datetimes.

  --past-ratio  controls the mix of already-expired vs future TTLs.
                0.5 (default) = 50 % expired, 50 % 1–120 min in the future.
  --rate        deployments created per second (default: 2).
  --namespace   target namespace (default: "default").
  --cleanup     delete all zombies created by this script on exit.
"""

import argparse
import random
import string
import sys
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from kubernetes import client, config


# ── Constants ────────────────────────────────────────────────────────

ANNOTATION_KEY = "kubetimer.io/ttl"
LABEL_KEY = "app"
LABEL_VALUE = "kubetimer-zombie"


# ── Helpers ──────────────────────────────────────────────────────────


def _random_suffix(length: int = 6) -> str:
    return "".join(
        random.choices(string.ascii_lowercase + string.digits, k=length)
    )


def _random_ttl(past_ratio: float) -> str:
    """Return a random ISO 8601 TTL string in America/Sao_Paulo.

    With probability `past_ratio` the TTL is in the past
    (1–20 minutes ago). Otherwise it's in the future
    (1–120 minutes from now).
    """
    tz = ZoneInfo("America/Sao_Paulo")
    now = datetime.now(tz)

    if random.random() < past_ratio:
        delta = timedelta(
            seconds=random.randint(1, 20 * 60)
        )
        dt = now - delta
    else:
        delta = timedelta(
            seconds=random.randint(60, 7200)
        )
        dt = now + delta

    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _build_deployment(name: str, namespace: str, ttl: str) -> dict:
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
            "replicas": 0,
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
                    "containers": [
                        {
                            "name": "pause",
                            "image": "registry.k8s.io/pause:3.10",
                        }
                    ],
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


def run(
    duration: int,
    rate: float,
    namespace: str,
    past_ratio: float,
    cleanup: bool,
):
    config.load_kube_config()
    api = client.AppsV1Api()

    interval = 1.0 / rate
    created = 0
    errors = 0
    past_count = 0
    future_count = 0

    print(
        f"Generating zombie deployments in {namespace!r} "
        f"for {duration}s at ~{rate}/s  "
        f"(past_ratio={past_ratio:.0%}) …\n"
    )

    start = time.monotonic()

    try:
        while (time.monotonic() - start) < duration:
            name = f"zombie-{_random_suffix()}"
            ttl = _random_ttl(past_ratio)
            body = _build_deployment(name, namespace, ttl)

            try:
                api.create_namespaced_deployment(
                    namespace=namespace, body=body
                )
                created += 1

                # Track past vs future
                ttl_dt = datetime.strptime(
                    ttl, "%Y-%m-%dT%H:%M:%SZ"
                ).replace(tzinfo=timezone.utc)
                if ttl_dt <= datetime.now(timezone.utc):
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

            time.sleep(interval)

    except KeyboardInterrupt:
        pass
    finally:
        wall = time.monotonic() - start
        _print_summary(
            wall, created, past_count, future_count, errors
        )
        if cleanup:
            _cleanup_zombies(api, namespace)


def _print_summary(
    wall: float,
    created: int,
    past_count: int,
    future_count: int,
    errors: int,
):
    print("\n" + "=" * 60)
    print("  KUBETIMER LOAD GENERATOR SUMMARY")
    print("=" * 60)
    print(f"\n  Wall clock         : {wall:.1f}s")
    print(f"  Deployments created: {created}")
    print(f"    Already expired  : {past_count}")
    print(f"    Future TTL       : {future_count}")
    if wall > 0:
        print(f"  Effective rate     : {created / wall:.2f}/s")
    print(f"  Errors             : {errors}")
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
        help=(
            "Fraction of TTLs set in the past, "
            "0.0–1.0 (default: 0.5)"
        ),
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        default=False,
        help="Delete all zombie deployments on exit",
    )
    args = parser.parse_args()
    run(
        args.duration,
        args.rate,
        args.namespace,
        args.past_ratio,
        args.cleanup,
    )
