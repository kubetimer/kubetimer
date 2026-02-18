#!/usr/bin/env python3
"""KubeTimer Pod Resource Monitor — tracks CPU and memory over time.

Usage:
    python tests/monitor_resources.py --duration 60
    python tests/monitor_resources.py --duration 3600 --interval 5

The script:
  1. Finds the kubetimer-operator pod in kubetimer-system.
  2. Polls the Metrics API (metrics-server) every --interval seconds.
  3. Records CPU (millicores) and memory (MiB) samples.
  4. On exit, prints a summary with min / avg / max / p95 and a
     simple ASCII chart of usage over time.

Requires: metrics-server installed in the cluster
  (minikube addons enable metrics-server)
"""

import argparse
import math
import statistics
import sys
import time
from dataclasses import dataclass, field

from kubernetes import client, config

# ── Constants ────────────────────────────────────────────────────────

NAMESPACE = "kubetimer-system"
LABEL_SELECTOR = "app=kubetimer-operator"
CONTAINER_NAME = "operator"


# ── Data ─────────────────────────────────────────────────────────────


@dataclass
class Sample:
    timestamp: float
    cpu_millicores: float
    memory_mib: float


@dataclass
class ResourceStats:
    samples: list[Sample] = field(default_factory=list)

    @property
    def cpu_values(self) -> list[float]:
        return [s.cpu_millicores for s in self.samples]

    @property
    def mem_values(self) -> list[float]:
        return [s.memory_mib for s in self.samples]


# ── Helpers ──────────────────────────────────────────────────────────


def _find_pod(v1: client.CoreV1Api) -> str:
    pods = v1.list_namespaced_pod(NAMESPACE, label_selector=LABEL_SELECTOR)
    for pod in pods.items:
        if pod.status.phase in ("Running", "Pending"):
            return pod.metadata.name
    print(
        f"ERROR  No running pod with label {LABEL_SELECTOR!r} "
        f"in namespace {NAMESPACE!r}",
        file=sys.stderr,
    )
    sys.exit(1)


def _parse_cpu(raw: str) -> float:
    """Convert K8s CPU string to millicores.

    Examples: '12345n' -> 0.012, '100m' -> 100, '1' -> 1000
    """
    if raw.endswith("n"):
        return int(raw[:-1]) / 1_000_000
    if raw.endswith("m"):
        return int(raw[:-1])
    return int(raw) * 1000


def _parse_memory(raw: str) -> float:
    """Convert K8s memory string to MiB.

    Examples: '65536Ki' -> 64, '128Mi' -> 128, '1Gi' -> 1024
    """
    if raw.endswith("Ki"):
        return int(raw[:-2]) / 1024
    if raw.endswith("Mi"):
        return int(raw[:-2])
    if raw.endswith("Gi"):
        return int(raw[:-2]) * 1024
    # raw bytes
    return int(raw) / (1024 * 1024)


def _percentile(data: list[float], pct: float) -> float:
    """Simple percentile without numpy."""
    if not data:
        return 0.0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * (pct / 100)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_data[int(k)]
    return sorted_data[int(f)] * (c - k) + sorted_data[int(c)] * (k - f)


def _ascii_chart(
    values: list[float],
    label: str,
    width: int = 50,
    height: int = 12,
):
    """Render a minimal ASCII bar chart."""
    if not values:
        return
    max_val = max(values) or 1
    print(f"\n  ── {label} over time ──")
    print(f"  (each row ≈ {len(values) / height:.1f} samples)")

    # Downsample to `height` rows
    chunk = max(1, len(values) // height)
    rows = []
    for i in range(0, len(values), chunk):
        chunk_vals = values[i : i + chunk]
        rows.append(statistics.mean(chunk_vals))

    for i, val in enumerate(rows):
        bar_len = int((val / max_val) * width)
        bar = "█" * bar_len
        print(f"  {i * chunk:>5} │ {bar} {val:.1f}")


def _print_summary(stats: ResourceStats, wall_seconds: float):
    print("\n" + "=" * 60)
    print("  KUBETIMER RESOURCE USAGE SUMMARY")
    print("=" * 60)

    n = len(stats.samples)
    print(f"\n  Samples collected  : {n}")
    print(f"  Wall clock         : {wall_seconds:.1f}s")

    if n == 0:
        print("\n  No metrics collected. " "Is metrics-server running?")
        print("  Try: minikube addons enable metrics-server")
        print("=" * 60)
        return

    cpu = stats.cpu_values
    mem = stats.mem_values

    print("\n  ── CPU (millicores) ──")
    print(f"    Min              : {min(cpu):.1f}m")
    print(f"    Avg              : {statistics.mean(cpu):.1f}m")
    print(f"    p95              : {_percentile(cpu, 95):.1f}m")
    print(f"    Max              : {max(cpu):.1f}m")

    print("\n  ── Memory (MiB) ──")
    print(f"    Min              : {min(mem):.1f}")
    print(f"    Avg              : {statistics.mean(mem):.1f}")
    print(f"    p95              : {_percentile(mem, 95):.1f}")
    print(f"    Max              : {max(mem):.1f}")

    _ascii_chart(cpu, "CPU (millicores)")
    _ascii_chart(mem, "Memory (MiB)")

    print("\n" + "=" * 60)


# ── Main ─────────────────────────────────────────────────────────────


def run(duration: int, interval: int):
    config.load_kube_config()
    v1 = client.CoreV1Api()
    custom = client.CustomObjectsApi()

    pod_name = _find_pod(v1)
    print(
        f"Monitoring pod {pod_name!r} in {NAMESPACE!r} "
        f"for {duration}s (poll every {interval}s) …"
    )

    stats = ResourceStats()
    start = time.monotonic()

    try:
        while (time.monotonic() - start) < duration:
            try:
                metrics = custom.get_namespaced_custom_object(
                    group="metrics.k8s.io",
                    version="v1beta1",
                    namespace=NAMESPACE,
                    plural="pods",
                    name=pod_name,
                )
            except client.exceptions.ApiException as e:
                if e.status == 404:
                    print(
                        "  WARN  metrics not available yet "
                        "(waiting for metrics-server) …"
                    )
                else:
                    print(f"  WARN  metrics API error: {e.status}")
                time.sleep(interval)
                continue

            for container in metrics.get("containers", []):
                if container["name"] != CONTAINER_NAME:
                    continue
                usage = container["usage"]
                sample = Sample(
                    timestamp=time.monotonic() - start,
                    cpu_millicores=_parse_cpu(usage["cpu"]),
                    memory_mib=_parse_memory(usage["memory"]),
                )
                stats.samples.append(sample)
                elapsed = time.monotonic() - start
                print(
                    f"  [{elapsed:>7.1f}s] "
                    f"cpu={sample.cpu_millicores:.1f}m  "
                    f"mem={sample.memory_mib:.1f}Mi"
                )

            time.sleep(interval)

    except KeyboardInterrupt:
        pass
    finally:
        wall = time.monotonic() - start
        _print_summary(stats, wall)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=("Monitor KubeTimer operator pod " "CPU/memory usage."),
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="How long to monitor, in seconds (default: 60)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=3,
        help=("Seconds between metric polls (default: 3)"),
    )
    args = parser.parse_args()
    run(args.duration, args.interval)
