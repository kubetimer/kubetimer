#!/usr/bin/env python3
"""KubeTimer 1-Hour Experimental Validation Suite.

Runs a structured sequence of stress-test phases against a live operator,
snapshots Prometheus metrics at each phase boundary, and writes a final
CSV + JSON report that can be imported into a spreadsheet or Jupyter
notebook for analysis.

Supports two modes:
  - Zero-replica (default): deployments have 0 replicas (no actual pods).
  - Pod mode (--with-pods): deployments have 1 replica each using the
    pause image (~1 MB).  Rates and caps are automatically lowered to
    respect Minikube's ~500-pod hard limit.

Phases (total ~ 60 min):
  1. Warmup          — 5 min   steady 1 dep/s, 50 % past
  2. Sustained load  — 15 min  steady 3 dep/s, 60 % past
  3. Burst           — 2 min   burst  10 dep/s, 90 % past
  4. Cool-down       — 3 min   no new deployments, let scheduler drain
  5. Reconciliation  — repeat 5x (create batch of expired, restart pod, measure)
  6. Scale burst     — 5 min   burst  20 dep/s, 100 % past (pure deletes)
  7. Mixed future    — 10 min  steady 2 dep/s, 0 % past (all future TTLs)
  8. Final cool-down — 5 min   drain remaining scheduled jobs

In pod mode, rates and batch sizes are reduced automatically.

Usage:
    python tests/experiment.py
    python tests/experiment.py --with-pods
    python tests/experiment.py --namespace test-ns --prometheus http://localhost:9090
    python tests/experiment.py --reconciliation-rounds 10 --reconciliation-batch 1000
    python tests/experiment.py --skip-reconciliation
"""

import argparse
import csv
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from urllib3.exceptions import MaxRetryError
from zoneinfo import ZoneInfo

from kubernetes import client, config

# Import the zombie generator
sys.path.insert(0, str(Path(__file__).resolve().parent))
from generate_zombies import run as generate_zombies, _cleanup_zombies, _count_cluster_pods

try:
    import requests
except ImportError:
    print("ERROR: 'requests' is required.  pip install requests")
    sys.exit(1)


# ── Configuration ────────────────────────────────────────────────────

ANNOTATION_KEY = "kubetimer.io/ttl"
LABEL_KEY = "app"
LABEL_VALUE = "kubetimer-zombie"
OPERATOR_NAMESPACE = "kubetimer-system"
OPERATOR_DEPLOYMENT = "kubetimer-operator"

TZ = ZoneInfo("America/Sao_Paulo")


@dataclass
class PhaseResult:
    phase: str
    started_at: str = ""
    ended_at: str = ""
    duration_seconds: float = 0
    deployments_created: int = 0
    deployments_remaining: int = 0
    metrics_snapshot: dict = field(default_factory=dict)
    notes: str = ""


def _now_iso() -> str:
    """Return current time as ISO 8601 in the local timezone."""
    return datetime.now(TZ).isoformat(timespec="seconds")


# ── Prometheus helpers ───────────────────────────────────────────────


def prom_query(base_url: str, query: str) -> list[dict]:
    """Execute an instant PromQL query and return result list."""
    try:
        resp = requests.get(
            f"{base_url}/api/v1/query",
            params={"query": query},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if data["status"] != "success":
            return []
        return data["data"]["result"]
    except Exception as e:
        print(f"    ⚠ Prometheus query failed: {e}")
        return []


def prom_scalar(base_url: str, query: str) -> float | None:
    """Return the first scalar value from a PromQL query, or None."""
    results = prom_query(base_url, query)
    if results and len(results) > 0:
        try:
            return float(results[0]["value"][1])
        except (KeyError, IndexError, ValueError):
            return None
    return None


def snapshot_metrics(base_url: str) -> dict:
    """Capture a point-in-time snapshot of all KubeTimer metrics."""
    snapshot = {}

    # Deletion latency percentiles (over last 5m window)
    for pct, name in [(0.5, "p50"), (0.95, "p95"), (0.99, "p99"), (1.0, "max"), (0.0, "min")]:
        val = prom_scalar(
            base_url,
            f"histogram_quantile({pct}, sum(rate(kubetimer_delete_duration_seconds_bucket[5m])) by (le))",
        )
        snapshot[f"delete_latency_{name}_s"] = val

    # Total deletions by source
    for source in ["event_handler", "scheduler", "reconciler"]:
        val = prom_scalar(
            base_url,
            f'sum(kubetimer_deployments_deleted_total{{source="{source}"}})',
        )
        snapshot[f"total_deletions_{source}"] = val

    # Total deletions by outcome
    for outcome in ["deleted", "dry_run", "error", "already_gone"]:
        val = prom_scalar(
            base_url,
            f'sum(kubetimer_deployments_deleted_total{{outcome="{outcome}"}})',
        )
        snapshot[f"total_outcome_{outcome}"] = val

    # Concurrent events (current)
    for etype in ["create", "update", "delete"]:
        val = prom_scalar(
            base_url,
            f'kubetimer_concurrent_events{{event_type="{etype}"}}',
        )
        snapshot[f"concurrent_{etype}"] = val

    # Scheduling counters
    snapshot["jobs_scheduled"] = prom_scalar(base_url, "kubetimer_jobs_scheduled_total")
    snapshot["jobs_cancelled"] = prom_scalar(base_url, "kubetimer_jobs_cancelled_total")

    # Startup / reconciliation
    snapshot["startup_duration_s"] = prom_scalar(base_url, "kubetimer_startup_duration_seconds_sum")
    snapshot["reconcile_duration_s"] = prom_scalar(base_url, "kubetimer_reconcile_duration_seconds_sum")

    for status in ["expired", "scheduled", "error"]:
        val = prom_scalar(
            base_url,
            f'kubetimer_reconcile_deployments_total{{status="{status}"}}',
        )
        snapshot[f"reconcile_{status}"] = val

    # Pod resource usage (from cAdvisor)
    snapshot["cpu_cores"] = prom_scalar(
        base_url,
        f'sum(rate(container_cpu_usage_seconds_total{{namespace="{OPERATOR_NAMESPACE}", pod=~"{OPERATOR_DEPLOYMENT}-.*"}}[1m]))',
    )
    snapshot["memory_bytes"] = prom_scalar(
        base_url,
        f'sum(container_memory_working_set_bytes{{namespace="{OPERATOR_NAMESPACE}", pod=~"{OPERATOR_DEPLOYMENT}-.*"}})',
    )

    # Zombie pod counts (only meaningful in pod mode but always safe to query)
    snapshot["zombie_pods_running"] = prom_scalar(
        base_url,
        'count(kube_pod_status_phase{namespace="default", pod=~"zombie-.*", phase="Running"} == 1) or vector(0)',
    )
    snapshot["zombie_pods_pending"] = prom_scalar(
        base_url,
        'count(kube_pod_status_phase{namespace="default", pod=~"zombie-.*", phase="Pending"} == 1) or vector(0)',
    )
    snapshot["cluster_pods_total"] = prom_scalar(
        base_url,
        'count(kube_pod_info)',
    )

    return snapshot


# ── Kubernetes helpers ───────────────────────────────────────────────


def count_zombies(api: client.AppsV1Api, namespace: str) -> int:
    for attempt in range(3):
        try:
            deps = api.list_namespaced_deployment(
                namespace, label_selector=f"{LABEL_KEY}={LABEL_VALUE}"
            )
            return len(deps.items)
        except (client.exceptions.ApiException, MaxRetryError) as e:
            if attempt < 2:
                print(f"    ⚠ API error (attempt {attempt+1}): {e!r:.80s}, retrying …")
                time.sleep(5 * (attempt + 1))
            else:
                raise
    return 0


def wait_for_zero_zombies(api: client.AppsV1Api, namespace: str, timeout: int = 300) -> float:
    """Wait until all zombie deployments are gone. Returns elapsed seconds."""
    start = time.monotonic()
    while (time.monotonic() - start) < timeout:
        try:
            remaining = count_zombies(api, namespace)
        except (client.exceptions.ApiException, MaxRetryError):
            time.sleep(5)
            continue
        if remaining == 0:
            return time.monotonic() - start
        time.sleep(1)
    return time.monotonic() - start


def restart_operator():
    """Restart the operator pod via rollout restart."""
    print("    ↻ Restarting operator pod …")
    subprocess.run(
        [
            "kubectl", "rollout", "restart", "deployment",
            OPERATOR_DEPLOYMENT, "-n", OPERATOR_NAMESPACE,
        ],
        check=True,
        capture_output=True,
    )
    # Wait for the new pod to be ready
    subprocess.run(
        [
            "kubectl", "rollout", "status", "deployment",
            OPERATOR_DEPLOYMENT, "-n", OPERATOR_NAMESPACE,
            "--timeout=120s",
        ],
        check=True,
        capture_output=True,
    )
    print("    ✓ Operator pod restarted and ready.")
    time.sleep(3)  # Give Kopf a moment to finish startup


# ── Phase runners ────────────────────────────────────────────────────


def run_load_phase(
    name: str,
    duration: int,
    rate: float,
    past_ratio: float,
    namespace: str,
    api: client.AppsV1Api,
    prom_url: str,
    max_existing: int | None = None,
    *,
    with_pods: bool = False,
    max_cluster_pods: int = 450,
) -> PhaseResult:
    """Run a generate_zombies phase and capture metrics."""
    cap_msg = f"  max_existing={max_existing}" if max_existing else ""
    pod_msg = f"  max_cluster_pods={max_cluster_pods}" if with_pods else ""
    print(f"\n{'='*60}")
    print(f"  PHASE: {name}")
    mode = " [POD MODE]" if with_pods else ""
    print(f"  duration={duration}s  rate={rate}/s  past_ratio={past_ratio:.0%}{cap_msg}{pod_msg}{mode}")
    print(f"{'='*60}")

    started_at = _now_iso()
    print(f"  ▶ Started at {started_at}")
    start = time.monotonic()
    generate_zombies(
        duration=duration,
        rate=rate,
        namespace=namespace,
        past_ratio=past_ratio,
        cleanup=False,
        count=None,
        max_existing=max_existing,
        with_pods=with_pods,
        max_cluster_pods=max_cluster_pods,
    )
    elapsed = time.monotonic() - start

    # Let the operator process for a few seconds
    time.sleep(5)

    remaining = count_zombies(api, namespace)
    metrics = snapshot_metrics(prom_url)

    ended_at = _now_iso()
    print(f"  ■ Ended at {ended_at}")

    return PhaseResult(
        phase=name,
        started_at=started_at,
        ended_at=ended_at,
        duration_seconds=round(elapsed, 2),
        deployments_created=int(duration * rate),
        deployments_remaining=remaining,
        metrics_snapshot=metrics,
    )


def run_cooldown_phase(
    name: str,
    duration: int,
    namespace: str,
    api: client.AppsV1Api,
    prom_url: str,
) -> PhaseResult:
    """Wait for a cool-down period, letting the operator drain work."""
    print(f"\n{'='*60}")
    print(f"  PHASE: {name}  (cool-down {duration}s)")
    print(f"{'='*60}")

    started_at = _now_iso()
    print(f"  ▶ Started at {started_at}")
    start = time.monotonic()
    for i in range(0, duration, 10):
        remaining = count_zombies(api, namespace)
        print(f"    [{i:>4}s] remaining zombies: {remaining}")
        if remaining == 0:
            break
        time.sleep(min(10, duration - i))

    elapsed = time.monotonic() - start
    remaining = count_zombies(api, namespace)
    metrics = snapshot_metrics(prom_url)

    ended_at = _now_iso()
    print(f"  ■ Ended at {ended_at}")

    return PhaseResult(
        phase=name,
        started_at=started_at,
        ended_at=ended_at,
        duration_seconds=round(elapsed, 2),
        deployments_remaining=remaining,
        metrics_snapshot=metrics,
    )


def run_reconciliation_phase(
    namespace: str,
    api: client.AppsV1Api,
    prom_url: str,
    rounds: int = 5,
    batch_size: int = 500,
    *,
    with_pods: bool = False,
    max_cluster_pods: int = 450,
) -> list[PhaseResult]:
    """Create expired deployments, restart operator, measure reconciliation."""
    results = []

    print(f"\n{'='*60}")
    print(f"  PHASE: Reconciliation Stress Test")
    print(f"  {rounds} rounds × {batch_size} expired deployments")
    print(f"{'='*60}")

    for i in range(1, rounds + 1):
        print(f"\n  ── Round {i}/{rounds} ──")

        round_started_at = _now_iso()
        print(f"  ▶ Started at {round_started_at}")

        # Clean slate
        _cleanup_zombies(api, namespace)
        time.sleep(3)

        # Create batch of expired deployments
        print(f"    Creating {batch_size} expired deployments …")
        generate_zombies(
            duration=600,  # generous upper bound
            rate=50,       # fast creation
            namespace=namespace,
            past_ratio=1.0,  # all expired
            cleanup=False,
            count=batch_size,
            with_pods=with_pods,
            max_cluster_pods=max_cluster_pods,
        )
        pre_count = count_zombies(api, namespace)
        print(f"    ✓ {pre_count} deployments exist before restart.")

        # Restart operator
        restart_operator()

        # Measure how long the operator takes to reconcile
        print(f"    ⏱ Timing reconciliation …")
        recon_time = wait_for_zero_zombies(api, namespace, timeout=300)
        post_count = count_zombies(api, namespace)
        metrics = snapshot_metrics(prom_url)

        round_ended_at = _now_iso()
        print(f"  ■ Ended at {round_ended_at}")

        result = PhaseResult(
            phase=f"reconciliation_round_{i}",
            started_at=round_started_at,
            ended_at=round_ended_at,
            duration_seconds=round(recon_time, 2),
            deployments_created=pre_count,
            deployments_remaining=post_count,
            metrics_snapshot=metrics,
            notes=f"batch={batch_size}, recon_time={recon_time:.2f}s",
        )
        results.append(result)

        print(
            f"    ✓ Round {i}: reconciled {pre_count} deployments "
            f"in {recon_time:.2f}s "
            f"({pre_count / max(recon_time, 0.01):.1f} dep/s)"
        )

    return results


# ── Report generation ────────────────────────────────────────────────


def write_report(results: list[PhaseResult], output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # JSON (full detail)
    json_path = os.path.join(output_dir, f"experiment_{ts}.json")
    with open(json_path, "w") as f:
        json.dump([asdict(r) for r in results], f, indent=2, default=str)
    print(f"\n  📄 JSON report: {json_path}")

    # CSV (flat summary)
    csv_path = os.path.join(output_dir, f"experiment_{ts}.csv")
    # Collect all metric keys across all phases
    all_metric_keys = set()
    for r in results:
        all_metric_keys.update(r.metrics_snapshot.keys())
    all_metric_keys = sorted(all_metric_keys)

    fieldnames = [
        "phase", "started_at", "ended_at", "duration_seconds",
        "deployments_created", "deployments_remaining", "notes",
    ] + all_metric_keys

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            row = {
                "phase": r.phase,
                "started_at": r.started_at,
                "ended_at": r.ended_at,
                "duration_seconds": r.duration_seconds,
                "deployments_created": r.deployments_created,
                "deployments_remaining": r.deployments_remaining,
                "notes": r.notes,
            }
            row.update(r.metrics_snapshot)
            writer.writerow(row)
    print(f"  📊 CSV report:  {csv_path}")

    # Print summary table
    print(f"\n{'='*100}")
    print("  EXPERIMENT SUMMARY")
    print(f"{'='*100}")
    print(f"  {'Phase':<25} {'Started':<22} {'Ended':<22} {'Duration':>10} {'Created':>8} {'Remain':>8}")
    print(f"  {'-'*25} {'-'*22} {'-'*22} {'-'*10} {'-'*8} {'-'*8}")
    for r in results:
        started = r.started_at[11:19] if r.started_at else ""
        ended = r.ended_at[11:19] if r.ended_at else ""
        print(
            f"  {r.phase:<25} {r.started_at:<22} {r.ended_at:<22} "
            f"{r.duration_seconds:>9.1f}s {r.deployments_created:>8} {r.deployments_remaining:>8}"
        )
    print(f"{'='*100}")


# ── Main ─────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="KubeTimer 1-hour experiment suite")
    parser.add_argument("--namespace", default="default", help="Target namespace")
    parser.add_argument("--prometheus", default="http://localhost:9090", help="Prometheus URL")
    parser.add_argument("--output-dir", default="tests/results", help="Report output directory")
    parser.add_argument("--skip-reconciliation", action="store_true", help="Skip reconciliation rounds")
    parser.add_argument("--reconciliation-rounds", type=int, default=5, help="Number of reconciliation rounds (default: 5)")
    parser.add_argument("--reconciliation-batch", type=int, default=500, help="Deployments per reconciliation round (default: 500)")
    parser.add_argument("--max-existing", type=int, default=None, help="Max simultaneous zombie deployments in cluster (auto-set based on mode)")
    parser.add_argument("--with-pods", action="store_true", default=False, help="Create deployments with 1 replica (actual pods, pause image)")
    parser.add_argument("--max-cluster-pods", type=int, default=450, help="Hard cap on total cluster pods (default: 450, safe for Minikube)")
    args = parser.parse_args()

    # Auto-set max_existing based on mode if not explicitly provided
    if args.max_existing is None:
        args.max_existing = 150 if args.with_pods else 3000

    config.load_kube_config()
    api = client.AppsV1Api()

    # Verify Prometheus is reachable
    try:
        resp = requests.get(f"{args.prometheus}/-/healthy", timeout=5)
        resp.raise_for_status()
        print(f"✓ Prometheus healthy at {args.prometheus}")
    except Exception as e:
        print(f"✗ Cannot reach Prometheus at {args.prometheus}: {e}")
        print("  Start port-forward first:")
        print("  kubectl port-forward -n monitoring svc/observability-kube-prometh-prometheus 9090:9090")
        sys.exit(1)

    # Verify operator is running
    pods = client.CoreV1Api().list_namespaced_pod(
        OPERATOR_NAMESPACE, label_selector=f"app={OPERATOR_DEPLOYMENT}",
    )
    if not pods.items:
        print(f"✗ No operator pods found in {OPERATOR_NAMESPACE}")
        sys.exit(1)
    print(f"✓ Operator pod: {pods.items[0].metadata.name}")

    # Clean slate
    print("\nCleaning up any leftover zombies …")
    _cleanup_zombies(api, args.namespace)
    time.sleep(3)

    results: list[PhaseResult] = []

    max_cap = args.max_existing
    with_pods = args.with_pods
    max_cluster_pods = args.max_cluster_pods

    mode = "POD MODE (replicas=1)" if with_pods else "ZERO-REPLICA MODE (replicas=0)"
    print(f"\n{'='*60}")
    print(f"  MODE: {mode}")
    print(f"  Max simultaneous deployments: {max_cap}")
    if with_pods:
        print(f"  Max cluster pods:            {max_cluster_pods}")
    print(f"{'='*60}")

    # In pod mode, reduce rates to avoid overwhelming Minikube's
    # single-node API server.  Each deployment with 1 replica generates
    # watch events + ReplicaSet + Pod scheduling + volume mount — much
    # heavier than zero-replica mode.
    if with_pods:
        phase_config = {
            "warmup":      {"duration": 300, "rate": 0.5, "past_ratio": 0.5},
            "sustained":   {"duration": 900, "rate": 1,   "past_ratio": 0.6},
            "burst":       {"duration": 120, "rate": 3,   "past_ratio": 0.9},
            "cooldown":    {"duration": 180},
            "recon_batch": min(args.reconciliation_batch, 100),
            "scale_burst": {"duration": 300, "rate": 4,   "past_ratio": 1.0},
            "mixed":       {"duration": 600, "rate": 0.5, "past_ratio": 0.0},
            "final_cool":  {"duration": 300},
        }
    else:
        phase_config = {
            "warmup":      {"duration": 300, "rate": 1,   "past_ratio": 0.5},
            "sustained":   {"duration": 900, "rate": 3,   "past_ratio": 0.6},
            "burst":       {"duration": 120, "rate": 10,  "past_ratio": 0.9},
            "cooldown":    {"duration": 180},
            "recon_batch": args.reconciliation_batch,
            "scale_burst": {"duration": 300, "rate": 20,  "past_ratio": 1.0},
            "mixed":       {"duration": 600, "rate": 2,   "past_ratio": 0.0},
            "final_cool":  {"duration": 300},
        }

    # ── Phase 1: Warmup ──────────────────────────────────────────
    p = phase_config["warmup"]
    results.append(run_load_phase(
        name="1_warmup",
        duration=p["duration"], rate=p["rate"], past_ratio=p["past_ratio"],
        namespace=args.namespace, api=api, prom_url=args.prometheus,
        max_existing=max_cap, with_pods=with_pods, max_cluster_pods=max_cluster_pods,
    ))

    # ── Phase 2: Sustained load ──────────────────────────────────
    p = phase_config["sustained"]
    results.append(run_load_phase(
        name="2_sustained_load",
        duration=p["duration"], rate=p["rate"], past_ratio=p["past_ratio"],
        namespace=args.namespace, api=api, prom_url=args.prometheus,
        max_existing=max_cap, with_pods=with_pods, max_cluster_pods=max_cluster_pods,
    ))

    # ── Phase 3: Burst ───────────────────────────────────────────
    p = phase_config["burst"]
    results.append(run_load_phase(
        name="3_burst",
        duration=p["duration"], rate=p["rate"], past_ratio=p["past_ratio"],
        namespace=args.namespace, api=api, prom_url=args.prometheus,
        max_existing=max_cap, with_pods=with_pods, max_cluster_pods=max_cluster_pods,
    ))

    # ── Phase 4: Cool-down ───────────────────────────────────────
    results.append(run_cooldown_phase(
        name="4_cooldown",
        duration=phase_config["cooldown"]["duration"],
        namespace=args.namespace, api=api, prom_url=args.prometheus,
    ))

    _cleanup_zombies(api, args.namespace)
    time.sleep(5)

    # ── Phase 5: Reconciliation stress ───────────────────────────
    if not args.skip_reconciliation:
        recon_results = run_reconciliation_phase(
            namespace=args.namespace, api=api, prom_url=args.prometheus,
            rounds=args.reconciliation_rounds,
            batch_size=phase_config["recon_batch"],
            with_pods=with_pods,
            max_cluster_pods=max_cluster_pods,
        )
        results.extend(recon_results)

    _cleanup_zombies(api, args.namespace)
    time.sleep(5)

    # ── Phase 6: Scale burst ─────────────────────────────────────
    p = phase_config["scale_burst"]
    results.append(run_load_phase(
        name="6_scale_burst",
        duration=p["duration"], rate=p["rate"], past_ratio=p["past_ratio"],
        namespace=args.namespace, api=api, prom_url=args.prometheus,
        max_existing=max_cap, with_pods=with_pods, max_cluster_pods=max_cluster_pods,
    ))

    # ── Phase 7: Mixed future ────────────────────────────────────
    p = phase_config["mixed"]
    results.append(run_load_phase(
        name="7_mixed_future",
        duration=p["duration"], rate=p["rate"], past_ratio=p["past_ratio"],
        namespace=args.namespace, api=api, prom_url=args.prometheus,
        max_existing=max_cap, with_pods=with_pods, max_cluster_pods=max_cluster_pods,
    ))

    # ── Phase 8: Final cool-down ─────────────────────────────────
    results.append(run_cooldown_phase(
        name="8_final_cooldown",
        duration=phase_config["final_cool"]["duration"],
        namespace=args.namespace, api=api, prom_url=args.prometheus,
    ))

    # Final cleanup
    _cleanup_zombies(api, args.namespace)

    # Write report
    write_report(results, args.output_dir)

    print("\n✅ Experiment complete!")


if __name__ == "__main__":
    main()
