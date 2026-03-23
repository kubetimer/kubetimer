#!/usr/bin/env python3
"""Export KubeTimer metrics from Prometheus to CSV/JSON for offline analysis.

Queries Prometheus for a time range and writes the raw time-series data
so you can import it into a spreadsheet, Jupyter, or any analysis tool.

Usage:
    # Export the last 1 hour at 15s resolution
    python tests/export_metrics.py

    # Export a specific range
    python tests/export_metrics.py --start "2026-03-11T20:00:00" --end "2026-03-11T21:00:00"

    # Custom step and output
    python tests/export_metrics.py --step 30 --output-dir tests/results/run1
"""

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timedelta, timezone

try:
    import requests
except ImportError:
    print("ERROR: 'requests' is required.  pip install requests")
    sys.exit(1)


# ── Queries to export ────────────────────────────────────────────────

QUERIES = {
    # Deletion latency percentiles
    "delete_latency_p50": 'histogram_quantile(0.5, sum(rate(kubetimer_delete_duration_seconds_bucket[1m])) by (le))',
    "delete_latency_p95": 'histogram_quantile(0.95, sum(rate(kubetimer_delete_duration_seconds_bucket[1m])) by (le))',
    "delete_latency_p99": 'histogram_quantile(0.99, sum(rate(kubetimer_delete_duration_seconds_bucket[1m])) by (le))',
    "delete_latency_max": 'histogram_quantile(1.0, sum(rate(kubetimer_delete_duration_seconds_bucket[1m])) by (le))',

    # Deletion rates by source
    "deletions_per_sec_event_handler": 'sum(rate(kubetimer_deployments_deleted_total{source="event_handler"}[1m]))',
    "deletions_per_sec_scheduler": 'sum(rate(kubetimer_deployments_deleted_total{source="scheduler"}[1m]))',
    "deletions_per_sec_reconciler": 'sum(rate(kubetimer_deployments_deleted_total{source="reconciler"}[1m]))',

    # Deletion rates by outcome
    "deletions_per_sec_deleted": 'sum(rate(kubetimer_deployments_deleted_total{outcome="deleted"}[1m]))',
    "deletions_per_sec_error": 'sum(rate(kubetimer_deployments_deleted_total{outcome="error"}[1m]))',

    # Total counters (cumulative)
    "total_deletions": "sum(kubetimer_deployments_deleted_total)",
    "total_jobs_scheduled": "kubetimer_jobs_scheduled_total",
    "total_jobs_cancelled": "kubetimer_jobs_cancelled_total",

    # Concurrent events
    "concurrent_create": 'kubetimer_concurrent_events{event_type="create"}',
    "concurrent_update": 'kubetimer_concurrent_events{event_type="update"}',
    "concurrent_delete": 'kubetimer_concurrent_events{event_type="delete"}',

    # Handler duration percentiles
    "handler_p50_create": 'histogram_quantile(0.5, sum(rate(kubetimer_event_handler_duration_seconds_bucket{event_type="create"}[1m])) by (le))',
    "handler_p95_create": 'histogram_quantile(0.95, sum(rate(kubetimer_event_handler_duration_seconds_bucket{event_type="create"}[1m])) by (le))',

    # Pod resources
    "cpu_cores": 'sum(rate(container_cpu_usage_seconds_total{namespace="kubetimer-system", pod=~"kubetimer-operator-.*"}[1m]))',
    "memory_bytes": 'sum(container_memory_working_set_bytes{namespace="kubetimer-system", pod=~"kubetimer-operator-.*"})',
    "memory_rss_bytes": 'sum(container_memory_rss{namespace="kubetimer-system", pod=~"kubetimer-operator-.*"})',

    # Startup / reconciliation (single values, but exported as time-series)
    "startup_duration_s": "kubetimer_startup_duration_seconds_sum",
    "reconcile_duration_s": "kubetimer_reconcile_duration_seconds_sum",

    # Zombie pod counts (populated when --with-pods mode is used)
    "zombie_pods_running": 'count(kube_pod_status_phase{namespace="default", pod=~"zombie-.*", phase="Running"} == 1) or vector(0)',
    "zombie_pods_pending": 'count(kube_pod_status_phase{namespace="default", pod=~"zombie-.*", phase="Pending"} == 1) or vector(0)',
    "cluster_pods_total": "count(kube_pod_info)",
}


def query_range(prom_url: str, query: str, start: float, end: float, step: int) -> list:
    """Execute a range query and return [[timestamp, value], ...]."""
    try:
        resp = requests.get(
            f"{prom_url}/api/v1/query_range",
            params={
                "query": query,
                "start": start,
                "end": end,
                "step": step,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if data["status"] != "success":
            return []
        results = data["data"]["result"]
        if results:
            return results[0]["values"]  # [[ts, val], ...]
        return []
    except Exception as e:
        print(f"  ⚠ Query failed ({query[:60]}…): {e}")
        return []


def export_metrics(prom_url: str, start: float, end: float, step: int, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    ts_str = datetime.now().strftime("%Y%m%d_%H%M%S")

    print(f"Exporting metrics from Prometheus …")
    print(f"  Range: {datetime.fromtimestamp(start, tz=timezone.utc).isoformat()} → "
          f"{datetime.fromtimestamp(end, tz=timezone.utc).isoformat()}")
    print(f"  Step:  {step}s")
    print(f"  Queries: {len(QUERIES)}")

    # Build a unified time-series table
    # Collect all timestamps and values per metric
    all_data: dict[str, dict[float, float]] = {}
    all_timestamps: set[float] = set()

    for name, query in QUERIES.items():
        print(f"  Querying: {name} …")
        values = query_range(prom_url, query, start, end, step)
        ts_map = {}
        for ts, val in values:
            try:
                ts_map[float(ts)] = float(val) if val != "NaN" else None
            except (ValueError, TypeError):
                ts_map[float(ts)] = None
            all_timestamps.add(float(ts))
        all_data[name] = ts_map

    if not all_timestamps:
        print("\n  ⚠ No data returned for any metric. Is the operator running?")
        return

    sorted_ts = sorted(all_timestamps)
    metric_names = sorted(all_data.keys())

    # Write CSV
    csv_path = os.path.join(output_dir, f"metrics_export_{ts_str}.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "datetime_utc"] + metric_names)
        for ts in sorted_ts:
            dt = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
            row = [ts, dt]
            for name in metric_names:
                val = all_data[name].get(ts)
                row.append(val if val is not None else "")
            writer.writerow(row)
    print(f"\n  📊 CSV:  {csv_path}  ({len(sorted_ts)} rows × {len(metric_names)} metrics)")

    # Write JSON (structured per-metric)
    json_path = os.path.join(output_dir, f"metrics_export_{ts_str}.json")
    json_output = {
        "meta": {
            "start": datetime.fromtimestamp(start, tz=timezone.utc).isoformat(),
            "end": datetime.fromtimestamp(end, tz=timezone.utc).isoformat(),
            "step_seconds": step,
            "total_points": len(sorted_ts),
            "metrics_count": len(metric_names),
            "exported_at": datetime.now(tz=timezone.utc).isoformat(),
        },
        "metrics": {},
    }
    for name in metric_names:
        json_output["metrics"][name] = [
            {"timestamp": ts, "value": all_data[name].get(ts)}
            for ts in sorted_ts
        ]
    with open(json_path, "w") as f:
        json.dump(json_output, f, indent=2, default=str)
    print(f"  📄 JSON: {json_path}")

    # Print quick stats
    print(f"\n  {'Metric':<40} {'Points':>8} {'Min':>12} {'Max':>12} {'Avg':>12}")
    print(f"  {'-'*40} {'-'*8} {'-'*12} {'-'*12} {'-'*12}")
    for name in metric_names:
        vals = [v for v in all_data[name].values() if v is not None]
        if vals:
            print(
                f"  {name:<40} {len(vals):>8} "
                f"{min(vals):>12.4f} {max(vals):>12.4f} "
                f"{sum(vals)/len(vals):>12.4f}"
            )
        else:
            print(f"  {name:<40} {'(no data)':>8}")


def main():
    parser = argparse.ArgumentParser(description="Export KubeTimer metrics from Prometheus")
    parser.add_argument("--prometheus", default="http://localhost:9090", help="Prometheus URL")
    parser.add_argument("--start", default=None, help="Start time (ISO 8601 or epoch). Default: 1h ago")
    parser.add_argument("--end", default=None, help="End time (ISO 8601 or epoch). Default: now")
    parser.add_argument("--step", type=int, default=15, help="Query step in seconds (default: 15)")
    parser.add_argument("--output-dir", default="tests/results", help="Output directory")
    args = parser.parse_args()

    now = datetime.now(tz=timezone.utc)
    if args.end:
        end_dt = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
    else:
        end_dt = now
    if args.start:
        start_dt = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    else:
        start_dt = end_dt - timedelta(hours=1)

    # Verify Prometheus
    try:
        resp = requests.get(f"{args.prometheus}/-/healthy", timeout=5)
        resp.raise_for_status()
        print(f"✓ Prometheus healthy at {args.prometheus}")
    except Exception as e:
        print(f"✗ Cannot reach Prometheus at {args.prometheus}: {e}")
        sys.exit(1)

    export_metrics(
        prom_url=args.prometheus,
        start=start_dt.timestamp(),
        end=end_dt.timestamp(),
        step=args.step,
        output_dir=args.output_dir,
    )

    print("\n✅ Export complete!")


if __name__ == "__main__":
    main()
