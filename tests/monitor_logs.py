#!/usr/bin/env python3
"""KubeTimer Pod Log Monitor — streams logs and prints a performance summary.

Usage:
    python tests/monitor_logs.py --duration 60          # run for 60 seconds
    python tests/monitor_logs.py --duration 3600        # run for 1 hour
    python tests/monitor_logs.py --duration 10 --json   # JSON-formatted logs

The script:
  1. Finds the kubetimer-operator pod in kubetimer-system.
  2. Streams its logs in real time, parsing structured events.
  3. On exit (duration elapsed or Ctrl+C), prints a summary:
     - Event counts by type
     - Deletion throughput (deletes / second)
     - Reconciliation duration
     - Errors / warnings
"""

import argparse
import re
import sys
import time
from collections import Counter
from datetime import datetime

from kubernetes import client, config, watch


# ── Constants ────────────────────────────────────────────────────────

NAMESPACE = "kubetimer-system"
LABEL_SELECTOR = "app=kubetimer-operator"

EVENT_PATTERN = re.compile(
    r"\[(\w+)\s*\]\s+([\w]+)"  # [level] event_name
)
DURATION_PATTERN = re.compile(
    r"duration_seconds=([\d.]+)"
)
DELETED_PATTERN = re.compile(
    r"deployment_deleted|expired_deleted=(\d+)"
)
RECONCILE_PATTERN = re.compile(
    r"reconcile_complete"
)
TIMESTAMP_PATTERN = re.compile(
    r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)"
)
KV_PATTERN = re.compile(
    r"(\w+)=('[^']*'|\"[^\"]*\"|\S+)"
)


# ── Helpers ──────────────────────────────────────────────────────────


def _find_pod(v1: client.CoreV1Api) -> str:
    """Return the name of the first running kubetimer-operator pod."""
    pods = v1.list_namespaced_pod(
        NAMESPACE, label_selector=LABEL_SELECTOR
    )
    for pod in pods.items:
        if pod.status.phase in ("Running", "Pending"):
            return pod.metadata.name
    print(
        f"ERROR  No running pod with label {LABEL_SELECTOR!r} "
        f"in namespace {NAMESPACE!r}",
        file=sys.stderr,
    )
    sys.exit(1)


def _parse_line(line: str, stats: dict):
    """Extract structured info from a single log line."""
    stats["total_lines"] += 1

    # Timestamp
    ts_match = TIMESTAMP_PATTERN.match(line)
    ts_str = ts_match.group(1) if ts_match else None
    if ts_str:
        try:
            ts = datetime.fromisoformat(
                ts_str.replace("Z", "+00:00")
            )
            if stats["first_ts"] is None:
                stats["first_ts"] = ts
            stats["last_ts"] = ts
        except ValueError:
            pass

    # Event type + level
    ev_match = EVENT_PATTERN.search(line)
    if ev_match:
        level = ev_match.group(1).lower()
        event = ev_match.group(2)
        stats["events"][event] += 1
        stats["levels"][level] += 1

    # Deletion events
    if "deployment_deleted" in line:
        stats["deletions"] += 1

    # Reconciliation duration
    if RECONCILE_PATTERN.search(line):
        dur = DURATION_PATTERN.search(line)
        if dur:
            stats["reconcile_durations"].append(
                float(dur.group(1))
            )

    # Bulk-delete expired count
    expired_match = re.search(
        r"expired_deleted=(\d+)", line
    )
    if expired_match:
        stats["bulk_expired"] += int(expired_match.group(1))

    # Errors / warnings — group by event name
    if "[error" in line.lower():
        stats["error_lines"].append(line.rstrip())
        ev = ev_match.group(2) if ev_match else "unknown"
        stats["errors_by_event"][ev] += 1
        if ev not in stats["error_samples"]:
            kvs = dict(KV_PATTERN.findall(line))
            stats["error_samples"][ev] = kvs
    if "[warning" in line.lower():
        stats["warning_lines"].append(line.rstrip())
        ev = ev_match.group(2) if ev_match else "unknown"
        stats["warnings_by_event"][ev] += 1
        if ev not in stats["warning_samples"]:
            kvs = dict(KV_PATTERN.findall(line))
            stats["warning_samples"][ev] = kvs


def _new_stats() -> dict:
    return {
        "total_lines": 0,
        "events": Counter(),
        "levels": Counter(),
        "deletions": 0,
        "bulk_expired": 0,
        "reconcile_durations": [],
        "error_lines": [],
        "errors_by_event": Counter(),
        "error_samples": {},
        "warning_lines": [],
        "warnings_by_event": Counter(),
        "warning_samples": {},
        "first_ts": None,
        "last_ts": None,
    }


def _print_summary(stats: dict, wall_seconds: float):
    """Print a human-readable summary of collected stats."""
    print("\n" + "=" * 60)
    print("  KUBETIMER LOG SUMMARY")
    print("=" * 60)

    print(f"\n  Wall clock         : {wall_seconds:.1f}s")
    print(f"  Total log lines    : {stats['total_lines']}")

    if stats["first_ts"] and stats["last_ts"]:
        span = (
            stats["last_ts"] - stats["first_ts"]
        ).total_seconds()
        print(f"  Log time span      : {span:.1f}s")

    # Event counts
    if stats["events"]:
        print("\n  ── Events ──")
        for event, count in stats["events"].most_common(20):
            print(f"    {event:40s} {count:>6}")

    # Log levels
    if stats["levels"]:
        print("\n  ── Log Levels ──")
        for level, count in stats["levels"].most_common():
            print(f"    {level:10s} {count:>6}")

    # Deletion performance
    total_del = stats["deletions"] + stats["bulk_expired"]
    print("\n  ── Deletions ──")
    print(f"    Event-driven     : {stats['deletions']}")
    print(f"    Bulk (reconcile) : {stats['bulk_expired']}")
    print(f"    Total            : {total_del}")
    if wall_seconds > 0 and total_del > 0:
        print(
            f"    Throughput       : "
            f"{total_del / wall_seconds:.2f} del/s"
        )

    # Reconciliation
    if stats["reconcile_durations"]:
        print("\n  ── Reconciliation ──")
        for i, d in enumerate(stats["reconcile_durations"], 1):
            print(f"    Run {i:>3}           : {d:.6f}s")

    # Errors
    n_err = len(stats["error_lines"])
    if n_err:
        print(f"\n  ── Errors ({n_err} total) ──")
        for ev, count in stats["errors_by_event"].most_common():
            print(f"    {ev:40s} ×{count}")
            sample = stats["error_samples"].get(ev, {})
            for k, v in sample.items():
                if k in ("error", "message", "name",
                         "namespace", "reason"):
                    print(f"      {k}={v}")
        if n_err <= 5:
            print("\n    Raw lines:")
            for line in stats["error_lines"]:
                print(f"      {line[:120]}")
    else:
        print("\n  ── Errors: none ✓ ──")

    # Warnings
    n_warn = len(stats["warning_lines"])
    if n_warn:
        print(f"\n  ── Warnings ({n_warn} total) ──")
        for ev, count in stats["warnings_by_event"].most_common():
            print(f"    {ev:40s} ×{count}")
            sample = stats["warning_samples"].get(ev, {})
            for k, v in sample.items():
                if k in ("error", "message", "name",
                         "namespace", "reason"):
                    print(f"      {k}={v}")
        if n_warn <= 5:
            print("\n    Raw lines:")
            for line in stats["warning_lines"]:
                print(f"      {line[:120]}")
    else:
        print("\n  ── Warnings: none ✓ ──")

    print("\n" + "=" * 60)


# ── Main ─────────────────────────────────────────────────────────────


def run(duration: int):
    config.load_kube_config()
    v1 = client.CoreV1Api()

    pod_name = _find_pod(v1)
    print(
        f"Monitoring pod {pod_name!r} in "
        f"{NAMESPACE!r} for {duration}s …"
    )

    stats = _new_stats()
    w = watch.Watch()
    start = time.monotonic()

    try:
        stream = w.stream(
            v1.read_namespaced_pod_log,
            name=pod_name,
            namespace=NAMESPACE,
            follow=True,
            _request_timeout=duration + 10,
        )
        for line in stream:
            elapsed = time.monotonic() - start
            if elapsed >= duration:
                break

            print(line)
            _parse_line(line, stats)

    except KeyboardInterrupt:
        pass
    finally:
        w.stop()
        wall = time.monotonic() - start
        _print_summary(stats, wall)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Monitor KubeTimer operator pod logs."
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="How long to monitor, in seconds (default: 60)",
    )
    args = parser.parse_args()
    run(args.duration)
