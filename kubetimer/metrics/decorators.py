from __future__ import annotations

import time
from contextlib import asynccontextmanager, contextmanager
from typing import AsyncIterator, Iterator

from prometheus_client import Gauge, Histogram


@asynccontextmanager
async def track_duration(
    histogram: Histogram,
    **labels: str,
) -> AsyncIterator[None]:
    """Async context manager that observes elapsed wall-clock time.

    Usage::
        async with track_duration(DELETE_DURATION, source="scheduler", namespace=ns):
            await async_delete_namespaced_deployment(ns, name)
    """
    child = histogram.labels(**labels) if labels else histogram
    start = time.monotonic()
    try:
        yield
    finally:
        child.observe(time.monotonic() - start)


@contextmanager
def track_duration_sync(
    histogram: Histogram,
    **labels: str,
) -> Iterator[None]:
    """Sync variant of :func:`track_duration`."""
    child = histogram.labels(**labels) if labels else histogram
    start = time.monotonic()
    try:
        yield
    finally:
        child.observe(time.monotonic() - start)


@asynccontextmanager
async def track_concurrency(
    gauge: Gauge,
    **labels: str,
) -> AsyncIterator[None]:
    """Async context manager that increments a gauge on entry, decrements on exit.

    Usage::
        async with track_concurrency(CONCURRENT_EVENTS, event_type="create"):
            ...  # handler body
    """
    child = gauge.labels(**labels) if labels else gauge
    child.inc()
    try:
        yield
    finally:
        child.dec()


@contextmanager
def track_concurrency_sync(
    gauge: Gauge,
    **labels: str,
) -> Iterator[None]:
    """Sync variant of :func:`track_concurrency`."""
    child = gauge.labels(**labels) if labels else gauge
    child.inc()
    try:
        yield
    finally:
        child.dec()
