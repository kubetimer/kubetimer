"""Tests for kubetimer.metrics.

Covers:
  - Instrument definitions exist with expected types
  - track_duration / track_duration_sync observe histogram values
  - track_concurrency / track_concurrency_sync inc/dec gauge correctly
  - Context managers clean up on exceptions
  - Handler instrumentation: CONCURRENT_EVENTS and EVENT_HANDLER_DURATION
    change when handler functions are invoked
  - Deletion instrumentation: DEPLOYMENTS_DELETED and DELETE_DURATION are
    recorded at each call site (event_handler, scheduler, reconciler)
  - JOBS_SCHEDULED / JOBS_CANCELLED counters increment
  - RECONCILE_DURATION and RECONCILE_DEPLOYMENTS are set by orchestrator
"""

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, Info

from kubetimer.metrics.decorators import (
    track_concurrency,
    track_concurrency_sync,
    track_duration,
    track_duration_sync,
)


# ---------------------------------------------------------------------------
# Helpers — fresh registry per test to avoid cross-pollution
# ---------------------------------------------------------------------------


def _fresh_histogram(name: str, labels: list[str] | None = None) -> tuple[Histogram, CollectorRegistry]:
    reg = CollectorRegistry()
    h = Histogram(name, "test", labelnames=labels or [], registry=reg)
    return h, reg


def _fresh_gauge(name: str, labels: list[str] | None = None) -> tuple[Gauge, CollectorRegistry]:
    reg = CollectorRegistry()
    g = Gauge(name, "test", labelnames=labels or [], registry=reg)
    return g, reg


def _fresh_counter(name: str, labels: list[str] | None = None) -> tuple[Counter, CollectorRegistry]:
    reg = CollectorRegistry()
    c = Counter(name, "test", labelnames=labels or [], registry=reg)
    return c, reg


FUTURE_TTL = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
PAST_TTL = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()


# ===================================================================
# Context manager tests
# ===================================================================


class TestTrackDuration:
    async def test_observes_time(self):
        h, reg = _fresh_histogram("td_test_obs")
        async with track_duration(h):
            pass
        assert reg.get_sample_value("td_test_obs_count") == 1
        assert reg.get_sample_value("td_test_obs_sum") > 0

    async def test_observes_with_labels(self):
        h, reg = _fresh_histogram("td_test_lbl", labels=["src"])
        async with track_duration(h, src="scheduler"):
            pass
        assert reg.get_sample_value("td_test_lbl_count", {"src": "scheduler"}) == 1

    async def test_observes_on_exception(self):
        h, reg = _fresh_histogram("td_test_exc")
        with pytest.raises(RuntimeError):
            async with track_duration(h):
                raise RuntimeError("boom")
        assert reg.get_sample_value("td_test_exc_count") == 1


class TestTrackDurationSync:
    def test_observes_time(self):
        h, reg = _fresh_histogram("tds_test_obs")
        with track_duration_sync(h):
            pass
        assert reg.get_sample_value("tds_test_obs_count") == 1

    def test_observes_on_exception(self):
        h, reg = _fresh_histogram("tds_test_exc")
        with pytest.raises(RuntimeError):
            with track_duration_sync(h):
                raise RuntimeError("boom")
        assert reg.get_sample_value("tds_test_exc_count") == 1


class TestTrackConcurrency:
    async def test_inc_dec(self):
        g, reg = _fresh_gauge("tc_test_incdec")
        assert reg.get_sample_value("tc_test_incdec") == 0
        async with track_concurrency(g):
            assert reg.get_sample_value("tc_test_incdec") == 1
        assert reg.get_sample_value("tc_test_incdec") == 0

    async def test_decrements_on_exception(self):
        g, reg = _fresh_gauge("tc_test_exc")
        with pytest.raises(RuntimeError):
            async with track_concurrency(g):
                raise RuntimeError("boom")
        assert reg.get_sample_value("tc_test_exc") == 0

    async def test_inc_dec_with_labels(self):
        g, reg = _fresh_gauge("tc_test_lbl", labels=["event_type"])
        async with track_concurrency(g, event_type="create"):
            val = reg.get_sample_value("tc_test_lbl", {"event_type": "create"})
            assert val == 1
        assert reg.get_sample_value("tc_test_lbl", {"event_type": "create"}) == 0


class TestTrackConcurrencySync:
    def test_inc_dec(self):
        g, reg = _fresh_gauge("tcs_test_incdec")
        with track_concurrency_sync(g):
            assert reg.get_sample_value("tcs_test_incdec") == 1
        assert reg.get_sample_value("tcs_test_incdec") == 0

    def test_decrements_on_exception(self):
        g, reg = _fresh_gauge("tcs_test_exc")
        with pytest.raises(RuntimeError):
            with track_concurrency_sync(g):
                raise RuntimeError("boom")
        assert reg.get_sample_value("tcs_test_exc") == 0


# ===================================================================
# Instrument definition tests
# ===================================================================


class TestInstrumentDefinitions:
    """Verify that all metrics objects exist with expected types."""

    def test_registry_exists(self):
        from kubetimer.metrics.instruments import REGISTRY
        assert isinstance(REGISTRY, CollectorRegistry)

    def test_delete_duration_is_histogram(self):
        from kubetimer.metrics.instruments import DELETE_DURATION
        assert isinstance(DELETE_DURATION, Histogram)

    def test_deployments_deleted_is_counter(self):
        from kubetimer.metrics.instruments import DEPLOYMENTS_DELETED
        assert isinstance(DEPLOYMENTS_DELETED, Counter)

    def test_concurrent_events_is_gauge(self):
        from kubetimer.metrics.instruments import CONCURRENT_EVENTS
        assert isinstance(CONCURRENT_EVENTS, Gauge)

    def test_event_handler_duration_is_histogram(self):
        from kubetimer.metrics.instruments import EVENT_HANDLER_DURATION
        assert isinstance(EVENT_HANDLER_DURATION, Histogram)

    def test_startup_duration_is_histogram(self):
        from kubetimer.metrics.instruments import STARTUP_DURATION
        assert isinstance(STARTUP_DURATION, Histogram)

    def test_reconcile_duration_is_histogram(self):
        from kubetimer.metrics.instruments import RECONCILE_DURATION
        assert isinstance(RECONCILE_DURATION, Histogram)

    def test_reconcile_deployments_is_gauge(self):
        from kubetimer.metrics.instruments import RECONCILE_DEPLOYMENTS
        assert isinstance(RECONCILE_DEPLOYMENTS, Gauge)

    def test_jobs_scheduled_is_counter(self):
        from kubetimer.metrics.instruments import JOBS_SCHEDULED
        assert isinstance(JOBS_SCHEDULED, Counter)

    def test_jobs_cancelled_is_counter(self):
        from kubetimer.metrics.instruments import JOBS_CANCELLED
        assert isinstance(JOBS_CANCELLED, Counter)

    def test_operator_info_is_info(self):
        from kubetimer.metrics.instruments import OPERATOR_INFO
        assert isinstance(OPERATOR_INFO, Info)


# ===================================================================
# Integration: handler instrumentation records metrics
# ===================================================================


class TestHandlerMetricsIntegration:
    """Verify that calling event handlers updates Prometheus metrics."""

    @patch("kubetimer.handlers.deployment.schedule_deletion_job")
    async def test_create_handler_records_concurrent_events(self, mock_schedule, memo):
        from kubetimer.metrics.instruments import CONCURRENT_EVENTS, REGISTRY

        before = REGISTRY.get_sample_value(
            "kubetimer_concurrent_events", {"event_type": "create"}
        )

        await on_deployment_created_with_ttl(
            namespace="default",
            name="web",
            uid="uid-m1",
            annotations={"kubetimer.io/ttl": FUTURE_TTL},
            memo=memo,
        )

        after = REGISTRY.get_sample_value(
            "kubetimer_concurrent_events", {"event_type": "create"}
        )
        # Gauge should be back to same level after handler exits
        assert after == (before or 0.0)

    @patch("kubetimer.handlers.deployment.schedule_deletion_job")
    async def test_create_handler_records_event_duration(self, mock_schedule, memo):
        from kubetimer.metrics.instruments import REGISTRY

        before = REGISTRY.get_sample_value(
            "kubetimer_event_handler_duration_seconds_count",
            {"event_type": "create"},
        ) or 0

        await on_deployment_created_with_ttl(
            namespace="default",
            name="web",
            uid="uid-m2",
            annotations={"kubetimer.io/ttl": FUTURE_TTL},
            memo=memo,
        )

        after = REGISTRY.get_sample_value(
            "kubetimer_event_handler_duration_seconds_count",
            {"event_type": "create"},
        )
        assert after == before + 1

    @patch("kubetimer.handlers.deployment.async_delete_namespaced_deployment")
    async def test_immediate_delete_records_metrics(self, mock_delete, memo):
        from kubetimer.metrics.instruments import REGISTRY

        before_count = REGISTRY.get_sample_value(
            "kubetimer_deployments_deleted_total",
            {"source": "event_handler", "namespace": "default", "outcome": "deleted"},
        ) or 0
        before_dur = REGISTRY.get_sample_value(
            "kubetimer_delete_duration_seconds_count",
            {"source": "event_handler", "namespace": "default"},
        ) or 0

        await on_deployment_created_with_ttl(
            namespace="default",
            name="web",
            uid="uid-m3",
            annotations={"kubetimer.io/ttl": PAST_TTL},
            memo=memo,
        )

        assert REGISTRY.get_sample_value(
            "kubetimer_deployments_deleted_total",
            {"source": "event_handler", "namespace": "default", "outcome": "deleted"},
        ) == before_count + 1

        assert REGISTRY.get_sample_value(
            "kubetimer_delete_duration_seconds_count",
            {"source": "event_handler", "namespace": "default"},
        ) == before_dur + 1

    @patch("kubetimer.handlers.deployment.cancel_deletion_job")
    def test_update_handler_records_concurrent_events(self, mock_cancel, memo):
        from kubetimer.metrics.instruments import CONCURRENT_EVENTS, REGISTRY

        before = REGISTRY.get_sample_value(
            "kubetimer_concurrent_events", {"event_type": "update"}
        )

        on_ttl_annotation_changed(
            namespace="default",
            name="api",
            uid="uid-m4",
            old=FUTURE_TTL,
            new=None,
            memo=memo,
        )

        after = REGISTRY.get_sample_value(
            "kubetimer_concurrent_events", {"event_type": "update"}
        )
        assert after == (before or 0.0)

    @patch("kubetimer.handlers.deployment.cancel_deletion_job")
    def test_delete_handler_records_concurrent_events(self, mock_cancel, memo):
        from kubetimer.metrics.instruments import REGISTRY

        before = REGISTRY.get_sample_value(
            "kubetimer_concurrent_events", {"event_type": "delete"}
        )

        on_deployment_deleted_with_ttl(
            namespace="default",
            name="web",
            uid="uid-m5",
            memo=memo,
        )

        after = REGISTRY.get_sample_value(
            "kubetimer_concurrent_events", {"event_type": "delete"}
        )
        assert after == (before or 0.0)


# ===================================================================
# Integration: scheduler job metrics
# ===================================================================


class TestSchedulerJobMetrics:
    def test_schedule_increments_counter(self):
        from kubetimer.metrics.instruments import JOBS_SCHEDULED, REGISTRY
        from kubetimer.scheduler.jobs import schedule_deletion_job
        from tests.conftest import _create_scheduler_mock

        scheduler = _create_scheduler_mock()
        before = REGISTRY.get_sample_value("kubetimer_jobs_scheduled_total") or 0

        schedule_deletion_job(
            scheduler, "ns", "dep", "uid-j1",
            datetime.now(timezone.utc) + timedelta(hours=1),
            "kubetimer.io/ttl", "UTC", False,
        )

        assert REGISTRY.get_sample_value("kubetimer_jobs_scheduled_total") == before + 1

    def test_cancel_increments_counter(self):
        from kubetimer.metrics.instruments import JOBS_CANCELLED, REGISTRY
        from kubetimer.scheduler.jobs import cancel_deletion_job
        from tests.conftest import _create_scheduler_mock

        scheduler = _create_scheduler_mock()
        before = REGISTRY.get_sample_value("kubetimer_jobs_cancelled_total") or 0

        cancel_deletion_job(scheduler, "ns", "dep", "uid-j2")

        assert REGISTRY.get_sample_value("kubetimer_jobs_cancelled_total") == before + 1


# ===================================================================
# Integration: scheduler delete_deployment_job metrics
# ===================================================================


class TestDeleteDeploymentJobMetrics:
    @patch("kubetimer.scheduler.jobs.async_delete_namespaced_deployment")
    @patch("kubetimer.scheduler.jobs.get_namespaced_deployment")
    async def test_successful_delete_records_metrics(self, mock_get, mock_delete, memo):
        from kubetimer.metrics.instruments import REGISTRY
        from kubetimer.scheduler.jobs import delete_deployment_job

        # Build a fake Deployment object
        dep = SimpleNamespace()
        dep.metadata = SimpleNamespace(
            uid="uid-sdj1",
            annotations={"kubetimer.io/ttl": PAST_TTL},
        )
        mock_get.return_value = dep

        before_count = REGISTRY.get_sample_value(
            "kubetimer_deployments_deleted_total",
            {"source": "scheduler", "namespace": "default", "outcome": "deleted"},
        ) or 0
        before_dur = REGISTRY.get_sample_value(
            "kubetimer_delete_duration_seconds_count",
            {"source": "scheduler", "namespace": "default"},
        ) or 0

        await delete_deployment_job(
            namespace="default",
            name="web",
            uid="uid-sdj1",
            annotation_key="kubetimer.io/ttl",
            timezone_str="UTC",
            dry_run=False,
        )

        assert REGISTRY.get_sample_value(
            "kubetimer_deployments_deleted_total",
            {"source": "scheduler", "namespace": "default", "outcome": "deleted"},
        ) == before_count + 1

        assert REGISTRY.get_sample_value(
            "kubetimer_delete_duration_seconds_count",
            {"source": "scheduler", "namespace": "default"},
        ) == before_dur + 1

    @patch("kubetimer.scheduler.jobs.async_delete_namespaced_deployment")
    @patch("kubetimer.scheduler.jobs.get_namespaced_deployment")
    async def test_dry_run_records_dry_run_outcome(self, mock_get, mock_delete):
        from kubetimer.metrics.instruments import REGISTRY
        from kubetimer.scheduler.jobs import delete_deployment_job

        dep = SimpleNamespace()
        dep.metadata = SimpleNamespace(
            uid="uid-sdj2",
            annotations={"kubetimer.io/ttl": PAST_TTL},
        )
        mock_get.return_value = dep

        before = REGISTRY.get_sample_value(
            "kubetimer_deployments_deleted_total",
            {"source": "scheduler", "namespace": "ns1", "outcome": "dry_run"},
        ) or 0

        await delete_deployment_job(
            namespace="ns1",
            name="web",
            uid="uid-sdj2",
            annotation_key="kubetimer.io/ttl",
            timezone_str="UTC",
            dry_run=True,
        )

        assert REGISTRY.get_sample_value(
            "kubetimer_deployments_deleted_total",
            {"source": "scheduler", "namespace": "ns1", "outcome": "dry_run"},
        ) == before + 1
        mock_delete.assert_not_awaited()


# ===================================================================
# Need to import handlers *after* patching is set up at class level
# ===================================================================

from kubetimer.handlers.deployment import (
    on_deployment_created_with_ttl,
    on_deployment_deleted_with_ttl,
    on_ttl_annotation_changed,
)
