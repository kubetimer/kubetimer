"""Tests for kubetimer startup and shutdown lifecycle handlers.

Covers:
  - startup_handler: K8s config loading, thread pool sizing,
    memo configuration, APScheduler start, reconciliation call
  - shutdown_handler: graceful scheduler shutdown, missing scheduler,
    scheduler already stopped, exception during shutdown
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kubetimer import shutdown_handler, startup_handler


def _kopf_settings():
    """Minimal stand-in for kopf.OperatorSettings."""
    s = SimpleNamespace()
    s.execution = SimpleNamespace(max_workers=None)
    s.posting = SimpleNamespace(level=None)
    return s


class TestStartupHandler:
    @pytest.mark.asyncio
    @patch("kubetimer.reconcile_existing_deployments", new_callable=AsyncMock)
    @patch("kubetimer.AsyncIOScheduler")
    @patch("kubetimer.configure_memo")
    @patch("kubetimer.get_connection_pool_maxsize", return_value=10)
    @patch("kubetimer.load_k8s_config")
    async def test_configures_thread_pool_from_k8s_pool_size(
        self, mock_load, mock_pool_size, mock_configure, mock_sched_cls, mock_reconcile
    ):
        """Thread pool max_workers should match the K8s connection pool size."""
        mock_scheduler = MagicMock()
        mock_sched_cls.return_value = mock_scheduler

        memo = SimpleNamespace()
        settings = _kopf_settings()

        loop = asyncio.get_event_loop()

        await startup_handler(settings, memo)

        executor = loop._default_executor
        assert executor._max_workers == 10
        assert memo.executor is executor

    @pytest.mark.asyncio
    @patch("kubetimer.reconcile_existing_deployments", new_callable=AsyncMock)
    @patch("kubetimer.AsyncIOScheduler")
    @patch("kubetimer.configure_memo")
    @patch("kubetimer.get_connection_pool_maxsize", return_value=4)
    @patch("kubetimer.load_k8s_config")
    async def test_starts_apscheduler_and_settings(
        self, mock_load, mock_pool_size, mock_configure, mock_sched_cls, mock_reconcile
    ):
        mock_scheduler = MagicMock()
        mock_sched_cls.return_value = mock_scheduler

        memo = SimpleNamespace()
        settings = _kopf_settings()

        await startup_handler(settings, memo)

        mock_scheduler.start.assert_called_once()
        assert memo.scheduler is mock_scheduler

        mock_reconcile.assert_awaited_once_with(memo=memo)

        assert settings.execution.max_workers == 20
        assert settings.posting.level is not None

        mock_configure.assert_called_once()
        args = mock_configure.call_args[0]
        assert args[0] is memo

        mock_load.assert_called_once()
        mock_sched_cls.assert_called_once_with()

    @pytest.mark.asyncio
    @patch("kubetimer.load_k8s_config", side_effect=Exception("no cluster"))
    async def test_raises_on_k8s_config_failure(self, mock_load):
        memo = SimpleNamespace()
        settings = _kopf_settings()

        with pytest.raises(Exception, match="no cluster"):
            await startup_handler(settings, memo)


class TestShutdownHandler:
    def test_graceful_shutdown_waits_for_jobs(self):
        """Scheduler should be shut down with wait=True."""
        memo = SimpleNamespace()
        memo.scheduler = MagicMock()
        memo.scheduler.running = True

        shutdown_handler(memo)

        memo.scheduler.shutdown.assert_called_once_with(wait=True)

    def test_no_scheduler_attribute_does_not_raise(self):
        """Missing scheduler attribute should not crash."""
        memo = SimpleNamespace()  # no .scheduler

        # Should complete without raising any exception
        shutdown_handler(memo)

    def test_scheduler_not_running_skips_shutdown(self):
        """If the scheduler isn't running, shutdown() should not be called."""
        memo = SimpleNamespace()
        memo.scheduler = MagicMock()
        memo.scheduler.running = False

        shutdown_handler(memo)

        memo.scheduler.shutdown.assert_not_called()

    def test_scheduler_shutdown_exception_is_caught(self):
        """If scheduler.shutdown() raises, the error should be caught."""
        memo = SimpleNamespace()
        memo.scheduler = MagicMock()
        memo.scheduler.running = True
        memo.scheduler.shutdown.side_effect = RuntimeError("shutdown failed")

        # Should not raise
        shutdown_handler(memo)

        memo.scheduler.shutdown.assert_called_once_with(wait=True)

    def test_executor_shutdown_on_cleanup(self):
        """ThreadPoolExecutor should be shut down with wait=True."""
        memo = SimpleNamespace()
        memo.scheduler = MagicMock()
        memo.scheduler.running = True
        memo.executor = MagicMock()

        shutdown_handler(memo)

        memo.executor.shutdown.assert_called_once_with(wait=True)

    def test_no_executor_does_not_raise(self):
        """Missing executor attribute should not crash."""
        memo = SimpleNamespace()
        # no .scheduler, no .executor

        shutdown_handler(memo)

    def test_executor_shutdown_exception_is_caught(self):
        """If executor.shutdown() raises, the error should be caught."""
        memo = SimpleNamespace()
        memo.scheduler = MagicMock()
        memo.scheduler.running = True
        memo.executor = MagicMock()
        memo.executor.shutdown.side_effect = RuntimeError("executor failed")

        # Should not raise
        shutdown_handler(memo)

        memo.executor.shutdown.assert_called_once_with(wait=True)
