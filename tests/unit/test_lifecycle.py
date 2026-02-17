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
from tests.conftest import _create_scheduler_mock

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
        self,
        mock_load,
        mock_pool_size,
        mock_configure,
        mock_sched_cls,
        mock_reconcile,
        memo,
    ):
        """Thread pool max_workers should match the K8s connection pool size."""
        mock_scheduler = _create_scheduler_mock()
        mock_sched_cls.return_value = mock_scheduler

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
        self,
        mock_load,
        mock_pool_size,
        mock_configure,
        mock_sched_cls,
        mock_reconcile,
        memo,
    ):
        mock_scheduler = _create_scheduler_mock()
        mock_sched_cls.return_value = mock_scheduler

        settings = _kopf_settings()

        await startup_handler(settings, memo)

        mock_scheduler.start.assert_called_once()
        assert memo.scheduler is mock_scheduler

        mock_reconcile.assert_awaited_once_with(memo=memo)

        assert memo.reconciliation_done is True
        assert memo.reconciling_uids == set()

        assert settings.execution.max_workers == 20
        assert settings.posting.level is not None

        mock_configure.assert_called_once()
        args = mock_configure.call_args[0]
        assert args[0] is memo

        mock_load.assert_called_once()
        mock_sched_cls.assert_called_once_with()

    @pytest.mark.asyncio
    @patch("kubetimer.load_k8s_config", side_effect=Exception("no cluster"))
    async def test_raises_on_k8s_config_failure(self, mock_load, memo):
        settings = _kopf_settings()

        with pytest.raises(Exception, match="no cluster"):
            await startup_handler(settings, memo)


class TestShutdownHandler:
    async def test_closes_k8s_clients_first(self, memo):
        """K8s API client sockets should be closed before other cleanup."""
        with patch("kubetimer.close_k8s_clients") as mock_close:
            await shutdown_handler(memo)

            mock_close.assert_called_once()

    async def test_graceful_shutdown_stops_scheduler(self, memo):
        """Scheduler should be shut down with wait=False."""
        await shutdown_handler(memo)

        memo.scheduler.shutdown.assert_called_once_with(wait=True)
        assert memo.scheduler.running is False

    async def test_scheduler_not_running_skips_shutdown(self, memo):
        """If the scheduler isn't running, shutdown() should not be called."""
        memo.scheduler._set_running(False)

        await shutdown_handler(memo)

        memo.scheduler.shutdown.assert_not_called()

    async def test_scheduler_shutdown_exception_is_caught(self, memo):
        """If scheduler.shutdown() raises, the error should be caught."""
        memo.scheduler.shutdown.side_effect = RuntimeError("shutdown failed")

        # Should not raise
        await shutdown_handler(memo)

        memo.scheduler.shutdown.assert_called_once_with(wait=True)

    async def test_executor_shutdown_on_cleanup(self, memo):
        """ThreadPoolExecutor should be shut down non-blocking."""
        memo.executor = MagicMock()

        await shutdown_handler(memo)

        memo.executor.shutdown.assert_called_once_with(wait=True)

    async def test_no_executor_does_not_raise(self):
        """Missing executor attribute should not crash."""
        memo = SimpleNamespace()
        # no .scheduler, no .executor

        with patch("kubetimer.close_k8s_clients"):
            # Should complete without raising any exception
            await shutdown_handler(memo)

    async def test_executor_shutdown_exception_is_caught(self, memo):
        """If executor.shutdown() raises, the error should be caught."""
        memo.executor = MagicMock()
        memo.executor.shutdown.side_effect = RuntimeError("executor failed")

        # Should not raise
        await shutdown_handler(memo)

        memo.executor.shutdown.assert_called_once_with(wait=True)
