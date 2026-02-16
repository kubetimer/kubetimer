from unittest.mock import AsyncMock, patch

import pytest

from kubetimer.reconcile.bulk_delete import _delete_one, bulk_delete_expired
from kubetimer.reconcile.models import TtlDeployment


def _dep(name: str = "test-dep", namespace: str = "default") -> TtlDeployment:
    return TtlDeployment(
        name=name,
        namespace=namespace,
        uid=f"uid-{name}",
        ttl_value="2026-01-01T00:00:00Z",
    )


class TestDeleteOne:
    @pytest.mark.asyncio
    @patch(
        "kubetimer.reconcile.bulk_delete.async_delete_namespaced_deployment",
        new_callable=AsyncMock,
    )
    async def test_successful_delete(self, mock_delete):
        result = await _delete_one(_dep(), dry_run=False)
        assert result == "deleted"
        mock_delete.assert_awaited_once_with("default", "test-dep")

    @pytest.mark.asyncio
    async def test_dry_run_skips_api_call(self):
        result = await _delete_one(_dep(), dry_run=True)
        assert result == "dry_run"

    @pytest.mark.asyncio
    @patch(
        "kubetimer.reconcile.bulk_delete.async_delete_namespaced_deployment",
        new_callable=AsyncMock,
    )
    async def test_api_error_returns_error(self, mock_delete):
        mock_delete.side_effect = Exception("API 503")
        result = await _delete_one(_dep(), dry_run=False)
        assert result == "error"


class TestBulkDeleteExpired:
    @pytest.mark.asyncio
    @patch(
        "kubetimer.reconcile.bulk_delete.async_delete_namespaced_deployment",
        new_callable=AsyncMock,
    )
    async def test_counts_correct(self, mock_delete):
        deps = [_dep(f"dep-{i}") for i in range(5)]
        count, errors = await bulk_delete_expired(deps, dry_run=False)
        assert count == 5
        assert errors == 0
        assert mock_delete.await_count == 5

    @pytest.mark.asyncio
    async def test_dry_run_counts(self):
        deps = [_dep(f"dep-{i}") for i in range(3)]
        count, errors = await bulk_delete_expired(deps, dry_run=True)
        assert count == 3
        assert errors == 0

    @pytest.mark.asyncio
    @patch(
        "kubetimer.reconcile.bulk_delete.async_delete_namespaced_deployment",
        new_callable=AsyncMock,
    )
    async def test_mixed_results(self, mock_delete):
        """Two succeed, one fails → counts reflect both."""
        call_count = 0

        async def fail_on_second(ns, name):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise Exception("boom")

        mock_delete.side_effect = fail_on_second

        deps = [_dep(f"dep-{i}") for i in range(3)]
        count, errors = await bulk_delete_expired(deps, dry_run=False)
        assert count == 2
        assert errors == 1
