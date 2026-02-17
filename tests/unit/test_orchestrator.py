"""Tests for kubetimer.reconcile.orchestrator.

Mocks the K8s list API and downstream helpers so each orchestrator
function can be tested in isolation, without a cluster.

Covers:
  - _fetch_ttl_deployments: happy path, API error, invalid TTL,
    namespace filtering
  - _triage_deployments: expired vs future, schedule failure
  - reconcile_existing_deployments: full flow, no scheduler,
    empty results, all-expired, all-future
"""

from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from tests.conftest import _create_scheduler_mock
from kubetimer.reconcile.orchestrator import (
    _fetch_ttl_deployments,
    _triage_deployments,
    reconcile_existing_deployments,
)
from kubetimer.reconcile.models import TtlDeployment

PAST_TTL = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
FUTURE_TTL = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()


def _k8s_deployment(name, namespace="default", annotations=None):
    dep = SimpleNamespace()
    dep.metadata = SimpleNamespace()
    dep.metadata.name = name
    dep.metadata.namespace = namespace
    dep.metadata.uid = f"uid-{name}"
    dep.metadata.annotations = annotations
    return dep


def _k8s_list(*deployments):
    result = SimpleNamespace()
    result.items = list(deployments)
    return result


class TestFetchTtlDeployments:
    @patch("kubetimer.reconcile.orchestrator.list_deployments_all_namespaces")
    def test_returns_deployments_with_ttl(self, mock_list):
        mock_list.return_value = _k8s_list(
            _k8s_deployment("web", annotations={"kubetimer.io/ttl": FUTURE_TTL}),
            _k8s_deployment("db", annotations={}),  # no TTL
        )

        result = _fetch_ttl_deployments("kubetimer.io/ttl", [], [])

        assert len(result) == 1
        assert result[0].name == "web"

    @patch("kubetimer.reconcile.orchestrator.list_deployments_all_namespaces")
    def test_api_error_returns_empty(self, mock_list):
        mock_list.side_effect = Exception("connection refused")

        result = _fetch_ttl_deployments("kubetimer.io/ttl", [], [])

        assert result == []

    @patch("kubetimer.reconcile.orchestrator.list_deployments_all_namespaces")
    def test_invalid_ttl_skipped(self, mock_list):
        mock_list.return_value = _k8s_list(
            _k8s_deployment("bad", annotations={"kubetimer.io/ttl": "not-a-date"}),
        )

        result = _fetch_ttl_deployments("kubetimer.io/ttl", [], [])

        assert result == []

    @patch("kubetimer.reconcile.orchestrator.list_deployments_all_namespaces")
    def test_excluded_namespace_filtered(self, mock_list):
        mock_list.return_value = _k8s_list(
            _k8s_deployment(
                "coredns", "kube-system", annotations={"kubetimer.io/ttl": FUTURE_TTL}
            ),
        )

        result = _fetch_ttl_deployments(
            "kubetimer.io/ttl",
            [],
            ["kube-system"],
        )

        assert result == []

    @patch("kubetimer.reconcile.orchestrator.list_deployments_all_namespaces")
    def test_no_annotations_field(self, mock_list):
        """Deployment with metadata.annotations = None should be skipped."""
        mock_list.return_value = _k8s_list(
            _k8s_deployment("bare", annotations=None),
        )

        result = _fetch_ttl_deployments("kubetimer.io/ttl", [], [])

        assert result == []


class TestTriageDeployments:
    def test_separates_expired_and_future(self):
        past = datetime.now(timezone.utc) - timedelta(hours=1)
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        scheduler = _create_scheduler_mock()

        deps = [
            TtlDeployment(name="old", namespace="default", uid="u1", ttl_value=past),
            TtlDeployment(name="new", namespace="default", uid="u2", ttl_value=future),
        ]

        expired, scheduled, errors = _triage_deployments(
            deps,
            scheduler,
            "kubetimer.io/ttl",
            "UTC",
            False,
        )

        assert len(expired) == 1
        assert expired[0].name == "old"
        assert scheduled == 1
        assert errors == 0

    def test_schedule_failure_increments_errors(self):
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        scheduler = _create_scheduler_mock()
        scheduler.add_job.side_effect = RuntimeError("boom")

        deps = [
            TtlDeployment(name="fail", namespace="default", uid="u3", ttl_value=future),
        ]

        expired, scheduled, errors = _triage_deployments(
            deps,
            scheduler,
            "kubetimer.io/ttl",
            "UTC",
            False,
        )

        assert expired == []
        assert scheduled == 0
        assert errors == 1


class TestReconcileExistingDeployments:
    @pytest.mark.asyncio
    async def test_skips_when_no_scheduler(self):
        memo = SimpleNamespace()  # no .scheduler attribute

        # Should not raise
        await reconcile_existing_deployments(memo)

    @pytest.mark.asyncio
    async def test_skips_when_scheduler_not_running(self, memo):
        memo.scheduler._set_running(False)

        await reconcile_existing_deployments(memo)

    @pytest.mark.asyncio
    @patch("kubetimer.reconcile.orchestrator.list_deployments_all_namespaces")
    async def test_no_ttl_deployments_returns_early(self, mock_list, memo):
        mock_list.return_value = _k8s_list()  # empty

        await reconcile_existing_deployments(memo)

        # No error, just an early return after log

    @pytest.mark.asyncio
    @patch(
        "kubetimer.reconcile.orchestrator.bulk_delete_expired", new_callable=AsyncMock
    )
    @patch("kubetimer.reconcile.orchestrator.list_deployments_all_namespaces")
    async def test_all_expired_deletes_all(self, mock_list, mock_bulk, memo):
        mock_list.return_value = _k8s_list(
            _k8s_deployment("old-1", annotations={"kubetimer.io/ttl": PAST_TTL}),
            _k8s_deployment("old-2", annotations={"kubetimer.io/ttl": PAST_TTL}),
        )
        mock_bulk.return_value = (2, 0)

        await reconcile_existing_deployments(memo)

        mock_bulk.assert_awaited_once()
        args, _ = mock_bulk.call_args
        assert len(args[0]) == 2  # two expired deployments passed

    @pytest.mark.asyncio
    @patch(
        "kubetimer.reconcile.orchestrator.bulk_delete_expired", new_callable=AsyncMock
    )
    @patch("kubetimer.reconcile.orchestrator.list_deployments_all_namespaces")
    async def test_expired_uids_added_to_reconciling_set(
        self, mock_list, mock_bulk, memo
    ):
        """Expired UIDs should be registered in reconciling_uids before deletion."""
        captured_uids = set()

        async def capture_uids(deps, dry_run):
            captured_uids.update(memo.reconciling_uids)
            return (len(deps), 0)

        mock_bulk.side_effect = capture_uids
        mock_list.return_value = _k8s_list(
            _k8s_deployment("old-1", annotations={"kubetimer.io/ttl": PAST_TTL}),
        )

        await reconcile_existing_deployments(memo)

        assert "uid-old-1" in captured_uids

    @pytest.mark.asyncio
    @patch(
        "kubetimer.reconcile.orchestrator.bulk_delete_expired", new_callable=AsyncMock
    )
    @patch("kubetimer.reconcile.orchestrator.schedule_deletion_job")
    @patch("kubetimer.reconcile.orchestrator.list_deployments_all_namespaces")
    async def test_all_future_schedules_all(
        self, mock_list, mock_schedule, mock_bulk, memo
    ):
        mock_list.return_value = _k8s_list(
            _k8s_deployment("new-1", annotations={"kubetimer.io/ttl": FUTURE_TTL}),
        )
        mock_schedule.return_value = True

        await reconcile_existing_deployments(memo)

        mock_schedule.assert_called_once()
        mock_bulk.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(
        "kubetimer.reconcile.orchestrator.bulk_delete_expired", new_callable=AsyncMock
    )
    @patch("kubetimer.reconcile.orchestrator.schedule_deletion_job")
    @patch("kubetimer.reconcile.orchestrator.list_deployments_all_namespaces")
    async def test_mixed_expired_and_future(
        self, mock_list, mock_schedule, mock_bulk, memo
    ):
        mock_list.return_value = _k8s_list(
            _k8s_deployment("old", annotations={"kubetimer.io/ttl": PAST_TTL}),
            _k8s_deployment("new", annotations={"kubetimer.io/ttl": FUTURE_TTL}),
        )
        mock_schedule.return_value = True
        mock_bulk.return_value = (1, 0)

        await reconcile_existing_deployments(memo)

        mock_schedule.assert_called_once()
        mock_bulk.assert_awaited_once()
