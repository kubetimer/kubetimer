"""Tests for kubetimer.reconcile.orchestrator.

Mocks the K8s list API and downstream helpers so each orchestrator
function can be tested in isolation, without a cluster.

Covers:
  - _fetch_ttl_deployments: happy path, API error, namespace filtering,
    expires-at annotation capture
  - _triage_deployments: expired vs future via expires-at, fallback via
    creation_timestamp + duration, schedule failure, invalid TTL
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

PAST_EXPIRES_AT = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
FUTURE_EXPIRES_AT = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()

CREATION_2H_AGO = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
CREATION_NOW = datetime.now(timezone.utc).isoformat()


def _k8s_deployment(
    name, namespace="default", annotations=None, creation_timestamp=None
):
    dep = SimpleNamespace()
    dep.metadata = SimpleNamespace()
    dep.metadata.name = name
    dep.metadata.namespace = namespace
    dep.metadata.uid = f"uid-{name}"
    dep.metadata.annotations = annotations
    dep.metadata.creation_timestamp = (
        creation_timestamp or datetime.now(timezone.utc).isoformat()
    )
    return dep


class TestFetchTtlDeployments:
    @patch("kubetimer.reconcile.orchestrator.list_deployments_all_namespaces_paginated")
    def test_returns_deployments_with_ttl(self, mock_list):
        mock_list.return_value = iter(
            [
                _k8s_deployment("web", annotations={"kubetimer.io/ttl": "1h"}),
                _k8s_deployment("db", annotations={}),  # no TTL
            ]
        )

        result = _fetch_ttl_deployments(
            "kubetimer.io/ttl", "kubetimer.io/expires-at", [], []
        )

        assert len(result) == 1
        assert result[0].name == "web"

    @patch("kubetimer.reconcile.orchestrator.list_deployments_all_namespaces_paginated")
    def test_captures_expires_at_annotation(self, mock_list):
        mock_list.return_value = iter(
            [
                _k8s_deployment(
                    "web",
                    annotations={
                        "kubetimer.io/ttl": "1h",
                        "kubetimer.io/expires-at": FUTURE_EXPIRES_AT,
                    },
                ),
            ]
        )

        result = _fetch_ttl_deployments(
            "kubetimer.io/ttl", "kubetimer.io/expires-at", [], []
        )

        assert result[0].expires_at == FUTURE_EXPIRES_AT

    @patch("kubetimer.reconcile.orchestrator.list_deployments_all_namespaces_paginated")
    def test_expires_at_is_none_when_absent(self, mock_list):
        mock_list.return_value = iter(
            [
                _k8s_deployment("web", annotations={"kubetimer.io/ttl": "1h"}),
            ]
        )

        result = _fetch_ttl_deployments(
            "kubetimer.io/ttl", "kubetimer.io/expires-at", [], []
        )

        assert result[0].expires_at is None

    @patch("kubetimer.reconcile.orchestrator.list_deployments_all_namespaces_paginated")
    def test_api_error_returns_empty(self, mock_list):
        mock_list.side_effect = Exception("connection refused")

        result = _fetch_ttl_deployments(
            "kubetimer.io/ttl", "kubetimer.io/expires-at", [], []
        )

        assert result == []

    @patch("kubetimer.reconcile.orchestrator.list_deployments_all_namespaces_paginated")
    def test_excluded_namespace_filtered(self, mock_list):
        mock_list.return_value = iter(
            [
                _k8s_deployment(
                    "coredns",
                    "kube-system",
                    annotations={"kubetimer.io/ttl": "1h"},
                ),
            ]
        )

        result = _fetch_ttl_deployments(
            "kubetimer.io/ttl",
            "kubetimer.io/expires-at",
            [],
            ["kube-system"],
        )

        assert result == []

    @patch("kubetimer.reconcile.orchestrator.list_deployments_all_namespaces_paginated")
    def test_no_annotations_field(self, mock_list):
        """Deployment with metadata.annotations = None should be skipped."""
        mock_list.return_value = iter(
            [
                _k8s_deployment("bare", annotations=None),
            ]
        )

        result = _fetch_ttl_deployments(
            "kubetimer.io/ttl", "kubetimer.io/expires-at", [], []
        )

        assert result == []


class TestTriageDeployments:
    def test_separates_expired_and_future_via_expires_at(self):
        """When expires-at is present, use it directly."""
        scheduler = _create_scheduler_mock()

        deps = [
            TtlDeployment(
                name="old",
                namespace="default",
                uid="u1",
                ttl_value="1h",
                creation_timestamp=CREATION_2H_AGO,
                expires_at=PAST_EXPIRES_AT,
            ),
            TtlDeployment(
                name="new",
                namespace="default",
                uid="u2",
                ttl_value="1h",
                creation_timestamp=CREATION_NOW,
                expires_at=FUTURE_EXPIRES_AT,
            ),
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

    def test_fallback_to_creation_timestamp_plus_duration(self):
        """When expires-at is absent, compute from creation_timestamp + TTL duration."""
        scheduler = _create_scheduler_mock()

        deps = [
            TtlDeployment(
                name="old-no-ea",
                namespace="default",
                uid="u1",
                ttl_value="1h",
                creation_timestamp=CREATION_2H_AGO,
                expires_at=None,
            ),
            TtlDeployment(
                name="new-no-ea",
                namespace="default",
                uid="u2",
                ttl_value="2h",
                creation_timestamp=CREATION_NOW,
                expires_at=None,
            ),
        ]

        expired, scheduled, errors = _triage_deployments(
            deps,
            scheduler,
            "kubetimer.io/ttl",
            "UTC",
            False,
        )

        assert len(expired) == 1
        assert expired[0].name == "old-no-ea"
        assert scheduled == 1
        assert errors == 0

    def test_invalid_ttl_increments_errors(self):
        scheduler = _create_scheduler_mock()

        deps = [
            TtlDeployment(
                name="bad",
                namespace="default",
                uid="u3",
                ttl_value="garbage",
                creation_timestamp=CREATION_NOW,
                expires_at=None,
            ),
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

    def test_schedule_failure_increments_errors(self):
        scheduler = _create_scheduler_mock()
        scheduler.add_job.side_effect = RuntimeError("boom")

        deps = [
            TtlDeployment(
                name="fail",
                namespace="default",
                uid="u3",
                ttl_value="1h",
                creation_timestamp=CREATION_NOW,
                expires_at=FUTURE_EXPIRES_AT,
            ),
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
    @patch("kubetimer.reconcile.orchestrator.list_deployments_all_namespaces_paginated")
    async def test_no_ttl_deployments_returns_early(self, mock_list, memo):
        mock_list.return_value = iter([])  # empty

        await reconcile_existing_deployments(memo)

        # No error, just an early return after log

    @pytest.mark.asyncio
    @patch(
        "kubetimer.reconcile.orchestrator.bulk_delete_expired", new_callable=AsyncMock
    )
    @patch("kubetimer.reconcile.orchestrator.list_deployments_all_namespaces_paginated")
    async def test_all_expired_deletes_all(self, mock_list, mock_bulk, memo):
        mock_list.return_value = iter(
            [
                _k8s_deployment(
                    "old-1",
                    annotations={
                        "kubetimer.io/ttl": "1h",
                        "kubetimer.io/expires-at": PAST_EXPIRES_AT,
                    },
                    creation_timestamp=CREATION_2H_AGO,
                ),
                _k8s_deployment(
                    "old-2",
                    annotations={
                        "kubetimer.io/ttl": "1h",
                        "kubetimer.io/expires-at": PAST_EXPIRES_AT,
                    },
                    creation_timestamp=CREATION_2H_AGO,
                ),
            ]
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
    @patch("kubetimer.reconcile.orchestrator.list_deployments_all_namespaces_paginated")
    async def test_expired_uids_added_to_reconciling_set(
        self, mock_list, mock_bulk, memo
    ):
        """Expired UIDs should be registered in reconciling_uids before deletion."""
        captured_uids = set()

        async def capture_uids(deps, dry_run, **kw):
            captured_uids.update(memo.reconciling_uids)
            return (len(deps), 0)

        mock_bulk.side_effect = capture_uids
        mock_list.return_value = iter(
            [
                _k8s_deployment(
                    "old-1",
                    annotations={
                        "kubetimer.io/ttl": "1h",
                        "kubetimer.io/expires-at": PAST_EXPIRES_AT,
                    },
                    creation_timestamp=CREATION_2H_AGO,
                ),
            ]
        )

        await reconcile_existing_deployments(memo)

        assert "uid-old-1" in captured_uids
        # After bulk delete completes, expired UIDs are removed
        assert "uid-old-1" not in memo.reconciling_uids

    @pytest.mark.asyncio
    @patch(
        "kubetimer.reconcile.orchestrator.bulk_delete_expired", new_callable=AsyncMock
    )
    @patch("kubetimer.reconcile.orchestrator.schedule_deletion_job")
    @patch("kubetimer.reconcile.orchestrator.list_deployments_all_namespaces_paginated")
    async def test_scheduled_uids_remain_in_reconciling_set(
        self, mock_list, mock_schedule, mock_bulk, memo
    ):
        """Scheduled (future) UIDs remain in reconciling_uids after reconcile."""
        mock_list.return_value = iter(
            [
                _k8s_deployment(
                    "new-1",
                    annotations={
                        "kubetimer.io/ttl": "1h",
                        "kubetimer.io/expires-at": FUTURE_EXPIRES_AT,
                    },
                ),
            ]
        )
        mock_schedule.return_value = True

        await reconcile_existing_deployments(memo)

        # Future UIDs stay in the set — cleaned up by delete_deployment_job
        assert "uid-new-1" in memo.reconciling_uids

    @pytest.mark.asyncio
    @patch(
        "kubetimer.reconcile.orchestrator.bulk_delete_expired", new_callable=AsyncMock
    )
    @patch("kubetimer.reconcile.orchestrator.schedule_deletion_job")
    @patch("kubetimer.reconcile.orchestrator.list_deployments_all_namespaces_paginated")
    async def test_mixed_only_expired_uids_removed(
        self, mock_list, mock_schedule, mock_bulk, memo
    ):
        """With mixed deployments, only expired UIDs are removed after reconcile."""
        mock_list.return_value = iter(
            [
                _k8s_deployment(
                    "old",
                    annotations={
                        "kubetimer.io/ttl": "1h",
                        "kubetimer.io/expires-at": PAST_EXPIRES_AT,
                    },
                    creation_timestamp=CREATION_2H_AGO,
                ),
                _k8s_deployment(
                    "new",
                    annotations={
                        "kubetimer.io/ttl": "1h",
                        "kubetimer.io/expires-at": FUTURE_EXPIRES_AT,
                    },
                ),
            ]
        )
        mock_schedule.return_value = True
        mock_bulk.return_value = (1, 0)

        await reconcile_existing_deployments(memo)

        assert "uid-old" not in memo.reconciling_uids
        assert "uid-new" in memo.reconciling_uids

    @pytest.mark.asyncio
    @patch(
        "kubetimer.reconcile.orchestrator.bulk_delete_expired", new_callable=AsyncMock
    )
    @patch("kubetimer.reconcile.orchestrator.schedule_deletion_job")
    @patch("kubetimer.reconcile.orchestrator.list_deployments_all_namespaces_paginated")
    async def test_all_future_schedules_all(
        self, mock_list, mock_schedule, mock_bulk, memo
    ):
        mock_list.return_value = iter(
            [
                _k8s_deployment(
                    "new-1",
                    annotations={
                        "kubetimer.io/ttl": "1h",
                        "kubetimer.io/expires-at": FUTURE_EXPIRES_AT,
                    },
                ),
            ]
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
    @patch("kubetimer.reconcile.orchestrator.list_deployments_all_namespaces_paginated")
    async def test_mixed_expired_and_future(
        self, mock_list, mock_schedule, mock_bulk, memo
    ):
        mock_list.return_value = iter(
            [
                _k8s_deployment(
                    "old",
                    annotations={
                        "kubetimer.io/ttl": "1h",
                        "kubetimer.io/expires-at": PAST_EXPIRES_AT,
                    },
                    creation_timestamp=CREATION_2H_AGO,
                ),
                _k8s_deployment(
                    "new",
                    annotations={
                        "kubetimer.io/ttl": "1h",
                        "kubetimer.io/expires-at": FUTURE_EXPIRES_AT,
                    },
                ),
            ]
        )
        mock_schedule.return_value = True
        mock_bulk.return_value = (1, 0)

        await reconcile_existing_deployments(memo)

        mock_schedule.assert_called_once()
        mock_bulk.assert_awaited_once()
