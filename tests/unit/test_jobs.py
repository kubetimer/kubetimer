"""Tests for kubetimer.scheduler.jobs.

Covers:
  - _make_job_id: format check
  - schedule_deletion_job: happy path, exception → False
  - cancel_deletion_job: happy path, JobLookupError → False, generic error → False
  - delete_deployment_job: all verification branches (already deleted,
    uid mismatch, annotation removed, TTL no longer expired, invalid TTL,
    dry_run, actual delete, unexpected exception)
"""

from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from apscheduler.jobstores.base import JobLookupError

from tests.conftest import _create_scheduler_mock
from kubetimer.scheduler.jobs import (
    _make_job_id,
    cancel_deletion_job,
    delete_deployment_job,
    schedule_deletion_job,
)

FUTURE = datetime.now(timezone.utc) + timedelta(hours=1)
PAST = datetime.now(timezone.utc) - timedelta(hours=1)


class TestMakeJobId:
    def test_format(self):
        assert _make_job_id("default", "web", "uid-123") == "default/web/uid-123"


class TestScheduleDeletionJob:
    def test_returns_true_on_success(self):
        scheduler = _create_scheduler_mock()

        result = schedule_deletion_job(
            scheduler,
            "default",
            "web",
            "uid-1",
            FUTURE,
            "kubetimer.io/ttl",
            "UTC",
            False,
        )

        assert result is True
        scheduler.add_job.assert_called_once()

    def test_passes_correct_kwargs_to_add_job(self):
        scheduler = _create_scheduler_mock()

        schedule_deletion_job(
            scheduler,
            "ns",
            "dep",
            "uid-2",
            FUTURE,
            "kubetimer.io/ttl",
            "UTC",
            True,
        )

        _, call_kwargs = scheduler.add_job.call_args
        assert call_kwargs["id"] == "ns/dep/uid-2"
        assert call_kwargs["replace_existing"] is True
        job_kwargs = call_kwargs["kwargs"]
        assert job_kwargs["namespace"] == "ns"
        assert job_kwargs["dry_run"] is True

    def test_returns_false_on_exception(self):
        scheduler = _create_scheduler_mock()
        scheduler.add_job.side_effect = RuntimeError("scheduler down")

        result = schedule_deletion_job(
            scheduler,
            "default",
            "web",
            "uid-3",
            FUTURE,
            "kubetimer.io/ttl",
            "UTC",
            False,
        )

        assert result is False


class TestCancelDeletionJob:
    def test_returns_true_on_success(self):
        scheduler = _create_scheduler_mock()

        result = cancel_deletion_job(scheduler, "default", "web", "uid-1")

        assert result is True
        scheduler.remove_job.assert_called_once_with("default/web/uid-1")

    def test_returns_false_on_job_lookup_error(self):
        scheduler = _create_scheduler_mock()
        scheduler.remove_job.side_effect = JobLookupError("default/web/uid-1")

        result = cancel_deletion_job(scheduler, "default", "web", "uid-1")

        assert result is False

    def test_returns_false_on_generic_error(self):
        scheduler = _create_scheduler_mock()
        scheduler.remove_job.side_effect = RuntimeError("oops")

        result = cancel_deletion_job(scheduler, "default", "web", "uid-1")

        assert result is False


def _mock_deployment(uid="uid-1", annotations=None):
    dep = SimpleNamespace()
    dep.metadata = SimpleNamespace()
    dep.metadata.uid = uid
    dep.metadata.annotations = annotations
    return dep


class TestDeleteDeploymentJob:
    @pytest.mark.asyncio
    @patch(
        "kubetimer.scheduler.jobs.async_delete_namespaced_deployment",
        new_callable=AsyncMock,
    )
    @patch("kubetimer.scheduler.jobs.get_namespaced_deployment")
    async def test_deletes_when_expired(self, mock_get, mock_delete):
        past_ttl = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        mock_get.return_value = _mock_deployment(
            uid="uid-1",
            annotations={"kubetimer.io/ttl": past_ttl},
        )

        await delete_deployment_job(
            "default",
            "web",
            "uid-1",
            "kubetimer.io/ttl",
            "UTC",
            dry_run=False,
        )

        mock_delete.assert_awaited_once_with("default", "web")

    @pytest.mark.asyncio
    @patch(
        "kubetimer.scheduler.jobs.async_delete_namespaced_deployment",
        new_callable=AsyncMock,
    )
    @patch("kubetimer.scheduler.jobs.get_namespaced_deployment")
    async def test_dry_run_skips_delete(self, mock_get, mock_delete):
        past_ttl = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        mock_get.return_value = _mock_deployment(
            uid="uid-1",
            annotations={"kubetimer.io/ttl": past_ttl},
        )

        await delete_deployment_job(
            "default",
            "web",
            "uid-1",
            "kubetimer.io/ttl",
            "UTC",
            dry_run=True,
        )

        mock_delete.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("kubetimer.scheduler.jobs.get_namespaced_deployment")
    async def test_skips_when_already_deleted(self, mock_get):
        mock_get.return_value = None

        # Should not raise
        await delete_deployment_job(
            "default",
            "web",
            "uid-1",
            "kubetimer.io/ttl",
            "UTC",
            False,
        )

    @pytest.mark.asyncio
    @patch(
        "kubetimer.scheduler.jobs.async_delete_namespaced_deployment",
        new_callable=AsyncMock,
    )
    @patch("kubetimer.scheduler.jobs.get_namespaced_deployment")
    async def test_skips_on_uid_mismatch(self, mock_get, mock_delete):
        mock_get.return_value = _mock_deployment(uid="different-uid")

        await delete_deployment_job(
            "default",
            "web",
            "uid-1",
            "kubetimer.io/ttl",
            "UTC",
            False,
        )

        mock_delete.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(
        "kubetimer.scheduler.jobs.async_delete_namespaced_deployment",
        new_callable=AsyncMock,
    )
    @patch("kubetimer.scheduler.jobs.get_namespaced_deployment")
    async def test_skips_when_annotation_removed(self, mock_get, mock_delete):
        mock_get.return_value = _mock_deployment(uid="uid-1", annotations={})

        await delete_deployment_job(
            "default",
            "web",
            "uid-1",
            "kubetimer.io/ttl",
            "UTC",
            False,
        )

        mock_delete.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(
        "kubetimer.scheduler.jobs.async_delete_namespaced_deployment",
        new_callable=AsyncMock,
    )
    @patch("kubetimer.scheduler.jobs.get_namespaced_deployment")
    async def test_skips_when_ttl_no_longer_expired(self, mock_get, mock_delete):
        future_ttl = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
        mock_get.return_value = _mock_deployment(
            uid="uid-1",
            annotations={"kubetimer.io/ttl": future_ttl},
        )

        await delete_deployment_job(
            "default",
            "web",
            "uid-1",
            "kubetimer.io/ttl",
            "UTC",
            False,
        )

        mock_delete.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(
        "kubetimer.scheduler.jobs.async_delete_namespaced_deployment",
        new_callable=AsyncMock,
    )
    @patch("kubetimer.scheduler.jobs.get_namespaced_deployment")
    async def test_skips_on_invalid_ttl(self, mock_get, mock_delete):
        mock_get.return_value = _mock_deployment(
            uid="uid-1",
            annotations={"kubetimer.io/ttl": "garbage"},
        )

        await delete_deployment_job(
            "default",
            "web",
            "uid-1",
            "kubetimer.io/ttl",
            "UTC",
            False,
        )

        mock_delete.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("kubetimer.scheduler.jobs.get_namespaced_deployment")
    async def test_catches_unexpected_exception(self, mock_get):
        mock_get.side_effect = RuntimeError("connection reset")

        # Should not propagate — the job logs and swallows
        await delete_deployment_job(
            "default",
            "web",
            "uid-1",
            "kubetimer.io/ttl",
            "UTC",
            False,
        )

    @pytest.mark.asyncio
    @patch(
        "kubetimer.scheduler.jobs.async_delete_namespaced_deployment",
        new_callable=AsyncMock,
    )
    @patch("kubetimer.scheduler.jobs.get_namespaced_deployment")
    async def test_discards_uid_from_reconciling_uids_on_success(
        self, mock_get, mock_delete
    ):
        """UID should be removed from reconciling_uids when job completes."""
        past_ttl = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        mock_get.return_value = _mock_deployment(
            uid="uid-1",
            annotations={"kubetimer.io/ttl": past_ttl},
        )
        uids = {"uid-1", "uid-other"}

        await delete_deployment_job(
            "default",
            "web",
            "uid-1",
            "kubetimer.io/ttl",
            "UTC",
            dry_run=False,
            reconciling_uids=uids,
        )

        assert "uid-1" not in uids
        assert "uid-other" in uids

    @pytest.mark.asyncio
    @patch("kubetimer.scheduler.jobs.get_namespaced_deployment")
    async def test_discards_uid_from_reconciling_uids_on_error(self, mock_get):
        """UID should be removed from reconciling_uids even when job fails."""
        mock_get.side_effect = RuntimeError("connection reset")
        uids = {"uid-1"}

        await delete_deployment_job(
            "default",
            "web",
            "uid-1",
            "kubetimer.io/ttl",
            "UTC",
            False,
            reconciling_uids=uids,
        )

        assert "uid-1" not in uids

    @pytest.mark.asyncio
    @patch("kubetimer.scheduler.jobs.get_namespaced_deployment")
    async def test_discards_uid_from_reconciling_uids_on_early_return(self, mock_get):
        """UID should be removed even when deployment is already gone."""
        mock_get.return_value = None
        uids = {"uid-1"}

        await delete_deployment_job(
            "default",
            "web",
            "uid-1",
            "kubetimer.io/ttl",
            "UTC",
            False,
            reconciling_uids=uids,
        )

        assert "uid-1" not in uids

    @pytest.mark.asyncio
    @patch(
        "kubetimer.scheduler.jobs.async_delete_namespaced_deployment",
        new_callable=AsyncMock,
    )
    @patch("kubetimer.scheduler.jobs.get_namespaced_deployment")
    async def test_no_error_when_reconciling_uids_is_none(
        self, mock_get, mock_delete
    ):
        """No error when reconciling_uids is not provided (normal event path)."""
        past_ttl = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        mock_get.return_value = _mock_deployment(
            uid="uid-1",
            annotations={"kubetimer.io/ttl": past_ttl},
        )

        # Should not raise — reconciling_uids defaults to None
        await delete_deployment_job(
            "default",
            "web",
            "uid-1",
            "kubetimer.io/ttl",
            "UTC",
            dry_run=False,
        )


class TestScheduleDeletionJobReconcilingUids:
    """Tests for reconciling_uids forwarding in schedule_deletion_job."""

    def test_forwards_reconciling_uids_in_kwargs(self):
        scheduler = _create_scheduler_mock()
        uids = {"uid-1", "uid-2"}

        schedule_deletion_job(
            scheduler,
            "default",
            "web",
            "uid-1",
            FUTURE,
            "kubetimer.io/ttl",
            "UTC",
            False,
            reconciling_uids=uids,
        )

        _, call_kwargs = scheduler.add_job.call_args
        assert call_kwargs["kwargs"]["reconciling_uids"] is uids

    def test_omits_reconciling_uids_when_none(self):
        scheduler = _create_scheduler_mock()

        schedule_deletion_job(
            scheduler,
            "default",
            "web",
            "uid-1",
            FUTURE,
            "kubetimer.io/ttl",
            "UTC",
            False,
        )

        _, call_kwargs = scheduler.add_job.call_args
        assert "reconciling_uids" not in call_kwargs["kwargs"]
