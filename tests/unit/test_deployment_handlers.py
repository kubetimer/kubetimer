from datetime import datetime, timezone, timedelta
from unittest.mock import patch, AsyncMock

import pytest

from kubetimer.handlers.deployment import (
    on_deployment_created_with_ttl,
    on_ttl_annotation_changed,
    on_deployment_deleted_with_ttl,
)


class TestOnDeploymentCreated:
    @patch("kubetimer.handlers.deployment.async_patch_deployment_annotations", new_callable=AsyncMock)
    @patch("kubetimer.handlers.deployment.schedule_deletion_job")
    async def test_future_ttl_schedules_job(self, mock_schedule, mock_patch, memo):
        await on_deployment_created_with_ttl(
            namespace="default",
            name="web",
            uid="uid-1",
            annotations={"kubetimer.io/ttl": "1h"},
            memo=memo,
        )
        mock_schedule.assert_called_once()
        mock_patch.assert_awaited_once()
        # Verify expires-at annotation is written
        patch_args = mock_patch.call_args
        annotations = patch_args[0][2]
        assert "kubetimer.io/expires-at" in annotations

    @patch("kubetimer.handlers.deployment.async_patch_deployment_annotations", new_callable=AsyncMock)
    @patch("kubetimer.handlers.deployment.schedule_deletion_job")
    async def test_excluded_namespace_skips(self, mock_schedule, mock_patch, memo):
        await on_deployment_created_with_ttl(
            namespace="kube-system",
            name="coredns",
            uid="uid-2",
            annotations={"kubetimer.io/ttl": "1h"},
            memo=memo,
        )
        mock_schedule.assert_not_called()
        mock_patch.assert_not_awaited()

    @patch("kubetimer.handlers.deployment.async_patch_deployment_annotations", new_callable=AsyncMock)
    @patch("kubetimer.handlers.deployment.schedule_deletion_job")
    async def test_missing_annotation_skips(self, mock_schedule, mock_patch, memo):
        await on_deployment_created_with_ttl(
            namespace="default",
            name="web",
            uid="uid-3",
            annotations={},
            memo=memo,
        )
        mock_schedule.assert_not_called()
        mock_patch.assert_not_awaited()

    @patch("kubetimer.handlers.deployment.async_patch_deployment_annotations", new_callable=AsyncMock)
    @patch("kubetimer.handlers.deployment.schedule_deletion_job")
    async def test_skips_when_uid_in_reconciling_set(
        self, mock_schedule, mock_patch, memo
    ):
        """If reconciliation is processing this UID, the handler should skip."""
        memo.reconciling_uids.add("uid-rc")

        await on_deployment_created_with_ttl(
            namespace="default",
            name="web",
            uid="uid-rc",
            annotations={"kubetimer.io/ttl": "1h"},
            memo=memo,
        )

        mock_schedule.assert_not_called()
        mock_patch.assert_not_awaited()

    @patch("kubetimer.handlers.deployment.async_patch_deployment_annotations", new_callable=AsyncMock)
    @patch("kubetimer.handlers.deployment.schedule_deletion_job")
    async def test_processes_when_uid_not_in_reconciling_set(
        self, mock_schedule, mock_patch, memo
    ):
        """If reconciling_uids exists but doesn't contain this UID, proceed."""
        memo.reconciling_uids = {"other-uid"}

        await on_deployment_created_with_ttl(
            namespace="default",
            name="web",
            uid="uid-1",
            annotations={"kubetimer.io/ttl": "30m"},
            memo=memo,
        )

        mock_schedule.assert_called_once()
        mock_patch.assert_awaited_once()

    @patch("kubetimer.handlers.deployment.async_patch_deployment_annotations", new_callable=AsyncMock)
    @patch("kubetimer.handlers.deployment.schedule_deletion_job")
    async def test_invalid_ttl_skips(self, mock_schedule, mock_patch, memo):
        await on_deployment_created_with_ttl(
            namespace="default",
            name="web",
            uid="uid-4",
            annotations={"kubetimer.io/ttl": "not-valid"},
            memo=memo,
        )
        mock_schedule.assert_not_called()
        mock_patch.assert_not_awaited()

    @patch("kubetimer.handlers.deployment.async_patch_deployment_annotations", new_callable=AsyncMock)
    @patch("kubetimer.handlers.deployment.schedule_deletion_job")
    async def test_expires_at_is_in_the_future(self, mock_schedule, mock_patch, memo):
        """The expires-at datetime written should be approximately now + duration."""
        before = datetime.now(timezone.utc)

        await on_deployment_created_with_ttl(
            namespace="default",
            name="web",
            uid="uid-5",
            annotations={"kubetimer.io/ttl": "2h"},
            memo=memo,
        )

        after = datetime.now(timezone.utc)
        patch_annotations = mock_patch.call_args[0][2]
        expires_at_str = patch_annotations["kubetimer.io/expires-at"]
        expires_at = datetime.fromisoformat(expires_at_str)
        assert before + timedelta(hours=2) <= expires_at <= after + timedelta(hours=2)


class TestOnTtlAnnotationChanged:
    @patch("kubetimer.handlers.deployment.async_patch_deployment_annotations", new_callable=AsyncMock)
    @patch("kubetimer.handlers.deployment.schedule_deletion_job")
    async def test_new_ttl_reschedules(self, mock_schedule, mock_patch, memo):
        await on_ttl_annotation_changed(
            namespace="default",
            name="api",
            uid="uid-4",
            old="30m",
            new="2h",
            memo=memo,
        )
        mock_schedule.assert_called_once()
        mock_patch.assert_awaited_once()
        annotations = mock_patch.call_args[0][2]
        assert "kubetimer.io/expires-at" in annotations

    @patch("kubetimer.handlers.deployment.async_patch_deployment_annotations", new_callable=AsyncMock)
    @patch("kubetimer.handlers.deployment.cancel_deletion_job")
    async def test_annotation_removed_cancels_and_clears_expires_at(
        self, mock_cancel, mock_patch, memo
    ):
        await on_ttl_annotation_changed(
            namespace="default",
            name="api",
            uid="uid-5",
            old="1h",
            new=None,
            memo=memo,
        )
        mock_cancel.assert_called_once()
        mock_patch.assert_awaited_once()
        annotations = mock_patch.call_args[0][2]
        assert annotations["kubetimer.io/expires-at"] is None

    @patch("kubetimer.handlers.deployment.async_patch_deployment_annotations", new_callable=AsyncMock)
    @patch("kubetimer.handlers.deployment.cancel_deletion_job")
    async def test_excluded_namespace_cancels(self, mock_cancel, mock_patch, memo):
        await on_ttl_annotation_changed(
            namespace="kube-system",
            name="coredns",
            uid="uid-6",
            old=None,
            new="1h",
            memo=memo,
        )
        mock_cancel.assert_called_once()

    @patch("kubetimer.handlers.deployment.async_patch_deployment_annotations", new_callable=AsyncMock)
    @patch("kubetimer.handlers.deployment.cancel_deletion_job")
    async def test_invalid_ttl_cancels_and_clears_expires_at(
        self, mock_cancel, mock_patch, memo
    ):
        await on_ttl_annotation_changed(
            namespace="default",
            name="api",
            uid="uid-7",
            old="1h",
            new="not-valid",
            memo=memo,
        )
        mock_cancel.assert_called_once()
        mock_patch.assert_awaited_once()
        annotations = mock_patch.call_args[0][2]
        assert annotations["kubetimer.io/expires-at"] is None

    @patch("kubetimer.handlers.deployment.async_patch_deployment_annotations", new_callable=AsyncMock)
    @patch("kubetimer.handlers.deployment.cancel_deletion_job")
    @patch("kubetimer.handlers.deployment.schedule_deletion_job")
    async def test_skips_when_uid_in_reconciling_set(
        self, mock_schedule, mock_cancel, mock_patch, memo
    ):
        """If reconciliation owns this UID, skip all processing."""
        memo.reconciling_uids.add("uid-rc")

        await on_ttl_annotation_changed(
            namespace="default",
            name="api",
            uid="uid-rc",
            old="30m",
            new="2h",
            memo=memo,
        )

        mock_schedule.assert_not_called()
        mock_cancel.assert_not_called()
        mock_patch.assert_not_awaited()


class TestOnDeploymentDeleted:
    @pytest.mark.asyncio
    @patch("kubetimer.handlers.deployment.cancel_deletion_job")
    async def test_cancels_scheduled_job(self, mock_cancel, memo):
        await on_deployment_deleted_with_ttl(
            namespace="default",
            name="web",
            uid="uid-8",
            memo=memo,
        )
        mock_cancel.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_scheduler_does_not_crash(self):
        """If memo doesn't have a scheduler yet, handler should not raise."""
        from types import SimpleNamespace

        memo = SimpleNamespace()  # no .scheduler attribute
        await on_deployment_deleted_with_ttl(
            namespace="default",
            name="web",
            uid="uid-9",
            memo=memo,
        )
