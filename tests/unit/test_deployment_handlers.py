from datetime import datetime, timezone, timedelta
from unittest.mock import patch

from kubetimer.handlers.deployment import (
    on_deployment_created_with_ttl,
    on_ttl_annotation_changed,
    on_deployment_deleted_with_ttl,
)

FUTURE_TTL = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
PAST_TTL = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()


class TestOnDeploymentCreated:
    @patch("kubetimer.handlers.deployment.schedule_deletion_job")
    async def test_future_ttl_schedules_job(self, mock_schedule, memo):
        await on_deployment_created_with_ttl(
            namespace="default",
            name="web",
            uid="uid-1",
            annotations={"kubetimer.io/ttl": FUTURE_TTL},
            memo=memo,
        )
        mock_schedule.assert_called_once()

    @patch("kubetimer.handlers.deployment.async_delete_namespaced_deployment")
    async def test_expired_ttl_deletes_immediately(self, mock_delete, memo):
        await on_deployment_created_with_ttl(
            namespace="default",
            name="web",
            uid="uid-1",
            annotations={"kubetimer.io/ttl": PAST_TTL},
            memo=memo,
        )
        mock_delete.assert_awaited_once_with("default", "web")

    @patch("kubetimer.handlers.deployment.schedule_deletion_job")
    async def test_excluded_namespace_skips(self, mock_schedule, memo):
        await on_deployment_created_with_ttl(
            namespace="kube-system",
            name="coredns",
            uid="uid-2",
            annotations={"kubetimer.io/ttl": FUTURE_TTL},
            memo=memo,
        )
        mock_schedule.assert_not_called()

    @patch("kubetimer.handlers.deployment.schedule_deletion_job")
    async def test_missing_annotation_skips(self, mock_schedule, memo):
        await on_deployment_created_with_ttl(
            namespace="default",
            name="web",
            uid="uid-3",
            annotations={},
            memo=memo,
        )
        mock_schedule.assert_not_called()

    @patch("kubetimer.handlers.deployment.async_delete_namespaced_deployment")
    @patch("kubetimer.handlers.deployment.schedule_deletion_job")
    async def test_skips_when_uid_in_reconciling_set(
        self, mock_schedule, mock_delete, memo
    ):
        """If reconciliation is processing this UID, the handler should skip."""
        memo.reconciling_uids.add("uid-rc")

        await on_deployment_created_with_ttl(
            namespace="default",
            name="web",
            uid="uid-rc",
            annotations={"kubetimer.io/ttl": PAST_TTL},
            memo=memo,
        )

        mock_delete.assert_not_awaited()
        mock_schedule.assert_not_called()

    @patch("kubetimer.handlers.deployment.async_delete_namespaced_deployment")
    async def test_processes_when_uid_not_in_reconciling_set(
        self, mock_delete, memo
    ):
        """If reconciling_uids exists but doesn't contain this UID, proceed."""
        memo.reconciling_uids = {"other-uid"}

        await on_deployment_created_with_ttl(
            namespace="default",
            name="web",
            uid="uid-1",
            annotations={"kubetimer.io/ttl": PAST_TTL},
            memo=memo,
        )

        mock_delete.assert_awaited_once_with("default", "web")


class TestOnTtlAnnotationChanged:
    @patch("kubetimer.handlers.deployment.schedule_deletion_job")
    def test_new_ttl_reschedules(self, mock_schedule, memo):
        on_ttl_annotation_changed(
            namespace="default",
            name="api",
            uid="uid-4",
            old=PAST_TTL,
            new=FUTURE_TTL,
            memo=memo,
        )
        mock_schedule.assert_called_once()

    @patch("kubetimer.handlers.deployment.cancel_deletion_job")
    def test_annotation_removed_cancels(self, mock_cancel, memo):
        on_ttl_annotation_changed(
            namespace="default",
            name="api",
            uid="uid-5",
            old=FUTURE_TTL,
            new=None,
            memo=memo,
        )
        mock_cancel.assert_called_once()

    @patch("kubetimer.handlers.deployment.cancel_deletion_job")
    def test_excluded_namespace_cancels(self, mock_cancel, memo):
        on_ttl_annotation_changed(
            namespace="kube-system",
            name="coredns",
            uid="uid-6",
            old=None,
            new=FUTURE_TTL,
            memo=memo,
        )
        mock_cancel.assert_called_once()

    @patch("kubetimer.handlers.deployment.cancel_deletion_job")
    def test_invalid_ttl_cancels(self, mock_cancel, memo):
        on_ttl_annotation_changed(
            namespace="default",
            name="api",
            uid="uid-7",
            old=FUTURE_TTL,
            new="not-a-date",
            memo=memo,
        )
        mock_cancel.assert_called_once()

    @patch("kubetimer.handlers.deployment.cancel_deletion_job")
    @patch("kubetimer.handlers.deployment.schedule_deletion_job")
    def test_skips_when_uid_in_reconciling_set(
        self, mock_schedule, mock_cancel, memo
    ):
        """If reconciliation owns this UID, skip all processing."""
        memo.reconciling_uids.add("uid-rc")

        on_ttl_annotation_changed(
            namespace="default",
            name="api",
            uid="uid-rc",
            old=PAST_TTL,
            new=FUTURE_TTL,
            memo=memo,
        )

        mock_schedule.assert_not_called()
        mock_cancel.assert_not_called()


class TestOnDeploymentDeleted:
    @patch("kubetimer.handlers.deployment.cancel_deletion_job")
    def test_cancels_scheduled_job(self, mock_cancel, memo):
        on_deployment_deleted_with_ttl(
            namespace="default",
            name="web",
            uid="uid-8",
            memo=memo,
        )
        mock_cancel.assert_called_once()

    def test_no_scheduler_does_not_crash(self):
        """If memo doesn't have a scheduler yet, handler should not raise."""
        from types import SimpleNamespace

        memo = SimpleNamespace()  # no .scheduler attribute
        on_deployment_deleted_with_ttl(
            namespace="default",
            name="web",
            uid="uid-9",
            memo=memo,
        )
