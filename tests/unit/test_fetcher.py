"""Tests for kubetimer.reconcile.fetcher.

All K8s API calls are mocked at the apps_v1_client boundary so no
cluster is needed.

Covers:
  - get_namespaced_deployment: success, ApiException → None
  - delete_namespaced_deployment: delegates to API with V1DeleteOptions
  - async_delete_namespaced_deployment: offloads to thread
  - list_deployments_all_namespaces: pass-through + kwargs forwarding
"""

from unittest.mock import MagicMock, patch

import pytest
from kubernetes.client.exceptions import ApiException

from kubetimer.reconcile.fetcher import (
    async_delete_namespaced_deployment,
    delete_namespaced_deployment,
    get_namespaced_deployment,
    list_deployments_all_namespaces,
)


@pytest.fixture
def mock_apps_v1():
    """Patch apps_v1_client() and return the mock API instance."""
    with patch("kubetimer.reconcile.fetcher.apps_v1_client") as factory:
        api = MagicMock()
        factory.return_value = api
        yield api


class TestGetNamespacedDeployment:
    def test_returns_deployment_on_success(self, mock_apps_v1):
        sentinel = object()
        mock_apps_v1.read_namespaced_deployment.return_value = sentinel

        result = get_namespaced_deployment("default", "web")

        assert result is sentinel
        mock_apps_v1.read_namespaced_deployment.assert_called_once_with(
            name="web", namespace="default",
        )

    def test_returns_none_on_api_exception(self, mock_apps_v1):
        mock_apps_v1.read_namespaced_deployment.side_effect = ApiException(status=404)

        result = get_namespaced_deployment("default", "gone")

        assert result is None


class TestDeleteNamespacedDeployment:
    def test_calls_api_with_delete_options(self, mock_apps_v1):
        delete_namespaced_deployment("staging", "api-server")

        mock_apps_v1.delete_namespaced_deployment.assert_called_once()
        _, kwargs = mock_apps_v1.delete_namespaced_deployment.call_args
        assert kwargs["name"] == "api-server"
        assert kwargs["namespace"] == "staging"

    def test_propagates_api_exception(self, mock_apps_v1):
        mock_apps_v1.delete_namespaced_deployment.side_effect = ApiException(status=403)

        with pytest.raises(ApiException):
            delete_namespaced_deployment("default", "forbidden")


class TestAsyncDeleteNamespacedDeployment:
    @pytest.mark.asyncio
    async def test_offloads_to_thread(self, mock_apps_v1):
        """The async wrapper should ultimately call the sync delete."""
        await async_delete_namespaced_deployment("default", "web")

        mock_apps_v1.delete_namespaced_deployment.assert_called_once()


class TestListDeploymentsAllNamespaces:
    def test_returns_api_result(self, mock_apps_v1):
        sentinel = object()
        mock_apps_v1.list_deployment_for_all_namespaces.return_value = sentinel

        result = list_deployments_all_namespaces()

        assert result is sentinel

    def test_forwards_kwargs(self, mock_apps_v1):
        list_deployments_all_namespaces(label_selector="app=web", limit=100)

        mock_apps_v1.list_deployment_for_all_namespaces.assert_called_once_with(
            label_selector="app=web", limit=100,
        )
