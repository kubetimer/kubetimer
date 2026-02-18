"""Shared fixtures for KubeTimer tests."""

from types import SimpleNamespace
from unittest.mock import MagicMock, PropertyMock

import pytest


def _create_scheduler_mock(running: bool = True):
    """Create a MagicMock that simulates AsyncIOScheduler behavior.

    Configures:
    - running: property that can be get/set
    - start(): sets running = True
    - shutdown(wait=True): sets running = False
    - add_job(): returns a mock Job object
    - remove_job(): succeeds by default (can be overridden with side_effect)
    """
    scheduler = MagicMock()

    state = {"running": running}

    running_prop = PropertyMock(return_value=state["running"])

    def running_setter(obj, value):
        state["running"] = value
        running_prop.return_value = value

    type(scheduler).running = running_prop
    scheduler._set_running = lambda value: running_setter(scheduler, value)

    def start_side_effect():
        scheduler._set_running(True)

    scheduler.start.side_effect = start_side_effect

    def shutdown_side_effect(wait=True):
        scheduler._set_running(False)

    scheduler.shutdown.side_effect = shutdown_side_effect

    mock_job = MagicMock()
    mock_job.id = "mock-job-id"
    scheduler.add_job.return_value = mock_job

    return scheduler


@pytest.fixture
def memo():
    """A lightweight stand-in for kopf.Memo.

    kopf.Memo is essentially a dict-like namespace, so SimpleNamespace
    gives us the same dot-access without importing kopf itself.

    The scheduler is a configured MagicMock that simulates AsyncIOScheduler:
    - start() and shutdown() properly update the running state
    - add_job() returns a mock Job object
    - remove_job() succeeds by default
    - Tests can modify running via scheduler._set_running(bool)
    """
    m = SimpleNamespace()
    m.annotation_key = "kubetimer.io/ttl"
    m.dry_run = False
    m.timezone = "UTC"
    m.namespace_include = frozenset()
    m.namespace_exclude = frozenset({"kube-system", "kube-public", "kube-node-lease"})
    m.max_concurrent_deletes = 25
    m.config_loaded = True
    m.scheduler = _create_scheduler_mock(running=True)
    m.reconciling_uids = set()
    m.reconciliation_done = True
    return m
