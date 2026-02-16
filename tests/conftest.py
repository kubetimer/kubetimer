"""Shared fixtures for KubeTimer tests."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def memo():
    """A lightweight stand-in for kopf.Memo.

    kopf.Memo is essentially a dict-like namespace, so SimpleNamespace
    gives us the same dot-access without importing kopf itself.
    """
    m = SimpleNamespace()
    m.annotation_key = "kubetimer.io/ttl"
    m.dry_run = False
    m.timezone = "UTC"
    m.namespace_include = []
    m.namespace_exclude = ["kube-system", "kube-public", "kube-node-lease"]
    m.config_loaded = True
    m.scheduler = MagicMock()
    m.scheduler.running = True
    return m
