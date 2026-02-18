"""Tests for kubetimer.handlers.registry.configure_memo.

Verifies that configure_memo correctly copies every Settings value
onto the kopf.Memo namespace.
"""

from types import SimpleNamespace

from kubetimer.config.settings import Settings
from kubetimer.handlers.registry import configure_memo


class TestConfigureMemo:
    def test_populates_all_fields(self, monkeypatch):
        monkeypatch.setenv("KUBETIMER_DRY_RUN", "true")
        monkeypatch.setenv("KUBETIMER_TIMEZONE", "America/Chicago")
        monkeypatch.setenv("KUBETIMER_NAMESPACE_INCLUDE", "staging, prod")
        monkeypatch.setenv("KUBETIMER_NAMESPACE_EXCLUDE", "kube-system")
        settings = Settings(_env_file=None)

        memo = SimpleNamespace()
        configure_memo(memo, settings)

        assert memo.annotation_key == "kubetimer.io/ttl"
        assert memo.dry_run is True
        assert memo.timezone == "America/Chicago"
        assert memo.namespace_include == frozenset({"staging", "prod"})
        assert memo.namespace_exclude == frozenset({"kube-system"})
        assert memo.max_concurrent_deletes == 25
        assert memo.config_loaded is True

    def test_defaults_produce_expected_memo(self, monkeypatch):
        # Clean env to get pure defaults
        for key in (
            "KUBETIMER_DRY_RUN",
            "KUBETIMER_TIMEZONE",
            "KUBETIMER_NAMESPACE_INCLUDE",
            "KUBETIMER_NAMESPACE_EXCLUDE",
            "KUBETIMER_ANNOTATION_KEY",
        ):
            monkeypatch.delenv(key, raising=False)

        settings = Settings(_env_file=None)
        memo = SimpleNamespace()
        configure_memo(memo, settings)

        assert memo.dry_run is False
        assert memo.timezone == "UTC"
        assert memo.namespace_include == frozenset()
        assert memo.namespace_exclude == frozenset(
            {
                "kube-system",
                "kube-public",
                "kube-node-lease",
            }
        )
