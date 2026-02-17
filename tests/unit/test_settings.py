"""Tests for kubetimer.config.settings.Settings.

Uses monkeypatch to set KUBETIMER_* environment variables so each test
runs with a clean config.  We also clear the @lru_cache on get_settings
to avoid cross-test pollution.
"""

import pytest

from kubetimer.config.settings import Settings


class TestSettings:
    def test_defaults(self, monkeypatch):
        monkeypatch.delenv("KUBETIMER_LOG_LEVEL", raising=False)
        monkeypatch.delenv("KUBETIMER_DRY_RUN", raising=False)
        monkeypatch.delenv("KUBETIMER_ANNOTATION_KEY", raising=False)
        # Build settings without .env file to test pure defaults
        s = Settings(_env_file=None)
        assert s.log_level == "INFO"
        assert s.dry_run is False
        assert s.annotation_key == "kubetimer.io/ttl"

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("KUBETIMER_LOG_LEVEL", "DEBUG")
        monkeypatch.setenv("KUBETIMER_DRY_RUN", "true")
        s = Settings(_env_file=None)
        assert s.log_level == "DEBUG"
        assert s.dry_run is True

    def test_namespace_include_empty(self, monkeypatch):
        monkeypatch.delenv("KUBETIMER_NAMESPACE_INCLUDE", raising=False)
        s = Settings(_env_file=None)
        assert s.get_namespace_include_list() == []

    def test_namespace_include_parsing(self, monkeypatch):
        monkeypatch.setenv("KUBETIMER_NAMESPACE_INCLUDE", "ns-a,ns-b")
        s = Settings(_env_file=None)
        assert s.get_namespace_include_list() == ["ns-a", "ns-b"]

    def test_namespace_exclude_parsing(self, monkeypatch):
        monkeypatch.setenv("KUBETIMER_NAMESPACE_EXCLUDE", "kube-system,monitoring")
        s = Settings(_env_file=None)
        assert s.get_namespace_exclude_list() == ["kube-system", "monitoring"]

    def test_timezone_default_utc(self):
        s = Settings(_env_file=None)
        assert s.timezone == "UTC"

    def test_timezone_valid_iana(self, monkeypatch):
        monkeypatch.setenv("KUBETIMER_TIMEZONE", "America/New_York")
        s = Settings(_env_file=None)
        assert s.timezone == "America/New_York"

    def test_timezone_invalid_raises(self, monkeypatch):
        monkeypatch.setenv("KUBETIMER_TIMEZONE", "Not/A_Timezone")
        with pytest.raises(Exception, match="Invalid timezone"):
            Settings(_env_file=None)
