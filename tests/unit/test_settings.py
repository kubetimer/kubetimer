"""Tests for kubetimer.config.settings.Settings.

Uses monkeypatch to set KUBETIMER_* environment variables so each test
runs with a clean config.  We also clear the @lru_cache on get_settings
to avoid cross-test pollution.
"""

import pytest

from kubetimer.config.settings import (
    Settings,
    _validate_prefix,
    _validate_name,
    _validate_prefix_label,
)


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

    def test_validate_prefix_passing(self):
        assert _validate_prefix("validprefix.io")
        assert _validate_prefix("my-team.example.com")

    def test_validate_prefix_invalid_size(self):
        max_length = 253
        prefix = "a" * (max_length + 1)
        assert not _validate_prefix(prefix)

    def test_validate_prefix_invalid_characters(self):
        prefix = "invalid_prefix!"
        assert not _validate_prefix(prefix)

    def test_validate_prefix_empty(self):
        prefix = ""
        assert not _validate_prefix(prefix)

    def test_validate_name_valid(self):
        assert _validate_name("valid-name_123")

    def test_validate_name_too_long(self):
        max_length = 63
        long_name = "a" * (max_length + 1)
        assert not _validate_name(long_name)

    def test_validate_name_invalid_start_end(self):
        assert not _validate_name("-invalid")
        assert not _validate_name("invalid-")
        assert not _validate_name(".invalid")
        assert not _validate_name("invalid.")
        assert not _validate_name("_invalid")
        assert not _validate_name("invalid_")

    def test_validate_name_invalid_characters(self):
        assert not _validate_name("invalid name!")

    def test_validate_name_empty(self):
        assert not _validate_name("")

    def test_settings_validate_annotation_key_invalid_prefix(self, monkeypatch):
        monkeypatch.setenv("KUBETIMER_ANNOTATION_KEY", ".invalidPrefix/annotation")
        error_pattern = (
            r"Invalid annotation key prefix or name: '\.invalidPrefix'/'annotation'\."
        )
        with pytest.raises(
            Exception,
            match=error_pattern,
        ):
            Settings(_env_file=None)

    def test_settings_validate_annotation_key_invalid_name(self, monkeypatch):
        monkeypatch.setenv("KUBETIMER_ANNOTATION_KEY", "validprefix/InvalidName!")
        error_pattern = (
            r"Invalid annotation key prefix or name: 'validprefix'/'InvalidName!'\."
        )
        with pytest.raises(
            Exception,
            match=error_pattern,
        ):
            Settings(_env_file=None)

    def test_settings_validate_annotation_key_empty(self, monkeypatch):
        monkeypatch.setenv("KUBETIMER_ANNOTATION_KEY", "")
        errormsg = "Annotation key cannot be empty."
        with pytest.raises(Exception, match=errormsg):
            Settings(_env_file=None)

    def test_settings_validate_annotation_key_valid(self, monkeypatch):
        monkeypatch.setenv("KUBETIMER_ANNOTATION_KEY", "validprefix/valid-name_123")
        s = Settings(_env_file=None)
        assert s.annotation_key == "validprefix/valid-name_123"

    def test_validate_prefix_label_passing(self):
        assert _validate_prefix_label("validprefix")
        assert _validate_prefix_label("my-team")
        assert _validate_prefix_label("myteam123")

    def test_validate_prefix_label_invalid(self):
        label = "invalid_label"
        assert not _validate_prefix_label(label)

    def test_validate_prefix_label_invalid_characters(self):
        label = "invalid_label!"
        assert not _validate_prefix_label(label)

    def test_validate_prefix_label_empty(self):
        label = ""
        assert _validate_prefix_label(label)
