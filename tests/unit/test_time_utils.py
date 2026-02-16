"""Tests for kubetimer.utils.time_utils.

Covers:
  - get_timezone: UTC shortcut, valid IANA name, invalid name
  - parse_ttl: naive → UTC, timezone-aware, empty / malformed input
  - is_ttl_expired: past, future, non-UTC timezone
"""

from datetime import datetime, timezone, timedelta

import pytest

from kubetimer.utils.time_utils import get_timezone, is_ttl_expired, parse_ttl


class TestGetTimezone:
    def test_utc_returns_utc(self):
        tz = get_timezone("UTC")
        assert tz is timezone.utc

    def test_valid_iana_timezone(self):
        tz = get_timezone("America/New_York")
        assert tz.key == "America/New_York"

    def test_invalid_timezone_raises(self):
        with pytest.raises(ValueError, match="Invalid timezone"):
            get_timezone("Not/A/Timezone")


class TestParseTtl:
    def test_naive_datetime_gets_utc(self):
        """A datetime string without offset should be treated as UTC."""
        dt = parse_ttl("2025-06-01T12:00:00")
        assert dt.tzinfo is timezone.utc
        assert dt.year == 2025

    def test_aware_datetime_preserved(self):
        """An explicit +05:00 offset must survive parsing."""
        dt = parse_ttl("2025-06-01T12:00:00+05:00")
        assert dt.utcoffset() == timedelta(hours=5)

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="TTL value is empty"):
            parse_ttl("")

    def test_malformed_string_raises(self):
        with pytest.raises(ValueError, match="Invalid ISO8601 format"):
            parse_ttl("not-a-date")

    def test_whitespace_trimmed(self):
        dt = parse_ttl("  2025-06-01T12:00:00  ")
        assert dt.year == 2025
        assert dt.tzinfo is timezone.utc


class TestIsTtlExpired:
    def test_past_datetime_is_expired(self):
        past = datetime.now(timezone.utc) - timedelta(hours=1)
        assert is_ttl_expired(past)

    def test_future_datetime_is_not_expired(self):
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        assert not is_ttl_expired(future)

    def test_non_utc_timezone(self):
        """Expiry check should work across timezone boundaries."""
        past = datetime.now(timezone.utc) - timedelta(hours=1)
        assert is_ttl_expired(past, "America/New_York")
