"""Tests for kubetimer.utils.time_utils.

Covers:
  - get_timezone: UTC shortcut, valid IANA name, invalid name
  - parse_ttl_duration: valid durations (s/m/h/d), empty, negative, zero,
    invalid unit, non-numeric
  - parse_expires_at: valid ISO 8601, naive → UTC, empty, invalid
  - is_ttl_expired: past, future, non-UTC timezone
"""

from datetime import datetime, timezone, timedelta

import pytest

from kubetimer.utils.time_utils import (
    get_timezone,
    is_ttl_expired,
    parse_expires_at,
    parse_ttl_duration,
)


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


class TestParseTtlDuration:
    def test_seconds(self):
        assert parse_ttl_duration("30s") == timedelta(seconds=30)

    def test_minutes(self):
        assert parse_ttl_duration("5m") == timedelta(minutes=5)

    def test_hours(self):
        assert parse_ttl_duration("2h") == timedelta(hours=2)

    def test_days(self):
        assert parse_ttl_duration("7d") == timedelta(days=7)

    def test_whitespace_trimmed(self):
        assert parse_ttl_duration("  10m  ") == timedelta(minutes=10)

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="TTL value is empty"):
            parse_ttl_duration("")

    def test_negative_raises(self):
        with pytest.raises(ValueError, match="Negative TTL values"):
            parse_ttl_duration("-5m")

    def test_zero_raises(self):
        with pytest.raises(ValueError, match="TTL value must be a positive number"):
            parse_ttl_duration("0s")

    def test_invalid_unit_raises(self):
        with pytest.raises(ValueError, match="Invalid TTL unit"):
            parse_ttl_duration("10x")

    def test_non_numeric_raises(self):
        with pytest.raises(ValueError, match="Invalid TTL format"):
            parse_ttl_duration("abch")


class TestParseExpiresAt:
    def test_valid_iso8601(self):
        dt = parse_expires_at("2026-06-01T12:00:00+00:00")
        assert dt.year == 2026
        assert dt.tzinfo is not None

    def test_naive_datetime_gets_utc(self):
        dt = parse_expires_at("2026-06-01T12:00:00")
        assert dt.tzinfo is timezone.utc
        assert dt.year == 2026

    def test_aware_datetime_preserved(self):
        dt = parse_expires_at("2026-06-01T12:00:00+05:00")
        assert dt.utcoffset() == timedelta(hours=5)

    def test_whitespace_trimmed(self):
        dt = parse_expires_at("  2026-06-01T12:00:00  ")
        assert dt.year == 2026

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="expires-at value is empty"):
            parse_expires_at("")

    def test_invalid_string_raises(self):
        with pytest.raises(ValueError, match="Invalid ISO 8601"):
            parse_expires_at("not-a-date")


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
