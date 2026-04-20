"""
Time utilities for TTL parsing and expiration checking.

Parses duration-based TTL values (e.g. "30m", "2h", "7d") into timedeltas,
and checks whether a computed expiry datetime has passed.
"""

from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


def get_timezone(timezone_str: str = "UTC") -> ZoneInfo | timezone:
    if timezone_str == "UTC":
        return timezone.utc

    try:
        return ZoneInfo(timezone_str)
    except KeyError:
        raise ValueError(
            f"Invalid timezone: {timezone_str}. Use IANA format like 'America/New_York'"
        )


def parse_ttl_duration(ttl_value: str) -> timedelta:
    """Parse a duration-based TTL string into a timedelta.

    Supported formats: ``<number>s``, ``<number>m``, ``<number>h``, ``<number>d``.

    Returns:
        timedelta representing the TTL duration.

    Raises:
        ValueError: If the value is empty, negative, or has an invalid unit.
    """
    ttl_value = ttl_value.strip()
    if not ttl_value:
        raise ValueError("TTL value is empty.")
    if "-" in ttl_value:
        raise ValueError("Negative TTL values are not allowed.")

    units_map = {"s", "m", "h", "d"}
    if ttl_value[-1] not in units_map:
        raise ValueError(
            f"Invalid TTL unit: {ttl_value[-1]}. Use one of {', '.join(units_map)}."
        )

    try:
        number = int(ttl_value[:-1])
    except ValueError as e:
        raise ValueError(f"Invalid TTL format: {e}")

    if number <= 0:
        raise ValueError("TTL value must be a positive number.")

    unit = ttl_value[-1]
    if unit == "s":
        return timedelta(seconds=number)
    elif unit == "m":
        return timedelta(minutes=number)
    elif unit == "h":
        return timedelta(hours=number)
    elif unit == "d":
        return timedelta(days=number)
    else:
        raise ValueError(f"Unsupported TTL unit: {unit}")


def parse_expires_at(expires_at_str: str) -> datetime:
    """Parse an ISO 8601 expires-at annotation value into a timezone-aware datetime.

    Naive datetimes (without offset) are treated as UTC.

    Raises:
        ValueError: If the string is empty or not valid ISO 8601.
    """
    if not expires_at_str or not expires_at_str.strip():
        raise ValueError("expires-at value is empty.")
    try:
        dt = datetime.fromisoformat(expires_at_str.strip())
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid ISO 8601 expires-at value: {e}")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def is_ttl_expired(expires_at: datetime, timezone_str: str = "UTC") -> bool:
    """Check whether the computed expiry time has passed.

    Args:
        expires_at: The absolute expiry datetime (timezone-aware).
        timezone_str: IANA timezone for the "now" comparison.

    Returns:
        True if ``expires_at`` is in the past (or exactly now).
    """
    tz = get_timezone(timezone_str)
    return expires_at <= datetime.now(tz)
