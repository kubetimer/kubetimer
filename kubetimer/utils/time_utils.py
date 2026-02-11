"""
Time utilities for TTL parsing and expiration checking.

Handles ISO8601 datetime parsing and expiration checks with timezone support.
"""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo


def get_timezone(timezone_str: str = "UTC") -> ZoneInfo | timezone:
    if timezone_str == "UTC":
        return timezone.utc
    
    try:
        return ZoneInfo(timezone_str)
    except KeyError:
        raise ValueError(f"Invalid timezone: {timezone_str}. Use IANA format like 'America/New_York'")


def parse_ttl(ttl_value: str) -> datetime:
    if not ttl_value:
        raise ValueError("TTL value is empty")
    
    ttl_value = ttl_value.strip()
    
    try:
        expiry_time = datetime.fromisoformat(ttl_value)
        
        if expiry_time.tzinfo is None:
            tz = get_timezone("UTC")
            expiry_time = expiry_time.replace(tzinfo=tz)
        return expiry_time
    except ValueError as e:
        raise ValueError(f"Invalid ISO8601 format: {e}") from e


def is_ttl_expired(expiry_time: datetime, timezone_str: str = "UTC") -> bool:
    tz = get_timezone(timezone_str)
    now = datetime.now(tz)

    expiry_in_tz = expiry_time.astimezone(tz)
    
    return now >= expiry_in_tz
