"""UTC time helpers.

Every timestamp emitted on the stream is UTC and formatted like the contract
sample: ``2026-09-17T12:00:00Z``.
"""

from __future__ import annotations

from datetime import datetime, timezone


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def to_iso8601(value: datetime) -> str:
    """Format a datetime as UTC ISO-8601 with a trailing ``Z`` and no offset."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    value = value.astimezone(timezone.utc)
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")
