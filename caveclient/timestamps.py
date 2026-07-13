"""Shared helpers for normalizing timestamps across all sub-clients.

All timestamps sent to or received from CAVE services are UTC. These helpers
exist so that every sub-client interprets user-provided timestamps the same
way; in particular, naive datetimes are always assumed to be UTC.
"""

import datetime
from typing import Union

TimestampLike = Union[datetime.datetime, int, float, str]


def to_utc(ts: TimestampLike) -> datetime.datetime:
    """Normalize a timestamp-like value to a timezone-aware UTC datetime.

    Parameters
    ----------
    ts :
        One of: a `datetime.datetime` (naive values are assumed to be UTC,
        aware values are converted to UTC), a unix epoch in seconds (int or
        float), an ISO-8601 formatted string (naive values assumed UTC), or
        the string "now" for the current time.

    Returns
    -------
    datetime.datetime
        A timezone-aware datetime in UTC.
    """
    if isinstance(ts, datetime.datetime):
        if ts.tzinfo is None:
            return ts.replace(tzinfo=datetime.timezone.utc)
        return ts.astimezone(datetime.timezone.utc)
    if isinstance(ts, bool):
        raise TypeError(f"Cannot interpret {ts!r} as a timestamp.")
    if isinstance(ts, (int, float)):
        return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
    if isinstance(ts, str):
        if ts == "now":
            return datetime.datetime.now(datetime.timezone.utc)
        try:
            parsed = datetime.datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError as e:
            raise ValueError(f"Could not parse '{ts}' as an ISO-8601 timestamp.") from e
        return to_utc(parsed)
    raise TypeError(
        f"Cannot interpret {type(ts).__name__} as a timestamp; expected a "
        "datetime, unix epoch in seconds, or ISO-8601 string."
    )
