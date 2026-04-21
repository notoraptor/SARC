from datetime import datetime, timedelta

UTCOFFSET = timedelta(0)


def expect_datetime_utc(dt: datetime) -> None:
    if dt.tzinfo is None:
        raise ValueError(f"Datetime must be timezone-aware: {dt}")
    if dt.utcoffset() != UTCOFFSET:
        raise ValueError(f"Datetime must be UTC: {dt}")
