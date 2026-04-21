from collections.abc import Iterator
from datetime import datetime

SPLIT_NONE = "none"
SPLIT_MONTH = "month"
SPLIT_CHOICES = (SPLIT_NONE, SPLIT_MONTH)


def iter_windows(
    time_from: datetime, time_to: datetime, split_by: str
) -> Iterator[tuple[datetime, datetime]]:
    """
    Yield (window_start, window_end) tuples covering [time_from, time_to].

    With split_by="none", yields a single window equal to the input range.
    With split_by="month", yields one window per calendar month, clipped to
    [time_from, time_to] at the edges. Month boundaries are computed in the
    timezone of the input datetimes (which must be timezone-aware and share
    a timezone).
    """
    if split_by == SPLIT_NONE:
        yield time_from, time_to
        return
    if split_by == SPLIT_MONTH:
        yield from _iter_months(time_from, time_to)
        return
    raise ValueError(f"Unknown split_by: {split_by!r}")


def _iter_months(
    time_from: datetime, time_to: datetime
) -> Iterator[tuple[datetime, datetime]]:
    current = time_from.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while current < time_to:
        if current.month == 12:
            next_start = current.replace(year=current.year + 1, month=1)
        else:
            next_start = current.replace(month=current.month + 1)

        yield max(current, time_from), min(next_start, time_to)
        current = next_start
