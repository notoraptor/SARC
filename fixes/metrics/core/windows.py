from collections.abc import Callable, Iterator
from datetime import datetime, timedelta

from fixes.metrics.core.utils import expect_datetime_utc

SPLIT_NONE = "none"
SPLIT_MONTH = "month"
SPLIT_WEEK = "week"
SPLIT_CHOICES = (SPLIT_NONE, SPLIT_MONTH, SPLIT_WEEK)


def iter_windows(
    time_from: datetime, time_to: datetime, split_by: str
) -> Iterator[tuple[datetime, datetime]]:
    """
    Yield (window_start, window_end) tuples covering [time_from, time_to].

    Inputs are expected to be UTC-aware. With split_by="none", yields a single
    window equal to the input range. With split_by="month", yields one window
    per calendar month aligned on UTC midnight of the 1st. With split_by="week",
    yields 7-day windows aligned on UTC Monday 00:00. Boundaries are clipped to
    [time_from, time_to] at the edges.
    """
    if split_by == SPLIT_NONE:
        yield time_from, time_to
        return
    if split_by == SPLIT_MONTH:
        yield from _iter_aligned(time_from, time_to, _month_floor, _next_month)
        return
    if split_by == SPLIT_WEEK:
        yield from _iter_aligned(time_from, time_to, _week_floor, _next_week)
        return
    raise ValueError(f"Unknown split_by: {split_by!r}")


def _iter_aligned(
    time_from: datetime,
    time_to: datetime,
    floor_fn: Callable[[datetime], datetime],
    next_fn: Callable[[datetime], datetime],
) -> Iterator[tuple[datetime, datetime]]:
    expect_datetime_utc(time_from)
    expect_datetime_utc(time_to)
    current = floor_fn(time_from)
    while current < time_to:
        nxt = next_fn(current)
        yield max(current, time_from), min(nxt, time_to)
        current = nxt


def _month_floor(dt: datetime) -> datetime:
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _next_month(dt: datetime) -> datetime:
    if dt.month == 12:
        return dt.replace(year=dt.year + 1, month=1)
    return dt.replace(month=dt.month + 1)


def _week_floor(dt: datetime) -> datetime:
    midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return midnight - timedelta(days=midnight.weekday())


def _next_week(dt: datetime) -> datetime:
    return dt + timedelta(days=7)
