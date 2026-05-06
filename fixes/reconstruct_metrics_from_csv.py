"""
Rebuild per-cluster metrics (nb_unique_users, nb_unique_jobs, total_rgu_secs)
directly from a jobs CSV produced by fixes/load_jobs_csv.py, without importing
sarc. Mirrors the logic in metrics/core/show_metrics.py +
metrics/core/job_aggregation.py + metrics/core/report.py for "scope=cluster".

Usage:
    uv run fixes/reconstruct_metrics_from_csv.py --csv path/to/jobs.csv \\
        --split month --start 2025-01-01 --end 2026-01-01 --output out.csv

    uv run fixes/reconstruct_metrics_from_csv.py --csv path/to/jobs.csv \\
        --split week --start 2026-01-01 --end 2026-05-04 --output out.csv
"""

import argparse
import csv
import logging
import sys
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

CLUSTER_TO_TYPE = {
    "mila": "mila",
    "tamia": "drac-paice",
    "killarney": "drac-paice",
    "vulcan": "drac-paice",
}


def _parse_dt(s: str) -> datetime | None:
    if not s:
        return None
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def _parse_float(s: str) -> float | None:
    if s == "" or s is None:
        return None
    return float(s)


def _parse_int(s: str) -> int | None:
    if s == "" or s is None:
        return None
    return int(s)


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


def iter_windows(time_from: datetime, time_to: datetime, split_by: str):
    if split_by == "none":
        yield time_from, time_to
        return
    if split_by == "month":
        floor_fn, next_fn = _month_floor, _next_month
    elif split_by == "week":
        floor_fn, next_fn = _week_floor, _next_week
    else:
        raise ValueError(f"Unknown split_by: {split_by!r}")
    current = floor_fn(time_from)
    while current < time_to:
        nxt = next_fn(current)
        yield max(current, time_from), min(nxt, time_to)
        current = nxt


class WindowAggregator:
    """Per-window aggregator: (cluster, job_id, user) -> total_rgu_seconds."""

    __slots__ = ("time_from", "time_to", "jobs_rgu")

    def __init__(self, time_from: datetime, time_to: datetime):
        self.time_from = time_from
        self.time_to = time_to
        self.jobs_rgu: dict[tuple[str, str, str], float] = {}

    def aggregate(
        self, cluster: str, job_id: str, user: str, rgu_seconds: float
    ) -> None:
        key = (cluster, job_id, user)
        if key in self.jobs_rgu:
            self.jobs_rgu[key] += rgu_seconds
        else:
            self.jobs_rgu[key] = rgu_seconds

    def per_cluster(self) -> dict[str, tuple[set[str], int, float]]:
        out: dict[str, tuple[set[str], int, float]] = {}
        for key, rgu in self.jobs_rgu.items():
            cluster, _, user = key
            if cluster not in out:
                out[cluster] = (set(), 0, 0.0)
            users, n_jobs, tot = out[cluster]
            users.add(user)
            out[cluster] = (users, n_jobs + 1, tot + rgu)
        return out


def iter_csv_jobs(path: Path) -> Iterator[dict]:
    """Stream rows of jobs.csv, only parsing/keeping the columns we need."""
    with open(path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cluster = row["cluster_name"]
            job_id = row["job_id"]
            user = row["user"]
            submit_time = _parse_dt(row["submit_time"])
            start_time = _parse_dt(row["start_time"])
            end_time = _parse_dt(row["end_time"])
            elapsed_time = _parse_float(row["elapsed_time"])
            gpu_type = row["allocated.gpu_type"] or None
            gres_gpu = _parse_int(row["allocated.gres_gpu"])
            gpu_type_rgu = _parse_float(row["allocated.gpu_type_rgu"])
            yield {
                "cluster": cluster,
                "job_id": job_id,
                "user": user,
                "submit_time": submit_time,
                "start_time": start_time,
                "end_time": end_time,
                "elapsed_time": elapsed_time,
                "gpu_type": gpu_type,
                "gres_gpu": gres_gpu,
                "gpu_type_rgu": gpu_type_rgu,
            }


def reconstruct(
    csv_path: Path, time_from: datetime, time_to: datetime, split_by: str
) -> list[WindowAggregator]:
    aggs = [
        WindowAggregator(ws, we)
        for ws, we in iter_windows(time_from, time_to, split_by)
    ]
    overall_from = aggs[0].time_from if aggs else time_from
    overall_to = aggs[-1].time_to if aggs else time_to

    n_seen = 0
    n_kept = 0
    n_no_user = 0
    n_no_start = 0
    n_no_gpu = 0
    n_outside = 0

    for job in iter_csv_jobs(csv_path):
        n_seen += 1
        if not job["user"]:
            n_no_user += 1
            continue
        if job["start_time"] is None:
            n_no_start += 1
            continue
        if (
            job["gpu_type"] is None
            or job["gres_gpu"] is None
            or job["gpu_type_rgu"] is None
        ):
            n_no_gpu += 1
            continue

        start = job["start_time"]
        end = job["end_time"]
        if end is None:
            elapsed = job["elapsed_time"] or 0.0
            end = start + timedelta(seconds=elapsed)

        # Quickly skip jobs that fall completely outside the global span.
        if end < overall_from or start > overall_to:
            n_outside += 1
            continue

        n_kept += 1
        rgu_per_sec = job["gpu_type_rgu"] * job["gres_gpu"]
        cluster = job["cluster"]
        job_id = job["job_id"]
        user = job["user"]

        for agg in aggs:
            if end < agg.time_from:
                continue
            if start > agg.time_to:
                continue
            isec_start = max(start, agg.time_from)
            isec_end = min(end, agg.time_to)
            if isec_start >= isec_end:
                continue
            consumed = (isec_end - isec_start).total_seconds()
            agg.aggregate(cluster, job_id, user, rgu_per_sec * consumed)

        if n_seen % 500_000 == 0:
            logger.info("processed %d rows (kept=%d)", n_seen, n_kept)

    logger.info(
        "summary: seen=%d kept=%d no_user=%d no_start=%d no_gpu=%d outside=%d",
        n_seen,
        n_kept,
        n_no_user,
        n_no_start,
        n_no_gpu,
        n_outside,
    )
    return aggs


def write_output(aggs: list[WindowAggregator], output: Path) -> None:
    rows: list[tuple[str, str, str, int, int, float]] = []
    for agg in aggs:
        per_cluster = agg.per_cluster()
        for cluster in sorted(per_cluster):
            users, n_jobs, total_rgu = per_cluster[cluster]
            rows.append(
                (
                    agg.time_from.isoformat(),
                    cluster,
                    CLUSTER_TO_TYPE.get(cluster, "drac"),
                    len(users),
                    n_jobs,
                    total_rgu,
                )
            )
    rows.sort(key=lambda r: (r[0], r[1]))
    with open(output, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "start",
                "cluster",
                "cluster_type",
                "nb_unique_users",
                "nb_unique_jobs",
                "total_rgu_secs",
            ]
        )
        w.writerows(rows)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s", force=True
    )
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True, type=Path)
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--split", choices=["none", "month", "week"], default="month")
    p.add_argument("--output", required=True, type=Path)
    args = p.parse_args()

    time_from = datetime.fromisoformat(args.start)
    time_to = datetime.fromisoformat(args.end)
    if time_from.tzinfo is None:
        time_from = time_from.replace(tzinfo=UTC)
    if time_to.tzinfo is None:
        time_to = time_to.replace(tzinfo=UTC)
    if time_from >= time_to:
        print("Expected start < end", file=sys.stderr)
        sys.exit(1)

    aggs = reconstruct(args.csv, time_from, time_to, args.split)
    write_output(aggs, args.output)
    logger.info("wrote %s", args.output)


if __name__ == "__main__":
    main()
