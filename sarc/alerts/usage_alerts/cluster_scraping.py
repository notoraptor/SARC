import logging
import math
import statistics
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import literal_column
from sqlmodel import func, select

from sarc.alerts.common import CheckResult, HealthCheck
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import SlurmJobDB

logger = logging.getLogger(__name__)


def check_nb_jobs_per_cluster_per_time(
    time_interval: timedelta | None = timedelta(days=7),
    time_unit: timedelta = timedelta(days=1),
    cluster_names: list[str] | None = None,
    nb_stddev: float = 2.0,
    verbose: bool = False,
) -> bool:
    """
    Check if we have scraped enough jobs per time unit per cluster on given time interval.
    Log an alert for each cluster where number of jobs per time unit is lower than a limit
    computed using mean and standard deviation statistics from this cluster.

    Parameters
    ----------
    time_interval: timedelta
        If given, only jobs which ran in [now - time_interval, now] will be used for checking.
        Default is last 7 days.
        If None, all jobs are used.
    time_unit: timedelta
        Time unit in which we must check cluster usage through time_interval. Default is 1 day.
    cluster_names: list
        Optional list of clusters to check.
        If empty (or not specified), use all clusters available among jobs retrieved with time_interval.
    nb_stddev: float
        Amount of standard deviation to remove from average statistics to compute checking threshold.
        For each cluster, threshold is computed as:
        max(0, average - nb_stddev * stddev)
    verbose: bool
        If True, print supplementary info about clusters statistics.

    Returns
    -------
    bool
        True if check succeeds, False otherwise
    """
    from sarc.config import config

    now = datetime.now(tz=UTC)

    # Effective end_time: replace NULL with `now` (for RUNNING/PENDING jobs).
    # Effective start_time: derived as eff_end - elapsed_time, since SLURM
    # start_time is sometimes unreliable (often equal to submit_time).
    eff_end = func.coalesce(SlurmJobDB.end_time, now)
    eff_start = eff_end - SlurmJobDB.elapsed_time * literal_column(
        "interval '1 second'"
    )

    with config().db.session() as sess:
        # Determine [start, end] bounds for frame iteration.
        if time_interval is None:
            start, end = sess.exec(
                select(func.min(eff_start), func.max(eff_end))
            ).one()
        else:
            window_end = now
            window_start = window_end - time_interval
            start, end = sess.exec(
                select(
                    func.min(func.greatest(eff_start, window_start)),
                    func.max(func.least(eff_end, window_end)),
                ).where(eff_start < window_end, eff_end > window_start)
            ).one()

        if start is None or end is None:
            return True

        # For each frame, count jobs per cluster.
        # Skip empty frames (no jobs in any cluster).
        timestamps: list[datetime] = []
        cluster_counts: dict[str, dict[datetime, int]] = {}
        frame_start = start
        while frame_start < end:
            frame_end = frame_start + time_unit
            rows = sess.exec(
                select(SlurmClusterDB.name, func.count())
                .select_from(SlurmJobDB)
                .join(SlurmClusterDB, SlurmJobDB.cluster_id == SlurmClusterDB.id)
                .where(eff_start < frame_end, eff_end > frame_start)
                .group_by(SlurmClusterDB.name)
            ).all()
            if rows:
                timestamps.append(frame_start)
                for cluster_name, count in rows:
                    cluster_counts.setdefault(cluster_name, {})[frame_start] = count
            frame_start = frame_end

    # Determine which clusters to report on.
    if cluster_names:
        sorted_clusters = sorted(cluster_names)
    else:
        sorted_clusters = sorted(cluster_counts.keys())

    ok = True
    for cluster_name in sorted_clusters:
        counts = [
            cluster_counts.get(cluster_name, {}).get(ts, 0) for ts in timestamps
        ]
        if not counts:
            continue
        avg = statistics.mean(counts)
        stddev = statistics.stdev(counts) if len(counts) > 1 else math.nan
        threshold = (
            0.0 if math.isnan(stddev) else max(0.0, avg - nb_stddev * stddev)
        )

        if verbose:
            print(f"[{cluster_name}]", file=sys.stderr)
            for ts, c in zip(timestamps, counts):
                print(f"  {ts}  {c}", file=sys.stderr)
            print(
                f"avg {avg}, stddev {stddev}, threshold {threshold}",
                file=sys.stderr,
            )
            print(file=sys.stderr)

        if threshold == 0:
            msg = f"[{cluster_name}] threshold 0 ({avg} - {nb_stddev} * {stddev})."
            if len(timestamps) == 1:
                msg += (
                    f" Only 1 timestamp found. Either time_interval ({time_interval}) is too short, "
                    f"or this cluster should not be currently checked"
                )
            else:
                msg += (
                    f" Either nb_stddev is too high, time_interval ({time_interval}) is too short, "
                    f"or this cluster should not be currently checked"
                )
            logger.error(msg)
            ok = False
        else:
            for ts, c in zip(timestamps, counts):
                if c < threshold:
                    logger.error(
                        f"[{cluster_name}][{ts}] "
                        f"insufficient cluster scraping: {c} jobs / cluster / time unit; "
                        f"minimum required for this cluster: {threshold} ({avg} - {nb_stddev} * {stddev}); "
                        f"time unit: {time_unit}"
                    )
                    ok = False

    return ok


@dataclass
class ClusterScrapingCheck(HealthCheck):
    """Health check for cluster scraping"""

    time_interval: timedelta | None = timedelta(days=7)
    time_unit: timedelta = timedelta(days=1)
    cluster_names: list[str] | None = None
    nb_stddev: float = 2.0
    verbose: bool = False

    def check(self) -> CheckResult:
        if check_nb_jobs_per_cluster_per_time(
            time_interval=self.time_interval,
            time_unit=self.time_unit,
            cluster_names=self.cluster_names,
            nb_stddev=self.nb_stddev,
            verbose=self.verbose,
        ):
            return self.ok()
        else:
            return self.fail()
