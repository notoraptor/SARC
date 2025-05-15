from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Sequence, Union

from sarc.client.job import SlurmJob
from sarc.config import MTL, UTC
from sarc.jobs import get_job_time_series_metric_names
from sarc.traces import trace_decorator


# pylint: disable=too-many-branches
@trace_decorator()
def _get_job_time_series_data_using_query_range(
    job: SlurmJob,
    metric: Union[str, Sequence[str]],
    min_interval: int = 30,
    max_points: int = 100,
    measure: str | None = None,
    aggregation: str = "total",
):
    """Fetch job metrics.

    Arguments:
        cluster: The cluster on which to fetch metrics.
        job: The job for which to fetch metrics.
        metric: The metric, which must be in ``slurm_job_metric_names``.
        min_interval: The minimal reporting interval, in seconds.
        max_points: The maximal number of data points to return.
        measure: The aggregation measure to use ("avg_over_time", etc.)
            A format string can be passed, e.g. ("quantile_over_time(0.5, {})")
            to get the median.
        aggregation: Either "total", to aggregate over the whole range, or
            "interval", to aggregate over each interval.
    """
    metrics = [metric] if isinstance(metric, str) else metric
    if not metrics:
        raise ValueError("No metrics given")
    for m in metrics:
        if m not in get_job_time_series_metric_names():
            raise ValueError(f"Unknown metric name: {m}")
    if aggregation not in ("interval", "total", None):
        raise ValueError(
            f"Aggregation must be one of ['total', 'interval', None]: {aggregation}"
        )

    if job.job_state != "RUNNING" and not job.elapsed_time:
        return []

    if len(metrics) == 1:
        (prefix,) = metrics
        label_exprs = []
    else:
        prefix = ""
        label_exprs = [f'__name__=~"^({"|".join(metrics)})$"']

    label_exprs.append(f'slurmjobid="{job.job_id}"')
    selector = prefix + "{" + ", ".join(label_exprs) + "}"

    now = datetime.now(tz=UTC).astimezone(MTL)

    if job.end_time and job.end_time <= now:
        duration = job.end_time - job.start_time
        end_time = job.end_time
    else:
        # Duration should not be looking in the future
        duration = now - job.start_time
        end_time = now

    duration_seconds = int(duration.total_seconds())

    if duration_seconds <= 0:
        return []

    interval = int(max(duration_seconds / max_points, min_interval))

    if measure and aggregation:
        if aggregation == "interval":
            range_seconds = interval
        elif aggregation == "total":
            range_seconds = duration_seconds
        else:
            raise ValueError(f"Unknown aggregation: {aggregation}")

        selector_with_range = f"{selector}[{range_seconds}s]"
        if "(" in measure:
            # NB: This case is never used nor tested anywhere
            nested_query = measure.format(selector_with_range)
        else:
            nested_query = f"{measure}({selector_with_range})"
        query = f"{nested_query}[{duration_seconds}s:{range_seconds}s]"
        # Query range must cover only range_seconds from end_time.
        start_time = end_time - timedelta(seconds=range_seconds)
        step_seconds = range_seconds
    else:
        query = selector
        # Query range must cover entire job time.
        start_time = job.start_time
        step_seconds = interval

    logging.warning(
        f"prometheus query range: {query} start={start_time} end={end_time} (now? {end_time == now}) step={step_seconds}"
    )
    return job.fetch_cluster_config().prometheus.custom_query_range(
        query=query, start_time=start_time, end_time=end_time, step=f"{step_seconds}s"
    )
