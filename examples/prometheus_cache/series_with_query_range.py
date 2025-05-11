from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

from prometheus_api_client import MetricRangeDataFrame

from sarc.client.job import SlurmJob
from sarc.config import MTL, UTC
from sarc.jobs.series import (
    _get_job_time_series_data,
    _get_job_time_series_data_cache_key,
    slurm_job_metric_names,
)
from sarc.traces import trace_decorator


@trace_decorator()
def new_get_job_time_series(
    job: SlurmJob,
    metric: str,
    min_interval: int = 30,
    max_points: int = 100,
    measure: str | None = None,
    aggregation: str = "total",
    dataframe: bool = True,
):
    cache = PromCache()
    cache_offset = PromCache("offset")
    cache_range = PromCache("range")

    results = results_offset = _get_job_time_series_data(
        job, metric, min_interval, max_points, measure, aggregation
    )

    # We don't cache if keystring (=> job.end_time) is not available.
    keystring = _get_job_time_series_data_cache_key(
        job, metric, min_interval, max_points, measure, aggregation
    )
    if keystring is not None:
        results_range = _get_job_time_series_using_query_range(
            job, metric, min_interval, max_points, measure, aggregation
        )

        saved_offset = Data(cache_offset.get_cache(keystring), "saved_offset")
        saved_range = Data(cache_range.get_cache(keystring), "saved_range")
        data_results_offset = Data(results_offset, "results_offset")
        data_results_range = Data(results_range, "results_range")

        if saved_offset:
            cache.compare(keystring, saved_offset, data_results_offset)
            cache.compare(keystring, saved_offset, data_results_range)
        else:
            cache_offset.set_cache(keystring, results_offset)
        if saved_range:
            cache.compare(keystring, saved_range, data_results_range)
            cache.compare(keystring, saved_range, data_results_offset)
        else:
            cache_range.set_cache(keystring, results_range)
        cache.compare(keystring, data_results_offset, data_results_range)

    if dataframe:
        return MetricRangeDataFrame(results) if results else None
    else:
        return results


# pylint: disable=too-many-branches
class Data:
    __slots__ = ("data", "name")

    def __init__(self, data: list, name: str):
        self.data = data
        self.name = name

    def __str__(self):
        return self.name

    def __repr__(self):
        return repr(self.name)

    def __bool__(self):
        return bool(self.data)

    def __eq__(self, other):
        return self.data == other.data

    def __ne__(self, other):
        return self.data != other.data


class PromCache:
    def __init__(self, name=""):
        self.folder = f".prometheus_cache" + (f".{name}" if name else "")
        os.makedirs(self.folder, exist_ok=True)

    def compare(self, keystring: str, data1: Data, data2: Data):
        if data1 != data2:
            message = (
                f"{data1} {self.len_results(data1.data)} "
                f"!= {data2} {self.len_results(data2.data)}"
            )
            logging.warning(f"{message} : {keystring}")
            output_path = os.path.join(
                self.folder, f"{keystring}.{data1}-vs-{data2}.err"
            )
            with open(output_path, mode="w", encoding="utf-8") as file:
                file.write(
                    f"\n\n"
                    f"{message}\n"
                    f"Keystring: {keystring}\n\n"
                    f"{PromCache.diff(data1.data, data2.data)}\n"
                )

    @classmethod
    def len_results(cls, results: list):
        return [len(data["values"]) for data in results]

    @classmethod
    def diff(cls, dict1, dict2, save_long_diff=False):
        import difflib

        d1_str = json.dumps(dict1, indent=1, sort_keys=True)
        d2_str = json.dumps(dict2, indent=1, sort_keys=True)

        diff = list(
            difflib.unified_diff(
                d1_str.splitlines(),
                d2_str.splitlines(),
                fromfile="dict1",
                tofile="dict2",
                lineterm="",
            )
        )

        text = "\n".join(diff)
        if len(diff) > 100 and save_long_diff:
            output_path = "out.diff"
            with open(output_path, mode="w", encoding="utf-8") as file:
                file.write(text)
            return f"({len(diff)} diff lines saved in {output_path})"
        else:
            return text

    def get_cache_path(self, key):
        return os.path.join(self.folder, key)

    def set_cache(self, key, results):
        path = self.get_cache_path(key)
        with open(path, mode="w", encoding="utf-8") as file:
            json.dump(results, file)

    def get_cache(self, key):
        path = self.get_cache_path(key)
        if os.path.isfile(path):
            with open(path, encoding="utf-8") as file:
                return json.load(file)
        return None


# pylint: disable=too-many-branches
@trace_decorator()
def _get_job_time_series_using_query_range(
    job: SlurmJob,
    metric: str,
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

    if aggregation not in ("interval", "total", None):
        raise ValueError(
            f"Aggregation must be one of ['total', 'interval', None]: {aggregation}"
        )

    if job.job_state != "RUNNING" and not job.elapsed_time:
        return []
    if metric not in slurm_job_metric_names:
        raise ValueError(f"Unknown metric name: {metric}")

    selector = f'{metric}{{slurmjobid=~"{job.job_id}"}}'

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

    logging.info(
        f"prometheus query range: {query} start={start_time} end={end_time} (now? {end_time == now}) step={step_seconds}"
    )
    return job.fetch_cluster_config().prometheus.custom_query_range(
        query=query,
        start_time=start_time,
        end_time=end_time,
        step=f"{step_seconds}s",
    )
