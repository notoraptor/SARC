from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import numpy as np
import pandas
from pandas import DataFrame
from prometheus_api_client import MetricRangeDataFrame

from sarc.cache import CachePolicy, with_cache
from sarc.client.job import JobStatistics, SlurmJob, Statistics
from sarc.config import MTL, UTC, ClusterConfig, config
from sarc.traces import trace_decorator

if TYPE_CHECKING:
    pass


# pylint: disable=too-many-branches
@trace_decorator()
def get_job_time_series(
    job: SlurmJob,
    metric: str,
    min_interval: int = 30,
    max_points: int = 100,
    measure: str | None = None,
    aggregation: str = "total",
    dataframe: bool = True,
):
    results_offset = _get_job_time_series(
        job, metric, min_interval, max_points, measure, aggregation, dataframe=False
    )
    results_range = _get_job_time_series_using_query_range(
        job, metric, min_interval, max_points, measure, aggregation, dataframe=False
    )

    fmt = "%Y-%m-%dT%Hh%Mm%Ss"
    keystring = (
        f"{job.cluster_name}"
        f".{job.job_id}"
        f".from-{job.start_time.strftime(fmt) if job.start_time else None}"
        f".to-{job.end_time.strftime(fmt) if job.end_time else None}"
        f".{metric}"
        f".min-interval-{min_interval}s"
        f".max-points-{max_points}"
        f".{f'measure-{measure}-{aggregation}' if measure else 'no_measure'}"
        f".json"
    )

    if results_offset == results_range:
        logging.info(
            f"query_range ({PromCache.len_results(results_offset)}): {keystring}"
        )
    else:
        raise RuntimeError(
            f"\n\n"
            f"Results with offset ({PromCache.len_results(results_offset)}) "
            f"!= Results with query range ({PromCache.len_results(results_range)})\n"
            f"Keystring: {keystring}\n\n"
            f"{PromCache.diff(results_offset, results_range)}\n"
        )

    results = results_range
    if dataframe:
        return MetricRangeDataFrame(results) if results else None
    else:
        return results


class PromCache:
    """
    Key:
    No need for `aggregation` in key, since it is used to compute range_seconds
        job.cluster_name
        job.job_id
        job.start_time
        job.end_time
        duration_seconds
        interval
        metric
        measure
        range_seconds
    """

    __slots__ = (
        "job",
        "query",
        "keystring",
        "cache_policy",
        "measure",
        "interval",
        "metric",
    )

    def __init__(
        self,
        job: SlurmJob,
        metric: str,
        now: datetime,
        query: str,
        min_interval: int = 30,
        max_points: int = 100,
        measure: str | None = None,
        aggregation: str = "total",
    ):
        keystring = None
        interval = None
        if (
            job.start_time is not None
            and job.end_time is not None
            and job.start_time <= job.end_time <= now
            and (measure is None or "(" not in measure)
        ):
            duration = job.end_time - job.start_time
            duration_seconds = int(duration.total_seconds())
            interval = int(max(duration_seconds / max_points, min_interval))

            range_seconds = None
            if measure and aggregation:
                if aggregation == "interval":
                    range_seconds = interval
                else:
                    assert aggregation == "total"
                    range_seconds = duration_seconds

            fmt = "%Y-%m-%dT%Hh%Mm%Ss"
            keystring = (
                f"{job.cluster_name}"
                f".{job.job_id}"
                f".from-{job.start_time.strftime(fmt)}"
                f".to-{job.end_time.strftime(fmt)}"
                f".for-{duration_seconds}s"
                f".each-{interval}s"
                f".{metric}"
                f".{f'measure-{measure}.in-{range_seconds}s' if measure else 'no_measure'}"
                f".json"
            )

        self.job = job
        self.measure = measure
        self.metric = metric
        self.interval = interval
        self.query = query
        self.keystring = keystring
        self.cache_policy = CachePolicy.use

    def debug_get(self):
        results = self._call_prometheus()
        r2 = self._call_query_range()

        if r2 is not None:
            if results == r2:
                logging.info(f"query_range ({self.len_results(r2)}): {self.keystring}")
            else:
                raise RuntimeError(
                    f"\n"
                    f"Results with offset != Results with query range\n"
                    f"Query: {self.query}\n"
                    f"Job timestamp: from {self.job.start_time.timestamp()} to {self.job.end_time.timestamp()}\n"
                    f"Keystring: {self.keystring}\n\n"
                    f"{self.diff(results, r2)}\n"
                )

        folder = ".local_cache"
        os.makedirs(folder, exist_ok=True)
        if self.keystring:
            path = os.path.join(folder, self.keystring)
            if os.path.isfile(path):
                with open(path, encoding="utf-8") as file:
                    cache = json.load(file)
                if cache["results"] != results:
                    raise RuntimeError(
                        f"\n"
                        f"Cache != Results\n"
                        f"Keystring: {self.keystring}\n"
                        f"----------------\n"
                        f"\n"
                        f"{self.diff(cache, self._to_cache(results))}\n"
                    )
                logging.info(
                    f"in cache{' w/range' if r2 is not None else ''} ({self.len_results(results)}) {self.keystring}"
                )
            else:
                with open(path, "w", encoding="utf-8") as file:
                    json.dump(self._to_cache(results), file)
                logging.info(f"cached {self.keystring}")
        return results

    def get(self):
        return with_cache(
            self._call_prometheus,
            subdirectory="prometheus_cache",
            key=self._cache_key,
        )(cache_policy=self.cache_policy)

    def _call_prometheus(self):
        return self.job.fetch_cluster_config().prometheus.custom_query(self.query)

    def _call_query_range(self):
        return (
            self.job.fetch_cluster_config().prometheus.custom_query_range(
                query=f'{self.metric}{{slurmjobid=~"{self.job.job_id}"}}',
                start_time=self.job.start_time,
                end_time=self.job.end_time,
                step=f"{self.interval}s",
            )
            if self.keystring and not self.measure
            else None
        )

    def _cache_key(self):
        return self.keystring

    def _to_cache(self, results):
        return {"query": self.query, "results": results}

    @classmethod
    def len_results(cls, results: list):
        return len(results[0]["values"]) if results else 0

    @classmethod
    def diff(cls, dict1, dict2):
        import difflib

        d1_str = json.dumps(dict1, indent=1, sort_keys=True)
        d2_str = json.dumps(dict2, indent=1, sort_keys=True)

        diff = difflib.unified_diff(
            d1_str.splitlines(),
            d2_str.splitlines(),
            fromfile="dict1",
            tofile="dict2",
            lineterm="",
        )

        return "\n".join(diff)


# pylint: disable=too-many-branches
@trace_decorator()
def _get_job_time_series_using_query_range(
    job: SlurmJob,
    metric: str,
    min_interval: int = 30,
    max_points: int = 100,
    measure: str | None = None,
    aggregation: str = "total",
    dataframe: bool = True,
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
        dataframe: If True, return a DataFrame. Otherwise, return the list of
            dicts returned by Prometheus's API.
    """

    if aggregation not in ("interval", "total", None):
        raise ValueError(
            f"Aggregation must be one of ['total', 'interval', None]: {aggregation}"
        )

    if job.job_state != "RUNNING" and not job.elapsed_time:
        return None if dataframe else []
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
        return None if dataframe else []

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

    results = job.fetch_cluster_config().prometheus.custom_query_range(
        query=query,
        start_time=start_time,
        end_time=end_time,
        step=f"{step_seconds}s",
    )
    if dataframe:
        return MetricRangeDataFrame(results) if results else None
    else:
        return results


# pylint: disable=too-many-branches
@trace_decorator()
def _get_job_time_series(
    job: SlurmJob,
    metric: str,
    min_interval: int = 30,
    max_points: int = 100,
    measure: str | None = None,
    aggregation: str = "total",
    dataframe: bool = True,
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
        dataframe: If True, return a DataFrame. Otherwise, return the list of
            dicts returned by Prometheus's API.
    """

    if aggregation not in ("interval", "total", None):
        raise ValueError(
            f"Aggregation must be one of ['total', 'interval', None]: {aggregation}"
        )

    if job.job_state != "RUNNING" and not job.elapsed_time:
        return None if dataframe else []
    if metric not in slurm_job_metric_names:
        raise ValueError(f"Unknown metric name: {metric}")

    selector = f'{metric}{{slurmjobid=~"{job.job_id}"}}'

    now = datetime.now(tz=UTC).astimezone(MTL)

    ago = now - job.start_time
    duration = (job.end_time or now) - job.start_time

    offset = int((ago - duration).total_seconds())
    offset_string = f" offset {offset}s" if offset > 0 else ""

    duration_seconds = int(duration.total_seconds())

    # Duration should not be looking in the future
    if offset < 0:
        duration_seconds += offset

    if duration_seconds <= 0:
        return None if dataframe else []

    interval = int(max(duration_seconds / max_points, min_interval))

    query = selector

    if measure and aggregation:
        if aggregation == "interval":
            range_seconds = interval
        elif aggregation == "total":
            # NB: `offset` has already been used above to generate `offset_string`
            # and is not used anymore later in this function. So, following line
            # does not have any effect.
            offset += duration_seconds
            range_seconds = duration_seconds
        else:
            raise ValueError(f"Unknown aggregation: {aggregation}")

        query = f"{query}[{range_seconds}s]"
        if "(" in measure:
            # NB: This case is never used nor tested anywhere
            query = measure.format(f"{query} {offset_string}")
        else:
            query = f"{measure}({query} {offset_string})"
        query = f"{query}[{duration_seconds}s:{range_seconds}s]"
    else:
        query = f"{query}[{duration_seconds}s:{interval}s] {offset_string}"

    results = job.fetch_cluster_config().prometheus.custom_query(query)
    if dataframe:
        return MetricRangeDataFrame(results) if results else None
    else:
        return results


def get_job_time_series_metric_names():
    """Return all the metric names that relate to slurm jobs."""
    return slurm_job_metric_names


@trace_decorator()
def compute_job_statistics_from_dataframe(
    df: DataFrame,
    statistics,
    normalization=float,
    unused_threshold=0.01,
    is_time_counter=False,
):
    df = df.reset_index()

    groupby = ["instance", "core", "gpu"]
    groupby = [col for col in groupby if col in df]

    gdf = df.groupby(groupby)

    if is_time_counter:
        # This is a time-based counter like the cpu counters in /proc/stat, with
        # a resolution of 1 nanosecond.
        df["timediffs"] = gdf["timestamp"].diff().map(lambda x: x.total_seconds())
        df["value"] = gdf["value"].diff() / df["timediffs"] / 1e9
        df = df.drop(index=0)

    if unused_threshold is not None:
        means = gdf["value"].mean()
        n_unused = (means < unused_threshold).astype(bool).sum()

        df_with_means = pandas.merge(
            df, means.reset_index().rename(columns={"value": "mean"}), on=groupby
        )

        df = df_with_means[df_with_means["mean"] >= unused_threshold]
    else:
        n_unused = 0

    rval = {name: normalization(fn(df["value"])) for name, fn in statistics.items()}
    return {**rval, "unused": n_unused}


def compute_job_statistics_one_metric(
    job: SlurmJob,
    metric_name,
    statistics,
    normalization=float,
    unused_threshold=0.01,
    is_time_counter=False,
):
    df = job.series(metric=metric_name, max_points=10_000)
    if df is None:
        return None
    return compute_job_statistics_from_dataframe(
        df=df,
        statistics=statistics,
        normalization=normalization,
        unused_threshold=unused_threshold,
        is_time_counter=is_time_counter,
    )


@trace_decorator()
def compute_job_statistics(job: SlurmJob):
    statistics_dict = {
        "mean": lambda self: self.mean(),
        "std": lambda self: self.std(),
        "max": lambda self: self.max(),
        "q25": lambda self: self.quantile(0.25),
        "median": lambda self: self.median(),
        "q75": lambda self: self.quantile(0.75),
        "q05": lambda self: self.quantile(0.05),
    }

    gpu_utilization = compute_job_statistics_one_metric(
        job,
        "slurm_job_utilization_gpu",
        statistics=statistics_dict,
        unused_threshold=0.01,
        normalization=lambda x: float(x / 100),
    )

    gpu_utilization_fp16 = compute_job_statistics_one_metric(
        job,
        "slurm_job_fp16_gpu",
        statistics=statistics_dict,
        unused_threshold=0.01,
        normalization=lambda x: float(x / 100),
    )

    gpu_utilization_fp32 = compute_job_statistics_one_metric(
        job,
        "slurm_job_fp32_gpu",
        statistics=statistics_dict,
        unused_threshold=0.01,
        normalization=lambda x: float(x / 100),
    )

    gpu_utilization_fp64 = compute_job_statistics_one_metric(
        job,
        "slurm_job_fp64_gpu",
        statistics=statistics_dict,
        unused_threshold=0.01,
        normalization=lambda x: float(x / 100),
    )

    gpu_sm_occupancy = compute_job_statistics_one_metric(
        job,
        "slurm_job_sm_occupancy_gpu",
        statistics=statistics_dict,
        unused_threshold=0.01,
        normalization=lambda x: float(x / 100),
    )

    gpu_memory = compute_job_statistics_one_metric(
        job,
        "slurm_job_utilization_gpu_memory",
        statistics=statistics_dict,
        normalization=lambda x: float(x / 100),
        unused_threshold=None,
    )

    gpu_power = compute_job_statistics_one_metric(
        job,
        "slurm_job_power_gpu",
        statistics=statistics_dict,
        unused_threshold=None,
    )

    cpu_utilization = compute_job_statistics_one_metric(
        job,
        "slurm_job_core_usage",
        statistics=statistics_dict,
        unused_threshold=0.01,
        is_time_counter=True,
    )

    system_memory = compute_job_statistics_one_metric(
        job,
        "slurm_job_memory_usage",
        statistics=statistics_dict,
        normalization=lambda x: float(x / 1e6 / job.allocated.mem),
        unused_threshold=None,
    )

    return JobStatistics(
        gpu_utilization=gpu_utilization and Statistics(**gpu_utilization),
        gpu_utilization_fp16=gpu_utilization_fp16
        and Statistics(**gpu_utilization_fp16),
        gpu_utilization_fp32=gpu_utilization_fp32
        and Statistics(**gpu_utilization_fp32),
        gpu_utilization_fp64=gpu_utilization_fp64
        and Statistics(**gpu_utilization_fp64),
        gpu_sm_occupancy=gpu_sm_occupancy and Statistics(**gpu_sm_occupancy),
        gpu_memory=gpu_memory and Statistics(**gpu_memory),
        gpu_power=gpu_power and Statistics(**gpu_power),
        cpu_utilization=cpu_utilization and Statistics(**cpu_utilization),
        system_memory=system_memory and Statistics(**system_memory),
    )


def update_cluster_job_series_rgu(
    df: pandas.DataFrame, cluster_config: ClusterConfig
) -> pandas.DataFrame:
    """
    Compute RGU information for jobs related to given cluster config in a data frame.

    Parameters
    ----------
    df: DataFrame
        Data frame to update, typically returned by `load_job_series`.
        Should contain fields:
        "cluster_name", "start_time", "allocated.gpu_type", "allocated.gres_gpu".
    cluster_config: ClusterConfig
        Configuration of cluster to which jobs to update belong.
        Should define following config:
        "rgu_start_date": date since when billing is given as RGU.
        "gpu_to_rgu_billing": path to a JSON file containing a dict which maps
        GPU type to RGU cost per GPU.

    Returns
    -------
    DataFrame
        Input data frame with:
        - column `allocated.gres_gpu` updated if necessary.
        - column `allocated.gres_rgu` added or updated to contain RGU billing.
          Set to NaN (or unchanged if already present) for jobs from other clusters.
        - column `gpu_type_rgu` added or updated to contain RGU cost per GPU (RGU/GPU ratio).
          Set to NaN (or unchanged if already present) for jobs from other clusters.

    Pseudocode describing how we update data frame:
    for each job: if job.cluster_name == cluster_config.name:
        if start_time < cluster_config.rgu_start_date:
            # We are BEFORE transition to RGU
            if allocated.gpu_type in gpu_to_rgu_billing:
                # compute rgu columns
                allocated.gres_rgu = allocated.gres_gpu * gpu_to_rgu_billing[allocated.gpu_type]
                allocated.gpu_type_rgu = gpu_to_rgu_billing[allocated.gpu_type]
            else:
                # set rgu columns to nan
                allocated.gres_rgu = nan
                allocated.gpu_type_rgu = nan
        else:
            # We are AFTER transition to RGU
            # Anyway, we assume gres_rgu is current gres_gpu
            allocated.gres_rgu = allocated.gres_gpu

            if allocated.gpu_type in gpu_to_rgu_billing:
                # we fix gres_gpu by dividing it with RGU/GPU ratio
                allocated.gres_gpu = allocated.gres_gpu / gpu_to_rgu_billing[allocated.gpu_type]
                # we save RGU/GPU ratio
                allocated.gpu_type_rgu = gpu_to_rgu_billing[allocated.gpu_type]
            else:
                # we cannot fix gres_gpu, so we set it to nan
                allocated.gres_gpu = nan
                # we cannot get RGU/GPU ratio, so we set it to nan
                allocated.gpu_type_rgu = nan
    """

    # Make sure frame will have new RGU columns anyway, with NaN as default value.
    if "allocated.gres_rgu" not in df.columns:
        df["allocated.gres_rgu"] = np.nan
    if "allocated.gpu_type_rgu" not in df.columns:
        df["allocated.gpu_type_rgu"] = np.nan

    if cluster_config.rgu_start_date is None:
        logging.warning(
            f"RGU update: no RGU start date for cluster {cluster_config.name}"
        )
        return df

    if cluster_config.gpu_to_rgu_billing is None:
        logging.warning(
            f"RGU update: no RGU/GPU JSON path for cluster {cluster_config.name}"
        )
        return df

    if not os.path.isfile(cluster_config.gpu_to_rgu_billing):
        logging.warning(
            f"RGU update: RGU/GPU JSON file not found for cluster {cluster_config.name} "
            f"at: {cluster_config.gpu_to_rgu_billing}"
        )
        return df

    # Otherwise, parse RGU start date.
    rgu_start_date = datetime.fromisoformat(cluster_config.rgu_start_date).astimezone(
        MTL
    )

    # Get RGU/GPU ratios.
    with open(cluster_config.gpu_to_rgu_billing, "r", encoding="utf-8") as file:
        gpu_to_rgu_billing = json.load(file)
        assert isinstance(gpu_to_rgu_billing, dict)
    if not gpu_to_rgu_billing:
        logging.warning(
            f"RGU update: no RGU/GPU available for cluster {cluster_config.name}"
        )
        return df

    # We have now both RGU stare date and RGU/GPU ratios. We can update columns.

    # Compute column allocated.gpu_type_rgu
    # If a GPU type is not found in RGU/GPU ratios,
    # then ratio will be set to NaN in output column.
    col_ratio_rgu_by_gpu = df["allocated.gpu_type"].map(gpu_to_rgu_billing)

    # Compute slices for both before and since RGU start date.
    slice_before_rgu_time = (df["cluster_name"] == cluster_config.name) & (
        df["start_time"] < rgu_start_date
    )
    slice_after_rgu_time = (df["cluster_name"] == cluster_config.name) & (
        df["start_time"] >= rgu_start_date
    )

    # We can already set column allocated.gpu_type_rgu anyway.
    df.loc[slice_before_rgu_time, "allocated.gpu_type_rgu"] = col_ratio_rgu_by_gpu[
        slice_before_rgu_time
    ]
    df.loc[slice_after_rgu_time, "allocated.gpu_type_rgu"] = col_ratio_rgu_by_gpu[
        slice_after_rgu_time
    ]

    # Compute allocated.gres_rgu where job started before RGU time.
    df.loc[slice_before_rgu_time, "allocated.gres_rgu"] = (
        df["allocated.gres_gpu"][slice_before_rgu_time]
        * col_ratio_rgu_by_gpu[slice_before_rgu_time]
    )

    # Set allocated.gres_rgu with previous allocated.gres_gpu where job started after RGU time.
    df.loc[slice_after_rgu_time, "allocated.gres_rgu"] = df["allocated.gres_gpu"][
        slice_after_rgu_time
    ]
    # Then update allocated.gres_gpu where job started after RGU time.
    df.loc[slice_after_rgu_time, "allocated.gres_gpu"] = (
        df["allocated.gres_gpu"][slice_after_rgu_time]
        / col_ratio_rgu_by_gpu[slice_after_rgu_time]
    )

    return df


def update_job_series_rgu(df: DataFrame):
    """
    Compute RGU information for jobs in given data frame.

    Parameters
    ----------
    df: DataFrame
        Data frame to update, typically returned by `load_job_series`.
        Should contain fields:
         "cluster_name", "start_time", "allocated.gpu_type", "allocated.gres_gpu".

    Returns
    -------
    DataFrame
        Input data frame with:
        - column `allocated.gres_gpu` updated if necessary.
        - column `allocated.gres_rgu` added or updated to contain RGU billing.
          Set to NaN (or unchanged if already present) for jobs from clusters without RGU.
        - column `gpu_type_rgu` added or updated to contain RGU cost per GPU (RGU/GPU ratio).
          Set to NaN (or unchanged if already present) for jobs from clusters without RGU.

    For more details about implementation, see function `update_cluster_job_series_rgu`
    """
    for cluster_config in config().clusters.values():
        update_cluster_job_series_rgu(df, cluster_config)
    return df


slurm_job_metric_names = [
    "slurm_job_core_usage",
    "slurm_job_core_usage_total",
    "slurm_job_fp16_gpu",
    "slurm_job_fp32_gpu",
    "slurm_job_fp64_gpu",
    "slurm_job_memory_active_file",
    "slurm_job_memory_cache",
    "slurm_job_memory_inactive_file",
    "slurm_job_memory_limit",
    "slurm_job_memory_mapped_file",
    "slurm_job_memory_max",
    "slurm_job_memory_rss",
    "slurm_job_memory_rss_huge",
    "slurm_job_memory_unevictable",
    "slurm_job_memory_usage",
    "slurm_job_memory_usage_gpu",
    "slurm_job_nvlink_gpu",
    "slurm_job_nvlink_gpu_total",
    "slurm_job_pcie_gpu",
    "slurm_job_pcie_gpu_total",
    "slurm_job_power_gpu",
    "slurm_job_process_count",
    "slurm_job_sm_occupancy_gpu",
    "slurm_job_states",
    "slurm_job_tensor_gpu",
    "slurm_job_threads_count",
    "slurm_job_utilization_gpu",
    "slurm_job_utilization_gpu_memory",
]
