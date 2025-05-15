from __future__ import annotations

import difflib
import json
import logging
import os
from dataclasses import dataclass

from prometheus_api_client import MetricRangeDataFrame

from examples.prometheus_cache.series_with_query_range import (
    _get_job_time_series_data_using_query_range,
)
from sarc.client.job import SlurmJob
from sarc.jobs.series import (
    _get_job_time_series_data,
    _get_job_time_series_data_cache_key,
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
    results = results_offset = _get_job_time_series_data(
        job, metric, min_interval, max_points, measure, aggregation
    )

    # We don't cache if keystring (=> job.end_time) is not available.
    keystring = _get_job_time_series_data_cache_key(
        job, metric, min_interval, max_points, measure, aggregation
    )
    if keystring is not None:
        results_range = _get_job_time_series_data_using_query_range(
            job, metric, min_interval, max_points, measure, aggregation
        )

        cache = PromCache()
        cache_offset = PromCache("offset")
        cache_range = PromCache("range")
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


@dataclass(slots=True)
class Data:
    data: list
    name: str

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


# pylint: disable=too-many-branches
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
