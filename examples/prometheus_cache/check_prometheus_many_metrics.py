import json
import logging
import pprint
import sys
from typing import List, Tuple
from unittest import mock

from series_with_query_range import PromCache, new_get_job_time_series

from sarc.client.job import SlurmJob, get_job
from sarc.config import scraping_mode_required
from sarc.jobs.series import (
    _get_job_time_series_data,
    _get_job_time_series_data_from_metrics,
)


@scraping_mode_required
def main():
    metrics = (
        "slurm_job_utilization_gpu",
        "slurm_job_fp16_gpu",
        "slurm_job_fp32_gpu",
        "slurm_job_fp64_gpu",
        "slurm_job_sm_occupancy_gpu",
        "slurm_job_utilization_gpu_memory",
        "slurm_job_power_gpu",
        "slurm_job_core_usage",
        "slurm_job_memory_usage",
    )

    logging.basicConfig(level=logging.INFO)

    with open(sys.argv[1], encoding="utf-8") as file:
        job_identifiers: List[Tuple[str, int]] = json.load(file)

    for i, (cluster_name, job_id) in enumerate(job_identifiers):
        print(f"[{i + 1}/{len(job_identifiers)}]", cluster_name, job_id)
        job = get_job(cluster=cluster_name, job_id=job_id)
        results = _get_job_time_series_data_from_metrics(
            job=job,
            metrics=metrics,
            max_points=10_000,
        )
        data = {metric: [] for metric in metrics}
        all_metrics = set(metrics)
        for result in results:
            metric = result["metric"]["__name__"]
            assert metric in all_metrics, metric
            all_metrics.remove(metric)
            data[metric].append(result)
        if all_metrics:
            print("missing:", all_metrics)

        one_results = {
            metric: _get_job_time_series_data(job, metric) for metric in metrics
        }

        for metric in metrics:
            if data[metric] == one_results[metric]:
                print("IDENTICAL", metric)
                print()
            else:
                print(f"DIFF {metric}")
                print("=" * 90)
                print(PromCache.diff(data[metric], one_results[metric]))


if __name__ == "__main__":
    main()
