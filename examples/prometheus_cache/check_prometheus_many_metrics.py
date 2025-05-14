import json
import logging
import pprint
import sys
from typing import List, Tuple
from unittest import mock

from series_with_query_range import new_get_job_time_series

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

        one_result = _get_job_time_series_data(job, "slurm_job_core_usage")

        print("ONE_RESULT: slurm_job_core_usage")
        print("=" * 80)
        print(one_result)
        print()
        print("RESULTS")
        print("=" * 80)
        pprint.pprint(data)


if __name__ == "__main__":
    main()
