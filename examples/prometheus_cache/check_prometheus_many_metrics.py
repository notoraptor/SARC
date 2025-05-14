import json
import logging
import sys
from typing import List, Tuple
from unittest import mock

from series_with_query_range import new_get_job_time_series
from sarc.jobs.series import _get_job_time_series_data, _get_job_time_series_data_from_metrics

from sarc.client.job import SlurmJob, get_job
from sarc.config import scraping_mode_required


@scraping_mode_required
def main():

    logging.basicConfig(level=logging.INFO)

    with open(sys.argv[1], encoding="utf-8") as file:
        job_identifiers: List[Tuple[str, int]] = json.load(file)

    for i, (cluster_name, job_id) in enumerate(job_identifiers):
        print(f"[{i + 1}/{len(job_identifiers)}]", cluster_name, job_id)
        job = get_job(cluster=cluster_name, job_id=job_id)
        results = _get_job_time_series_data_from_metrics(
            job=job,
            metrics=(
                "slurm_job_utilization_gpu",
                "slurm_job_fp16_gpu",
                "slurm_job_fp32_gpu",
                "slurm_job_fp64_gpu",
                "slurm_job_sm_occupancy_gpu",
                "slurm_job_utilization_gpu_memory",
                "slurm_job_power_gpu",
                "slurm_job_core_usage",
                "slurm_job_memory_usage",
            ),
            max_points=10_000,
        )

        one_result = _get_job_time_series_data(job, "slurm_job_utilization_gpu")

        print('ONE_RESULT "slurm_job_utilization_gpu"')
        print('=' * 80)
        print(one_result)
        print()
        print('RESULTS')
        print('=' * 80)
        print(results)

if __name__ == "__main__":
    main()
