import json
import logging
import sys
import time
from typing import List, Tuple

from series_with_query_range import PromCache

from sarc.client.job import get_job
from sarc.config import scraping_mode_required
from sarc.jobs.series import (
    _get_job_time_series_data,
    _get_job_time_series_data_from_metrics,
)


class Profiler:
    __slots__ = ("start", "end", "duration")

    def __enter__(self):
        self.start = time.perf_counter()
        return self  # peut être utilisé comme "profiler"

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end = time.perf_counter()
        self.duration = self.end - self.start

    def __str__(self):
        return f"{self.duration:.6f} sec"


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
        logging.info(f"[{i + 1}/{len(job_identifiers)}] {cluster_name} {job_id}")
        job = get_job(cluster=cluster_name, job_id=job_id)

        with Profiler() as pf_one_results:
            one_results = {
                metric: _get_job_time_series_data(
                    job=job, metric=metric, max_points=10_000
                )
                for metric in metrics
            }
        logging.info(f"Time one results: {pf_one_results}")

        with Profiler() as pf_multiple:
            data = _get_job_time_series_data_from_metrics(
                job=job,
                metrics=metrics,
                max_points=10_000,
            )
        logging.info(f"Time multiple results: {pf_multiple}")

        for metric in metrics:
            data_metric = data[metric]
            one_result_metric = one_results[metric]
            if data_metric != one_result_metric:
                message = f"DIFF {metric} {PromCache.len_results(data_metric)} vs {PromCache.len_results(one_result_metric)}"
                logging.info(message)
                print(message)
                print("=" * 90)
                print(PromCache.diff(data_metric, one_result_metric))
                print()


if __name__ == "__main__":
    main()
