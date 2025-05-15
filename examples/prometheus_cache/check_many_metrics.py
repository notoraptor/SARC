import difflib
import json
import logging
import sys
import time
from typing import List, Tuple

from sarc.client.job import get_job
from sarc.config import scraping_mode_required
from sarc.jobs.series import get_job_time_series


class Profiler:
    __slots__ = ("start", "end", "duration")

    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end = time.perf_counter()
        self.duration = self.end - self.start

    def __str__(self):
        return f"{self.duration:.6f} sec"


@scraping_mode_required
def main():
    logging.basicConfig(level=logging.INFO)

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

    with open(sys.argv[1], encoding="utf-8") as file:
        job_identifiers: List[Tuple[str, int]] = json.load(file)

    for i, (cluster_name, job_id) in enumerate(job_identifiers):
        logging.info(f"[{i + 1}/{len(job_identifiers)}] {cluster_name} {job_id}")
        job = get_job(cluster=cluster_name, job_id=job_id)

        with Profiler() as pf_one_results:
            one_results = {
                metric: get_job_time_series(
                    job=job, metric=metric, max_points=10_000, dataframe=False
                )
                for metric in metrics
            }
        logging.info(f"Time one results: {pf_one_results}")

        with Profiler() as pf_multiple:
            results = get_job_time_series(
                job=job, metric=metrics, max_points=10_000, dataframe=False
            )
        logging.info(f"Time multiple results: {pf_multiple}")

        data = {metric: [] for metric in metrics}
        for result in results:
            data[result["metric"]["__name__"]].append(result)

        for metric in metrics:
            data_metric = data[metric]
            one_result_metric = one_results[metric]
            if data_metric == one_result_metric:
                logging.info(
                    f"Identical: {metric}, {_nb_values(data_metric)} vs {_nb_values(one_result_metric)}"
                )
            else:
                message = f"DIFF {metric} {_nb_values(data_metric)} vs {_nb_values(one_result_metric)}"
                logging.info(message)
                print(message)
                print("=" * 90)
                print(_diff(data_metric, one_result_metric))
                print()


def _nb_values(results: List[dict]) -> List[int]:
    return [len(result["values"]) for result in results]


def _diff(dict1, dict2) -> str:

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

    return "\n".join(diff)


if __name__ == "__main__":
    main()
