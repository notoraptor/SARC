import difflib
import json
import logging
import os
import time
from typing import List

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
    os.environ["SARC_CACHE"] = "ignore"
    assert os.getenv("SARC_CACHE") == "ignore"

    job_identifiers = [
        # ["narval", 43481649],
        # ["narval", 43502060],
        # ["narval", 43522972],
        # ["narval", 43528764],
        # ["narval", 43535251],
        # ["narval", 43539058],
        # ["narval", 43539060],
        # ["narval", 43539421],
        # ["narval", 43540479],
        ["narval", 43541060],
    ]

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

    for i, (cluster_name, job_id) in enumerate(job_identifiers):
        logging.info(f"[{i + 1}/{len(job_identifiers)}] {cluster_name} {job_id}")
        job = get_job(cluster=cluster_name, job_id=job_id)

        with Profiler() as pf_many_calls:
            ret_many_calls = {
                metric: get_job_time_series(
                    job=job, metric=metric, max_points=10_000, dataframe=False
                )
                for metric in metrics
            }
        logging.info(f"Time results with many calls: {pf_many_calls}")

        with Profiler() as pf_one_call:
            ret_one_call = get_job_time_series(
                job=job, metric=metrics, max_points=10_000, dataframe=False
            )
        logging.info(f"Time results with one call: {pf_one_call}")

        data = {metric: [] for metric in metrics}
        for result in ret_one_call:
            data[result["metric"]["__name__"]].append(result)

        for metric in metrics:
            series_from_many = data[metric]
            series_from_one = ret_many_calls[metric]
            if series_from_many == series_from_one:
                logging.info(
                    f"SAME: {metric}, "
                    f"{_nb_values(series_from_many)} vs {_nb_values(series_from_one)}"
                )
            else:
                message = (
                    f"DIFF: {metric}, "
                    f"{_nb_values(series_from_many)} vs {_nb_values(series_from_one)}"
                )
                logging.info(message)
                print(message)
                print("=" * 90)
                print(_diff(series_from_many, series_from_one))
                print()


def _nb_values(results: List[dict]) -> List[int]:
    return [len(result["values"]) for result in results]


def _diff(dict1, dict2) -> str:
    d1_str = json.dumps(dict1, indent=1, sort_keys=True)
    d2_str = json.dumps(dict2, indent=1, sort_keys=True)

    return "\n".join(
        difflib.unified_diff(
            d1_str.splitlines(),
            d2_str.splitlines(),
            fromfile="dict1",
            tofile="dict2",
            lineterm="",
        )
    )


if __name__ == "__main__":
    main()
