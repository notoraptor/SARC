import json
import sys
from typing import List, Tuple

from sarc.client.job import SlurmJob, get_job
from sarc.config import scraping_mode_required
from sarc.jobs.series import get_job_time_series


@scraping_mode_required
def main():
    with open(sys.argv[1], encoding="utf-8") as file:
        job_identifiers: List[Tuple[str, int]] = json.load(file)

    for cluster_name, job_id in job_identifiers:
        job = get_job(cluster=cluster_name, job_id=job_id)
        gpu_type = _get_gpu_type(job)
        stats = job.statistics(recompute=True, save=False)


def _get_gpu_type(entry: SlurmJob):
    output = get_job_time_series(
        job=entry,
        metric="slurm_job_utilization_gpu_memory",
        max_points=1,
        dataframe=False,
    )
    if output:
        return output[0]["metric"]["gpu_type"]
    return None


if __name__ == "__main__":
    main()
