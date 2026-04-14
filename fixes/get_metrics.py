import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

from mypy.checkpattern import defaultdict
from tqdm import tqdm

from sarc.client.gpumetrics import get_rgus
from sarc.client.job import _jobs_collection, SlurmJob

logger = logging.getLogger("sarc-metrics")


@dataclass(slots=True)
class Metrics:
    time_from: datetime
    time_to: datetime

    nb_unique_users: int
    nb_unique_jobs: int
    nb_jobs_per_unique_user: float

    total_rgu_seconds: float
    rgu_sec_per_unique_user: dict[str, float]
    rgu_sec_per_cluster_type: dict[str, float]
    rgu_sec_per_cluster: dict[str, float]


class Computation:
    __slots__ = ("_gpu_type_to_rgu",)

    def __init__(self):
        self._gpu_type_to_rgu = get_rgus()

    def get_gpu_type_rgu(self, job: SlurmJob) -> float | None:
        gpu_type = job.allocated.gpu_type
        if gpu_type is None:
            return None
        # NB: If GPU type is a MIG
        # (e.g: "A100-SXM4-40GB : a100_1g.5gb"),
        # we currently return RGU for the main GPU type
        # (in this example: "A100-SXM4-40GB")
        return self._gpu_type_to_rgu.get(gpu_type.split(":")[0].rstrip(), None)


def get_metrics(time_from: datetime, time_to: datetime):
    computer = Computation()
    coll_jobs = _jobs_collection()
    job_total_rgu: dict[tuple[str, int], float] = defaultdict(float)

    query = {
        "submit_time": {"$lte": time_to},
        "$or": [{"end_time": {"$gte": time_from}}, {"end_time": None}],
    }

    print(f"Counting jobs for period {time_from} to {time_to}...")
    expected = coll_jobs.get_collection().count_documents(query)
    print(f"Processing {expected} jobs...")

    count = 0
    with tqdm(total=expected, desc="Analyzing jobs") as pbar:
        # Iterate directly on the cursor without complex pagination
        for job in coll_jobs.find_by(query):
            pbar.update(1)
            count += 1

            # User safety check
            if not job.user:
                logger.error(
                    f"Job {job.cluster_name}/{job.job_id} has no user. Skipping."
                )
                continue

            # A job must have started to consume time
            if job.start_time is None:
                continue

            # Determine real or "virtual" end (for RUNNING jobs)
            # Use start_time + elapsed_time if end_time is None
            job_end = job.end_time or (
                job.start_time + timedelta(seconds=job.elapsed_time)
            )
            if job_end < time_from:
                # Maybe an old running job.
                logger.debug(
                    f"{job.cluster_name}/{job.job_id}/{job.job_state.name} end before time frame. Skipping"
                )
                continue

            # Calculate intersection with given time frame
            intersect_start = max(job.start_time, time_from)
            intersect_end = min(job_end, time_to)
            if intersect_start > intersect_end:
                logger.error(
                    f"Job {job.cluster_name}/{job.job_id}: "
                    f"incorrect clipped time: {intersect_start} > {intersect_end}. Skipping."
                )
                continue
            if intersect_start == intersect_end:
                # This job entry has no run time inside this frame. We can skip it.
                continue

            # Compute consumed time for this job inside this time frame
            consumed_seconds = (intersect_end - intersect_start).total_seconds()
            # Compute job RGUs*second inside this time frame
            gpu_type_rgu = computer.get_gpu_type_rgu(job)
            job_rgu_seconds = (
                None if gpu_type_rgu is None else gpu_type_rgu * consumed_seconds
            )
            # Aggregate RGUs*second per job
            # If a jab was rescheduled many times inside this time frame,
            # and consumed some resources more than 1 time across all these occurrences,
            # then we sum all consumptions for this same job
            job_total_rgu[(job.cluster_name, job.job_id)] += job_rgu_seconds
