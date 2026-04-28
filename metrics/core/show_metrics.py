import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Iterator

from tqdm import tqdm

from metrics.core.job_aggregation import JobAggregation
from metrics.core.report import Report, ReportOutput, TextOutputFile
from metrics.core.utils import expect_datetime_utc
from sarc.client.job import SlurmJob, _jobs_collection
from sarc.rest.client import count_jobs, get_jobs

logger = logging.getLogger(__name__)


class AbstractFactory(ABC):
    """Abstract class to get jobs for metrics"""

    __slots__ = ("time_from", "time_to")

    def __init__(self, time_from: datetime, time_to: datetime):
        self.time_from = time_from
        self.time_to = time_to

    @abstractmethod
    def count(self) -> int:
        raise NotImplementedError()

    @abstractmethod
    def find(self) -> Iterator[SlurmJob]:
        raise NotImplementedError()


class MongoDBFactory(AbstractFactory):
    """Legacy MongoDB factory (intended to be replaced with ApiFactory)"""

    __slots__ = ("query", "coll_jobs")

    def __init__(self, time_from: datetime, time_to: datetime):
        super().__init__(time_from, time_to)
        self.query = {
            "start_time": {"$lte": time_to},
            "allocated.gpu_type": {"$ne": None},
            "allocated.gres_gpu": {"$ne": None},
            "$or": [{"end_time": {"$gte": time_from}}, {"end_time": None}],
        }
        self.coll_jobs = _jobs_collection()

    def count(self) -> int:
        return self.coll_jobs.get_collection().count_documents(self.query)

    def find(self) -> Iterator[SlurmJob]:
        # Iterate directly on the cursor without complex pagination
        yield from self.coll_jobs.find_by(self.query)


class ApiFactory(AbstractFactory):
    """New API factory. May be slower (how much ?), and not better (how much ?) than MongoDB factory."""

    __slots__ = ()

    def count(self) -> int:
        return count_jobs(start=self.time_from, end=self.time_to)

    def find(self) -> Iterator[SlurmJob]:
        for job in get_jobs(start=self.time_from, end=self.time_to):
            if (
                job.allocated.gpu_type is not None
                and job.allocated.gres_gpu is not None
            ):
                yield job


def show_metrics(
    time_from: datetime, time_to: datetime, *, output: ReportOutput | None = None
):
    expect_datetime_utc(time_from)
    expect_datetime_utc(time_to)

    factory = MongoDBFactory(time_from, time_to)
    # factory = ApiFactory(time_from, time_to)
    jagg = JobAggregation(time_from, time_to)

    print(f"Counting jobs for period {time_from} to {time_to}...")
    expected = factory.count()
    print(f"Processing {expected} jobs...")

    count = 0
    with tqdm(total=expected, desc="Analyzing jobs") as pbar:
        # Iterate directly on the cursor without complex pagination
        for job in factory.find():
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

            # Determine real or "virtual" end (for RUNNING jobs).
            # If end_time is None, use start_time + elapsed_time.
            # We don't use current time (now), to avoid any assumption about
            # a running job (we don't know if it really ran until current time).
            # We rely only on available info from latest SLURM scraping.
            job_end = job.end_time or (
                job.start_time + timedelta(seconds=job.elapsed_time)
            )
            if job_end < time_from:
                # Maybe an old running job.
                logger.debug(
                    "Job %s/%s/%s ends before time frame (%s .. %s): "
                    "submit %s, start %s, end %s, elapsed %s, used end: %s. Skipping",
                    job.cluster_name,
                    job.job_id,
                    job.job_state.name,
                    time_from,
                    time_to,
                    job.submit_time,
                    job.start_time,
                    job.end_time,
                    job.elapsed_time,
                    job_end,
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
            jagg.aggregate(job, consumed_seconds)

    report = Report(jagg)
    output = output or TextOutputFile(None)
    output.dump(report)
