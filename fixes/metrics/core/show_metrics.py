import logging
from datetime import datetime, timedelta

from tqdm import tqdm

from fixes.metrics.core.job_aggregation import JobAggregation
from fixes.metrics.core.report import Report
from sarc.client.job import _jobs_collection

logger = logging.getLogger("sarc-metrics")


def show_metrics(time_from: datetime, time_to: datetime, *, output: str | None = None):
    jagg = JobAggregation(time_from, time_to)
    coll_jobs = _jobs_collection()

    query = {
        "start_time": {"$lte": time_to},
        "allocated.gpu_type": {"$ne": None},
        "allocated.gres_gpu": {"$ne": None},
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
                    f"Job {job.cluster_name}/{job.job_id}/{job.job_state.name} ends before time frame: "
                    f"submit {job.submit_time}, start {job.start_time}, end {job.end_time}, "
                    f"elapsed {job.elapsed_time}, used end: {job_end}. Skipping"
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
    report.dump_text_file(output)
