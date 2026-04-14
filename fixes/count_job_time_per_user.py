import logging
from collections import defaultdict
from datetime import datetime, timedelta

from tqdm import tqdm

from sarc.client.job import _jobs_collection
from sarc.config import UTC

logger = logging.getLogger(__name__)


def main():
    # Time window for analysis (Year 2025)
    time_from = datetime(2025, 1, 1, tzinfo=UTC)
    time_to = datetime(2026, 1, 1, tzinfo=UTC)
    now = datetime.now(UTC)

    cluster_to_users: dict[str, set[str]] = defaultdict(set)
    cluster_to_time: dict[str, float] = defaultdict(float)
    cluster_to_nb_jobs: dict[str, int] = defaultdict(int)

    coll_jobs = _jobs_collection()

    # Optimized query using (submit_time, end_time) indexes
    # Capture all jobs that might have run in 2025:
    # 1. Submitted before the end of 2025
    # 2. FINISHED after the start of 2025 OR still running (None)
    query = {
        "submit_time": {"$lt": time_to},
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
                logger.warning(
                    f"Job {job.job_id} on {job.cluster_name} has no user. Skipping."
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

            # Calculate intersection with the 2025 window
            # The job consumed time between max(start, 2025) and min(end, 2026)
            intersect_start = max(job.start_time, time_from)
            intersect_end = min(job_end, time_to)

            if intersect_start < intersect_end:
                consumed_seconds = (intersect_end - intersect_start).total_seconds()

                cluster_name = job.cluster_name
                cluster_to_users[cluster_name].add(job.user)
                cluster_to_time[cluster_name] += consumed_seconds
                cluster_to_nb_jobs[cluster_name] += 1

    # Display statistics
    if cluster_to_users:
        print("\n" + "=" * 40)
        print(f"USAGE STATS FOR YEAR 2025 (Calculated at {now})")
        print("=" * 40)

        for cluster_name, unique_users in sorted(cluster_to_users.items()):
            total_time = cluster_to_time[cluster_name]
            nb_users = len(unique_users)
            avg_time = total_time / nb_users

            delta_total_time = timedelta(seconds=total_time)
            delta_avg_time = timedelta(seconds=avg_time)
            print(
                f"[{cluster_name}]\n"
                f" Jobs active in 2025:  {cluster_to_nb_jobs[cluster_name]}\n"
                f" unique users:         {nb_users}\n"
                f" total time in 2025:   {delta_total_time} ({total_time} seconds)\n"
                f" time per user:        {delta_avg_time} ({avg_time} seconds)\n"
            )
    else:
        print("\nNo jobs found for the specified period.")


if __name__ == "__main__":
    main()
