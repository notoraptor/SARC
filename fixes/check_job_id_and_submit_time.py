import sys
from sarc.config import config, scraping_mode_required, MTL
from datetime import datetime, timedelta
from sarc.client.job import SlurmJob, _jobs_collection, count_jobs, get_jobs
from sarc.jobs.node_gpu_mapping import get_node_to_gpu
from tqdm import tqdm


def main():
    # Set to collect couples (job ID, submit time)
    job_keys: set[tuple[int, datetime]] = set()

    coll_jobs = _jobs_collection()

    # Asser every job in DB does have a submit_time
    print("Counting jobs without submit_time ...")
    nb_jobs_without_submit_time = config().mongo.database_instance.jobs.count_documents(
        {"submit_time": None}
    )
    print("Jobs without submit time:", nb_jobs_without_submit_time)
    assert nb_jobs_without_submit_time == 0

    # Get oldest submit_time.
    # Will be used to get jobs through pagination (every 7 days),
    (oldest_job,) = coll_jobs.find_by({}, sort=[("submit_time", 1)], limit=1)
    oldest_time = oldest_job.submit_time
    newest_time = datetime.now(tz=MTL)
    assert isinstance(oldest_time, datetime)
    assert oldest_time < newest_time
    print(f"Oldest time: {oldest_time} (since {newest_time - oldest_time})")

    base_query = {}
    print("Counting jobs ...")
    expected = config().mongo.database_instance.jobs.count_documents(base_query)
    print("Jobs:", expected)

    interval = timedelta(days=7)
    count = 0
    current_time = oldest_time
    with tqdm(total=expected, desc="jobs") as pbar:
        while current_time < newest_time:
            # Get jobs so that: current_time <= job.submit_time < current_time + interval
            next_time = current_time + interval
            for job in coll_jobs.find_by(
                {
                    **base_query,
                    "submit_time": {"$gte": current_time, "$lt": next_time},
                }
            ):
                # work with job
                job_keys.add((job.job_id, job.submit_time))
                # Update progres bar
                pbar.update(1)
                # Update job count
                count += 1
            current_time = next_time

    # Check count
    exit_code = 0
    if count != expected:
        print(
            f"WARNING: Expected {expected} jobs, actually processed {count} jobs",
            file=sys.stderr,
        )
        exit_code = -1

    # Post-work
    print("Keys (job ID, submit time):", len(job_keys))
    if len(job_keys) != expected:
        print(
            f"WARNING: keys ({len(job_keys)}) != expected ({expected})", file=sys.stderr
        )
        exit_code = 1

    # Exit
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
