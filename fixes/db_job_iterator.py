from datetime import datetime, timedelta
from typing import Iterable

from tqdm import tqdm

from sarc.client.job import SlurmJob, _jobs_collection
from sarc.config import UTC, config


def get_database_jobs(base_query: dict | None = None) -> Iterable[SlurmJob]:
    coll_jobs = _jobs_collection()
    # Get oldest submit_time.
    # Will be used to get jobs through pagination (every 7 days),
    (oldest_job,) = coll_jobs.find_by({}, sort=[("submit_time", 1)], limit=1)
    oldest_time = oldest_job.submit_time
    newest_time = datetime.now(tz=UTC)
    assert isinstance(oldest_time, datetime)
    assert oldest_time < newest_time
    print(f"Oldest time: {oldest_time} (since {newest_time - oldest_time})")

    base_query = base_query or {}

    # Count documents
    print("Counting query jobs ...")
    expected = config().mongo.database_instance.jobs.count_documents(base_query)
    print("Query jobs:", expected)

    interval = timedelta(days=7)
    count = 0
    current_time = oldest_time
    with tqdm(total=expected, desc="job(s)") as pbar:
        while current_time < newest_time:
            # Get jobs so that: current_time <= job.submit_time < current_time + interval
            next_time = current_time + interval
            for job in coll_jobs.find_by(
                {**base_query, "submit_time": {"$gte": current_time, "$lt": next_time}},
                sort=[("submit_time", 1)],
            ):
                yield job
                pbar.update(1)
                count += 1
            current_time = next_time

    if count != expected:
        raise RuntimeError(f"Expected {expected} jobs, actually processed {count} jobs")
