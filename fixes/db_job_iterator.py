import logging
from datetime import datetime, timedelta
from typing import Iterator

from tqdm import tqdm

from sarc.client.job import SlurmJob, _jobs_collection
from sarc.config import UTC

logger = logging.getLogger(__name__)


def _expect_no_submit_time(query: dict) -> None:
    if "submit_time" in query:
        raise ValueError(f"submit_time not expected in query: {query}")
    for value in query.values():
        if isinstance(value, dict):
            _expect_no_submit_time(value)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    _expect_no_submit_time(item)
                else:
                    _expect_basic_value(item)
        else:
            _expect_basic_value(value)


def _expect_basic_value(value):
    if value is not None and not isinstance(value, (str, bool, int, float, datetime)):
        raise ValueError(f"Unexpected value type in query: {type(value)}: {value}")


def get_database_jobs(base_query: dict | None = None) -> Iterator[SlurmJob]:
    base_query = base_query or {}
    _expect_no_submit_time(base_query)

    coll_jobs = _jobs_collection()
    # Get oldest submit_time.
    # Will be used to get jobs through pagination (every 7 days),
    first = list(coll_jobs.find_by(base_query, sort=[("submit_time", 1)], limit=1))
    if not first:
        logger.warning("No jobs found")
        return
    (oldest_job,) = first
    oldest_time = oldest_job.submit_time
    newest_time = datetime.now(tz=UTC)
    assert isinstance(oldest_time, datetime)
    assert oldest_time < newest_time
    logger.info(f"Oldest time: {oldest_time} (since {newest_time - oldest_time})")

    # Count documents
    logger.info("Counting query jobs ...")
    expected = coll_jobs.get_collection().count_documents(base_query)
    logger.info(f"Query jobs: {expected}")

    interval = timedelta(days=120)
    count = 0
    current_time = oldest_time
    with tqdm(total=expected, desc="job(s)") as pbar:
        while current_time < newest_time:
            # Get jobs so that: current_time <= job.submit_time < current_time + interval
            next_time = min(current_time + interval, newest_time)
            for job in coll_jobs.find_by(
                {**base_query, "submit_time": {"$gte": current_time, "$lt": next_time}}
            ):
                yield job
                pbar.update(1)
                count += 1
            current_time = next_time

    if count != expected:
        logger.warning(f"Expected {expected} jobs, actually processed {count} jobs")
