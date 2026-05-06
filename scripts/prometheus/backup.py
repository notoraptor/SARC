import json
import logging
import sys
from datetime import datetime, timedelta
from typing import Any

from pydantic import BaseModel
from tqdm import tqdm

from sarc.client.job import JobStatistics, _jobs_collection
from sarc.config import UTC, config
from sarc.core.models.validators import datetime_utc

logger = logging.getLogger("prometheus_dump.backup")


DATE_FORMAT_HOUR = "%Y-%m-%dT%H:%M"


class JobPrometheusData(BaseModel):
    """Helper class to store Prometheus metrics for a job"""

    cluster_name: str
    job_id: int
    submit_time: datetime_utc
    stored_statistics: JobStatistics | None = None
    gpu_type: str | None = None


def main() -> int:
    """
    Backup Prometheus metrics from database into cache.
    Currently, save:
    - SlurmJob.stored_statistics,
      if not null and not empty.
    - SlurmJob.allocated_gpu_type.
    """
    cfg = config("scraping")
    coll_jobs = _jobs_collection()
    # Get oldest submit_time.
    # Will be used to get jobs through pagination.
    (oldest_job,) = coll_jobs.find_by({}, sort=[("submit_time", 1)], limit=1)
    oldest_time = oldest_job.submit_time
    newest_time = datetime.now(tz=UTC)
    assert isinstance(oldest_time, datetime)
    assert oldest_time < newest_time
    logger.info(
        f"Oldest submit time in db: {oldest_time} (since {newest_time - oldest_time})"
    )
    # Count jobs
    base_query = {}
    logger.info("Counting jobs ...")
    expected = cfg.mongo.database_instance.jobs.count_documents(base_query)
    # We will save Prometheus metrics in a JSONL file,
    # one line per job, in `<sarc-cache>/prometheus_backup`.
    # Note that there is also a folder `<sarc-cache>/prometheus`,
    # which instead contains raw Prometheus data cached from calls
    # to get_job_time_series().
    assert cfg.cache is not None
    output_dir = (cfg.cache / "prometheus_backup").resolve()
    output_path = output_dir / f"{newest_time.isoformat()}.jsonl"
    logger.info(f"Writing prometheus backup to {output_path}")
    output_dir.mkdir(parents=True, exist_ok=True)
    # Get Prometheus metrics from jobs
    interval = timedelta(days=180)
    count = 0
    updated = 0
    gpu_collected = 0
    stats_collected = 0
    current_time = oldest_time
    # We save jobs gradually in a JSONL file,
    # instead of collecting-then-saving, to prevent any lack-of-memory issue.
    with open(output_path, "w", encoding="utf-8") as jsonl_file:
        with tqdm(total=expected, desc="job(s)") as pbar:
            while current_time < newest_time:
                # Get jobs so that: current_time <= job.submit_time < current_time + interval
                next_time = current_time + interval
                for job in coll_jobs.find_by(
                    {
                        **base_query,
                        "submit_time": {"$gte": current_time, "$lt": next_time},
                    }
                ):
                    pbar.update(1)
                    count += 1
                    if job.allocated.gpu_type is None and job.stored_statistics is None:
                        continue
                    assert (
                        job.allocated.gpu_type is not None
                        or job.stored_statistics is not None
                    )
                    # Get Prometheus metrics
                    data: dict[str, Any] = {}

                    if (
                        job.stored_statistics is not None
                        and not job.stored_statistics.empty()
                    ):
                        data["stored_statistics"] = job.stored_statistics
                        stats_collected += 1

                    if job.allocated.gpu_type is not None:
                        data["gpu_type"] = job.allocated.gpu_type
                        gpu_collected += 1

                    if data:
                        # There are metrics to save.
                        # Create a record.
                        record = JobPrometheusData(
                            cluster_name=job.cluster_name,
                            job_id=job.job_id,
                            submit_time=job.submit_time.astimezone(UTC),
                            **data,
                        )
                        # Convert record to JSON-compatible dict.
                        # We don't directly use record.model_dump_json()
                        # because it replace NaN with null, and we don't want that.
                        # There can be NaN values in stored_statistics.
                        record_dict = record.model_dump(mode="json", exclude_unset=True)
                        # Now, we generate JSON with NaN explicitly allowed.
                        json_output = json.dumps(record_dict, allow_nan=True)
                        # Finally, we save JSON in a line in JSONL file.
                        print(json_output, file=jsonl_file)
                        updated += 1
                current_time = next_time

    # Log some info about backup.
    logger.info(f"Wrote prometheus backup to {output_path}")
    logger.info(f"Collected Prometheus metrics for {updated}/{expected} jobs")
    logger.info(f"Collected GPU types: {gpu_collected}")
    logger.info(f"Collected statistics: {stats_collected}")
    if count != expected:
        logger.warning(
            f"ERROR: Expected {expected} jobs, actually processed {count} jobs"
        )
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, force=True)
    sys.exit(main())
