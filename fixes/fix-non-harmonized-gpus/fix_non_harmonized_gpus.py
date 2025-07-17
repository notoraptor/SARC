import sys
from sarc.config import config, scraping_mode_required, MTL
from datetime import datetime, timedelta
from sarc.client.job import SlurmJob, _jobs_collection
from tqdm import tqdm
from sarc.client import get_rgus


@scraping_mode_required
def main():
    save = len(sys.argv) == 2 and sys.argv[1].strip().lower() == "save"
    if not save:
        print("[read-only]")

    harmonized_names = sorted(get_rgus().keys())
    harmonized_set = set(harmonized_names)
    print("Harmonized names:", len(harmonized_names))
    coll_jobs = _jobs_collection()

    # Get oldest submit_time.
    # Will be used to get jobs through pagination (every 7 days),
    (oldest_job,) = coll_jobs.find_by({}, sort=[("submit_time", 1)], limit=1)
    oldest_time = oldest_job.submit_time
    newest_time = datetime.now(tz=MTL)
    assert isinstance(oldest_time, datetime)
    assert oldest_time < newest_time
    print(f"Oldest time: {oldest_time} (since {newest_time - oldest_time})")

    # Count GPU jobs with non-harmonized GPUs.
    base_query = {
        "allocated.gpu_type": {"$ne": None, "$nin": harmonized_names},
    }
    print("Counting non-harmonized GPUs ...")
    expected = config().mongo.database_instance.jobs.count_documents(base_query)
    print("Non-harmonized GPUs:", expected)

    interval = timedelta(days=7)
    count = 0
    updated = 0
    current_time = oldest_time
    with tqdm(total=expected, desc="Non-harmonized GPUs") as pbar:
        while current_time < newest_time:
            # Get jobs so that: current_time <= job.submit_time < current_time + interval
            next_time = current_time + interval
            for job in coll_jobs.find_by(
                {
                    **base_query,
                    "submit_time": {"$gte": current_time, "$lt": next_time},
                }
            ):
                assert job.allocated.gpu_type is not None
                assert job.allocated.gpu_type not in harmonized_set
                gpu_type = get_harmonized_gpu_type(job)
                if gpu_type is not None and gpu_type != job.allocated.gpu_type:
                    if save:
                        job.allocated.gpu_type = gpu_type
                        job.save()
                    updated += 1
                pbar.update(1)
                count += 1
            current_time = next_time

    print(f"Updated {updated}/{expected} jobs", "" if save else "(not saved)")
    if count != expected:
        print(
            f"WARNING: Expected {expected} jobs, actually processed {count} jobs",
            file=sys.stderr,
        )
        sys.exit(-1)
    sys.exit(0)


def get_harmonized_gpu_type(job: SlurmJob) -> str | None:
    """Harmonize GPU name if possible."""
    gpu_type = job.allocated.gpu_type
    cluster = job.fetch_cluster_config()
    harmonized_gpu_names = {
        cluster.harmonize_gpu(nodename, gpu_type) for nodename in (job.nodes or [""])
    }
    harmonized_gpu_names.discard(None)
    # If we found 1 harmonized name, return it.
    # Otherwise, we return None.
    return harmonized_gpu_names.pop() if len(harmonized_gpu_names) == 1 else None


if __name__ == "__main__":
    main()
