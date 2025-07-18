"""
Requirements:
- scraping mode
- node->gpu mappings available (run `sarc acquire slurmconfig` on all clusters)

Find GPU jobs without GPU type and try to infer GPU type using node->gpu mappings.

**NB**:
By default, the script will just check GPU Jobs without GPU type:
    uv run fix_gpu_jobs_without_gpu_type.py
To formerly save inferred GPU type, call script with argument `save`:
    uv run fix_gpu_jobs_without_gpu_type.py save
"""

import sys
from sarc.config import config, scraping_mode_required, MTL
from datetime import datetime, timedelta
from sarc.client.job import SlurmJob, _jobs_collection
from sarc.jobs.node_gpu_mapping import get_node_to_gpu
from tqdm import tqdm


@scraping_mode_required
def main():
    save = len(sys.argv) == 2 and sys.argv[1].strip().lower() == "save"
    if not save:
        print("[read-only]")

    coll_jobs = _jobs_collection()

    # Get oldest submit_time.
    # Will be used to get jobs through pagination (every 7 days),
    (oldest_job,) = coll_jobs.find_by({}, sort=[("submit_time", 1)], limit=1)
    oldest_time = oldest_job.submit_time
    newest_time = datetime.now(tz=MTL)
    assert isinstance(oldest_time, datetime)
    assert oldest_time < newest_time
    print(f"Oldest time: {oldest_time} (since {newest_time - oldest_time})")

    # Count GPU jobs without GPU type.
    base_query = {
        "requested.gres_gpu": {"$gt": 0},
        "allocated.gres_gpu": {"$gt": 0},
        "allocated.gpu_type": None,
    }
    print("Counting GPU jobs w/o GPU type ...")
    expected = config().mongo.database_instance.jobs.count_documents(base_query)
    print("GPU jobs w/o GPU type:", expected)

    interval = timedelta(days=7)
    count = 0
    updated = 0
    current_time = oldest_time
    with tqdm(total=expected, desc="GPU jobs w/o GPU type") as pbar:
        while current_time < newest_time:
            # Get jobs so that: current_time <= job.submit_time < current_time + interval
            next_time = current_time + interval
            for job in coll_jobs.find_by(
                {
                    **base_query,
                    "submit_time": {"$gte": current_time, "$lt": next_time},
                }
            ):
                gpu_type = get_harmonized_gpu_type(job)
                if gpu_type is not None:
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
    """Find GPU type and return harmonized GPU name if possible."""

    assert job.allocated.gres_gpu > 0 and job.allocated.gpu_type is None

    # We need node->gpu mapping.
    node_gpu_mapping = get_node_to_gpu(
        job.cluster_name, job.start_time or job.submit_time
    )
    if node_gpu_mapping:
        node_to_gpu = node_gpu_mapping.node_to_gpu
        gpu_types = {
            gpu for nodename in job.nodes for gpu in node_to_gpu.get(nodename, ())
        }
        # We must find 1 gpu type across job nodes.
        if len(gpu_types) == 1:
            gpu_type = gpu_types.pop()
            # We can now try to harmonize GPU name.
            cluster = job.fetch_cluster_config()
            harmonized_gpu_names = {
                cluster.harmonize_gpu(nodename, gpu_type)
                for nodename in (job.nodes or [""])
            }
            harmonized_gpu_names.discard(None)
            # If we found 1 harmonized name, return it.
            # Otherwise, we just return GPU type.
            return (
                harmonized_gpu_names.pop()
                if len(harmonized_gpu_names) == 1
                else gpu_type
            )
    return None


if __name__ == "__main__":
    main()
