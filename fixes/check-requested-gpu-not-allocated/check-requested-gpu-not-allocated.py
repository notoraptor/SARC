import sys

from sarc.client import get_jobs
from sarc.config import config, scraping_mode_required, MTL
from datetime import datetime, timedelta
from sarc.client.job import SlurmJob, _jobs_collection, SlurmState
from sarc.jobs.node_gpu_mapping import get_node_to_gpu
from tqdm import tqdm


def main():
    coll_jobs = _jobs_collection()
    db_jobs = config().mongo.database_instance.jobs
    assert (
        db_jobs.count_documents(
            {
                "requested.gres_gpu": {"$gt": 0},
                "allocated.gres_gpu": 0,
            }
        )
        == 0
    )

    assert (
        db_jobs.count_documents(
            {
                "requested.gres_gpu": {"$gt": 0},
                "allocated.gres_gpu": None,
                "job_state": None,
            }
        )
        == 0
    )

    query = {
        "requested.gres_gpu": {"$gt": 0},
        "allocated.gres_gpu": None,
    }
    expected = db_jobs.count_documents(query)
    print("expected", expected)

    count = 0
    state_to_jobs: dict[SlurmState, list[SlurmJob]] = {}
    for job in tqdm(
        coll_jobs.find_by(query), total=expected, desc="GPU jobs not allocated"
    ):
        assert job.requested.gres_gpu > 0
        assert job.allocated.gres_gpu is None
        state_to_jobs.setdefault(job.job_state, []).append(job)
        count += 1
    assert count == expected
    print(sorted(s.name for s in state_to_jobs.keys()))
    for state, jobs in state_to_jobs.items():
        print(state.name, len(jobs))


if __name__ == "__main__":
    main()
