import sys
import pickle
from typing import Any, TypedDict
from sarc.client.job import SlurmState
from datetime import datetime
import pprint


ENTRY = TypedDict(
    "ENTRY",
    {
        "cluster_name": str,
        "job_id": int,
        "job_state": str,
        "submit_time": datetime,
        "requested": int | None,
        "allocated": int | None,
        "gpu_type": str | None,
    },
)


def main():
    pickle_path = sys.argv[1]
    with open(pickle_path, mode="rb") as file:
        documents: list[list[ENTRY]] = pickle.load(file)
    print("Documents:", len(documents))
    print()

    requeued = SlurmState.REQUEUED.name
    node_fail = SlurmState.NODE_FAIL.name
    pending = SlurmState.PENDING.name
    for duplicates in documents:
        state_to_jobs: dict[str, list[ENTRY]] = {}
        first = duplicates[0]
        for dup in duplicates:
            state_to_jobs.setdefault(dup["job_state"], []).append(dup)
        if requeued not in state_to_jobs and node_fail not in state_to_jobs:
            if pending in state_to_jobs:
                assert len(state_to_jobs) > 1
                (pending_job,) = state_to_jobs[pending]
                assert pending_job["gpu_type"] is None
                nb_other = 0
                for key, jobs in state_to_jobs.items():
                    if key != pending:
                        for job in jobs:
                            nb_other += 1
                            if pending_job["submit_time"] > job["submit_time"]:
                                print(
                                    "Pending more recent:",
                                    first["cluster_name"],
                                    first["job_id"],
                                )
                                pprint.pprint(duplicates)
                assert nb_other
            else:
                print(
                    "Missing: requeued, node_fail, pending:",
                    first["cluster_name"],
                    first["job_id"],
                )
                pprint.pprint(duplicates)
                print()


if __name__ == "__main__":
    main()
