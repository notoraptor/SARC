from sarc.config import config
from sarc.client.job import SlurmJob, _jobs_collection, SlurmState
from tqdm import tqdm
from typing import Iterable, Any
from io import StringIO


def main():
    coll_jobs = _jobs_collection()
    db_jobs = config().mongo.database_instance.jobs

    # allocated.gres_gpu is either None or > 0, neither 0.
    assert (
        db_jobs.count_documents(
            {
                "requested.gres_gpu": {"$gt": 0},
                "allocated.gres_gpu": 0,
            }
        )
        == 0
    )

    # We always have a job state.
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
    print("Jobs with query:", query)
    print("expected", expected)

    count = 0
    state_to_elapsed_to_jobs: dict[SlurmState, dict[float, list[SlurmJob]]] = {}
    for job in tqdm(
        coll_jobs.find_by(query), total=expected, desc="GPU jobs not allocated"
    ):
        assert job.requested.gres_gpu > 0
        assert job.allocated.gres_gpu is None
        state_to_elapsed_to_jobs.setdefault(job.job_state, {}).setdefault(
            job.elapsed_time, []
        ).append(job)
        count += 1
    assert count == expected

    print("Job states found:")
    print(sorted(s.name for s in state_to_elapsed_to_jobs.keys()))
    print()

    for state, elapsed_to_jobs in state_to_elapsed_to_jobs.items():
        nb_jobs = sum(len(jobs) for jobs in elapsed_to_jobs.values())
        print(f"{state.name}: {nb_jobs}")

        elapsed_zero = elapsed_to_jobs.pop(0, [])
        print(f"\tElapsed 0 sec: {len(elapsed_zero)}")

        if elapsed_to_jobs:
            nb_elapsed = sum(len(jobs) for jobs in elapsed_to_jobs.values())
            assert nb_elapsed == nb_jobs - len(elapsed_zero)
            print(f"\tElapsed >0 sec: {nb_elapsed}")
            fields = ("cluster_name", "job_id", "submit_time", "elapsed_time")
            print(
                _pretty_table(
                    (
                        job
                        for elapsed in sorted(elapsed_to_jobs.keys())
                        for job in elapsed_to_jobs[elapsed]
                    ),
                    fields,
                    indent="\t\t",
                )
            )
            # for elapsed in sorted(elapsed_to_jobs.keys()):
            #     for job in elapsed_to_jobs[elapsed]:
            #         print(
            #             f"\t\t"
            #             f"cluster_name={job.cluster_name} "
            #             f"job_id={job.job_id} "
            #             f"submit_time={job.submit_time} "
            #             f"elapsed={job.elapsed_time}"
            #         )


def _pretty_table(
    iterable: Iterable[Any],
    fields: Iterable[str],
    index: int | None = 1,
    indent: str = "",
    space: int = 2,
) -> str:
    fields = list(fields)
    headers = [f"[{field}]" for field in fields]
    table = [[str(getattr(el, field)) for field in fields] for el in iterable]
    if index is not None:
        headers = ["#"] + headers
        table = [([str(index + i)] + row) for i, row in enumerate(table)]
    col_sizes = [
        max([len(headers[i])] + [len(row[i]) for row in table]) + space
        for i in range(len(headers))
    ]

    with StringIO() as output:
        for row in [headers] + table:
            print(
                indent + "".join(col.rjust(size) for size, col in zip(col_sizes, row)),
                file=output,
            )
        return output.getvalue()


if __name__ == "__main__":
    main()
