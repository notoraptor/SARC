from sarc.client import get_jobs
from sarc.client.job import SlurmState, SlurmJob

START = "2025-06-01"
END = "2025-07-01"


def main_with_get_jobs():
    jobs = list(get_jobs(start=START, end=END))
    print("Rows:", len(jobs))
    missing_gpu_type = [
        job
        for job in jobs
        if ((job.allocated.gres_gpu or 0) > 0) and job.allocated.gpu_type is None
    ]
    print("[Missing GPU type]", len(missing_gpu_type))
    cluster_to_jobs: dict[str, list[SlurmJob]] = {}
    for job in missing_gpu_type:
        cluster_to_jobs.setdefault(job.cluster_name, []).append(job)
    cluster_names = sorted(cluster_to_jobs.keys())
    print("Clusters:", cluster_names)
    print()
    for cluster_name in cluster_names:
        cluster_missing = cluster_to_jobs[cluster_name]
        print(f"[{cluster_name}] {len(cluster_missing)}")
        state_to_jobs: dict[SlurmState, list[SlurmJob]] = {}
        for job in cluster_missing:
            state_to_jobs.setdefault(job.job_state, []).append(job)
        statuses = sorted(state_to_jobs.keys())
        print("Job status:", [s.name for s in statuses])
        for status in statuses:
            print(status, len(state_to_jobs[status]))
        if SlurmState.RUNNING in state_to_jobs:
            for job in state_to_jobs[SlurmState.RUNNING]:
                assert job.allocated.gpu_type is None
                assert job.end_time is None
                print(
                    job.job_id,
                    job.nodes,
                    job.job_state,
                    job.submit_time,
                    job.start_time,
                    job.elapsed_time,
                )
        print()


if __name__ == "__main__":
    main_with_get_jobs()
