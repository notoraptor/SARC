from sarc.client import load_job_series
from sarc.client.job import SlurmState

START = "2025-06-01"
END = "2025-07-01"


def main_with_dataframe():
    df = load_job_series(start=START, end=END)
    print("Rows:", len(df))

    missing_gpu_type = df.query(
        "(`requested.gres_gpu`>0)&(`allocated.gpu_type`.isna())"
    )
    # missing_gpu_type = missing_gpu_type[
    #     (missing_gpu_type["job_state"] != SlurmState.PENDING)
    #     & (missing_gpu_type["elapsed_time"] > 0)
    # ]
    missing_gpu_type = missing_gpu_type[missing_gpu_type["elapsed_time"] > 0]
    print("[Missing GPU type]", len(missing_gpu_type))

    cluster_names = sorted(missing_gpu_type["cluster_name"].unique())
    print("Clusters:", cluster_names)
    print()
    for cluster_name in cluster_names:
        cluster_missing = missing_gpu_type[
            missing_gpu_type["cluster_name"] == cluster_name
        ]
        print(f"[{cluster_name}] {len(cluster_missing)}")
        statuses: list[SlurmState] = sorted(cluster_missing["job_state"].unique())
        print("Job status:", [s.name for s in statuses])
        for status in statuses:
            print(status, len(cluster_missing[cluster_missing["job_state"] == status]))

        special_status = SlurmState.RUNNING
        if special_status in statuses:
            print(special_status.name)
            print(
                cluster_missing[cluster_missing["job_state"] == special_status][
                    [
                        "job_id",
                        "job_state",
                        "submit_time",
                        "start_time",
                        "end_time",
                        "elapsed_time",
                        "time_limit",
                    ]
                ].to_markdown()
            )
        print()


if __name__ == "__main__":
    main_with_dataframe()
    # main_with_get_jobs()
