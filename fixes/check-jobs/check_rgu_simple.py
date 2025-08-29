import argparse

from sarc.client import (
    load_job_series,
    update_job_series_rgu,
    get_available_clusters,
)

START = "2025-06-01"
END = "2025-07-01"

MARKDOWN_FIELDS = [
    "job_id",
    "cluster_name",
    "start_time",
    "job_state",
    "allocated.gpu_type",
    "allocated.gres_gpu",
]
MARKDOWN_FIELDS_WITH_RGU = MARKDOWN_FIELDS + [
    "allocated.gres_rgu",
    "allocated.gpu_type_rgu",
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cluster", type=str, required=True, help="Cluster name")
    args = parser.parse_args()

    cluster_name = args.cluster

    for cluster in get_available_clusters():
        if cluster.cluster_name == cluster_name:
            print("Cluster:", cluster)
            print()

    # Get jobs dataframe
    df = load_job_series(start=START, end=END, cluster=cluster_name)
    # Get RGU
    update_job_series_rgu(df)

    # print(df[MARKDOWN_FIELDS_WITH_RGU].to_markdown(),)


if __name__ == "__main__":
    main()
