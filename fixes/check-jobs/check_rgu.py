import json
import os
import argparse

from sarc.client import (
    load_job_series,
    update_job_series_rgu,
    get_available_clusters,
    get_cluster_gpu_billings,
)
from sarc.client.job import SlurmState

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
    parser.add_argument("-o", "--output", type=str, required=True, help="Output folder")
    parser.add_argument("-c", "--cluster", type=str, required=True, help="Cluster name")
    args = parser.parse_args()

    cluster_name = args.cluster
    output_dir = args.output
    os.makedirs(output_dir, exist_ok=True)

    for cluster in get_available_clusters():
        if cluster.cluster_name == cluster_name:
            print("Cluster:", cluster)
            print()

    billings = get_cluster_gpu_billings(cluster_name)
    if billings:
        print("Billings:")
        print("---------")
        for billing in billings:
            print(f"since: {billing.since}")
            print(json.dumps(billing.gpu_to_billing, indent=1))
        print()

    df = load_job_series(start=START, end=END, cluster=cluster_name)

    # PENDING jobs should not have GPU type
    assert df[df["job_state"] == SlurmState.PENDING]["allocated.gpu_type"].unique() == [
        None
    ]

    # Ignore PENDING jobs
    df = df[df["job_state"] != SlurmState.PENDING]

    # Save RAW dataframe in output folder
    with open(f"{output_dir}/{cluster_name}.raw.md", mode="w") as file:
        print("Rows:", len(df), file=file)
        print(
            df[MARKDOWN_FIELDS].to_markdown(),
            file=file,
        )

    # Get RGU
    update_job_series_rgu(df)

    # Save RGU dataframe in output folder
    with open(f"{output_dir}/{cluster_name}.rgu.md", mode="w") as file:
        print("Rows:", len(df), file=file)
        print(
            df[MARKDOWN_FIELDS_WITH_RGU].to_markdown(),
            file=file,
        )

    # Print some debug info

    print()
    suspect_rgu = df[
        (df["allocated.gres_rgu"].notna()) & (df["allocated.gres_rgu"] < 1)
    ]
    print("Suspect RGU:", len(suspect_rgu))
    if len(suspect_rgu):
        print("Example:")
        print(suspect_rgu.iloc[0][MARKDOWN_FIELDS_WITH_RGU].to_markdown())

    df_no_rgu = df[df["allocated.gres_rgu"].isna()]
    print("Failed RGU:", len(df_no_rgu))
    print(
        "Failed RGU but has gpu_type:",
        len(df_no_rgu[df_no_rgu["allocated.gpu_type"].notnull()]),
    )
    print("[GPUs without RGU]:")
    for gpu_type in sorted(set(df_no_rgu["allocated.gpu_type"].unique()) - {None}):
        print(f"\t{gpu_type}")


if __name__ == "__main__":
    main()
