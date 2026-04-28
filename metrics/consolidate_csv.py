"""
Input: a folder containing many sub-folders, each for a CSV output from get_metrics, ie. containing:
- metrics.csv
- user-to-rgu.csv
Each sub-folder should have been named using get_metrics._default_name ("report-<time_from>-<time_to>")

Output: output csv file joining data from all sub-folders, only for scope type "cluster":
- all-metrics-by-cluster.csv, with columns:
  - start (time_from)
  - cluster
  - cluster_type (Report.CLUSTER_TO_TYPE.get(cluster_name, "drac"))
  - nb_unique_users
  - nb_unique_jobs
  - total_rgu_secs
No other column, for now.
"""

import argparse
import csv
from pathlib import Path

from metrics.core.report import Report

OUTPUT_FILENAME = "all-metrics-by-cluster.csv"
METRICS_FILENAME = "metrics.csv"
SUBFOLDER_PREFIX = "report-"
OUTPUT_COLUMNS = [
    "start",
    "cluster",
    "cluster_type",
    "nb_unique_users",
    "nb_unique_jobs",
    "total_rgu_secs",
]


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("input_dir", help="Folder containing get_metrics sub-folders.")
    parser.add_argument(
        "-o",
        "--output",
        help=f"Output CSV path. Defaults to <input-dir>/{OUTPUT_FILENAME}.",
    )
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    if not input_dir.is_dir():
        raise NotADirectoryError(f"Input is not a directory: {input_dir}")

    output_path = Path(args.output) if args.output else input_dir / OUTPUT_FILENAME

    subfolders = sorted(p for p in input_dir.iterdir() if p.is_dir())
    if not subfolders:
        raise ValueError(f"No sub-folders found in {input_dir}")

    rows: list[tuple[str, str, str, str, str, str]] = []
    for sub in subfolders:
        if not sub.name.startswith(SUBFOLDER_PREFIX):
            raise ValueError(
                f"Unexpected sub-folder name (expected "
                f"'{SUBFOLDER_PREFIX}<time_from>-<time_to>'): {sub}"
            )
        metrics_path = sub / METRICS_FILENAME
        if not metrics_path.is_file():
            raise FileNotFoundError(f"Missing {METRICS_FILENAME}: {metrics_path}")

        with open(metrics_path, encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                if row["scope_type"] != "cluster":
                    continue
                cluster = row["scope_name"]
                rows.append(
                    (
                        row["time_from"],
                        cluster,
                        Report.CLUSTER_TO_TYPE.get(cluster, "drac"),
                        row["nb_unique_users"],
                        row["nb_unique_jobs"],
                        row["total_rgu_secs"],
                    )
                )

    rows.sort(key=lambda r: (r[0], r[1]))

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(OUTPUT_COLUMNS)
        writer.writerows(rows)

    print(f"[output] {output_path}")


if __name__ == "__main__":
    main()
