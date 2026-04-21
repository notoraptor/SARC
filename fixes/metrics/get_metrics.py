"""
SARC_MODE=scraping SARC_CONFIG=secrets/sarc-client-distant.yaml uv run fixes/get_metrics.py --start 2026-04-07T00:00 --end 2026-04-07T17:35 -o report.txt --all
"""

import argparse
import logging
import sys
from datetime import UTC, datetime

from fixes.metrics.core.show_metrics import show_metrics


def main():
    logging.basicConfig(level=logging.INFO, force=True)

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start", type=str, required=True)
    parser.add_argument("-e", "--end", type=str, required=True)
    parser.add_argument("-a", "--all", action="store_true", help="Show all metrics")
    parser.add_argument(
        "-t", "--total", action="store_true", help="Show metrics for total"
    )
    parser.add_argument(
        "-d",
        "--cluster-type",
        action="store_true",
        help="Show metrics per cluster type",
    )
    parser.add_argument(
        "-c", "--cluster", action="store_true", help="Show metrics per cluster"
    )
    parser.add_argument(
        "-o", "--output", type=str, help="Output file (defaults to stdout)"
    )

    args = parser.parse_args()
    time_from = datetime.fromisoformat(args.start).astimezone(UTC)
    time_to = datetime.fromisoformat(args.end).astimezone(UTC)

    if time_from > time_to:
        print("Expected time_from <= time_to", time_from, time_to, file=sys.stderr)
        sys.exit(1)

    per_total = args.all or args.total
    per_cluster_type = args.all or args.cluster_type
    per_cluster = args.all or args.cluster
    output = args.output

    if not per_total and not per_cluster_type and not per_cluster:
        print(
            "No metrics required, nothing to do. Pass either --all, --total, --cluster-type or --cluster."
        )
        sys.exit(1)

    show_metrics(
        time_from,
        time_to,
        per_total=per_total,
        per_cluster_type=per_cluster_type,
        per_cluster=per_cluster,
        output=(
            output
            or _default_output_path(
                time_from, time_to, per_total, per_cluster_type, per_cluster
            )
        ),
    )


def _default_output_path(
    time_from, time_to, per_total, per_cluster_type, per_cluster
) -> str:
    suffixes = []
    if per_total:
        suffixes.append("total")
    if per_cluster_type:
        suffixes.append("type")
    if per_cluster:
        suffixes.append("cluster")
    return (
        f"report-{time_from.isoformat()}-{time_to.isoformat()}-{'-'.join(suffixes)}.txt"
    )


if __name__ == "__main__":
    main()
