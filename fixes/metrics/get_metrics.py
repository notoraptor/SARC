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
    parser.add_argument(
        "-o", "--output", type=str, help="Output file (defaults to stdout)"
    )

    args = parser.parse_args()
    time_from = datetime.fromisoformat(args.start).astimezone(UTC)
    time_to = datetime.fromisoformat(args.end).astimezone(UTC)

    if time_from > time_to:
        print("Expected time_from <= time_to", time_from, time_to, file=sys.stderr)
        sys.exit(1)

    output = args.output

    show_metrics(
        time_from, time_to, output=(output or _default_output_path(time_from, time_to))
    )


def _default_output_path(time_from: datetime, time_to: datetime) -> str:
    return f"report-{time_from.isoformat()}-{time_to.isoformat()}.txt"


if __name__ == "__main__":
    main()
