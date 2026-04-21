"""
SARC_MODE=scraping SARC_CONFIG=secrets/sarc-client-distant.yaml uv run fixes/metrics/get_metrics.py --start 2026-04-07T00:00 --end 2026-04-07T17:35 --output-text report.txt
"""

import argparse
import logging
import sys
from datetime import UTC, datetime

from fixes.metrics.core.report import CsvOutputFolder, TextOutputFile
from fixes.metrics.core.show_metrics import show_metrics


def main():
    logging.basicConfig(level=logging.INFO, force=True)

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start", type=str, required=True)
    parser.add_argument("-e", "--end", type=str, required=True)

    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument(
        "--output-default",
        action="store_true",
        help="Output in a default output TEXT file",
    )
    output_group.add_argument(
        "--output-text", type=str, help="Output in specified text file"
    )
    output_group.add_argument(
        "--output-csv", type=str, help="Output CSV files in specified folder"
    )

    args = parser.parse_args()
    time_from = datetime.fromisoformat(args.start).astimezone(UTC)
    time_to = datetime.fromisoformat(args.end).astimezone(UTC)

    if time_from > time_to:
        print("Expected time_from <= time_to", time_from, time_to, file=sys.stderr)
        sys.exit(1)

    if args.output_csv:
        output = CsvOutputFolder(args.output_csv)
    elif args.output_text:
        output = TextOutputFile(args.output_text)
    elif args.output_default:
        output = TextOutputFile(get_default_output_path(time_from, time_to))
    else:
        output = TextOutputFile(None)

    show_metrics(time_from, time_to, output=output)


def get_default_output_path(time_from: datetime, time_to: datetime) -> str:
    return f"report-{time_from.isoformat()}-{time_to.isoformat()}.txt"


if __name__ == "__main__":
    main()
