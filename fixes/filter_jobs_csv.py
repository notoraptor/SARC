"""
Filter a SARC jobs CSV dump by submit_time, keeping rows whose submit_time is
>= a given threshold. Streams row by row; no full in-memory load.

Usage:
    uv run fixes/filter_jobs_csv.py INPUT.csv --since 2025-07-01 -o OUTPUT.csv

The threshold accepts any ISO 8601 form parseable by datetime.fromisoformat;
naive thresholds are assumed UTC (matching the CSV's submit_time convention).
"""

import argparse
import csv
import os
import sys
from datetime import UTC, datetime, timedelta

from tqdm import tqdm


def _parse_threshold(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", help="Path to the jobs CSV dump.")
    parser.add_argument(
        "--since",
        required=True,
        type=_parse_threshold,
        help="Keep rows with submit_time >= this ISO date/datetime (UTC if naive).",
    )
    parser.add_argument("-o", "--output", required=True, help="Output CSV path.")
    args = parser.parse_args()

    if not os.path.exists(args.csv):
        print(f"CSV not found: {args.csv}", file=sys.stderr)
        return 1

    n_total = 0
    n_kept = 0
    with (
        open(args.csv, encoding="utf-8", newline="") as fin,
        open(args.output, "w", encoding="utf-8", newline="") as fout,
    ):
        reader = csv.DictReader(fin)
        assert reader.fieldnames is not None
        writer = csv.DictWriter(
            fout, fieldnames=reader.fieldnames, extrasaction="raise"
        )
        writer.writeheader()
        for row in tqdm(reader, desc="rows", unit=" rows"):
            n_total += 1
            submit = datetime.fromisoformat(row["submit_time"])
            assert submit.tzinfo is not None
            assert submit.utcoffset() == timedelta(0)
            if submit >= args.since:
                writer.writerow(row)
                n_kept += 1

    print(f"  parsed : {n_total:,} rows")
    print(f"  kept   : {n_kept:,} rows  (since {args.since.isoformat()})")
    print(f"  Saved {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
