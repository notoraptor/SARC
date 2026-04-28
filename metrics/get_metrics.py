"""
Generate RGU*seconds metrics over a time range.

Examples:
    # single window, text to stdout
    uv run metrics/get_metrics.py --start 2026-04-07T00:00Z --end 2026-04-07T17:35Z --output-stdout

    # single window, text to a file
    uv run metrics/get_metrics.py --start 2026-04-07T00:00Z --end 2026-04-08T00:00Z --format text --output report.txt

    # single window, CSVs in a folder
    uv run metrics/get_metrics.py --start 2026-04-07T00:00Z --end 2026-04-08T00:00Z --format csv --output report-dir/

    # split by month, one text file per month in reports/
    uv run metrics/get_metrics.py --start 2026-01-01T00:00Z --end 2026-04-15T00:00Z --split-by month --output reports/

    # split by week (Monday-aligned), one CSV subfolder per week in reports/
    uv run metrics/get_metrics.py --start 2026-01-05T00:00Z --end 2026-02-02T00:00Z --split-by week --format csv --output reports/

Inputs must be UTC-aware ISO strings (naive datetimes are rejected). Split
boundaries are anchored on UTC midnight of the 1st of each month or on UTC
Monday 00:00 for weeks.

Previously-generated outputs that already exist and are non-empty are skipped.
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from metrics.core.report import CsvOutputFolder, ReportOutput, TextOutputFile
from metrics.core.show_metrics import show_metrics
from metrics.core.utils import expect_datetime_utc
from metrics.core.windows import SPLIT_CHOICES, SPLIT_NONE, iter_windows

logger = logging.getLogger(__name__)

FORMAT_TEXT = "text"
FORMAT_CSV = "csv"
FORMAT_CHOICES = (FORMAT_TEXT, FORMAT_CSV)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("-s", "--start", required=True, help="UTC, ISO format.")
    parser.add_argument("-e", "--end", required=True, help="UTC, ISO format.")
    parser.add_argument(
        "--split-by", choices=SPLIT_CHOICES, default=SPLIT_NONE, help="Window split"
    )
    parser.add_argument(
        "--format", choices=FORMAT_CHOICES, default=FORMAT_TEXT, help="Output format"
    )
    parser.add_argument(
        "--log-file",
        help="Write all INFO logs to this file (console stays at WARNING).",
    )

    output_group = parser.add_mutually_exclusive_group(required=True)
    output_group.add_argument(
        "-o",
        "--output",
        help="Output path. A file for (text, split=none); a directory otherwise.",
    )
    output_group.add_argument(
        "--output-stdout",
        action="store_true",
        help="Write to stdout. Only valid with --format text and --split-by none.",
    )
    output_group.add_argument(
        "--output-default",
        action="store_true",
        help="Auto-name output in the current directory.",
    )

    args = parser.parse_args()
    _setup_logging(args.log_file)

    time_from = _parse_dt(args.start)
    time_to = _parse_dt(args.end)

    if time_from > time_to:
        print("Expected time_from <= time_to", time_from, time_to, file=sys.stderr)
        sys.exit(1)

    is_split = args.split_by != SPLIT_NONE
    use_stdout = args.output_stdout

    if use_stdout and (is_split or args.format != FORMAT_TEXT):
        parser.error("--output-stdout requires --format text and --split-by none")

    base_path = _resolve_base_path(
        args, time_from, time_to, fmt=args.format, is_split=is_split
    )

    for window_start, window_end in iter_windows(time_from, time_to, args.split_by):
        output = _build_output(
            base_path=base_path,
            fmt=args.format,
            is_split=is_split,
            window_start=window_start,
            window_end=window_end,
        )

        if is_split and output.is_present():
            logger.warning("[skip] %s already exists and is non-empty.", output.path)
            continue

        logger.info(
            "[generate] %s -> %s",
            f"{window_start.isoformat()}..{window_end.isoformat()}",
            output.path if output.path is not None else "stdout",
        )
        show_metrics(window_start, window_end, output=output)


def _parse_dt(s: str) -> datetime:
    """Parse an ISO datetime string. Raises if not explicitly UTC."""
    dt = datetime.fromisoformat(s)
    expect_datetime_utc(dt)
    return dt


def _setup_logging(log_file: str | None) -> None:
    base_level = logging.INFO
    public_level = logging.WARNING

    root = logging.getLogger()
    # Root captures everything; handlers filter at their own level.
    root.setLevel(base_level)
    root.handlers.clear()

    console = logging.StreamHandler()
    console.setLevel(public_level)
    console.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
    root.addHandler(console)

    if log_file:
        log_file_path = Path(log_file)
        log_file_dir = log_file_path.parent
        log_file_dir.mkdir(parents=True, exist_ok=True)

        fh = logging.FileHandler(log_file, mode="w", encoding="utf-8")
        fh.setLevel(base_level)
        fh.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        )
        root.addHandler(fh)


def _resolve_base_path(
    args: argparse.Namespace,
    time_from: datetime,
    time_to: datetime,
    *,
    fmt: str,
    is_split: bool,
) -> Path | None:
    if args.output_stdout:
        return None
    if args.output_default:
        return Path(_default_name(time_from, time_to, fmt=fmt, is_split=is_split))
    return Path(args.output)


def _build_output(
    *,
    base_path: Path | None,
    fmt: str,
    is_split: bool,
    window_start: datetime,
    window_end: datetime,
) -> ReportOutput:
    if base_path is None:
        return TextOutputFile(None)

    if is_split:
        name = _window_name(window_start, window_end)
        if fmt == FORMAT_TEXT:
            return TextOutputFile(base_path / f"{name}.txt")
        return CsvOutputFolder(base_path / name)

    if fmt == FORMAT_TEXT:
        return TextOutputFile(base_path)
    return CsvOutputFolder(base_path)


def _window_name(time_from: datetime, time_to: datetime) -> str:
    return f"report-{time_from.isoformat()}-{time_to.isoformat()}"


def _default_name(
    time_from: datetime, time_to: datetime, *, fmt: str, is_split: bool
) -> str:
    base = _window_name(time_from, time_to)
    # Split mode and CSV single mode both produce a directory → no extension.
    if is_split or fmt == FORMAT_CSV:
        return base
    return f"{base}.txt"


if __name__ == "__main__":
    main()
