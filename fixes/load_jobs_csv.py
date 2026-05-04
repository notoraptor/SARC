import argparse
import csv
import logging
import sys
from datetime import UTC, datetime, timedelta

from tqdm import tqdm

from sarc.client.job import (
    JobStatistics,
    SlurmJob,
    SlurmResources,
    Statistics,
    _jobs_collection,
)
from sarc.client.rgumetrics import GpuError, get_gpu_type_rgu
from sarc.config import scraping_mode_required
from sarc.utils import flatten

UTCOFFSET = timedelta(0)


def _expect_datetime_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        raise ValueError(f"Datetime must be timezone-aware: {dt}")
    if dt.utcoffset() != UTCOFFSET:
        raise ValueError(f"Datetime must be UTC: {dt}")
    return dt


def _fmt_date(dt: datetime) -> str:
    return _expect_datetime_utc(dt).strftime("%Y-%m-%dT%Hh%Mm%SZ")


def _empty_stats_dict():
    return dict.fromkeys(Statistics.model_fields, None)


def _empty_job_stats_dict():
    return {key: _empty_stats_dict() for key in JobStatistics.model_fields}


def _compute_gpu_type_rgu(job: SlurmJob) -> tuple[float | None, bool]:
    gpu_type = job.allocated.gpu_type
    rgu = None
    manually_harmonized = False
    if gpu_type is not None:
        try:
            rgu = get_gpu_type_rgu(gpu_type)
        except GpuError:
            h_gpu_type = job.fetch_cluster_config().harmonize_gpu_from_nodes(
                job.nodes, gpu_type
            )
            if h_gpu_type is not None:
                manually_harmonized = True
                rgu = get_gpu_type_rgu(h_gpu_type)
    return rgu, manually_harmonized


def _expected_columns() -> list[str]:
    skeleton = dict.fromkeys(SlurmJob.model_fields, None)
    skeleton.pop("id")
    skeleton["requested"] = dict.fromkeys(SlurmResources.model_fields, None)
    skeleton["allocated"] = dict.fromkeys(SlurmResources.model_fields, None)
    skeleton["allocated"]["gpu_type_rgu"] = None
    skeleton.pop("stored_statistics")
    skeleton.update(_empty_job_stats_dict())
    return list(flatten(skeleton).keys())


@scraping_mode_required
def main():
    logging.basicConfig(level=logging.INFO, force=True)

    default_start = datetime(2025, 1, 1, tzinfo=UTC)
    default_end = datetime(2026, 1, 1, tzinfo=UTC)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s", "--start", type=datetime.fromisoformat, default=default_start
    )
    parser.add_argument("-e", "--end", type=datetime.fromisoformat, default=default_end)
    args = parser.parse_args()
    start = _expect_datetime_utc(args.start)
    end = _expect_datetime_utc(args.end)
    if start >= end:
        logging.error(f"Start date must be strictly before end date: {start}, {end}")
        sys.exit(1)

    logging.info(f"Job(s) submitted in [{start}, {end})")

    coll_jobs = _jobs_collection()
    expected = coll_jobs.get_collection().count_documents(
        {"submit_time": {"$gte": start, "$lt": end}}
    )
    count = 0
    nb_rgu = 0
    nb_harmonized = 0

    field_names = _expected_columns()
    expected_set = set(field_names)
    now = datetime.now(tz=UTC)
    csv_name = f"jobs-{_fmt_date(start)}-{_fmt_date(end)}-at-{_fmt_date(now)}.csv"
    with open(csv_name, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=field_names, extrasaction="raise")
        writer.writeheader()

        interval = timedelta(days=60)
        current = start
        with tqdm(total=expected, desc="job(s)") as pbar:
            while current < end:
                next_time = min(current + interval, end)
                for job in coll_jobs.find_by(
                    {"submit_time": {"$gte": current, "$lt": next_time}}
                ):
                    gpu_type_rgu, manually_harmonized = _compute_gpu_type_rgu(job)
                    nb_rgu += gpu_type_rgu is not None
                    nb_harmonized += manually_harmonized
                    job_dict = job.model_dump(exclude={"id"})
                    job_dict["allocated"]["gpu_type_rgu"] = gpu_type_rgu
                    statistics = job_dict.pop("stored_statistics")
                    if statistics is None:
                        statistics = _empty_job_stats_dict()
                    else:
                        for key in JobStatistics.model_fields:
                            if statistics[key] is None:
                                statistics[key] = _empty_stats_dict()
                    job_dict.update(statistics)
                    flat_job = flatten(job_dict)
                    missing = expected_set - flat_job.keys()
                    if missing:
                        raise ValueError(
                            f"[{job.cluster_name}/{job.job_id}/{job.submit_time}]: missing columns {sorted(missing)}"
                        )
                    writer.writerow(flat_job)
                    pbar.update(1)
                    count += 1
                current = next_time

    if count != expected:
        logging.warning(f"Got {count} jobs out of {expected} expected.")
    logging.info(
        f"Found RGU for {nb_rgu} job(s), had to manually harmonize GPU type for {nb_harmonized} job(s)."
    )
    logging.info(f"Saved {count} job(s) into {csv_name}")


if __name__ == "__main__":
    main()
