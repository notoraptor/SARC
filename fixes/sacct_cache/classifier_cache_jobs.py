import os
from typing import Literal, TypedDict

from fixes.sacct_cache.common import get_sacct_scrapers
from fixes.sacct_cache.job_state_classifier import EPOCHS
from sarc.cache import with_cache
from sarc.config import config


def _get_last_content_modification_time(directory_path) -> float | None:
    """
    Finds the most recent modification time among all files and subdirectories
    within a given directory.
    """
    if not os.path.isdir(directory_path):
        raise ValueError(f"'{directory_path}' is not a valid directory.")

    latest_modification_time = 0.0  # Initialize with a very old timestamp

    for root, dirs, files in os.walk(directory_path):
        # Check modification time of files
        for file_name in files:
            file_path = os.path.join(root, file_name)
            try:
                mod_time = os.path.getmtime(file_path)
                latest_modification_time = max(latest_modification_time, mod_time)
            except OSError:
                # Handle cases where file might be inaccessible or removed
                pass

        # Check modification time of subdirectories
        for dir_name in dirs:
            dir_path = os.path.join(root, dir_name)
            try:
                mod_time = _get_last_content_modification_time(dir_path)
                latest_modification_time = max(
                    latest_modification_time, mod_time or 0.0
                )
            except OSError:
                # Handle cases where directory might be inaccessible or removed
                pass

    if latest_modification_time == 0.0:
        return None  # No files or subdirectories found with a modification time

    return latest_modification_time


def _key():
    cfg = config()
    sacct_cache = cfg.cache / "sacct"
    latest_mtime = _get_last_content_modification_time(sacct_cache)
    return f"classifier.{latest_mtime}.json"


ClusterName = str
Epoch = Literal[0, 1]
JobId = int
SubmitTime = str
ScraperTime = str


class JobInfo(TypedDict):
    start_time: str
    end_time: str | None
    elapsed_time: float
    time_limit: int | None
    job_state: str


Classifier = dict[
    ClusterName, dict[Epoch, dict[JobId, dict[SubmitTime, dict[ScraperTime, JobInfo]]]]
]


@with_cache(key=_key)
def get_classified_cache_jobs() -> Classifier:
    classifier = {}
    for scraper in get_sacct_scrapers():
        scraper_time = str(scraper.start)
        for job in scraper:
            if job:
                cluster_name = job.cluster_name
                submit_time = job.submit_time
                epoch_limit = EPOCHS.get(cluster_name)
                epoch: Literal[0, 1] = (
                    1 if (epoch_limit is not None and submit_time >= epoch_limit) else 0
                )
                classifier.setdefault(cluster_name, {}).setdefault(
                    epoch, {}
                ).setdefault(job.job_id, {}).setdefault(str(submit_time), {})[
                    scraper_time
                ] = {
                    "start_time": None
                    if job.start_time is None
                    else str(job.start_time),
                    "end_time": None if job.end_time is None else str(job.end_time),
                    "elapsed_time": job.elapsed_time,
                    "time_limit": job.time_limit,
                    "job_state": job.job_state.name,
                }
    return classifier
