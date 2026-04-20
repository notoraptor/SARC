import logging
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from tqdm import tqdm

from sarc.alerts.common import CheckResult, HealthCheck
from sarc.core.models.validators import datetime_utc

logger = logging.getLogger(__name__)


def check_old_running_jobs(since: datetime_utc | None = None) -> bool:
    """
    Check if database contains RUNNING jobs which should have already finished.

    Parameters
    ----------
    since: datetime_utc | None
        Date since when we must check jobs. Only jobs with a submit time >= `since` will be checked.
        If None, all jobs are checked.

    Return
    ------
    bool
        True if check succeeds (no old RUNNING job in database), False otherwise.
    """
    from sarc.client.job import SlurmJob, SlurmState, _jobs_collection

    now = datetime.now(tz=UTC)
    coll_jobs = _jobs_collection()
    db_jobs = coll_jobs.get_collection()
    # Search RUNNING jobs
    base_query: dict[str, Any] = {"job_state": SlurmState.RUNNING}
    if since is not None:
        base_query["submit_time"] = {"$gte": since}
    expected = db_jobs.count_documents(base_query)
    nb_jobs_without_time_limit: int = 0
    jobs_over_limit: list[SlurmJob] = []
    for job in tqdm(
        coll_jobs.find_by(base_query), total=expected, desc="running job(s)"
    ):
        assert job.job_state == SlurmState.RUNNING, job
        assert job.start_time is not None, job
        assert job.end_time is None, job
        if job.time_limit is None:
            nb_jobs_without_time_limit += 1
        else:
            # A running job should have already finished
            # if maximum allowed end time is before current time.
            max_end_time = job.start_time + timedelta(seconds=job.time_limit)
            if max_end_time < now:
                jobs_over_limit.append(job)

    if nb_jobs_without_time_limit:
        logger.error(
            f"Found {nb_jobs_without_time_limit} RUNNING job entries without time limit"
        )

    if jobs_over_limit:
        # We have old RUNNING jobs
        # Check if this job was re-submitted with a more recent status

        # First, get job keys :cluster name + job ID
        # NB: Database may contain many job entries with same cluster name,
        # same job ID, AND same job state `RUNNING`
        index_jobs = {(job.cluster_name, job.job_id) for job in jobs_over_limit}
        job_story: dict[tuple[str, int], list[SlurmJob]] = {}
        for cluster_name, job_id in tqdm(
            index_jobs, total=len(index_jobs), desc="running job states"
        ):
            local_query: dict[str, Any] = {
                "cluster_name": cluster_name,
                "job_id": job_id,
            }
            if since is not None:
                local_query["submit_time"] = {"$gte": since}
            local_jobs = list(coll_jobs.find_by(local_query))
            assert local_jobs
            local_jobs.sort(key=lambda job: job.submit_time)
            job_story[(cluster_name, job_id)] = local_jobs

        # Now we get some stats

        # nb. initial entries
        nb_entries = len(jobs_over_limit)

        # nb. initial jobs
        nb_jobs = len(job_story)

        # nb. jobs not re-submitted
        nb_uniques = 0

        # nb. latest found states for re-submitted jobs
        nb_latest_state: Counter[SlurmState] = Counter()

        for jobs in job_story.values():
            if len(jobs) == 1:
                nb_uniques += 1
            else:
                # Jobs are already sorted by submit time
                # Get and count latest job state
                latest_job = jobs[-1]
                nb_latest_state.update([latest_job.job_state])
        # Now log detailed error
        message = f"Found {nb_entries} RUNNING job entries"
        if since is not None:
            message += f", submitted since {since},"
        message += " which should have already finished"
        if nb_entries != nb_jobs:
            message += f", distributed in {nb_jobs} jobs (cluster name + job ID)"
        message += f", from which {nb_uniques} not re-submitted"
        for latest_state, latest_state_count in nb_latest_state.most_common():
            message += f", {latest_state_count} with a latest entry {latest_state.name}"
        logger.error(message)

        for job_key in sorted(job_story.keys()):
            cluster_name, job_id = job_key
            print(f"[{cluster_name}/{job_id}] job story:")
            for job in job_story[job_key]:
                job_message = f"\tsubmit: {job.submit_time}, start: {job.start_time}, end: {job.end_time}, state: {job.job_state.name}"
                if job.end_time is None and job.start_time is not None:
                    max_end_time = job.start_time + timedelta(seconds=job.time_limit)
                    job_message += f", limit: {max_end_time}"
                    if max_end_time < now:
                        job_message += " EXPIRED"
                print(job_message)

    return not jobs_over_limit


@dataclass
class OldRunningJobCheck(HealthCheck):
    since: datetime_utc | None = None

    def check(self) -> CheckResult:
        if check_old_running_jobs(since=self.since):
            return self.ok()
        else:
            return self.fail()
