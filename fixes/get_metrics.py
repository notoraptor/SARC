"""
SARC_MODE=scraping SARC_CONFIG=secrets/sarc-client-distant.yaml uv run fixes/get_metrics.py --start 2026-04-07T00:00 --end 2026-04-07T17:35 -o report.txt --all
"""

import argparse
import contextlib
import logging
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

from tqdm import tqdm

from sarc.client.gpumetrics import get_rgus
from sarc.client.job import SlurmJob, _jobs_collection
from sarc.config import ClusterConfig, config

logger = logging.getLogger("sarc-metrics")


@dataclass(slots=True)
class ScopeMetrics:
    time_from: datetime
    time_to: datetime
    scope_name: str

    nb_unique_jobs: int = 0
    total_rgu_seconds: float = 0.0
    unique_users: set[str] = field(default_factory=set)
    unique_user_to_rgu_sec: dict[str, float] = field(default_factory=dict)

    @property
    def nb_unique_users(self) -> int:
        return len(self.unique_users)

    @property
    def nb_jobs_per_unique_user(self) -> float:
        return self.nb_unique_jobs / (self.nb_unique_users or 1)

    @property
    def rgu_sec_per_unique_user(self) -> float:
        return self.total_rgu_seconds / (self.nb_unique_users or 1)

    def __str__(self):
        pieces = [
            f"[{self.scope_name} - {self.time_from.isoformat()} - {self.time_to.isoformat()}]",
            f"Unique users:                {self.nb_unique_users:_}",
            f"Unique jobs:                 {self.nb_unique_jobs:_}",
            f"Total RGU*seconds:           {self.total_rgu_seconds:_}",
            f"Jobs per unique user:        {self.nb_jobs_per_unique_user:_}",
            f"RGU*seconds per unique user: {self.rgu_sec_per_unique_user:_}",
            "Greatest consumers:",
        ]
        sorted_consumers = sorted(
            self.unique_user_to_rgu_sec.items(), key=lambda item: (-item[1], item[0])
        )
        for user, rgu_sec in sorted_consumers[:20]:
            pieces.append(f"\t{user}: {rgu_sec:_}")
        if len(sorted_consumers) > 20:
            pieces.append(f"\t(... first 20 of {len(sorted_consumers)})")
        return "\n".join(pieces) + "\n\n"


@dataclass(slots=True)
class _JobRecord:
    cluster_name: str
    job_id: int
    user: str

    total_rgu_seconds: float = 0.0


class JobAggregation:
    __slots__ = (
        "_gpu_type_to_rgu",
        "_jobs_rgu",
        "_time_from",
        "_time_to",
        "_cluster_configs",
    )

    def __init__(self, time_from: datetime, time_to: datetime):
        self._time_from = time_from
        self._time_to = time_to
        self._gpu_type_to_rgu = get_rgus()
        self._jobs_rgu: dict[tuple, _JobRecord] = {}
        self._cluster_configs: dict[str, ClusterConfig] = {}

    def _harmonize_gpu(self, job: SlurmJob) -> str | None:
        """
        Fallback: harmonize GPU type, if not yet harmonized in job.allocated.gpu_type.

        If this happens, then we may need to update `gpus_per_nodes` for job cluster
        in SARC configuration file, then either re-run `parse jobs`, or use a dedicated
        script to fix harmonized GPUs in SARC database.
        """
        if not self._cluster_configs:
            self._cluster_configs = config("scraping").clusters
        cluster_cfg = self._cluster_configs[job.cluster_name]
        return cluster_cfg.harmonize_gpu_from_nodes(job.nodes, job.allocated.gpu_type)

    def _get_gpu_type_rgu(self, job: SlurmJob) -> float | None:
        gpu_type = job.allocated.gpu_type
        if gpu_type is None:
            return None

        # NB: If GPU type is a MIG
        # (e.g: "A100-SXM4-40GB : a100_1g.5gb"),
        # we currently return RGU for the main GPU type
        # (in this example: "A100-SXM4-40GB")
        gpu_type = gpu_type.split(":")[0].rstrip()

        # If GPU type not found, maybe it's not yet harmonized.
        if gpu_type not in self._gpu_type_to_rgu:
            h_gpu_type = self._harmonize_gpu(job)
            if h_gpu_type is not None:
                logger.warning(
                    f"had to manually harmonize GPU type for job "
                    f"{job.cluster_name}/{job.job_id}: {gpu_type} -> {h_gpu_type}"
                )
                gpu_type = h_gpu_type.split(":")[0].rstrip()

        return self._gpu_type_to_rgu.get(gpu_type, None)

    def aggregate(self, job: SlurmJob, consumed_seconds: float) -> None:
        """Save job record and related RGUs*second"""

        # Compute job RGUs*second inside this time frame
        gpu_type_rgu = self._get_gpu_type_rgu(job)
        if gpu_type_rgu is None:
            logger.error(
                f"Job {job.cluster_name}/{job.job_id}: cannot infer RGU for GPU type: {job.allocated.gpu_type}"
            )
            return

        gres_gpu = job.allocated.gres_gpu
        if not gres_gpu:
            logger.error(
                f"Job {job.cluster_name}/{job.job_id}: no gres_gpu (gpu_type={job.allocated.gpu_type})"
            )
            return

        job_rgu_seconds = gpu_type_rgu * gres_gpu * consumed_seconds
        # Aggregate RGUs*second per job.
        # If a job was rescheduled many times inside this time frame,
        # and consumed some resources more than 1 time across all these occurrences,
        # then we sum all consumptions for this same job.
        job_key = (job.cluster_name, job.job_id, job.user)
        if job_key in self._jobs_rgu:
            self._jobs_rgu[job_key].total_rgu_seconds += job_rgu_seconds
        else:
            self._jobs_rgu[job_key] = _JobRecord(
                job.cluster_name, job.job_id, job.user, job_rgu_seconds
            )

    def report(
        self, classifier: Callable[[_JobRecord], str]
    ) -> dict[str, ScopeMetrics]:
        """
        Compute metrics, using given classifier.

        Classifier must associate a job record to a scope name.

        Returns a dictionary mapping a scope name to computed ScopeMetrics.
        """
        name_to_metrics: dict[str, ScopeMetrics] = {}
        for record in self._jobs_rgu.values():
            name = classifier(record)
            if name not in name_to_metrics:
                name_to_metrics[name] = ScopeMetrics(
                    time_from=self._time_from, time_to=self._time_to, scope_name=name
                )
            scope = name_to_metrics[name]
            scope.nb_unique_jobs += 1
            scope.unique_users.add(record.user)
            scope.total_rgu_seconds += record.total_rgu_seconds

            scope.unique_user_to_rgu_sec.setdefault(record.user, 0.0)
            scope.unique_user_to_rgu_sec[record.user] += record.total_rgu_seconds
        return name_to_metrics


@contextlib.contextmanager
def _get_output_file(filename: str | Path | None = None):
    if filename is None:
        output = sys.stdout
    else:
        output = open(filename, mode="w", encoding="utf-8")
        print("[output]", filename)
    try:
        yield output
    finally:
        if filename is not None:
            output.close()


# Cluster to category (default: drac)
CLUSTER_TO_TYPE = {
    "mila": "mila",
    "tamia": "drac-paice",
    "killarney": "drac-paice",
    "vulcan": "drac-paice",
}


def show_metrics(
    time_from: datetime,
    time_to: datetime,
    *,
    per_total=True,
    per_cluster_type=True,
    per_cluster=True,
    output: str | None = None,
):
    jagg = JobAggregation(time_from, time_to)
    coll_jobs = _jobs_collection()

    query = {
        "start_time": {"$lte": time_to},
        "allocated.gpu_type": {"$ne": None},
        "allocated.gres_gpu": {"$ne": None},
        "$or": [{"end_time": {"$gte": time_from}}, {"end_time": None}],
    }

    print(f"Counting jobs for period {time_from} to {time_to}...")
    expected = coll_jobs.get_collection().count_documents(query)
    print(f"Processing {expected} jobs...")

    count = 0
    with tqdm(total=expected, desc="Analyzing jobs") as pbar:
        # Iterate directly on the cursor without complex pagination
        for job in coll_jobs.find_by(query):
            pbar.update(1)
            count += 1

            # User safety check
            if not job.user:
                logger.error(
                    f"Job {job.cluster_name}/{job.job_id} has no user. Skipping."
                )
                continue

            # A job must have started to consume time
            if job.start_time is None:
                continue

            # Determine real or "virtual" end (for RUNNING jobs).
            # If end_time is None, use start_time + elapsed_time.
            # We don't use current time (now), to avoid any assumption about
            # a running job (we don't know if it really ran until current time).
            # We rely only on available info from latest SLURM scraping.
            job_end = job.end_time or (
                job.start_time + timedelta(seconds=job.elapsed_time)
            )
            if job_end < time_from:
                # Maybe an old running job.
                logger.debug(
                    f"Job {job.cluster_name}/{job.job_id}/{job.job_state.name} ends before time frame: "
                    f"submit {job.submit_time}, start {job.start_time}, end {job.end_time}, "
                    f"elapsed {job.elapsed_time}, used end: {job_end}. Skipping"
                )
                continue

            # Calculate intersection with given time frame
            intersect_start = max(job.start_time, time_from)
            intersect_end = min(job_end, time_to)
            if intersect_start > intersect_end:
                logger.error(
                    f"Job {job.cluster_name}/{job.job_id}: "
                    f"incorrect clipped time: {intersect_start} > {intersect_end}. Skipping."
                )
                continue
            if intersect_start == intersect_end:
                # This job entry has no run time inside this frame. We can skip it.
                continue

            # Compute consumed time for this job inside this time frame
            consumed_seconds = (intersect_end - intersect_start).total_seconds()
            jagg.aggregate(job, consumed_seconds)

    with _get_output_file(output) as file:
        if per_total:
            report_total = jagg.report(lambda record: "total")
            if report_total:
                print(report_total["total"], file=file)
        if per_cluster_type:
            report_per_cluster_type = jagg.report(
                lambda record: CLUSTER_TO_TYPE.get(record.cluster_name, "drac")
            )
            if report_per_cluster_type:
                print("Per cluster type:", file=file)
                print("=================", file=file)
                print(file=file)
                for cluster_type in sorted(report_per_cluster_type.keys()):
                    print(report_per_cluster_type[cluster_type], file=file)
        if per_cluster:
            report_per_cluster = jagg.report(lambda record: record.cluster_name)
            if report_per_cluster:
                print("Per cluster:", file=file)
                print("============", file=file)
                print(file=file)
                for cluster in sorted(report_per_cluster.keys()):
                    print(report_per_cluster[cluster], file=file)


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
