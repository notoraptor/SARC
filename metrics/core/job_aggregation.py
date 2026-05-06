import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Callable

from metrics.core.scope_metrics import ScopeMetrics
from sarc.client.job import SlurmJob
from sarc.client.rgumetrics import GpuError, get_gpu_type_rgu

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class _JobRecord:
    cluster_name: str
    job_id: int
    user: str

    total_rgu_seconds: float = 0.0


class JobAggregation:
    __slots__ = ("_jobs_rgu", "_time_from", "_time_to")

    def __init__(self, time_from: datetime, time_to: datetime):
        self._time_from = time_from
        self._time_to = time_to
        self._jobs_rgu: dict[tuple, _JobRecord] = {}

    def _get_gpu_type_rgu(self, job: SlurmJob) -> float | None:
        gpu_type = job.allocated.gpu_type
        if gpu_type is None:
            return None

        rgu = None
        try:
            rgu = get_gpu_type_rgu(gpu_type)
        except GpuError:
            h_gpu_type = job.fetch_cluster_config().harmonize_gpu_from_nodes(
                job.nodes, gpu_type
            )
            if h_gpu_type is not None:
                logger.info(
                    f"had to manually harmonize GPU type for job "
                    f"{job.cluster_name}/{job.job_id}: {gpu_type} -> {h_gpu_type}"
                )
                rgu = get_gpu_type_rgu(h_gpu_type)

        return rgu

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
