import math

import sarc.client.job as _job_mod
from fixes.db_job_iterator import get_database_jobs
from sarc.client.job import SlurmJob


def _install_rgu_cache() -> None:
    # SlurmJob.rgu hits MongoDB through get_available_clusters() and
    # get_cluster_gpu_billings() on every call, and rebuilds the RGU dict via
    # get_rgus(). On millions of jobs this dominates the runtime. Prebuild all
    # three and patch them in sarc.client.job's namespace so the calls inside
    # the property short-circuit to dict lookups.
    clusters = list(_job_mod.get_available_clusters())
    billings = {
        c.cluster_name: _job_mod.get_cluster_gpu_billings(cluster_name=c.cluster_name)
        for c in clusters
    }
    rgus = _job_mod.get_rgus()

    _job_mod.get_available_clusters = lambda: clusters
    _job_mod.get_cluster_gpu_billings = lambda cluster_name: billings[cluster_name]
    _job_mod.get_rgus = lambda rgu_version="1.0": rgus


def _warning(job: SlurmJob, *args, **kwargs):
    print(
        f"[{job.cluster_name}/{job.job_id}/{job.submit_time}][{job.allocated.gpu_type}]",
        *args,
        **kwargs,
    )


def main():
    _install_rgu_cache()

    threshold = 1e-3

    nb_requested_without_allocated = 0
    nb_allocated_without_requested = 0
    nb_requested_neq_allocated = 0
    nb_both_nan = 0
    nb_simple_only = 0
    nb_complex_only = 0
    nb_rgu_computation_diff = 0

    # Skip CPU-only jobs at MongoDB level: they would hit the "nothing to check"
    # branch after being deserialized. The $or preserves the asymmetric cases
    # (requested without allocated, and vice-versa) that the script counts.
    base_query = {
        "$or": [{"requested.gres_gpu": {"$gt": 0}}, {"allocated.gres_gpu": {"$gt": 0}}]
    }

    for job in get_database_jobs(base_query):
        if job.requested.gres_gpu and not job.allocated.gres_gpu:
            _warning(job, f"requested ({job.requested.gres_gpu}) without allocated")
            nb_requested_without_allocated += 1
            continue
        if not job.requested.gres_gpu and job.allocated.gres_gpu:
            _warning(job, f"allocated ({job.allocated.gres_gpu}) without requested")
            nb_allocated_without_requested += 1
            continue
        if not job.requested.gres_gpu and not job.allocated.gres_gpu:
            # Nothing to check
            continue

        if job.requested.gres_gpu != job.allocated.gres_gpu:
            _warning(
                job,
                f"requested ({job.requested.gres_gpu}) != allocated ({job.allocated.gres_gpu})",
            )
            nb_requested_neq_allocated += 1
            # warn, but still check rgu computation

        complex_rgu = job.rgu
        # We would want to be able to compute rgu using simple_rgu computation, not complex one.
        # We must check if actual simple computation is equal to complex one, ie. if we
        # can get rid off billing in computation.
        # NB: Apart from billing, there's also the start/end times which can influence complex rgu.
        simple_rgu = job.gpu_type_rgu * job.allocated.gres_gpu
        complex_is_nan = math.isnan(complex_rgu)
        simple_is_nan = math.isnan(simple_rgu)
        if complex_is_nan and simple_is_nan:
            nb_both_nan += 1
            continue
        elif complex_is_nan:
            _warning(job, f"can compute only simple rgu: {simple_rgu}")
            nb_simple_only += 1
        elif simple_is_nan:
            _warning(job, f"can compute only complex rgu: {complex_rgu}")
            nb_complex_only += 1
        else:
            rgu_diff = abs(complex_rgu - simple_rgu)
            if rgu_diff >= threshold:
                _warning(
                    job,
                    f"rgu diff ({rgu_diff}): simple rgu ({simple_rgu}) != complex rgu ({complex_rgu})",
                )
                nb_rgu_computation_diff += 1

    print()
    print("Nb. requested without allocated:", nb_requested_without_allocated)
    print("Nb. allocated without requested:", nb_allocated_without_requested)
    print("Nb. requested != allocated:", nb_requested_neq_allocated)
    print("Nb. where we can't compute RGU:", nb_both_nan)
    print("Nb. where complex RGU is nan but simple is available:", nb_simple_only)
    print("Nb. where simple RGU is nan but complex is available:", nb_complex_only)
    print(
        f"Nb. where simple rgu != complex rgu (threshold {threshold}):",
        nb_rgu_computation_diff,
    )


if __name__ == "__main__":
    main()
