import bisect
import pprint
import sys

from sarc.client import get_cluster_gpu_billings, get_jobs
from sarc.client.gpumetrics import GPUBilling
from sarc.client.job import SlurmJob
from sarc.jobs.node_gpu_mapping import get_node_to_gpu


def _get_gpu_billing(job: SlurmJob) -> GPUBilling | None:
    billings = get_cluster_gpu_billings(job.cluster_name)
    if not billings:
        return None
    index_mapping = max(
        0,
        bisect.bisect_right([mapping.since for mapping in billings], job.start_time)
        - 1,
    )
    return billings[index_mapping]


def main():
    cluster_name = sys.argv[1]
    job_id = int(sys.argv[2])
    jobs = list(get_jobs(cluster=cluster_name, job_id=job_id))
    print(f"Found {len(jobs)} job(s)")
    print()
    for job in jobs:
        print("=" * 80)
        pprint.pprint(job.model_dump())
        ngm = get_node_to_gpu(job.cluster_name, job.start_time)
        if ngm is not None:
            node_to_gpu = ngm.node_to_gpu
            job_node_to_gpu = {node: node_to_gpu.get(node, None) for node in job.nodes}
            print()
            print("node->gpu mapping: since", ngm.since)
            pprint.pprint(job_node_to_gpu)
            billing = _get_gpu_billing(job)
            # billings = [b.model_dump() for b in get_cluster_gpu_billings(cluster_name)]
            print()
            print("gpu->billing:")
            if billing:
                pprint.pprint(billing.model_dump())
            else:
                print(None)
        print()


if __name__ == "__main__":
    main()
