import pprint

from fixes.db_job_iterator import get_database_jobs
from fixes.utils.outputfile import get_output_file
from sarc.client import get_available_clusters, get_cluster_gpu_billings, get_rgus


def main():
    gpu_billings: dict[str, set[str]] = {}
    for cluster in get_available_clusters():
        for billing in get_cluster_gpu_billings(cluster.cluster_name):
            gpu_billings.setdefault(cluster.cluster_name, set()).update(
                billing.gpu_to_billing.keys()
            )
    pprint.pprint(gpu_billings)
    with get_output_file() as output:
        harmonized_names = sorted(get_rgus().keys())
        harmonized_set = set(harmonized_names)
        print("Harmonized names:", len(harmonized_names))
        assert len(harmonized_set) == len(harmonized_names), harmonized_names

        base_query = {"allocated.gpu_type": {"$ne": None, "$nin": harmonized_names}}
        for job in get_database_jobs(base_query):
            gpu_type = job.allocated.gpu_type
            if any(
                gpu_type.startswith(f"{harmonized} : ")
                for harmonized in harmonized_names
            ):
                # Harmonized MIG GPU, ok
                pass
            else:
                print(
                    job.cluster_name,
                    job.job_id,
                    job.submit_time,
                    job.allocated.gpu_type,
                    file=output,
                )


if __name__ == "__main__":
    main()
