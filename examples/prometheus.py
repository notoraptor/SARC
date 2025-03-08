from sarc.config import config


def main():
    for cluster in config().clusters.values():
        if cluster.prometheus_url and cluster.name != "mila":
            query = "slurm_job_utilization_gpu_memory"
            query = 'slurm_job_utilization_gpu_memory{slurmjobid=~"47622739"}[2371s:2371s]  offset 24929460s'
            query = "slurm_job_utilization_gpu_memory[1m]"
            print("PROMETHEUS", cluster.name)
            ret = cluster.prometheus.custom_query(query)
            print(type(ret), len(ret))
            break


if __name__ == '__main__':
    main()
