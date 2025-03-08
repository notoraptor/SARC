from sarc.config import config


def main():
    for cluster in config().clusters:
        if cluster.prometheus_url:
            print("PROMETHEUS")
            ret = cluster.prometheus.custom_query("slurm_job_utilization_gpu_memory")
            print(type(ret), len(ret))
            break


if __name__ == '__main__':
    main()
