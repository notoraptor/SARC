from sarc.client import get_jobs
from collections import Counter


jobs = list(get_jobs(cluster="tamia"))

print("Tamia jobs:", len(jobs))

gpu_jobs = [
    job
    for job in jobs
    if (job.requested.gres_gpu or 0) > 0 and (job.allocated.gres_gpu or 0) > 0
]

print("Tamia GPU jobs:", len(gpu_jobs))

gpu_types = Counter(job.allocated.gpu_type for job in gpu_jobs)

print("Tamia GPU types:")
print(gpu_types)
