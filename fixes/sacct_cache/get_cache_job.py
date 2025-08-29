import argparse
import pprint
from datetime import datetime, timedelta

from fixes.sacct_cache.common import MyScraper
from sarc.config import config


def main():
    parser = argparse.ArgumentParser(description="SACCT job from cache")
    parser.add_argument("-c", "--cluster-name", required=True)
    parser.add_argument("-j", "--job-id", type=int, required=True)
    parser.add_argument("-d", "--date", required=True)

    args = parser.parse_args()

    cluster_name = args.cluster_name
    job_id = args.job_id
    day = datetime.strptime(args.date, "%Y-%m-%d")

    cfg = config()
    clusters = cfg.clusters

    scraper = MyScraper(clusters[cluster_name], day)
    print(
        f"[{scraper.cluster.name}][{scraper.day}]", len(scraper), "job(s) in this cache"
    )
    print("Looking for:", repr(cluster_name), repr(job_id))
    for job in scraper:
        if job and job.cluster_name == cluster_name and job.job_id == job_id:
            elapsed = timedelta(seconds=job.elapsed_time)
            current_end = job.start_time + elapsed

            limit_from_submit = None
            limit_from_start = None
            if job.time_limit:
                time_limit = timedelta(seconds=job.time_limit)
                limit_from_submit = job.submit_time + time_limit
                limit_from_start = job.start_time + time_limit

            print("=" * 80)
            pprint.pprint(job.model_dump())
            pprint.pprint(
                {
                    "current_end": current_end,
                    "limit_from_submit": limit_from_submit,
                    "limit_from_start": limit_from_start,
                    "current_end_gte_limit_from_submit": current_end
                    >= limit_from_submit,
                    "current_end_gte_limit_from_start": current_end >= limit_from_start,
                }
            )
            print()


if __name__ == "__main__":
    main()
