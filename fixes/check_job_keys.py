import csv
import sys

from sarc.client import get_jobs
from datetime import datetime, timedelta

from sarc.config import MTL


def main():
    limit = datetime(2025, 9, 1, tzinfo=MTL)
    now = datetime.now(tz=MTL)
    nb_rows = 0
    csv_file = sys.argv[1]
    with open(csv_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            nb_rows += 1
            submit_time = datetime.fromisoformat(row["submit_time"])
            jobs = list(
                get_jobs(cluster=row["cluster_name"], job_id=int(row["job_id"]))
            )
            assert jobs
            (job,) = [j for j in jobs if j.submit_time == submit_time]
            end_time = job.end_time or now
            start_time = end_time - timedelta(seconds=job.elapsed_time)
            assert job.start_time >= job.submit_time
            assert start_time >= job.submit_time
            assert job.submit_time > limit
    print(nb_rows)


if __name__ == "__main__":
    main()
