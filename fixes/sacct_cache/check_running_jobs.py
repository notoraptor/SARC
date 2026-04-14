import sys
from collections import Counter
from datetime import datetime, timedelta

from fixes.sacct_cache.classifier_cache_jobs import JobInfo, get_classified_cache_jobs
from fixes.sacct_cache.job_state_classifier import EPOCHS
from sarc.client.job import SlurmState


def get_latest_job_state(
    submitt_to_scrapert_to_job: dict[str, dict[str, JobInfo]],
) -> str:
    last_submitt = sorted(submitt_to_scrapert_to_job.keys())[-1]
    scraper_time_to_job = submitt_to_scrapert_to_job[last_submitt]
    last_scrapert = sorted(scraper_time_to_job.keys())[-1]
    last_job = scraper_time_to_job[last_scrapert]
    return last_job["job_state"]


def get_limits_info(job: JobInfo, submit_timestring: str):
    output = {}
    current_end = None
    if job["start_time"]:
        elapsed = timedelta(seconds=job["elapsed_time"])
        start_time = datetime.fromisoformat(job["start_time"])
        current_end = start_time + elapsed
        output["start_time"] = job["start_time"]
        output["elapsed"] = f"{job['elapsed_time']} ({elapsed})"
        output["current end"] = current_end
    # limit_from_start = None
    limit_from_submit = None
    if job["time_limit"]:
        submit_time = datetime.fromisoformat(submit_timestring)
        time_limit = timedelta(seconds=job["time_limit"])
        limit_from_submit = submit_time + time_limit
        # limit_from_start = start_time + time_limit
        output["time_limit"] = f"{job['time_limit']} ({time_limit})"
        output["limit from submit"] = limit_from_submit
        if current_end:
            output["current end >= limit from submit"] = (
                current_end >= limit_from_submit
            )
    return output


def main():
    output_path = sys.argv[1] if len(sys.argv) == 2 else None
    if output_path:
        output = open(output_path, mode="w", encoding="utf-8")
        print("Output in:", output_path)
    else:
        output = sys.stdout

    try:
        print("EPOCHS:", file=output)
        for cluster_name, epoch_limit in EPOCHS.items():
            print(f"\t{cluster_name}\t{epoch_limit}", file=output)
        print(file=output)

        classifier = get_classified_cache_jobs()

        key_running = SlurmState.RUNNING.name

        cluster_to_nb_jobs = Counter()
        cluster_to_nb_running = Counter()

        for cluster_name, cluster_epoch in classifier.items():
            for epoch, job_indices in cluster_epoch.items():
                for job_id, submitt_to_scrapert_to_job in job_indices.items():
                    cluster_to_nb_jobs.update(
                        [cluster_name] * len(submitt_to_scrapert_to_job)
                    )
                    if get_latest_job_state(submitt_to_scrapert_to_job) == key_running:
                        # Found a job with latest status still running
                        cluster_to_nb_running.update(
                            [cluster_name] * len(submitt_to_scrapert_to_job)
                        )

                        other_epoch = str((int(epoch) + 1) % 2)
                        in_another_epoch = (
                            classifier.get(cluster_name, {})
                            .get(other_epoch, {})
                            .get(job_id, None)
                        )

                        print(f"{cluster_name}[epoch {epoch}]\t{job_id}", file=output)
                        for submit_time in sorted(submitt_to_scrapert_to_job.keys()):
                            scrapert_to_job = submitt_to_scrapert_to_job[submit_time]
                            print(f"\t[submit] {submit_time}", file=output)
                            for scraper_time in sorted(scrapert_to_job.keys()):
                                job = scrapert_to_job[scraper_time]
                                print(
                                    f"\t\t[scraping] {scraper_time}\t{job['job_state']}",
                                    file=output,
                                )
                                if True:
                                    limits = get_limits_info(job, submit_time)
                                    for limit_key, limit_info in limits.items():
                                        print(
                                            f"\t\t\t[{limit_key}]\t{limit_info}",
                                            file=output,
                                        )
                        if in_another_epoch:
                            print(f"\t[also in epoch {other_epoch}]", file=output)
                            for submit_time in sorted(in_another_epoch.keys()):
                                other_scrapert_to_job = in_another_epoch[submit_time]
                                print(f"\t[submit] {submit_time}", file=output)
                                for scraper_time in sorted(
                                    other_scrapert_to_job.keys()
                                ):
                                    other_job = other_scrapert_to_job[scraper_time]
                                    print(
                                        f"\t\t[scraping] {scraper_time}\t{other_job['job_state']}",
                                        file=output,
                                    )

        print("Cluster -> nb. jobs:")
        print(cluster_to_nb_jobs)
        print("Cluster -> nb. running:")
        print(cluster_to_nb_running)

    finally:
        if output_path:
            output.close()


if __name__ == "__main__":
    main()
