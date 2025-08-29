from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime

import matplotlib.pyplot as plt

from fixes.sacct_cache.common import get_sacct_scrapers
from sarc.config import config


@dataclass(slots=True)
class JobIDGroup:
    day: datetime
    indices: list[int] = field(default_factory=list)

    @property
    def min_job_id(self) -> int:
        return min(self.indices)

    @property
    def max_job_id(self) -> int:
        return max(self.indices)


def main():
    cluster_to_job_indices: dict[str, list[JobIDGroup]] = defaultdict(list)
    for scraper in get_sacct_scrapers():
        # scraper.get_raw(cache_policy=CachePolicy.always)
        cluster_to_job_indices[scraper.cluster.name].append(
            JobIDGroup(day=scraper.day, indices=[job.job_id for job in scraper if job])
        )

    print(
        "Job entries:",
        sum(
            len(group.indices)
            for groups in cluster_to_job_indices.values()
            for group in groups
        ),
    )
    plots_dir = config().cache / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    for cluster_name, job_id_groups in cluster_to_job_indices.items():
        job_id_groups.sort(key=lambda jig: jig.day)
        valid_groups = [group for group in job_id_groups if group.indices]
        days = [job_id_group.day for job_id_group in valid_groups]
        mins = [job_id_group.min_job_id for job_id_group in valid_groups]
        maxs = [job_id_group.max_job_id for job_id_group in valid_groups]

        fig, ax = plt.subplots()
        ax.plot(days, mins, label="min_job_id")
        ax.plot(days, maxs, label="max_job_id")
        fig.autofmt_xdate()
        ax.legend()
        plt.title(cluster_name)
        plt.savefig(plots_dir / f"{cluster_name}.png")

        min_mins = min(mins)
        min_maxs = min(maxs)
        day_min_mins = days[mins.index(min_mins)]
        day_min_maxs = days[maxs.index(min_maxs)]
        print(f"[{cluster_name}] min of mins: {min_mins} at {day_min_mins}")
        print(f"[{cluster_name}] min of maxs: {min_maxs} at {day_min_maxs}")


if __name__ == "__main__":
    main()
