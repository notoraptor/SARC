import os
import sys
from datetime import datetime
from typing import Generator

from tqdm import tqdm

from sarc.client.job import SlurmJob
from sarc.config import config, MTL
from sarc.jobs.sacct import SAcctScraper
from sarc.traces import trace_decorator


class MyScraper(SAcctScraper):
    @trace_decorator()
    def fetch_raw(self) -> dict:
        raise ValueError(f"No cache: {self.cluster.name} / {self.day}")

    def convert(self, entry: dict, version: dict | None = None) -> SlurmJob | None:
        try:
            return super().convert(entry, version)
        except KeyError as e:
            print(
                f"[{self.cluster.name}][{self.day}] key error: {e}, version: {version}",
                file=sys.stderr,
            )
            return None


def get_sacct_scrapers() -> Generator[SAcctScraper, None, None]:
    cfg = config()
    clusters = cfg.clusters
    sacct_cache = cfg.cache / "sacct"

    cluster_last_day: dict[str, datetime] = {}

    for name in tqdm(sorted(os.listdir(sacct_cache)), desc="Sacct Cache"):
        cluster_name, daystr, _ = name.split(".")
        day = datetime.strptime(daystr, "%Y-%m-%d").replace(tzinfo=MTL)

        if cluster_name in cluster_last_day:
            assert cluster_last_day[cluster_name] < day
        cluster_last_day[cluster_name] = day

        yield MyScraper(clusters[cluster_name], day)
