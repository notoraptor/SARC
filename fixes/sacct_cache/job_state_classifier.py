import os
from datetime import datetime
from typing import Literal, TypeAlias

from fixes.sacct_cache.common import get_sacct_scrapers
from sarc.cache import with_cache

from sarc.config import MTL, config

EPOCH_LIMITS = {"cedar": "2023-05-03", "graham": "2022-11-02"}
EPOCHS = {
    cluster_name: datetime.strptime(date_string, "%Y-%m-%d").replace(tzinfo=MTL)
    for cluster_name, date_string in EPOCH_LIMITS.items()
}


def _get_classifier_key():
    cfg = config()
    sacct_cache = cfg.cache / "sacct"
    dir_content = sorted(os.listdir(sacct_cache))
    nb_files = len(dir_content)
    pieces = ["classifier", str(nb_files)]
    if nb_files:
        pieces.extend(("from", dir_content[0][:-5], "to", dir_content[-1][:-5]))
    return "_".join(pieces) + ".json"


@with_cache(key=_get_classifier_key)
def get_states_classifier() -> dict[
    str, dict[Literal[0, 1], dict[int, dict[str, list[str]]]]
]:
    # cluster_name -> epoch -> job_id -> submit_time <=> [job_state]
    classifier: dict[str, dict[Literal[0, 1], dict[int, dict[str, list[str]]]]] = {}
    for scraper in get_sacct_scrapers():
        for job in scraper:
            if job:
                cluster_name = job.cluster_name
                submit_time = job.submit_time
                epoch_limit = EPOCHS.get(cluster_name)
                epoch: Literal[0, 1] = (
                    1 if (epoch_limit is not None and submit_time >= epoch_limit) else 0
                )
                states = (
                    classifier.setdefault(cluster_name, {})
                    .setdefault(epoch, {})
                    .setdefault(job.job_id, {})
                    .setdefault(str(submit_time), [])
                )
                state = job.job_state.name
                if not states or states[-1] != state:
                    states.append(state)
    return classifier


# cluster_name -> epoch -> job_id -> submit_time -> scraper_time: job_state
Classifier: TypeAlias = dict[
    str, dict[Literal[0, 1], dict[int, dict[str, dict[str, str]]]]
]


def _get_classifier_dated_key():
    cfg = config()
    sacct_cache = cfg.cache / "sacct"
    dir_content = sorted(os.listdir(sacct_cache))
    nb_files = len(dir_content)
    pieces = ["classifier_dated", str(nb_files)]
    if nb_files:
        pieces.extend(("from", dir_content[0][:-5], "to", dir_content[-1][:-5]))
    return "_".join(pieces) + ".json"


@with_cache(key=_get_classifier_dated_key)
def get_states_classifier_dated() -> Classifier:
    classifier: Classifier = {}
    for scraper in get_sacct_scrapers():
        scraper_time = str(scraper.start)
        for job in scraper:
            if job:
                cluster_name = job.cluster_name
                submit_time = job.submit_time
                epoch_limit = EPOCHS.get(cluster_name)
                epoch: Literal[0, 1] = (
                    1 if (epoch_limit is not None and submit_time >= epoch_limit) else 0
                )
                classifier.setdefault(cluster_name, {}).setdefault(
                    epoch, {}
                ).setdefault(job.job_id, {}).setdefault(str(submit_time), {})[
                    scraper_time
                ] = job.job_state.name
    return classifier
