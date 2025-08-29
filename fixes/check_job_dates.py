from typing import Callable, Any, TypeVar

from fixes.db_job_iterator import get_database_jobs
from datetime import datetime
from collections import Counter

from sarc.config import UTC


T = TypeVar("T")


class FlagDomain[T]:
    def __init__(
        self,
        *flags: str,
        builder: Callable[[], T] = dict,
        formatter: Callable[[T], str] = str,
        summarizer: Callable[[T], Any] | None = None,
    ):
        assert flags
        assert len(set(flags)) == len(flags), "duplicated flags"
        self._flags = flags
        self._builder = builder
        self._formatter = formatter
        self._summarizer = summarizer
        self._key_to_instance: dict[tuple[bool | None, ...], T] = {}

    def get(self, **kwargs) -> T:
        key = []
        for flag in self._flags:
            value = bool(kwargs[flag]) if flag in kwargs else None
            key.append(value)
        inst_key = tuple(key)
        if inst_key not in self._key_to_instance:
            self._key_to_instance[inst_key] = self._builder()
        return self._key_to_instance[inst_key]

    def report(self):
        for key, inst in self._key_to_instance.items():
            key_names = []
            assert len(self._flags) == len(key)
            for flag, value in zip(self._flags, key):
                if value:
                    name = f"{flag}:yes"
                elif value is False:
                    name = f"{flag}:no "
                else:
                    assert value is None
                    name = (" " * len(flag)) + "    "
                key_names.append(name)
            inst_name = " ".join(key_names)
            if self._summarizer:
                print(f"[{inst_name}] {self._summarizer(inst)}")
            else:
                print(f"[{inst_name}]")
            print(self._formatter(inst))
            print()


def main():
    now = datetime.now(tz=UTC)
    job_keys = set()
    flagdom = FlagDomain[Counter](
        "valid",
        "end",
        "start",
        "expired",
        "start_is_submit",
        builder=Counter,
        summarizer=Counter.total,
    )
    flagdom.get(valid=True)
    flagdom.get(valid=True, start_is_submit=True)
    flagdom.get(end=True)
    flagdom.get(end=True, start_is_submit=True)
    flagdom.get(end=False, start=False)
    flagdom.get(end=False, start=True)
    flagdom.get(end=False, start=True, expired=False)
    flagdom.get(end=False, start=True, expired=True)
    flagdom.get(end=False, start=True, expired=True, start_is_submit=True)

    for job in get_database_jobs():
        # Just check if key is unique for this job
        key = (job.cluster_name, job.job_id, job.submit_time)
        assert key not in job_keys, key
        job_keys.add(key)

        # Check dates
        job_state = job.job_state.name

        if job.end_time is not None:
            assert job.start_time is not None
            inferred_duration = job.end_time - job.start_time
            if inferred_duration.total_seconds() == job.elapsed_time:
                flagdom.get(valid=True).update([job_state])
                if job.start_time == job.submit_time:
                    flagdom.get(valid=True, start_is_submit=True).update([job_state])
            else:
                flagdom.get(end=True).update([job_state])
                if job.start_time == job.submit_time:
                    flagdom.get(end=True, start_is_submit=True).update([job_state])
        elif job.start_time is None:
            flagdom.get(end=False, start=False).update([job_state])
        else:
            flagdom.get(end=False, start=True).update([job_state])
            elapsed_to_now = now - job.start_time
            if elapsed_to_now.total_seconds() < job.time_limit:
                flagdom.get(end=False, start=True, expired=False).update([job_state])
            else:
                flagdom.get(end=False, start=True, expired=True).update([job_state])
                if job.start_time == job.submit_time:
                    flagdom.get(
                        end=False, start=True, expired=True, start_is_submit=True
                    ).update([job_state])

    print()
    flagdom.report()


if __name__ == "__main__":
    main()
