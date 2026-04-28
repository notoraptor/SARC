from dataclasses import dataclass, field
from datetime import datetime


@dataclass(slots=True)
class ScopeMetrics:
    time_from: datetime
    time_to: datetime
    scope_name: str

    nb_unique_jobs: int = 0
    total_rgu_seconds: float = 0.0
    unique_users: set[str] = field(default_factory=set)
    unique_user_to_rgu_sec: dict[str, float] = field(default_factory=dict)

    @property
    def nb_unique_users(self) -> int:
        return len(self.unique_users)

    @property
    def nb_jobs_per_unique_user(self) -> float:
        return self.nb_unique_jobs / (self.nb_unique_users or 1)

    @property
    def rgu_sec_per_unique_user(self) -> float:
        return self.total_rgu_seconds / (self.nb_unique_users or 1)

    def __str__(self):
        pieces = [
            f"[{self.scope_name} - {self.time_from.isoformat()} - {self.time_to.isoformat()}]",
            f"Unique users:                {self.nb_unique_users:_}",
            f"Unique jobs:                 {self.nb_unique_jobs:_}",
            f"Total RGU*seconds:           {self.total_rgu_seconds:_}",
            f"Jobs per unique user:        {self.nb_jobs_per_unique_user:_}",
            f"RGU*seconds per unique user: {self.rgu_sec_per_unique_user:_}",
            "Greatest consumers:",
        ]
        sorted_consumers = sorted(
            self.unique_user_to_rgu_sec.items(), key=lambda item: (-item[1], item[0])
        )
        for user, rgu_sec in sorted_consumers[:20]:
            pieces.append(f"\t{user}: {rgu_sec:_}")
        if len(sorted_consumers) > 20:
            pieces.append(f"\t(... first 20 of {len(sorted_consumers)})")
        return "\n".join(pieces) + "\n\n"
