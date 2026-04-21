import contextlib
import csv
import sys
from pathlib import Path

from fixes.metrics.core.job_aggregation import JobAggregation
from fixes.metrics.core.scope_metrics import ScopeMetrics


@contextlib.contextmanager
def _get_output_file(filename: str | Path | None = None):
    if filename is None:
        output = sys.stdout
    else:
        output = open(filename, mode="w", encoding="utf-8")
        print("[output]", filename)
    try:
        yield output
    finally:
        if filename is not None:
            output.close()


# Cluster to category (default: drac)
CLUSTER_TO_TYPE = {
    "mila": "mila",
    "tamia": "drac-paice",
    "killarney": "drac-paice",
    "vulcan": "drac-paice",
}


class Report:
    __slots__ = ("per_total", "per_cluster_type", "per_cluster")

    def __init__(
        self,
        jagg: JobAggregation,
        *,
        per_total: bool = False,
        per_cluster_type: bool = False,
        per_cluster: bool = False,
    ):
        self.per_total: ScopeMetrics | None = None
        self.per_cluster_type: dict[str, ScopeMetrics] | None = None
        self.per_cluster: dict[str, ScopeMetrics] | None = None

        if per_total:
            report_total = jagg.report(lambda record: "total")
            if report_total:
                self.per_total = report_total["total"]
        if per_cluster_type:
            report_per_cluster_type = jagg.report(
                lambda record: CLUSTER_TO_TYPE.get(record.cluster_name, "drac")
            )
            if report_per_cluster_type:
                self.per_cluster_type = report_per_cluster_type
        if per_cluster:
            report_per_cluster = jagg.report(lambda record: record.cluster_name)
            if report_per_cluster:
                self.per_cluster = report_per_cluster

    def dump_text_file(self, output: str | None = None):
        with _get_output_file(output) as file:
            if self.per_total:
                print(self.per_total, file=file)
            if self.per_cluster_type:
                print("Per cluster type:", file=file)
                print("=================", file=file)
                print(file=file)
                for cluster_type in sorted(self.per_cluster_type.keys()):
                    print(self.per_cluster_type[cluster_type], file=file)
            if self.per_cluster:
                print("Per cluster:", file=file)
                print("============", file=file)
                print(file=file)
                for cluster in sorted(self.per_cluster.keys()):
                    print(self.per_cluster[cluster], file=file)

    def dump_multi_csv(self, folder: Path):
        # NB: Not yet used
        scope_type_metrics: list[tuple[str, str, ScopeMetrics]] = []
        if self.per_total:
            scope_type_metrics.append(("total", "total", self.per_total))
        if self.per_cluster_type:
            for cluster_type, metrics in self.per_cluster_type.items():
                scope_type_metrics.append(("domain", cluster_type, metrics))
        if self.per_cluster:
            for cluster_name, metrics in self.per_cluster.items():
                scope_type_metrics.append(("cluster", cluster_name, metrics))

        csv_metrics: list[list] = [
            [
                "scope_type",
                "scope_name",
                "nb_unique_users",
                "nb_unique_jobs",
                "nb_jobs_per_unique_user",
                "total_rgu_secs",
                "rgu_sec_per_unique_user",
            ]
        ]
        csvs_users: dict[str, list[list]] = {}
        for scope, name, metrics in scope_type_metrics:
            csv_metrics.append(
                [
                    scope,
                    name,
                    metrics.nb_unique_users,
                    metrics.nb_unique_jobs,
                    metrics.nb_jobs_per_unique_user,
                    metrics.total_rgu_seconds,
                    metrics.rgu_sec_per_unique_user,
                ]
            )
            csvs_users[f"{scope}-{name}"] = [["user", "rgu_sec"]] + [
                [user, rgu_sec]
                for user, rgu_sec in sorted(
                    metrics.unique_user_to_rgu_sec.items(),
                    key=lambda item: (-item[1], item[0]),
                )
            ]

        _write_csv(folder / "metrics.csv", csv_metrics)
        for csv_user_name, csv_user_content in csvs_users.items():
            _write_csv(folder / f"user-to-rgu-{csv_user_name}.csv", csv_user_content)


def _write_csv(path: Path, rows: list[list]):
    with open(path, "w", newline="") as f:
        csv.writer(f).writerows(rows)
