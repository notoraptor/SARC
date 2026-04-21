import contextlib
import csv
import sys
from abc import ABC, abstractmethod
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


def _write_csv(path: Path, rows: list[list]):
    with open(path, "w", newline="") as f:
        csv.writer(f).writerows(rows)


class Report:
    # Cluster to category (default: drac)
    CLUSTER_TO_TYPE = {
        "mila": "mila",
        "tamia": "drac-paice",
        "killarney": "drac-paice",
        "vulcan": "drac-paice",
    }

    __slots__ = ("per_total", "per_cluster_type", "per_cluster")

    def __init__(
        self,
        jagg: JobAggregation,
        *,
        per_total: bool = True,
        per_cluster_type: bool = True,
        per_cluster: bool = True,
    ):
        self.per_total: ScopeMetrics | None = None
        self.per_cluster_type: dict[str, ScopeMetrics] | None = None
        self.per_cluster: dict[str, ScopeMetrics] | None = None

        if per_total:
            report_total = jagg.report(lambda _: "total")
            if report_total:
                self.per_total = report_total["total"]
        if per_cluster_type:
            report_per_cluster_type = jagg.report(
                lambda record: self.CLUSTER_TO_TYPE.get(record.cluster_name, "drac")
            )
            if report_per_cluster_type:
                self.per_cluster_type = report_per_cluster_type
        if per_cluster:
            report_per_cluster = jagg.report(lambda record: record.cluster_name)
            if report_per_cluster:
                self.per_cluster = report_per_cluster

    def dump_text(self, output: Path | str | None = None):
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

    def dump_multi_csv_files(self, folder: Path):
        scope_type_metrics: list[tuple[str, str, ScopeMetrics]] = []
        if self.per_total:
            scope_type_metrics.append(("total", "total", self.per_total))
        if self.per_cluster_type:
            for cluster_type, metrics in sorted(
                self.per_cluster_type.items(), key=lambda item: item[0]
            ):
                scope_type_metrics.append(("domain", cluster_type, metrics))
        if self.per_cluster:
            for cluster_name, metrics in sorted(
                self.per_cluster.items(), key=lambda item: item[0]
            ):
                scope_type_metrics.append(("cluster", cluster_name, metrics))

        csv_metrics: list[list] = [
            [
                "start",
                "end",
                "scope_type",
                "scope_name",
                "nb_unique_users",
                "nb_unique_jobs",
                "nb_jobs_per_unique_user",
                "total_rgu_secs",
                "rgu_sec_per_unique_user",
            ]
        ]
        csv_users: list[list] = [
            ["start", "end", "scope_type", "scope_name", "user", "rgu_sec"]
        ]
        for scope, name, metrics in scope_type_metrics:
            csv_metrics.append(
                [
                    metrics.time_from.isoformat(),
                    metrics.time_to.isoformat(),
                    scope,
                    name,
                    metrics.nb_unique_users,
                    metrics.nb_unique_jobs,
                    metrics.nb_jobs_per_unique_user,
                    metrics.total_rgu_seconds,
                    metrics.rgu_sec_per_unique_user,
                ]
            )
            csv_users.extend(
                [
                    metrics.time_from.isoformat(),
                    metrics.time_to.isoformat(),
                    scope,
                    name,
                    user,
                    rgu_sec,
                ]
                for user, rgu_sec in sorted(
                    metrics.unique_user_to_rgu_sec.items(),
                    key=lambda item: (-item[1], item[0]),
                )
            )

        _write_csv(folder / "metrics.csv", csv_metrics)
        _write_csv(folder / "user-to-rgu.csv", csv_users)


class ReportOutput(ABC):
    __slots__ = ("path",)

    def __init__(self, path: Path | str | None):
        self.path: Path | None = (
            None if path is None else (path if isinstance(path, Path) else Path(path))
        )

    @abstractmethod
    def dump(self, report: Report):
        raise NotImplementedError()

    @abstractmethod
    def is_present(self) -> bool:
        """True if an artifact already exists at self.path (used to skip in split mode)."""
        raise NotImplementedError()


class TextOutputFile(ReportOutput):
    __slots__ = ()

    def dump(self, report: Report):
        report.dump_text(self.path)

    def is_present(self) -> bool:
        return (
            self.path is not None
            and self.path.exists()
            and self.path.stat().st_size > 0
        )


class CsvOutputFolder(ReportOutput):
    __slots__ = ()

    def __init__(self, path: Path | str):
        super().__init__(path)
        assert self.path is not None

    def dump(self, report: Report):
        assert self.path is not None
        self.path.mkdir(parents=True, exist_ok=True)
        report.dump_multi_csv_files(self.path)

    def is_present(self) -> bool:
        assert self.path is not None
        return self.path.exists() and any(self.path.iterdir())
