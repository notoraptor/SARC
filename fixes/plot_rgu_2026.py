"""
Plot weekly RGU consumption per cluster for 2026 from a SARC job CSV dump.

Reads a CSV produced by ``fixes/load_jobs_csv.py`` row by row with the
stdlib ``csv`` module — no duckdb, no full in-memory load. For every row,
the Monday of the ``submit_time`` week is computed; only rows whose
Monday falls in 2026 are kept. Per (week, cluster_name) we accumulate::

    allocated_rgu_week = sum(gpu_type_rgu * gres_gpu * elapsed_time) / 604800

and per week we count distinct users. The result is a stacked bar chart
plus a line of distinct user count per week, mirroring the corresponding
PowerBI dashboard view so the two can be compared.
"""

import argparse
import csv
import math
import os
import sys
from collections import defaultdict
from datetime import UTC, datetime, timedelta

import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
from tqdm import tqdm

# PowerBI default theme — chosen so the rendered plot lines up with what
# the dashboard produces (visual comparability is the whole point here).
CLUSTER_COLORS = {
    "mila": "#118DFF",
    "fir": "#12239E",
    "narval": "#E66C37",
    "nibi": "#6B007B",
    "rorqual": "#E044A7",
    "tamia": "#744EC2",
    "trillium": "#D9B300",
    "trillium-gpu": "#D64550",
    "cedar": "#607D8B",
    "beluga": "#26A69A",
    "graham": "#795548",
}
# Preferred legend order — alphabetical, matching PowerBI's legend layout.
# Inside each bar, segments are restacked per-week largest-first (matches
# PowerBI's "sort by value descending" default on stacked column charts).
LEGEND_ORDER = ("fir", "mila", "narval", "nibi", "rorqual")
USER_LINE_COLOR = "#1F9D6B"
WEEK_SECONDS = 7 * 24 * 3600  # 604800

# Window of accepted week-Mondays: first Monday of 2026 through last Monday
# whose week still belongs to 2026. (Jan 1 2026 is a Thursday; its week
# starts on Mon Dec 29 2025 and would otherwise sneak in.)
FIRST_MONDAY_2026 = datetime(2026, 1, 5, tzinfo=UTC)
FIRST_MONDAY_2027 = datetime(2027, 1, 4, tzinfo=UTC)


def _to_float(s: str | None) -> float | None:
    if s is None or s == "":
        return None
    v = float(s)
    assert not math.isnan(v)
    return v


def _monday_of(dt: datetime) -> datetime:
    # weekday(): Monday = 0. Strip the time so the bucket key is a
    # midnight on Monday — easier to use as a dict key and to
    # feed back to matplotlib.
    days_since_monday = dt.weekday()
    return dt.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
        days=days_since_monday
    )


def _aggregate(
    csv_path: str,
) -> tuple[dict[tuple[datetime, str], float], dict[datetime, set[str]]]:
    rgu_by_wc: dict[tuple[datetime, str], float] = defaultdict(float)
    users_by_w: dict[datetime, set[str]] = defaultdict(set)

    n_total = 0
    n_kept_user = 0
    n_kept_rgu = 0
    with open(csv_path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in tqdm(reader, desc="rows", unit=" rows"):
            n_total += 1
            submit = datetime.fromisoformat(row["submit_time"])
            assert submit.tzinfo is not None
            assert submit.utcoffset() == timedelta(0)

            wk = _monday_of(submit)
            if wk < FIRST_MONDAY_2026 or wk >= FIRST_MONDAY_2027:
                continue

            cluster = row["cluster_name"]
            user = row.get("user") or ""
            if user:
                users_by_w[wk].add(user)
                n_kept_user += 1

            rgu = _to_float(row.get("allocated.gpu_type_rgu"))
            gres = _to_float(row.get("allocated.gres_gpu"))
            elapsed = _to_float(row.get("elapsed_time"))
            if rgu is None or gres is None or elapsed is None:
                continue
            if gres <= 0 or elapsed <= 0:
                continue
            rgu_by_wc[(wk, cluster)] += rgu * gres * elapsed / WEEK_SECONDS
            n_kept_rgu += 1

    print(f"  parsed                       : {n_total:,} rows")
    print(f"  kept for user count (2026)   : {n_kept_user:,}")
    print(f"  kept for RGU sum   (2026)    : {n_kept_rgu:,}")
    return rgu_by_wc, users_by_w


def _plot(
    rgu_by_wc: dict[tuple[datetime, str], float],
    users_by_w: dict[datetime, set[str]],
    output: str,
) -> None:
    weeks = sorted({w for w, _ in rgu_by_wc} | set(users_by_w))
    clusters_seen = sorted({c for _, c in rgu_by_wc})
    legend_clusters = [c for c in LEGEND_ORDER if c in clusters_seen] + sorted(
        c for c in clusters_seen if c not in LEGEND_ORDER
    )

    by_cluster: dict[str, dict[datetime, float]] = {
        c: {w: rgu_by_wc.get((w, c), 0.0) for w in weeks} for c in legend_clusters
    }
    users_by_week = {w: len(users_by_w.get(w, set())) for w in weeks}

    fig, ax = plt.subplots(figsize=(13, 6))
    # PowerBI restacks each bar with the largest segment at the bottom; we
    # replicate that by drawing one bar per (week, cluster) segment after
    # sorting clusters by descending value within that week.
    for w in weeks:
        sorted_clusters = sorted(
            legend_clusters, key=lambda c: by_cluster[c][w], reverse=True
        )
        offset = 0.0
        for c in sorted_clusters:
            v = by_cluster[c][w]
            if v <= 0:
                continue
            ax.bar(
                w,  # type: ignore[arg-type]  # mpl accepts datetime
                v,
                bottom=offset,
                width=5,
                color=CLUSTER_COLORS.get(c, "#999999"),
                edgecolor="none",
            )
            offset += v

    ax.set_ylabel("Somme de Allocated_RGU_week")
    ax.set_xlabel("Semaine (lundi de submit_time)")
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0))  # 0 = Monday
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    ax.grid(axis="y", linestyle=":", alpha=0.4)
    ax.set_axisbelow(True)

    ax2 = ax.twinx()
    user_values = [users_by_week[w] for w in weeks]
    (line,) = ax2.plot(
        weeks,  # type: ignore[arg-type]  # mpl accepts list[datetime]
        user_values,
        color=USER_LINE_COLOR,
        marker="o",
        linewidth=2,
        label="Nombre de user",
    )
    for w, v in zip(weeks, user_values):
        if v:
            ax2.annotate(
                str(v),
                (w, v),  # type: ignore[arg-type]  # mpl accepts (datetime, int)
                textcoords="offset points",
                xytext=(0, 6),
                ha="center",
                fontsize=8,
                color=USER_LINE_COLOR,
            )
    ax2.set_ylabel("Nombre de user")

    bar_handles = [
        mpatches.Patch(color=CLUSTER_COLORS.get(c, "#999999"), label=c)
        for c in legend_clusters
    ]
    ax.legend(
        bar_handles + [line],
        legend_clusters + ["Nombre de user"],
        loc="upper left",
        ncol=len(legend_clusters) + 1,
        frameon=False,
    )
    ax.set_title(
        "Somme de Allocated_RGU_week et Nombre de user par semaine et cluster (2026)"
    )

    fig.tight_layout()
    fig.savefig(output, dpi=140)
    print(f"Saved {output}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", help="Path to the jobs CSV dump.")
    parser.add_argument(
        "-o",
        "--output",
        default="rgu_2026.png",
        help="Output PNG path (default: rgu_2026.png).",
    )
    args = parser.parse_args()

    if not os.path.exists(args.csv):
        print(f"CSV not found: {args.csv}", file=sys.stderr)
        return 1

    print(f"Streaming {args.csv}…")
    rgu_by_wc, users_by_w = _aggregate(args.csv)
    if not rgu_by_wc:
        print("No 2026 weekly RGU data found.", file=sys.stderr)
        return 2

    _plot(rgu_by_wc, users_by_w, args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
