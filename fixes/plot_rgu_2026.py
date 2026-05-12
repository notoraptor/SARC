"""
Plot weekly RGU consumption per cluster for 2026 from a SARC job CSV dump.

Reads a CSV produced by ``fixes/load_jobs_csv.py``, keeps only jobs whose
``submit_time`` falls in 2026, buckets them by ISO week (Monday-start), and
computes per (week, cluster_name)::

    allocated_rgu_week = sum(gpu_type_rgu * gres_gpu * elapsed_time) / 604800

Renders a stacked bar chart plus a line of distinct user count per week,
mirroring the corresponding PowerBI dashboard view so the two can be
compared.
"""

import argparse
import os
import sys

import duckdb
import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt

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


def _query_weekly_rgu(conn: duckdb.DuckDBPyConnection, table: str):
    # date_part / date_trunc work on both naive and tz-aware timestamps in
    # duckdb; we cast the truncated value to DATE so the Python side gets a
    # uniform key regardless of how the CSV reader inferred submit_time.
    # Filter on the Monday of the week (not the year of submit_time) so the
    # Dec 29 → Jan 4 split-week, which would have a 2025 Monday, is excluded.
    return conn.execute(f"""
        SELECT
            date_trunc('week', submit_time)::DATE AS week_start,
            cluster_name,
            sum("allocated.gpu_type_rgu" * "allocated.gres_gpu" * elapsed_time)
                / {WEEK_SECONDS}.0 AS rgu_week
        FROM {table}
        WHERE date_trunc('week', submit_time)::DATE >= DATE '2026-01-05'
          AND date_trunc('week', submit_time)::DATE <  DATE '2027-01-04'
          AND "allocated.gpu_type_rgu" IS NOT NULL
          AND "allocated.gres_gpu" > 0
          AND elapsed_time > 0
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).fetchall()


def _query_weekly_users(conn: duckdb.DuckDBPyConnection, table: str):
    return conn.execute(f"""
        SELECT
            date_trunc('week', submit_time)::DATE AS week_start,
            count(DISTINCT "user") AS n_user
        FROM {table}
        WHERE date_trunc('week', submit_time)::DATE >= DATE '2026-01-05'
          AND date_trunc('week', submit_time)::DATE <  DATE '2027-01-04'
        GROUP BY 1 ORDER BY 1
    """).fetchall()


def _load_csv(conn: duckdb.DuckDBPyConnection, path: str) -> None:
    print(f"Loading {path}…")
    conn.execute(
        "CREATE TABLE jobs AS SELECT * FROM read_csv_auto(?, parallel=true, sample_size=-1)",
        [path],
    )
    row = conn.execute("SELECT count(*) FROM jobs").fetchone()
    assert row is not None
    print(f"  {row[0]:,} rows")


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

    conn = duckdb.connect(":memory:")
    conn.execute("SET preserve_insertion_order=false")
    _load_csv(conn, args.csv)

    rgu_rows = _query_weekly_rgu(conn, "jobs")
    user_rows = _query_weekly_users(conn, "jobs")
    if not rgu_rows:
        print("No 2026 RGU rows in the CSV.", file=sys.stderr)
        return 2

    weeks = sorted({r[0] for r in rgu_rows} | {r[0] for r in user_rows})
    clusters_seen = sorted({r[1] for r in rgu_rows})
    legend_clusters = [c for c in LEGEND_ORDER if c in clusters_seen] + sorted(
        c for c in clusters_seen if c not in LEGEND_ORDER
    )
    by_cluster: dict[str, dict] = {
        c: dict.fromkeys(weeks, 0.0) for c in legend_clusters
    }
    for w, c, v in rgu_rows:
        by_cluster[c][w] = v or 0.0
    users_by_week = dict.fromkeys(weeks, 0)
    for w, n_user in user_rows:
        users_by_week[w] = n_user or 0

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
                w,
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
        weeks,
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
                (w, v),
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
    fig.savefig(args.output, dpi=140)
    print(f"Saved {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
