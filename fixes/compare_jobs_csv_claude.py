"""
Compare two CSV dumps produced by ``fixes/load_jobs_csv.py``.

The two CSVs share the same 114-column schema. We use duckdb to load both
files, then run a battery of set / join queries to characterize the
differences:

* schema check
* total volumetry and per-cluster breakdown
* set-diff on the canonical MongoDB key (cluster_name, job_id, submit_time)
  — same index used by SlurmJobRepository.save_job and the unique index on
  the jobs collection (sarc/core/db_init.py)
* per-column diff over the intersection (how many shared rows differ on
  each column, ignoring NULL-NULL)
* stored-statistics coverage per group

The duckdb file is persisted on disk so the script can be re-run for
follow-up queries without re-ingesting the CSVs (which is expensive).
"""

import argparse
import os.path
import sys

import duckdb

KEY_COLS = ("cluster_name", "job_id", "submit_time")

STAT_GROUPS = (
    "gpu_utilization",
    "gpu_utilization_fp16",
    "gpu_utilization_fp32",
    "gpu_utilization_fp64",
    "gpu_sm_occupancy",
    "gpu_memory",
    "gpu_power",
    "cpu_utilization",
    "system_memory",
)


def _table_exists(conn: duckdb.DuckDBPyConnection, name: str) -> bool:
    (n,) = conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [name]
    ).fetchone()
    return n > 0


def _load_csv(
    conn: duckdb.DuckDBPyConnection, table: str, path: str, *, force: bool
) -> None:
    if _table_exists(conn, table) and not force:
        print(f"[skip] {table} already loaded")
        return
    if force:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
    print(f"[load] {table} <- {path}")
    conn.execute(
        f"CREATE TABLE {table} AS SELECT * FROM read_csv_auto(?, parallel=true, sample_size=-1)",
        [path],
    )
    (n,) = conn.execute(f"SELECT count(*) FROM {table}").fetchone()
    print(f"[load] {table}: {n:,} rows")


def _schemas_match(conn: duckdb.DuckDBPyConnection) -> bool:
    left = conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'jobs_left' ORDER BY ordinal_position"
    ).fetchall()
    right = conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'jobs_right' ORDER BY ordinal_position"
    ).fetchall()
    if left == right:
        print(f"[schema] OK ({len(left)} columns, identical types)")
        return True

    print("[schema] MISMATCH")
    by_name_left = dict(left)
    by_name_right = dict(right)
    only_left = sorted(set(by_name_left) - set(by_name_right))
    only_right = sorted(set(by_name_right) - set(by_name_left))
    common = sorted(set(by_name_left) & set(by_name_right))
    if only_left:
        print(f"  columns only in left: {only_left}")
    if only_right:
        print(f"  columns only in right: {only_right}")
    diff_type = [
        (c, by_name_left[c], by_name_right[c])
        for c in common
        if by_name_left[c] != by_name_right[c]
    ]
    if diff_type:
        print("  same name, different type:")
        for c, lt, rt in diff_type:
            print(f"    {c}: left={lt!r} right={rt!r}")
    return False


def _build_keys(conn: duckdb.DuckDBPyConnection) -> None:
    # Materialize the join keys to small tables to make subsequent set
    # operations cheap (avoid scanning the full CSV-derived tables each time).
    for side in ("left", "right"):
        tbl = f"keys_{side}"
        if _table_exists(conn, tbl):
            conn.execute(f"DROP TABLE {tbl}")
        cols = ", ".join(KEY_COLS)
        conn.execute(f"CREATE TABLE {tbl} AS SELECT {cols} FROM jobs_{side}")
        print(f"[keys] {tbl} built")


def _sql_key_eq(left_alias: str, right_alias: str) -> str:
    # All three key columns are NOT NULL in MongoDB (cluster_name/job_id/submit_time
    # are required fields on SlurmJob), so a plain `=` would suffice; IS NOT DISTINCT
    # FROM is kept for defensive safety against malformed CSV rows.
    return " AND ".join(
        f"{left_alias}.{c} IS NOT DISTINCT FROM {right_alias}.{c}" for c in KEY_COLS
    )


def _section(title: str) -> None:
    print()
    print(f"## {title}")
    print()


def _print_count(label: str, n: int) -> None:
    print(f"- {label}: **{n:,}**")


def section_volumetry(conn: duckdb.DuckDBPyConnection) -> None:
    _section("Volumétrie globale")
    (left_n,) = conn.execute("SELECT count(*) FROM jobs_left").fetchone()
    (right_n,) = conn.execute("SELECT count(*) FROM jobs_right").fetchone()
    (left_k,) = conn.execute("SELECT count(*) FROM keys_left").fetchone()
    (right_k,) = conn.execute("SELECT count(*) FROM keys_right").fetchone()
    (left_uk,) = conn.execute(
        "SELECT count(*) FROM (SELECT DISTINCT * FROM keys_left)"
    ).fetchone()
    (right_uk,) = conn.execute(
        "SELECT count(*) FROM (SELECT DISTINCT * FROM keys_right)"
    ).fetchone()
    _print_count("Lignes gauche", left_n)
    _print_count("Lignes droite", right_n)
    _print_count("Clés distinctes gauche", left_uk)
    _print_count("Clés distinctes droite", right_uk)
    if left_n != left_k or right_n != right_k:
        print("  (sanity: row count == key count)")
    if left_n != left_uk:
        print(f"  ⚠ gauche: {left_n - left_uk:,} doublons sur la clé composite")
    if right_n != right_uk:
        print(f"  ⚠ droite: {right_n - right_uk:,} doublons sur la clé composite")


def section_setdiff(conn: duckdb.DuckDBPyConnection) -> None:
    _section("Set-diff sur la clé canonique (cluster_name, job_id, submit_time)")
    # Use INTERSECT / EXCEPT — duckdb handles NULL-equality for set ops.
    (inter,) = conn.execute(
        "SELECT count(*) FROM (SELECT * FROM keys_left INTERSECT SELECT * FROM keys_right)"
    ).fetchone()
    (only_l,) = conn.execute(
        "SELECT count(*) FROM (SELECT * FROM keys_left EXCEPT SELECT * FROM keys_right)"
    ).fetchone()
    (only_r,) = conn.execute(
        "SELECT count(*) FROM (SELECT * FROM keys_right EXCEPT SELECT * FROM keys_left)"
    ).fetchone()
    _print_count("Intersection (présents des deux côtés)", inter)
    _print_count("Uniquement à gauche", only_l)
    _print_count("Uniquement à droite", only_r)


def section_by_cluster(conn: duckdb.DuckDBPyConnection) -> None:
    _section("Par cluster — comptes")
    # Build per-cluster counts on each side and the diff.
    rows = conn.execute(
        """
        WITH l AS (
            SELECT cluster_name, count(*) AS n FROM keys_left GROUP BY 1
        ),
        r AS (
            SELECT cluster_name, count(*) AS n FROM keys_right GROUP BY 1
        )
        SELECT
            coalesce(l.cluster_name, r.cluster_name) AS cluster,
            coalesce(l.n, 0) AS n_left,
            coalesce(r.n, 0) AS n_right,
            coalesce(r.n, 0) - coalesce(l.n, 0) AS delta
        FROM l FULL OUTER JOIN r USING (cluster_name)
        ORDER BY abs(coalesce(r.n, 0) - coalesce(l.n, 0)) DESC, cluster
        """
    ).fetchall()
    print("| cluster | gauche | droite | Δ |")
    print("| --- | ---: | ---: | ---: |")
    for cluster, nl, nr, delta in rows:
        print(f"| `{cluster}` | {nl:,} | {nr:,} | {delta:+,} |")

    _section("Par cluster — clés uniquement à droite (jobs ajoutés)")
    rows = conn.execute(
        """
        WITH only_right AS (
            SELECT * FROM keys_right EXCEPT SELECT * FROM keys_left
        )
        SELECT cluster_name, count(*) AS n
        FROM only_right GROUP BY 1 ORDER BY n DESC, 1
        """
    ).fetchall()
    if not rows:
        print("Aucun.")
    else:
        print("| cluster | jobs ajoutés |")
        print("| --- | ---: |")
        for cluster, n in rows:
            print(f"| `{cluster}` | {n:,} |")

    _section("Par cluster — clés uniquement à gauche (jobs disparus)")
    rows = conn.execute(
        """
        WITH only_left AS (
            SELECT * FROM keys_left EXCEPT SELECT * FROM keys_right
        )
        SELECT cluster_name, count(*) AS n
        FROM only_left GROUP BY 1 ORDER BY n DESC, 1
        """
    ).fetchall()
    if not rows:
        print("Aucun.")
    else:
        print("| cluster | jobs perdus |")
        print("| --- | ---: |")
        for cluster, n in rows:
            print(f"| `{cluster}` | {n:,} |")


def _column_names(conn: duckdb.DuckDBPyConnection) -> list[str]:
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'jobs_left' ORDER BY ordinal_position"
    ).fetchall()
    return [r[0] for r in rows]


def section_column_diff(conn: duckdb.DuckDBPyConnection) -> None:
    _section("Diff par colonne sur l'intersection")
    columns = _column_names(conn)
    # Skip the join keys themselves (always equal on the intersection).
    diff_cols = [c for c in columns if c not in KEY_COLS]

    # One JOIN, three CASE per column. We derive total_diff = gained + lost + modified.
    # - gained:   left NULL, right NOT NULL
    # - lost:     left NOT NULL, right NULL
    # - modified: both NOT NULL but different
    select_pieces: list[str] = []
    for i, c in enumerate(diff_cols):
        qc = f'"{c}"'
        select_pieces.append(
            f"sum(CASE WHEN l.{qc} IS NULL AND r.{qc} IS NOT NULL THEN 1 ELSE 0 END) AS g{i}, "
            f"sum(CASE WHEN l.{qc} IS NOT NULL AND r.{qc} IS NULL THEN 1 ELSE 0 END) AS lo{i}, "
            f"sum(CASE WHEN l.{qc} IS NOT NULL AND r.{qc} IS NOT NULL "
            f"AND l.{qc} <> r.{qc} THEN 1 ELSE 0 END) AS m{i}"
        )

    sql = f"""
        WITH inter AS (
            SELECT * FROM keys_left INTERSECT SELECT * FROM keys_right
        )
        SELECT {", ".join(select_pieces)}
        FROM jobs_left l
        JOIN inter i ON ({_sql_key_eq("l", "i")})
        JOIN jobs_right r ON ({_sql_key_eq("r", "i")})
    """
    print("Calcul du diff par colonne (single-pass JOIN sur l'intersection)…")
    row = conn.execute(sql).fetchone()
    assert row is not None
    gained_by_col: dict[str, int] = {}
    lost_by_col: dict[str, int] = {}
    modified_by_col: dict[str, int] = {}
    total_by_col: dict[str, int] = {}
    for i, c in enumerate(diff_cols):
        g, lo, m = row[3 * i], row[3 * i + 1], row[3 * i + 2]
        gained_by_col[c] = g
        lost_by_col[c] = lo
        modified_by_col[c] = m
        total_by_col[c] = g + lo + m

    print("\n### Colonnes avec au moins une différence")
    print(
        "(triées par nombre total décroissant; gained = gauche NULL → droite a une valeur, "
        "lost = gauche a une valeur → droite NULL, modified = les deux non NULL mais ≠)\n"
    )
    print("| colonne | total diff | gained | lost | modified |")
    print("| --- | ---: | ---: | ---: | ---: |")
    nonzero = [
        (c, total_by_col[c], gained_by_col[c], lost_by_col[c], modified_by_col[c])
        for c in diff_cols
        if total_by_col[c]
    ]
    nonzero.sort(key=lambda t: (-t[1], t[0]))
    for c, t, g, lo, m in nonzero:
        print(f"| `{c}` | {t:,} | {g:,} | {lo:,} | {m:,} |")
    if not nonzero:
        print("(aucune différence détectée sur l'intersection)")
    print(
        f"\n{len(diff_cols) - len(nonzero)} colonne(s) strictement identique(s) sur toute l'intersection."
    )


def section_stats_coverage(conn: duckdb.DuckDBPyConnection) -> None:
    _section(
        "Couverture des statistiques stockées (jobs avec `<groupe>.mean` non NULL)"
    )
    print("| groupe | gauche | droite | Δ |")
    print("| --- | ---: | ---: | ---: |")
    for group in STAT_GROUPS:
        col = f'"{group}.mean"'
        (nl,) = conn.execute(
            f"SELECT count(*) FROM jobs_left WHERE {col} IS NOT NULL"
        ).fetchone()
        (nr,) = conn.execute(
            f"SELECT count(*) FROM jobs_right WHERE {col} IS NOT NULL"
        ).fetchone()
        print(f"| `{group}` | {nl:,} | {nr:,} | {nr - nl:+,} |")


def section_stats_coverage_by_cluster(conn: duckdb.DuckDBPyConnection) -> None:
    _section("Couverture par cluster sur le côté droit (`<groupe>.mean` non NULL)")
    selects = [
        f'sum(CASE WHEN "{g}.mean" IS NOT NULL THEN 1 ELSE 0 END) AS "{g}"'
        for g in STAT_GROUPS
    ]
    rows = conn.execute(
        f"""
        SELECT cluster_name AS cluster, count(*) AS jobs, {", ".join(selects)}
        FROM jobs_right
        GROUP BY 1 ORDER BY jobs DESC
        """
    ).fetchall()
    header = "| cluster | jobs | " + " | ".join(f"`{g}`" for g in STAT_GROUPS) + " |"
    sep = "| --- | ---: " + ("| ---: " * len(STAT_GROUPS)) + "|"
    print(header)
    print(sep)
    for row in rows:
        cluster, jobs, *vals = row
        cells = [
            f"{v:,} ({(100 * v / jobs):.1f}%)" if jobs else f"{v:,} (-)" for v in vals
        ]
        print(f"| `{cluster}` | {jobs:,} | " + " | ".join(cells) + " |")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("left", help="Left CSV (older dump)")
    parser.add_argument("right", help="Right CSV (newer dump)")
    parser.add_argument(
        "--db",
        required=True,
        help="Path to a persistent duckdb file (created if missing; "
        "existing jobs_left/jobs_right tables are reused unless --reload).",
    )
    parser.add_argument(
        "--reload", action="store_true", help="Force re-ingestion of both CSV files."
    )
    parser.add_argument(
        "--sections",
        nargs="+",
        choices=[
            "schema",
            "volumetry",
            "setdiff",
            "by_cluster",
            "column_diff",
            "stats",
            "stats_by_cluster",
            "all",
        ],
        default=["all"],
        help="Which analysis sections to run.",
    )
    args = parser.parse_args()

    if not os.path.exists(args.left):
        print(f"left file not found: {args.left}", file=sys.stderr)
        return 1
    if not os.path.exists(args.right):
        print(f"right file not found: {args.right}", file=sys.stderr)
        return 1

    conn = duckdb.connect(args.db)
    conn.execute("SET preserve_insertion_order=false")

    _load_csv(conn, "jobs_left", args.left, force=args.reload)
    _load_csv(conn, "jobs_right", args.right, force=args.reload)

    sections = set(args.sections)
    run_all = "all" in sections

    if run_all or "schema" in sections:
        _section("Schémas")
        _schemas_match(conn)

    # keys tables are needed by several downstream sections
    _build_keys(conn)

    if run_all or "volumetry" in sections:
        section_volumetry(conn)
    if run_all or "setdiff" in sections:
        section_setdiff(conn)
    if run_all or "by_cluster" in sections:
        section_by_cluster(conn)
    if run_all or "column_diff" in sections:
        section_column_diff(conn)
    if run_all or "stats" in sections:
        section_stats_coverage(conn)
    if run_all or "stats_by_cluster" in sections:
        section_stats_coverage_by_cluster(conn)

    return 0


if __name__ == "__main__":
    sys.exit(main())
