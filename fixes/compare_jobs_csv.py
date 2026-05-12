import os.path
import sys
import tempfile

import duckdb


def main():
    filename_left, filename_right = sys.argv[1:]
    with tempfile.TemporaryDirectory() as tmpdir:
        left_duck = os.path.join(tmpdir, "left.duckdb")
        conn = duckdb.connect(left_duck)

        # Intended as optimization
        conn.execute("SET preserve_insertion_order=false")

        print("Convert left file to duckdb ...")
        conn.execute(
            "CREATE TABLE jobs_left AS SELECT * FROM read_csv_auto(?)", [filename_left]
        )
        print("Converted.")

        print("Convert right file to duckdb ...")
        conn.execute(
            "CREATE TABLE jobs_right AS SELECT * FROM read_csv_auto(?)",
            [filename_right],
        )
        print("Converted.")

        (count_left,) = conn.execute("SELECT COUNT(*) FROM jobs_left").fetchone()
        (count_right,) = conn.execute("SELECT COUNT(*) FROM jobs_right").fetchone()
        print(count_left, "vs", count_right)


if __name__ == "__main__":
    main()
