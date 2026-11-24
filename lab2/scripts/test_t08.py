"""Self-test for the T08 additions to ``vinyl.snapshots`` (no pytest needed).

Covers ``major_root_check``, ``major_moves``, ``explain_move`` and
``cataloguing_lag_by`` on tiny hand-built DataFrames, then runs them on
the real Parquet tables: both full ``labels`` tables, and the 2026
``release_formats`` limited to the years 2016 and 2017. Exit code 0 only
if every check passes.

Usage, from ``lab2/``:
``DISCOGS_DATA_DIR=$PWD/data timeout 900 python scripts/test_t08.py``
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--driver-memory 4g pyspark-shell")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F  # noqa: E402

from vinyl import config, ingest, snapshots  # noqa: E402

FAILURES: list[str] = []
LABEL_SCHEMA = "label_id long, label_name string, parent_id long, parent_name string"


def check(name: str, ok: bool, detail: str = "") -> None:
    """Record and print one check result."""
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" - {detail}" if detail and not ok else ""))
    if not ok:
        FAILURES.append(name)


def close(a, b, tol: float = 1e-9) -> bool:
    """True when two numbers are equal within ``tol``."""
    return a is not None and abs(a - b) <= tol


_T0 = [time.time()]


def lap(label: str) -> None:
    """Print the seconds since the previous call, labelled."""
    now = time.time()
    print(f"  [{now - _T0[0]:6.1f} s] {label}", flush=True)
    _T0[0] = now


def _tiny_labels(spark):
    """Hand-built labels tables. Majors: Universal 38404, Sony 353657, Warner 2345."""
    root = [
        (212, None, None, None),
        (38404, "Universal Music Group", 212, None),
        (2345, "Warner Music Group", 212, None),
        (353657, "Sony Corporation", None, None),
        (15, "Indie A", None, None),
        (16, "Indie B", None, None),
    ]
    old = root + [
        (10, "Alpha", None, None),             # independent -> Warner
        (11, "Bravo", 353657, "Sony Corporation"),   # Sony -> independent
        (12, "Charlie", 353657, "Sony Corporation"),  # Sony -> Universal
        (13, "Delta", 2345, "Warner Music Group"),    # Warner both: unchanged
        (14, "Echo", 15, "Indie A"),            # independent both, parent changes
        (21, "OnlyOld", 2345, "Warner Music Group"),  # only in old
        (22, "Golf", 10, "Alpha"),              # grandchild: root 10 -> root 2345
        (30, "Loop A", 31, "Loop B"),           # cycle in the new table only
        (31, "Loop B", None, None),
    ]
    new = root + [
        (10, "Alpha", 2345, "Warner Music Group"),
        (11, "Bravo Renamed", None, None),
        (12, "Charlie", 38404, "Universal Music Group"),
        (13, "Delta", 2345, "Warner Music Group"),
        (14, "Echo", 16, "Indie B"),
        (20, "OnlyNew", 2345, "Warner Music Group"),  # only in new
        (22, "Golf", 10, "Alpha"),
        (30, "Loop A", 31, "Loop B"),
        (31, "Loop B", 30, "Loop A"),
        (40, "Orphan", 999, "Gone"),            # parent id not in the table
    ]
    return spark.createDataFrame(old, LABEL_SCHEMA), spark.createDataFrame(new, LABEL_SCHEMA)


def test_major_root_check(spark) -> None:
    """Missing root and changed parent are flagged; matching roots are ok."""
    print("major_root_check")
    old = spark.createDataFrame(
        [(38404, "Universal Music Group", 212, None), (2345, "Warner Music Group", 7, None)],
        LABEL_SCHEMA,
    )
    new = spark.createDataFrame(
        [
            (38404, "Universal Music Group", 212, None),
            (2345, "Warner Music Group", 212, None),
            (353657, "Sony Corporation", None, None),
        ],
        LABEL_SCHEMA,
    )
    out = snapshots.major_root_check(old, new).set_index("major")
    check("same parent -> ok", bool(out.loc["Universal", "ok"]))
    check("different parent -> not ok", not out.loc["Warner", "ok"]
          and out.loc["Warner", "parent_id_old"] == 7 and out.loc["Warner", "parent_id_new"] == 212)
    check("missing in old -> not ok", not out.loc["Sony", "ok"] and not out.loc["Sony", "in_old"]
          and out.loc["Sony", "in_new"])
    both_none = snapshots.major_root_check(new, new).set_index("major")
    check("no parent in both -> ok", bool(both_none.loc["Sony", "ok"]))
    custom = snapshots.major_root_check(old, new, {"X": 38404})
    check("custom ids", list(custom.major) == ["X"] and bool(custom.ok.iloc[0]))


def test_major_moves(spark) -> None:
    """Into, out of, between; unchanged and one-sided labels excluded."""
    print("major_moves")
    old, new = _tiny_labels(spark)
    rl = spark.createDataFrame(
        [
            (100, 10), (101, 10), (101, 10),  # label 10: two distinct releases
            (102, 11),
            (103, 22), (104, 22), (105, 22),
            (106, 13), (107, 20),             # excluded labels
        ],
        "release_id long, label_id long",
    )
    plain = snapshots.major_moves(old, new)
    check("no n_releases without release_labels", "n_releases" not in plain.columns)
    check("columns", plain.columns == [
        "label_id", "label_name", "major_old", "major_new", "root_id_old", "root_id_new",
        "parent_id_old", "parent_id_new", "move"], str(plain.columns))
    out = snapshots.major_moves(old, new, rl)
    rows = out.collect()
    by_id = {r.label_id: r for r in rows}
    check("moved labels only", set(by_id) == {10, 11, 12, 22}, str(sorted(by_id)))
    check("unchanged 13 excluded", 13 not in by_id)
    check("independent-to-independent 14 excluded", 14 not in by_id)
    check("only-new 20 excluded", 20 not in by_id)
    check("only-old 21 excluded", 21 not in by_id)
    a, b, c, g = by_id[10], by_id[11], by_id[12], by_id[22]
    check("into Warner", a.move == "into_major" and a.major_old == "Independent"
          and a.major_new == "Warner" and a.root_id_old == 10 and a.root_id_new == 2345
          and a.parent_id_old is None and a.parent_id_new == 2345, str(a))
    check("out of Sony", b.move == "out_of_major" and b.major_old == "Sony"
          and b.major_new == "Independent" and b.root_id_old == 353657 and b.root_id_new == 11
          and b.parent_id_old == 353657 and b.parent_id_new is None, str(b))
    check("name taken from new snapshot", b.label_name == "Bravo Renamed")
    check("between majors", c.move == "between_majors" and c.major_old == "Sony"
          and c.major_new == "Universal" and c.root_id_new == 38404, str(c))
    check("move through a grandparent", g.move == "into_major" and g.root_id_new == 2345
          and g.parent_id_new == 10, str(g))
    check("n_releases (distinct)", (a.n_releases, b.n_releases, c.n_releases, g.n_releases)
          == (2, 1, 0, 3), str((a.n_releases, b.n_releases, c.n_releases, g.n_releases)))
    check("ordered by n_releases desc", [r.label_id for r in rows] == [22, 10, 11, 12],
          str([r.label_id for r in rows]))
    counts = {(r.major_old, r.major_new): r["count"] for r in
              out.groupBy("major_old", "major_new").count().collect()}
    check("counts by (old, new)", counts == {("Independent", "Warner"): 2, ("Sony", "Independent"): 1,
                                             ("Sony", "Universal"): 1}, str(counts))


def test_explain_move(spark) -> None:
    """Parent chains in both snapshots, and each stop reason."""
    print("explain_move")
    old, new = _tiny_labels(spark)
    out = snapshots.explain_move(22, old, new)
    check("columns", list(out.columns) == ["snapshot", "hop", "label_id", "label_name",
                                           "parent_id", "parent_name", "note"])
    o, n = out[out.snapshot == "old"], out[out.snapshot == "new"]
    check("old chain 22 -> 10", list(o.label_id) == [22, 10] and list(o.hop) == [0, 1]
          and o.note.iloc[-1] == "root", str(o))
    check("new chain 22 -> 10 -> 2345, placeholder not followed",
          list(n.label_id) == [22, 10, 2345] and n.iloc[-1].parent_id == 212
          and n.note.iloc[-1] == "placeholder parent, not followed", str(n))
    check("middle rows have empty note", list(n.note.iloc[:2]) == ["", ""])
    check("parent names carried", n.iloc[1].parent_name == "Warner Music Group")

    gone = snapshots.explain_move(20, old, new)
    g_old = gone[gone.snapshot == "old"]
    check("label missing in old", len(g_old) == 1 and g_old.note.iloc[0] == "label not in this snapshot"
          and g_old.label_name.iloc[0] is None)
    check("label present in new", list(gone[gone.snapshot == "new"].label_id) == [20, 2345])

    loop = snapshots.explain_move(30, old, new)
    l_new = loop[loop.snapshot == "new"]
    check("cycle stops", list(l_new.label_id) == [30, 31] and l_new.note.iloc[-1] == "cycle", str(l_new))
    miss = snapshots.explain_move(40, old, new)
    m_new = miss[miss.snapshot == "new"]
    check("parent missing from table", list(m_new.label_id) == [40]
          and m_new.note.iloc[-1] == "parent missing from labels table", str(m_new))
    short = snapshots.explain_move(22, old, new, max_depth=1)
    s_new = short[short.snapshot == "new"]
    check("max_depth", list(s_new.label_id) == [22, 10] and s_new.note.iloc[-1] == "stopped at max_depth",
          str(s_new))


def test_lag_by(spark) -> None:
    """Two formats, a release in both counted in each, completeness by hand."""
    print("cataloguing_lag_by")
    rel_old = spark.createDataFrame([(1, 2018), (2, 2018), (3, 2019)], "release_id long, year int")
    rel_new = spark.createDataFrame(
        [(1, 2018), (2, 2018), (4, 2018), (5, 2018), (3, 2019), (6, 2019), (7, 2019)],
        "release_id long, year int",
    )
    groups = spark.createDataFrame(
        [
            (1, 1999, "Vinyl"), (1, 1999, "CD"),      # in both groups
            (2, 2018, "Vinyl"),
            (4, 2018, "CD"),
            (5, 2018, "Vinyl"), (5, 2018, "Vinyl"),    # duplicate row: counted once
            (3, 2019, "Vinyl"),
            (6, 2019, "CD"),
            (7, 2019, None),                            # null group: dropped
            (99, 2019, "Vinyl"),                        # not in releases_new: dropped
        ],
        "release_id long, year int, format_name string",
    )
    out = snapshots.cataloguing_lag_by(rel_old, rel_new, groups, "format_name")
    check("columns", out.columns == ["year", "format_name", "n_new", "n_already",
                                     "n_added_later", "completeness"], str(out.columns))
    rows = {(r.year, r.format_name): r for r in out.collect()}
    check("four groups", set(rows) == {(2018, "Vinyl"), (2018, "CD"), (2019, "Vinyl"), (2019, "CD")},
          str(sorted(rows)))
    v18, c18, v19, c19 = rows[(2018, "Vinyl")], rows[(2018, "CD")], rows[(2019, "Vinyl")], rows[(2019, "CD")]
    check("2018 Vinyl: releases 1, 2, 5; 1 and 2 already", (v18.n_new, v18.n_already) == (3, 2)
          and close(v18.completeness, 2 / 3), str(v18))
    check("2018 CD: releases 1, 4; 1 already", (c18.n_new, c18.n_already, c18.n_added_later) == (2, 1, 1)
          and close(c18.completeness, 0.5), str(c18))
    check("2019 Vinyl: release 3 already", (v19.n_new, v19.n_already) == (1, 1)
          and close(v19.completeness, 1.0), str(v19))
    check("2019 CD: release 6 new", (c19.n_new, c19.n_already) == (1, 0) and close(c19.completeness, 0.0))
    check("ordered by year then group", [(r.year, r.format_name) for r in out.collect()]
          == [(2018, "CD"), (2018, "Vinyl"), (2019, "CD"), (2019, "Vinyl")])
    # Release 1 has two formats, so the group counts add up to more than the releases.
    total = sum(r.n_new for r in rows.values() if r.year == 2018)
    check("multi-format release counted in each group", total == 5, str(total))
    # Same function with another group column name.
    genres = groups.withColumnRenamed("format_name", "genre")
    by_genre = snapshots.cataloguing_lag_by(rel_old, rel_new, genres, "genre")
    check("other group_col", "genre" in by_genre.columns and by_genre.count() == 4)


def test_real_data(spark) -> None:
    """Both full labels tables, and release_formats of 2016-2017 only."""
    print("real-data smoke run")
    old_key, new_key = config.BASE_SNAPSHOT, config.MAIN_SNAPSHOT
    years = [2016, 2017]
    _T0[0] = time.time()
    # Cache the labels tables: resolve_label_roots joins the table with itself 12 times,
    # and without a cache every join re-reads the Parquet files.
    cols = ["label_id", "label_name", "parent_id", "parent_name"]
    labels_old = ingest.read_table(spark, old_key, "labels").select(*cols).cache()
    labels_new = ingest.read_table(spark, new_key, "labels").select(*cols).cache()
    rl = (ingest.read_table(spark, new_key, "release_labels")
          .where(F.col("year").isin(years)).select("release_id", "label_id").cache())
    rel_new = (ingest.read_table(spark, new_key, "releases")
               .where(F.col("year").isin(years)).select("release_id", "year").cache())
    rel_old = (ingest.read_table(spark, old_key, "releases")
               .where(F.col("year").isin(years)).select("release_id").cache())
    rf = (ingest.read_table(spark, new_key, "release_formats")
          .where(F.col("year").isin(years)).select("release_id", "format_name").cache())
    print(f"  rows: release_labels={rl.count():,} releases_new={rel_new.count():,} "
          f"releases_old={rel_old.count():,} release_formats={rf.count():,} "
          f"labels={labels_old.count():,}/{labels_new.count():,}")
    lap("load and cache the two-year tables")

    roots = snapshots.major_root_check(labels_old, labels_new)
    print(roots.to_string(index=False))
    check("all three roots exist in both snapshots", bool((roots.in_old & roots.in_new).all()))
    # A root with another parent is reported, not a failure: in 2020 Universal and
    # Warner have no parent where 2026 has the placeholder 212. Both read as "no parent".
    for r in roots[~roots.ok].itertuples():
        print(f"  WARNING {r.major} root {r.root_id}: parent {r.parent_id_old} (old) vs {r.parent_id_new} (new)")
    lap("major_root_check")

    moves = snapshots.major_moves(labels_old, labels_new, rl).cache()
    n = moves.count()
    lap(f"major_moves ({n:,} rows)")
    mv = moves.limit(5).toPandas()
    print(mv[["label_id", "label_name", "major_old", "major_new", "move", "n_releases"]].to_string(index=False))
    check("moves have a different major", moves.where(F.col("major_old") == F.col("major_new")).count() == 0)
    check("move values", {r.move for r in moves.select("move").distinct().collect()}
          <= {"into_major", "out_of_major", "between_majors"})
    counts = [r.n_releases for r in moves.select("n_releases").limit(200).collect()]
    check("ordered by n_releases desc", counts == sorted(counts, reverse=True))
    if n:
        first = int(mv.label_id.iloc[0])
        chain = snapshots.explain_move(first, labels_old, labels_new)
        print(chain.to_string(index=False))
        check("explain_move starts at the label", set(chain.snapshot) <= {"old", "new"}
              and (chain[chain.hop == 0].label_id == first).all())
        lap("explain_move")

    lag = snapshots.cataloguing_lag_by(rel_old, rel_new, rf, "format_name").cache()
    lag_pd = lag.where(F.col("format_name").isin("Vinyl", "CD", "Cassette", "File")).toPandas()
    print(lag_pd.to_string(index=False))
    lap("cataloguing_lag_by (formats, 2016-2017)")
    check("has Vinyl and CD", {"Vinyl", "CD"} <= set(lag_pd.format_name))
    check("completeness within 0..1", bool(lag_pd.completeness.between(0, 1).all()))
    check("n_already <= n_new", bool((lag_pd.n_already <= lag_pd.n_new).all()))
    direct = {(r.year, r.format_name): r.n for r in rf.join(rel_new, "release_id")
              .groupBy("year", "format_name").agg(F.countDistinct("release_id").alias("n")).collect()}
    check("n_new matches a direct count", all(direct[(r.year, r.format_name)] == r.n_new
                                              for r in lag_pd.itertuples()))
    lap("cross-checks")


def main() -> int:
    """Run every test, return the process exit code."""
    t0 = time.time()
    spark = ingest.get_spark(
        app_name="test-t08", shuffle_partitions=8,
        extra_conf={"spark.ui.showConsoleProgress": "false"},
    )
    spark.sparkContext.setLogLevel("ERROR")
    for test in (test_major_root_check, test_major_moves, test_explain_move, test_lag_by,
                 test_real_data):
        test(spark)
    print(f"\n{len(FAILURES)} failure(s) in {time.time() - t0:.0f} s" + (": " + ", ".join(FAILURES) if FAILURES else ""))
    spark.stop()
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
