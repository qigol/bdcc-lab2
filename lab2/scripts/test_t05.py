"""Self-test for the T05 functions in ``vinyl/trends.py`` (no pytest needed).

Covers ``cooccurrence_graph_stats``, ``entropy_decomposition`` and
``entropy_sensitivity`` on tiny hand-built data, then runs all three on
the real 2026 ``release_styles`` table limited to 2016 and 2025. Exit code
0 only if every check passes.

Usage, from ``lab2/``:
``DISCOGS_DATA_DIR=$PWD/data python scripts/test_t05.py``
"""

from __future__ import annotations

import math
import os
import sys
from pathlib import Path

os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--driver-memory 4g pyspark-shell")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from vinyl import config, ingest, trends  # noqa: E402

FAILURES: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    """Record and print one check result."""
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" - {detail}" if detail and not ok else ""))
    if not ok:
        FAILURES.append(name)


def close(a: float, b: float, tol: float = 1e-9) -> bool:
    """Return True if two floats are equal within ``tol``."""
    return a is not None and b is not None and abs(a - b) <= tol


def test_graph_stats(spark) -> None:
    """Two years: 2020 has two components (a-b-c and d-e), 2021 has one."""
    print("cooccurrence_graph_stats")
    rows = [
        (2020, "a", "b", 0.5),
        (2020, "b", "c", 0.4),
        (2020, "a", "c", 0.3),
        (2020, "d", "e", 0.6),
        (2021, "a", "b", 0.5),
        (2021, "b", "c", 0.4),
        (2021, "c", "d", 0.2),
    ]
    cooc = spark.createDataFrame(rows, "year int, style_a string, style_b string, jaccard double")
    out = trends.cooccurrence_graph_stats(cooc).set_index("year")
    check("one row per year", list(out.index) == [2020, 2021], str(list(out.index)))
    r20, r21 = out.loc[2020], out.loc[2021]
    check("2020 n_nodes", r20.n_nodes == 5, str(r20.n_nodes))
    check("2020 n_edges", r20.n_edges == 4, str(r20.n_edges))
    check("2020 n_components", r20.n_components == 2, str(r20.n_components))
    check("2020 largest_component_share", close(r20.largest_component_share, 3 / 5))
    check("2020 density", close(r20.density, 4 / 10))
    check("2021 n_components", r21.n_components == 1, str(r21.n_components))
    check("2021 largest_component_share", close(r21.largest_component_share, 1.0))
    check("modularity in [-0.5, 1]", bool(out.modularity.between(-0.5, 1).all()), str(out.modularity.tolist()))
    check("2020 modularity above 0 (two clusters)", r20.modularity > 0, str(r20.modularity))


def test_decomposition(spark) -> None:
    """Year 1: A, B. Year 2: A, B and a new tag C."""
    print("entropy_decomposition")
    # Year 2016 pairs: A 2, B 2. Year 2017 pairs: A 2, B 1, C 1.
    rows = [
        (1, 2016, "A"),
        (2, 2016, "A"),
        (3, 2016, "B"),
        (4, 2016, "B"),
        (5, 2017, "A"),
        (6, 2017, "A"),
        (7, 2017, "B"),
        (8, 2017, "C"),
    ]
    tags = spark.createDataFrame(rows, "release_id long, year int, style string")
    out = trends.entropy_decomposition(tags, "style", base_year=2016).toPandas().set_index("year")
    check("columns", list(out.columns) == [
        "entropy", "evenness", "entropy_base_tags", "n_new_tags", "new_tag_pair_share"
    ], str(list(out.columns)))
    check("n_new_tags base year is 0", out.loc[2016, "n_new_tags"] == 0)
    check("n_new_tags year 2 is 1", out.loc[2017, "n_new_tags"] == 1)
    h_base = -(2 / 3 * math.log2(2 / 3) + 1 / 3 * math.log2(1 / 3))
    check("entropy_base_tags year 2 (hand-computed)", close(out.loc[2017, "entropy_base_tags"], h_base),
          str(out.loc[2017, "entropy_base_tags"]))
    check("entropy_base_tags base year equals entropy",
          close(out.loc[2016, "entropy_base_tags"], out.loc[2016, "entropy"]))
    check("entropy year 2 (1.5 bits)", close(out.loc[2017, "entropy"], 1.5))
    check("evenness year 2", close(out.loc[2017, "evenness"], 1.5 / math.log2(3)))
    check("new_tag_pair_share year 2", close(out.loc[2017, "new_tag_pair_share"], 0.25))
    check("new_tag_pair_share base year is 0", close(out.loc[2016, "new_tag_pair_share"], 0.0))
    try:
        trends.entropy_decomposition(tags, "style", base_year=1999)
        check("missing base_year raises ValueError", False, "no error")
    except ValueError:
        check("missing base_year raises ValueError", True)


def test_sensitivity(spark) -> None:
    """Years 2016 (4 releases, 2 untagged) and 2017 (8); the sample size is 4.

    The untagged releases count towards the sample size, so 2016 is the
    smallest year with 4 releases although only 2 carry tags.
    """
    print("entropy_sensitivity")
    styles = ["A", "B", "C", "D"]
    tag_rows, release_rows = [], []
    for i in range(4):  # 2016: ids 201600..201603; only 0 and 1 are tagged
        rid = 201600 + i
        release_rows.append((rid, 2016))
        if i < 2:
            tag_rows.append((rid, 2016, styles[i]))
            tag_rows.append((rid, 2016, styles[i + 2]))
    for i in range(8):  # 2017: all tagged
        rid = 201700 + i
        release_rows.append((rid, 2017))
        tag_rows.append((rid, 2017, styles[i % 4]))
        if i % 2 == 0:
            tag_rows.append((rid, 2017, styles[(i + 1) % 4]))
    tags = spark.createDataFrame(tag_rows, "release_id long, year int, style string")
    releases = spark.createDataFrame(release_rows, "release_id long, year int")
    out = trends.entropy_sensitivity(tags, releases, "style", n_draws=5, seed=42).toPandas()
    check("one row per year", out["year"].tolist() == [2016, 2017], str(out["year"].tolist()))
    check("columns", list(out.columns) == [
        "year", "evenness_full", "evenness_sub_mean", "evenness_sub_min", "evenness_sub_max"
    ], str(list(out.columns)))
    eps = 1e-12
    ordered = (
        (out.evenness_sub_min <= out.evenness_sub_mean + eps)
        & (out.evenness_sub_mean <= out.evenness_sub_max + eps)
    )
    check("min <= mean <= max", bool(ordered.all()), out.to_string())
    r16 = out.set_index("year").loc[2016]
    # 2016 has 4 releases = the sample size (untagged ones included), so it is used whole.
    check("smallest year is used whole (sub == full)",
          close(r16.evenness_sub_min, r16.evenness_full) and close(r16.evenness_sub_max, r16.evenness_full))
    # Population check. 2016 has 3 releases but only 1 is tagged; 2017 has 4
    # releases, each with its own single tag. All releases define the sample size,
    # so m = 3 and every 2017 sample holds 3 distinct tags (evenness 1.0). If
    # untagged releases were dropped, m would be 1, a 2017 sample would hold one
    # tag, and evenness would be null.
    pop_releases = spark.createDataFrame(
        [(1, 2016), (2, 2016), (3, 2016), (11, 2017), (12, 2017), (13, 2017), (14, 2017)],
        "release_id long, year int",
    )
    pop_tags = spark.createDataFrame(
        [(1, 2016, "A"), (11, 2017, "A"), (12, 2017, "B"), (13, 2017, "C"), (14, 2017, "D")],
        "release_id long, year int, style string",
    )
    pop = trends.entropy_sensitivity(pop_tags, pop_releases, "style", n_draws=4, seed=1).toPandas().set_index("year")
    check("sample size comes from all releases, not only tagged ones",
          pop.loc[2017, "evenness_sub_min"] is not None
          and close(pop.loc[2017, "evenness_sub_min"], 1.0)
          and close(pop.loc[2017, "evenness_sub_max"], 1.0), pop.to_string())
    again = trends.entropy_sensitivity(tags, releases, "style", n_draws=5, seed=42).toPandas()
    check("same seed gives same output", out.round(12).equals(again.round(12)))
    try:
        trends.entropy_sensitivity(tags, releases, "style", n_draws=0)
        check("n_draws=0 raises ValueError", False, "no error")
    except ValueError:
        check("n_draws=0 raises ValueError", True)


def test_real_data(spark) -> None:
    """All three functions on the real 2026 release_styles, years 2016 and 2025."""
    print("real data (release_styles 2026 snapshot, years 2016 and 2025)")
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", 20)
    years = [2016, 2025]
    styles = ingest.read_table(spark, config.MAIN_SNAPSHOT, "release_styles").where(F.col("year").isin(years))
    releases = ingest.read_table(spark, config.MAIN_SNAPSHOT, "releases").where(F.col("year").isin(years))

    cooc = trends.style_cooccurrence(styles, min_count=50).cache()
    stats = trends.cooccurrence_graph_stats(cooc)
    print(stats.to_string(index=False))
    check("graph stats: one row per year", stats["year"].tolist() == years, str(stats["year"].tolist()))
    check("graph stats: modularity in [-0.5, 1]", bool(stats.modularity.between(-0.5, 1).all()))
    check("graph stats: shares in (0, 1]", bool(stats.largest_component_share.between(1e-9, 1).all()))
    cooc.unpersist()

    decomp = trends.entropy_decomposition(styles, "style", base_year=2016).toPandas()
    print(decomp.to_string(index=False))
    check("decomposition: one row per year", decomp["year"].tolist() == years)
    check("decomposition: base year has no new tags", decomp.loc[0, "n_new_tags"] == 0)
    check("decomposition: base year entropy_base_tags == entropy",
          close(decomp.loc[0, "entropy_base_tags"], decomp.loc[0, "entropy"]))

    sens = trends.entropy_sensitivity(styles, releases, "style", n_draws=3, seed=42).toPandas()
    print(sens.to_string(index=False))
    check("sensitivity: one row per year", sens["year"].tolist() == years)
    eps = 1e-12
    check("sensitivity: min <= mean <= max", bool(
        ((sens.evenness_sub_min <= sens.evenness_sub_mean + eps)
         & (sens.evenness_sub_mean <= sens.evenness_sub_max + eps)).all()))


def main() -> int:
    """Run all checks and return the process exit code."""
    spark = ingest.get_spark(app_name="vinyl-test-t05", shuffle_partitions=16)
    spark.sparkContext.setLogLevel("ERROR")
    try:
        test_graph_stats(spark)
        test_decomposition(spark)
        test_sensitivity(spark)
        test_real_data(spark)
    finally:
        spark.stop()
    if FAILURES:
        print(f"\nFAILED: {FAILURES}")
        return 1
    print("\nAll checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
