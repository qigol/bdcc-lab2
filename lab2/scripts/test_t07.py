"""Self-test for the T07 functions in ``vinyl/labels.py`` (no pytest needed).

Covers ``group_concentration``, ``parent_coverage`` and
``credit_level_coverage`` on tiny hand-built data, then runs them on the
real 2026 tables limited to 2016 and 2025. Exit code 0 only if every
check passes.

Usage, from ``lab2/``:
``DISCOGS_DATA_DIR=$PWD/data python scripts/test_t07.py``
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--driver-memory 4g pyspark-shell")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F  # noqa: E402

from vinyl import config, ingest, networks  # noqa: E402
from vinyl import labels as label_power  # noqa: E402

FAILURES: list[str] = []
SMOKE_YEARS = [2016, 2025]


def check(name: str, ok: bool, detail: str = "") -> None:
    """Record and print one check result."""
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" - {detail}" if detail and not ok else ""))
    if not ok:
        FAILURES.append(name)


def close(a: float, b: float, tol: float = 1e-9) -> bool:
    """Return True if two floats are equal within ``tol``."""
    return a is not None and b is not None and abs(a - b) <= tol


def test_group_concentration(spark) -> None:
    """Two majors; release 1 sits on two Universal labels."""
    print("group_concentration")
    tagged = spark.createDataFrame(
        [(1, "Universal"), (2, "Universal"), (3, "Sony"), (4, "Independent")],
        "label_id long, major string",
    )
    release_labels = spark.createDataFrame(
        [
            (1, 2020, 1),
            (1, 2020, 2),  # same release, second Universal label: counts once
            (2, 2020, 1),
            (3, 2021, 2),
            (4, 2020, 3),
            (5, 2020, 3),
            (6, 2020, 99),  # label not in tagged_labels: dropped
        ],
        "release_id long, year int, label_id long",
    )
    release_artists = spark.createDataFrame(
        [
            (1, 10, "A"),
            (1, 10, "A2"),  # duplicate (release, artist) with a name variant
            (1, 11, "B"),
            (2, 10, "A"),
            (3, 11, "B"),
            (4, 12, "C"),
            (5, 12, "C"),
            (5, 13, "D"),
            (6, 10, "A"),
        ],
        "release_id long, artist_id long, artist_name string",
    )

    # Universal: pairs r1-A, r1-B, r2-A, r3-B -> A 2, B 2. HHI 0.5.
    # Sony: r4-C, r5-C, r5-D -> C 2, D 1. HHI 4/9 + 1/9 = 5/9.
    out = (
        label_power.group_concentration(release_labels, release_artists, tagged, min_releases=1)
        .toPandas()
        .set_index("major")
    )
    check("groups are Universal and Sony", sorted(out.index) == ["Sony", "Universal"], str(list(out.index)))
    u, s = out.loc["Universal"], out.loc["Sony"]
    check("Universal n_pairs counts the double-label release once", u.n_pairs == 4, str(u.n_pairs))
    check("Universal n_artists", u.n_artists == 2, str(u.n_artists))
    check("Universal hhi", close(u.hhi, 0.5), str(u.hhi))
    check("Universal top artist (tie goes to lowest id)", u.top_artist_id == 10 and u.top_artist_name == "A")
    check("Universal top_artist_share", close(u.top_artist_share, 0.5))
    check("Sony n_pairs", s.n_pairs == 3, str(s.n_pairs))
    check("Sony hhi", close(s.hhi, 5 / 9), str(s.hhi))
    check("Sony top artist", s.top_artist_id == 12 and close(s.top_artist_share, 2 / 3))

    filtered = label_power.group_concentration(
        release_labels, release_artists, tagged, min_releases=4
    ).toPandas()
    check("min_releases drops Sony", list(filtered.major) == ["Universal"], str(list(filtered.major)))

    by_year = (
        label_power.group_concentration(
            release_labels, release_artists, tagged, min_releases=1, by_year=True
        )
        .toPandas()
        .set_index(["major", "year"])
    )
    check(
        "by_year has one row per (major, year)",
        sorted(by_year.index) == [("Sony", 2020), ("Universal", 2020), ("Universal", 2021)],
        str(sorted(by_year.index)),
    )
    u20, u21 = by_year.loc[("Universal", 2020)], by_year.loc[("Universal", 2021)]
    # 2020: r1-A, r1-B, r2-A -> A 2, B 1. HHI 5/9. 2021: r3-B only, HHI 1.
    check("Universal 2020 n_pairs", u20.n_pairs == 3, str(u20.n_pairs))
    check("Universal 2020 hhi", close(u20.hhi, 5 / 9), str(u20.hhi))
    check("Universal 2020 top share", close(u20.top_artist_share, 2 / 3))
    check("Universal 2021 hhi", close(u21.hhi, 1.0) and u21.n_artists == 1)
    check("Universal 2021 top artist", u21.top_artist_id == 11)
    check("Sony 2020 matches overall Sony", close(by_year.loc[("Sony", 2020)].hhi, 5 / 9))


def test_parent_coverage(spark) -> None:
    """One release for each class, plus mixed-class releases."""
    print("parent_coverage")
    labels = spark.createDataFrame(
        [
            (1, "Parent", None),  # root with a child (label 2)
            (2, "Child", 1),  # has a parent
            (3, "Lone", None),  # no parent, no children
            (4, "UnderPlaceholder", 212),  # placeholder parent only
            (7, "Orphan", 99),  # parent id missing from the table: still has a parent
            (212, "", None),  # placeholder: only child is label 4, so it stays alone
        ],
        "label_id long, label_name string, parent_id long",
    )
    roots = label_power.resolve_label_roots(labels, max_depth=3)
    release_labels = spark.createDataFrame(
        [
            (1, 2020, 1),  # label with a child -> in_tree
            (2, 2020, 2),  # label with a parent -> in_tree
            (3, 2020, 3),  # lone -> on_root_alone
            (4, 2020, 4),  # placeholder parent -> on_root_alone
            (5, 2020, 555),  # not in labels table -> unknown_label
            (6, 2020, 3),  # lone + tree label -> in_tree
            (6, 2020, 2),
            (7, 2021, 3),  # lone + unknown -> on_root_alone
            (7, 2021, 555),
            (8, 2021, 555),  # unknown
            (9, 2021, 7),  # parent missing from table -> in_tree
            (10, 2021, 212),  # placeholder label itself -> on_root_alone
            (11, 2021, None),  # no label id -> unknown_label
        ],
        "release_id long, year int, label_id long",
    )
    out = label_power.parent_coverage(release_labels, roots).toPandas().set_index("year")
    check("one row per year", list(out.index) == [2020, 2021], str(list(out.index)))
    a, b = out.loc[2020], out.loc[2021]
    check("2020 in_tree", a.in_tree == 3, str(a.in_tree))
    check("2020 on_root_alone", a.on_root_alone == 2, str(a.on_root_alone))
    check("2020 unknown_label", a.unknown_label == 1, str(a.unknown_label))
    check("2020 n_releases", a.n_releases == 6, str(a.n_releases))
    check("2020 share_in_tree", close(a.share_in_tree, 0.5))
    check("2021 in_tree", b.in_tree == 1, str(b.in_tree))
    check("2021 on_root_alone", b.on_root_alone == 2, str(b.on_root_alone))
    check("2021 unknown_label", b.unknown_label == 2, str(b.unknown_label))
    check("2021 n_releases", b.n_releases == 5, str(b.n_releases))
    check("2021 share_unknown_label", close(b.share_unknown_label, 2 / 5), str(b.share_unknown_label))
    shares = out[["share_on_root_alone", "share_in_tree", "share_unknown_label"]].sum(axis=1)
    check("shares add up to 1", bool(((shares - 1).abs() < 1e-9).all()), str(shares.tolist()))


def test_credit_level_coverage(spark) -> None:
    """Writer credits at release level, track level and both."""
    print("credit_level_coverage")
    credits = spark.createDataFrame(
        [
            (1, 2020, "Written-By", "release"),
            (1, 2020, "Written-By", "track"),  # r1 at both levels
            (2, 2020, "Lyrics By", "track"),
            (3, 2020, "Producer", "release"),  # not a writer role
            (4, 2021, "Composed By", "release"),
        ],
        "release_id long, year int, role string, level string",
    )
    out = (
        label_power.credit_level_coverage(credits, config.WRITING_ROLES)
        .toPandas()
        .set_index(["year", "level"])
        .n_releases
    )
    check("2020 release level", out[(2020, "release")] == 1, str(out.to_dict()))
    check("2020 track level", out[(2020, "track")] == 2, str(out.to_dict()))
    check("2020 any level counts r1 once", out[(2020, "any")] == 2, str(out.to_dict()))
    check("2021 any level", out[(2021, "any")] == 1, str(out.to_dict()))
    check("producer-only release is excluded", len(out) == 5, str(out.to_dict()))


def test_real_data(spark) -> None:
    """Run the new functions on the 2026 tables for 2016 and 2025 only."""
    print("real data smoke run (2016, 2025)")
    snap = config.MAIN_SNAPSHOT
    labels = ingest.read_table(spark, snap, "labels")
    in_years = F.col("year").isin(SMOKE_YEARS)
    release_labels = ingest.read_table(spark, snap, "release_labels").where(in_years).cache()
    release_artists = (
        ingest.read_table(spark, snap, "release_artists")
        .where(in_years)
        .where(~F.col("artist_id").isin(list(networks.PLACEHOLDER_ARTIST_IDS)))
        .cache()
    )
    credits = ingest.read_table(spark, snap, "release_credits").where(in_years)
    n_rl = release_labels.count()
    print(f"  release_labels rows in {SMOKE_YEARS}: {n_rl:,}")
    check("release_labels not empty", n_rl > 0)

    tagged = label_power.tag_major_labels(label_power.resolve_label_roots(labels)).cache()
    counts = {r["major"]: r["n"] for r in tagged.groupBy("major").agg(F.count("*").alias("n")).collect()}
    print(f"  tagged labels: {counts}")
    check("all three majors have labels", all(counts.get(m, 0) > 0 for m in config.MAJOR_ROOT_IDS), str(counts))

    conc = label_power.group_concentration(release_labels, release_artists, tagged).toPandas()
    print(conc.to_string(index=False))
    check("group_concentration returns rows", len(conc) > 0)
    check("hhi in (0, 1]", bool(((conc.hhi > 0) & (conc.hhi <= 1 + 1e-9)).all()))
    check("top_artist_share in (0, 1]", bool(((conc.top_artist_share > 0) & (conc.top_artist_share <= 1 + 1e-9)).all()))
    check("hhi at least 1 / n_artists", bool((conc.hhi >= 1 / conc.n_artists - 1e-9).all()))
    check("n_pairs at least min_releases", bool((conc.n_pairs >= 50).all()))

    by_year = label_power.group_concentration(
        release_labels, release_artists, tagged, by_year=True
    ).toPandas()
    print(by_year.sort_values(["major", "year"]).to_string(index=False))
    check("by_year only has the requested years", set(by_year.year) <= set(SMOKE_YEARS), str(set(by_year.year)))
    check("by_year hhi in (0, 1]", bool(((by_year.hhi > 0) & (by_year.hhi <= 1 + 1e-9)).all()))
    check("by_year has a Universal row", "Universal" in set(by_year.major))

    cov = label_power.parent_coverage(release_labels, tagged).toPandas().set_index("year")
    print(cov.to_string())
    expected = {
        r["year"]: r["n"]
        for r in release_labels.groupBy("year").agg(F.countDistinct("release_id").alias("n")).collect()
    }
    check("one row per smoke year", sorted(cov.index) == SMOKE_YEARS, str(list(cov.index)))
    check(
        "n_releases equals distinct releases per year",
        {y: int(cov.loc[y, "n_releases"]) for y in cov.index} == expected,
        f"{dict(cov.n_releases)} vs {expected}",
    )
    check(
        "class counts add up to n_releases",
        bool((cov.on_root_alone + cov.in_tree + cov.unknown_label == cov.n_releases).all()),
    )

    lvl = label_power.credit_level_coverage(credits, config.WRITING_ROLES).toPandas()
    print(lvl.to_string(index=False))
    wide = lvl.pivot(index="year", columns="level", values="n_releases").fillna(0)
    check("any-level count is at least each level", bool((wide["any"] >= wide.drop(columns="any").max(axis=1)).all()))


def main() -> int:
    """Run all checks and return the process exit code."""
    spark = ingest.get_spark(app_name="vinyl-test-t07", shuffle_partitions=8)
    spark.sparkContext.setLogLevel("ERROR")
    test_group_concentration(spark)
    test_parent_coverage(spark)
    test_credit_level_coverage(spark)
    test_real_data(spark)
    if FAILURES:
        print(f"\nFAILED: {len(FAILURES)} check(s): {FAILURES}")
        return 1
    print("\nAll checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
