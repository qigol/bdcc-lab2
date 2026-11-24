"""Self-test for the T08b functions (no pytest needed).

Covers ``labels.self_released_label_ids``, ``labels.parent_coverage`` with
``self_released``, ``trends.prolific_artist_years`` and
``trends.drop_prolific_releases`` on tiny hand-built data only, then calls
the tiny-data checks of ``test_t07.py`` (not its real-data run). Exit code
0 only if every check passes.

Usage, from ``lab2/``:
``DISCOGS_DATA_DIR=$PWD/data python scripts/test_t08b.py``
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--driver-memory 4g pyspark-shell")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_t07  # noqa: E402

from vinyl import config, ingest, networks, trends  # noqa: E402
from vinyl import labels as label_power  # noqa: E402

FAILURES: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    """Record and print one check result."""
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" - {detail}" if detail and not ok else ""))
    if not ok:
        FAILURES.append(name)


def close(a: float, b: float, tol: float = 1e-9) -> bool:
    """Return True if two floats are equal within ``tol``."""
    return a is not None and b is not None and abs(a - b) <= tol


def test_config() -> None:
    """The three new constants have the specified values."""
    print("config")
    check("SELF_RELEASED_LABEL_IDS", config.SELF_RELEASED_LABEL_IDS == (1818,))
    check("SELF_RELEASED_LABEL_PATTERN", config.SELF_RELEASED_LABEL_PATTERN == r"(?i)^not on label\b")
    check("PROLIFIC_ARTIST_MIN_PER_YEAR", config.PROLIFIC_ARTIST_MIN_PER_YEAR == 300)


def test_self_released_label_ids(spark) -> None:
    """Fixed id, name pattern, and the near-miss name that must not match."""
    print("self_released_label_ids")
    labels = spark.createDataFrame(
        [
            (1, "Real Label", None),
            (3550708, "Not On Label (X Self-released)", None),  # pattern
            (50, "NOT ON LABEL", None),  # pattern, any case
            (51, "Not On Labelle", None),  # must not match
            (52, "Song Not On Label", None),  # only matches at the start
            (53, None, None),  # null name
        ],
        "label_id long, label_name string, parent_id long",
    )
    out = label_power.self_released_label_ids(labels)
    check("one column label_id", out.columns == ["label_id"], str(out.columns))
    ids = sorted(r.label_id for r in out.collect())
    check(
        "fixed id (not in table) + pattern matches",
        ids == [50, 1818, 3550708],
        str(ids),
    )
    labels_with_1818 = labels.unionByName(
        spark.createDataFrame([(1818, "Not On Label", 205754)], "label_id long, label_name string, parent_id long")
    )
    ids = sorted(r.label_id for r in label_power.self_released_label_ids(labels_with_1818).collect())
    check("1818 listed once when also in the table and matching", ids == [50, 1818, 3550708], str(ids))


def test_parent_coverage_self_released(spark) -> None:
    """Self-released class, class order, shares, and the default call."""
    print("parent_coverage with self_released")
    labels = spark.createDataFrame(
        [
            (1, "Parent", None),
            (2, "Child", 1),  # in_tree
            (3, "Lone", None),  # on_root_alone
            (1818, "Not On Label", 205754),  # has a (wrong) parent
            (205754, "Stolen Riff", 212),
            (212, "", None),
            (60, "Not On Label (A Self-released)", None),
        ],
        "label_id long, label_name string, parent_id long",
    )
    roots = label_power.resolve_label_roots(labels, max_depth=3)
    release_labels = spark.createDataFrame(
        [
            (1, 2020, 1818),  # 1818 only -> self_released
            (2, 2020, 1818),  # 1818 + tree label -> in_tree
            (2, 2020, 2),
            (3, 2020, 1818),  # 1818 + lone label -> on_root_alone
            (3, 2020, 3),
            (4, 2020, 60),  # name-matched label, no parent -> self_released
            (5, 2020, 555),  # not in labels -> unknown_label
            (6, 2020, 1818),  # 1818 + unknown -> self_released
            (6, 2020, 555),
            (7, 2020, 777),  # in `self_released` but not in labels -> self_released
            (8, 2021, 3),  # on_root_alone
        ],
        "release_id long, year int, label_id long",
    )
    self_ids = spark.createDataFrame([(1818,), (60,), (777,), (1818,)], "label_id long")
    out = label_power.parent_coverage(release_labels, roots, self_released=self_ids).toPandas()
    out = out.set_index("year")
    a, b = out.loc[2020], out.loc[2021]
    check("2020 self_released", a.self_released == 4, str(a.self_released))
    check("2020 in_tree", a.in_tree == 1, str(a.in_tree))
    check("2020 on_root_alone", a.on_root_alone == 1, str(a.on_root_alone))
    check("2020 unknown_label", a.unknown_label == 1, str(a.unknown_label))
    check("2020 n_releases", a.n_releases == 7, str(a.n_releases))
    check("2020 share_self_released", close(a.share_self_released, 4 / 7))
    check("2021 on_root_alone", b.on_root_alone == 1 and b.self_released == 0, str(b.to_dict()))
    share_cols = [c for c in out.columns if c.startswith("share_")]
    check(
        "share columns",
        sorted(share_cols)
        == ["share_in_tree", "share_on_root_alone", "share_self_released", "share_unknown_label"],
        str(share_cols),
    )
    check("shares add up to 1", bool(((out[share_cols].sum(axis=1) - 1).abs() < 1e-9).all()))

    # The label in self_released but whose label_roots row has a parent and children.
    only_1818 = spark.createDataFrame([(1, 2020, 1818)], "release_id long, year int, label_id long")
    one = label_power.parent_coverage(only_1818, roots, self_released=self_ids).toPandas().iloc[0]
    check("1818 alone is self_released", one.self_released == 1 and one.in_tree == 0, str(one.to_dict()))

    # Default call: no self-released class.
    default = label_power.parent_coverage(release_labels, roots).toPandas().set_index("year")
    check("default call: self_released == 0", bool((default.self_released == 0).all()), str(default.self_released.tolist()))
    check("default call: share_self_released == 0", bool((default.share_self_released == 0).all()))
    check(
        "default call: 1818 alone counts as in_tree (has a parent)",
        default.loc[2020].in_tree == 4,
        str(default.loc[2020].to_dict()),
    )
    default_sum = default[[c for c in default.columns if c.startswith("share_")]].sum(axis=1)
    check("default call: shares add up to 1", bool(((default_sum - 1).abs() < 1e-9).all()))


def _release_artists(spark):
    """Main artists: artist 10 has 3 releases in 2019 and 1 in 2020."""
    return spark.createDataFrame(
        [
            (1, 2019, 10, "Noise"),
            (2, 2019, 10, "Noise"),
            (2, 2019, 10, "Noise"),  # duplicate row: still one release
            (3, 2019, 10, "Noise"),
            (3, 2019, 11, "Duo Partner"),  # second main artist on release 3
            (4, 2020, 10, "Noise"),
            (5, 2019, 12, "Normal"),
            (6, 2019, 194, "Various"),
            (7, 2019, 194, "Various"),
            (8, 2019, 194, "Various"),
            (9, 2019, None, "No Id"),
        ],
        "release_id long, year int, artist_id long, artist_name string",
    )


def test_prolific_artist_years(spark) -> None:
    """Threshold, distinct release count and placeholder exclusion."""
    print("prolific_artist_years")
    ra = _release_artists(spark)
    out = trends.prolific_artist_years(ra, min_per_year=3).toPandas()
    check(
        "columns",
        list(out.columns) == ["artist_id", "artist_name", "year", "n_releases"],
        str(list(out.columns)),
    )
    check("only artist 10 in 2019 reaches 3", out[["artist_id", "year"]].values.tolist() == [[10, 2019]], str(out.values.tolist()))
    check("distinct releases (duplicate row counted once)", out.iloc[0].n_releases == 3, str(out.n_releases.tolist()))
    check("artist 10 in 2020 (1 release) is below threshold", not ((out.artist_id == 10) & (out.year == 2020)).any())
    check("placeholder 194 (3 releases) excluded by default", 194 not in set(out.artist_id))
    kept = trends.prolific_artist_years(ra, min_per_year=3, exclude_ids=()).toPandas()
    check("placeholder kept when exclude_ids is empty", 194 in set(kept.artist_id), str(kept.artist_id.tolist()))
    two = trends.prolific_artist_years(ra, min_per_year=1).toPandas()
    check("threshold is inclusive and null ids are dropped", set(two.artist_id) == {10, 11, 12}, str(sorted(set(two.artist_id))))
    check("sorted by n_releases descending", two.n_releases.is_monotonic_decreasing, str(two.n_releases.tolist()))
    default = trends.prolific_artist_years(ra).toPandas()
    check("default threshold is config.PROLIFIC_ARTIST_MIN_PER_YEAR", len(default) == 0, str(default.values.tolist()))
    check("placeholder constant is the one in networks", networks.PLACEHOLDER_ARTIST_IDS == (194, 355))


def test_drop_prolific_releases(spark) -> None:
    """Only the releases of the matching (artist, year) are removed."""
    print("drop_prolific_releases")
    ra = _release_artists(spark)
    tags = spark.createDataFrame(
        [
            (1, 2019, "Harsh Noise Wall"),
            (1, 2019, "Noise"),  # both tags of a dropped release go
            (2, 2019, "Harsh Noise Wall"),
            (3, 2019, "Harsh Noise Wall"),  # co-credited with artist 11: still dropped
            (4, 2020, "Harsh Noise Wall"),  # same artist, other year: kept
            (5, 2019, "Techno"),
            (6, 2019, "Pop"),
            (3, 2020, "Techno"),  # same release_id, different year: kept
        ],
        "release_id long, year int, style string",
    )
    prolific = trends.prolific_artist_years(ra, min_per_year=3)
    out = trends.drop_prolific_releases(tags, ra, prolific)
    check("columns unchanged", out.columns == tags.columns, str(out.columns))
    kept = sorted((r.release_id, r.year, r.style) for r in out.collect())
    expected = [(3, 2020, "Techno"), (4, 2020, "Harsh Noise Wall"), (5, 2019, "Techno"), (6, 2019, "Pop")]
    check("only releases 1-3 of 2019 removed", kept == expected, str(kept))
    empty = spark.createDataFrame([], "artist_id long, artist_name string, year int, n_releases long")
    same = trends.drop_prolific_releases(tags, ra, empty)
    check("empty prolific table removes nothing", same.count() == tags.count())


def test_t07_tiny(spark) -> None:
    """Run the tiny-data checks of test_t07.py; skip its real-data run."""
    print("test_t07 tiny-data checks (test_real_data skipped)")
    test_t07.FAILURES.clear()
    test_t07.test_group_concentration(spark)
    test_t07.test_parent_coverage(spark)
    test_t07.test_credit_level_coverage(spark)
    for name in test_t07.FAILURES:
        FAILURES.append(f"test_t07: {name}")


def main() -> int:
    """Run all checks and return the process exit code."""
    spark = ingest.get_spark(app_name="vinyl-test-t08b", shuffle_partitions=8)
    spark.sparkContext.setLogLevel("ERROR")
    test_config()
    test_self_released_label_ids(spark)
    test_parent_coverage_self_released(spark)
    test_prolific_artist_years(spark)
    test_drop_prolific_releases(spark)
    test_t07_tiny(spark)
    if FAILURES:
        print(f"\nFAILED: {len(FAILURES)} check(s): {FAILURES}")
        return 1
    print("\nAll checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
