"""Self-test for the T04 changes (no pytest needed).

Covers the label root walk with placeholder parents, track-level credits,
the partial-year undercount and a sample run on staged releases. Exit
code 0 only if every check passes.

Usage, from ``lab2/``: ``python scripts/test_t04.py``

The sample run reads at most 20,000 staged releases (``limit`` before any
explode). It uses a T02 sample stage under ``/tmp/$USER/discogs_sample*``
if there is one, else the real ``releases_staged`` of the main snapshot,
else it is skipped.
"""

from __future__ import annotations

import glob
import os
import sys
from pathlib import Path

os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--driver-memory 4g pyspark-shell")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from vinyl import config, ingest, labels, snapshots, tables  # noqa: E402

FAILURES: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    """Record and print one check result."""
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" - {detail}" if detail and not ok else ""))
    if not ok:
        FAILURES.append(name)


def test_label_roots(spark) -> None:
    """Root walk with a placeholder parent, sub-label chain and missing parent."""
    print("label roots")
    rows = [
        (212, None, None),  # unnamed placeholder parent
        (1, "Group A", 212),
        (2, "Group B", 212),
        (3, "Sub A", 1),
        (4, "Sub-sub A", 3),
        (5, "Sub B", 2),
        (6, "Orphan", 999),  # parent id not in the file
        (7, "Small label", 212),
        (8, "Top", None),
    ]
    df = spark.createDataFrame(rows, "label_id long, label_name string, parent_id long")
    roots = {r["label_id"]: r for r in labels.resolve_label_roots(df).collect()}
    check("columns kept", set(next(iter(roots.values())).asDict()) >= {
        "label_id", "label_name", "parent_id", "root_id", "root_name", "depth", "reached_root"})
    check("groups under 212 have different roots", roots[1]["root_id"] == 1 and roots[2]["root_id"] == 2)
    check("sub-label takes its group's root", roots[3]["root_id"] == 1 and roots[5]["root_id"] == 2)
    check("two-hop chain", roots[4]["root_id"] == 1 and roots[4]["depth"] == 2)
    check("placeholder is its own root", roots[212]["root_id"] == 212 and roots[212]["reached_root"])
    check("label under 212 is its own root", roots[7]["root_id"] == 7 and roots[7]["root_name"] == "Small label")
    check("original parent_id kept", roots[1]["parent_id"] == 212)
    check("missing parent: reached_root False", roots[6]["reached_root"] is False)
    check("others reached root", all(roots[i]["reached_root"] for i in (1, 2, 3, 4, 5, 7, 8, 212)))

    no_stop = {
        r["label_id"]: r["root_id"] for r in labels.resolve_label_roots(df, stop_ids=()).collect()
    }
    check("stop_ids=() walks into 212", no_stop[1] == 212 and no_stop[2] == 212)

    tagged = {
        r["label_id"]: r["major"]
        for r in labels.tag_major_labels(
            labels.resolve_label_roots(df), {"Universal": 1, "Warner": 2}
        ).collect()
    }
    check("tag by id", tagged[1] == "Universal" and tagged[4] == "Universal" and tagged[5] == "Warner")
    check("others independent", all(tagged[i] == "Independent" for i in (6, 7, 8, 212)))


CREDIT_SCHEMA = (
    "release_id long, year int, "
    "extraartists struct<artist: array<struct<id: long, name: string, role: string>>>, "
    "tracklist struct<track: array<struct<extraartists: "
    "struct<artist: array<struct<id: long, name: string, role: string>>>>>>"
)


def _artists(*people):
    return {"artist": [{"id": i, "name": n, "role": r} for i, n, r in people]}


def test_credits(spark) -> None:
    """Release- and track-level credits, duplicates and null handling."""
    print("credits")
    rows = [
        (
            1, 2020,
            _artists((10, "Ann", "Producer, Mixed By [Mix]"), (11, "Bob", "Written-By")),
            {"track": [
                {"extraartists": _artists((10, "Ann", "Producer"), (12, "Cy", "Lacquer Cut By"))},
                {"extraartists": None},
                {"extraartists": _artists((12, "Cy", "Lacquer Cut By [A]"), (13, "Di", "Written-By"))},
            ]},
        ),
        (2, 2021, _artists((20, "Eve", "Engineer")), None),  # null tracklist
        (3, 2022, None, {"track": None}),  # nothing at all
        (4, 2023, None, {"track": [{"extraartists": _artists((30, "Flo", "Producer"))}]}),
    ]
    staged = spark.createDataFrame(rows, CREDIT_SCHEMA)
    out = tables.build_release_credits(staged)
    check("columns", out.columns == [
        "release_id", "year", "artist_id", "artist_name", "role", "level"], str(out.columns))
    got = {(r["release_id"], r["artist_id"], r["role"]): r["level"] for r in out.collect()}
    expected = {
        (1, 10, "Producer"): "release",  # at both levels -> release
        (1, 10, "Mixed By"): "release",
        (1, 11, "Written-By"): "release",
        (1, 12, "Lacquer Cut By"): "track",  # on two tracks, once
        (1, 13, "Written-By"): "track",
        (2, 20, "Engineer"): "release",
        (4, 30, "Producer"): "track",
    }
    check("row count", out.count() == 7, f"got {out.count()}")
    check("rows and levels", got == expected, f"got {got}")
    years = {r["release_id"]: r["year"] for r in out.select("release_id", "year").distinct().collect()}
    check("year kept", years == {1: 2020, 2: 2021, 4: 2023}, str(years))

    no_tracklist = tables.build_release_credits(staged.drop("tracklist"))
    check("no tracklist column: release level only",
          no_tracklist.count() == 4 and no_tracklist.where("level != 'release'").count() == 0)


def test_undercount(spark) -> None:
    """Partial-year scaling in ``estimate_undercount``."""
    print("undercount")
    lag = spark.createDataFrame(
        [(2017, 0.95), (2018, 0.9), (2019, 0.8)], "year int, completeness double"
    )
    counts = spark.createDataFrame(
        [(2020, 100), (2025, 900), (2026, 592)], "year int, n_releases long"
    )
    out = {r["year"]: r for r in snapshots.estimate_undercount(lag, counts).collect()}
    check("partial_year flags", [out[y]["partial_year"] for y in (2020, 2025, 2026)] == [False, False, True])
    check("old year read as complete", abs(out[2020]["n_estimated"] - 100) < 1e-9)
    check("full year: n / completeness", abs(out[2025]["n_estimated"] - 900 / 0.9) < 1e-6)
    want = 592 / 0.739 / 0.8
    check("partial year: n / share / completeness", abs(out[2026]["n_estimated"] - want) < 1e-6,
          f"got {out[2026]['n_estimated']}, want {want}")
    off = {r["year"]: r for r in snapshots.estimate_undercount(lag, counts, partial_year_share={}).collect()}
    check("empty share dict switches it off",
          not off[2026]["partial_year"] and abs(off[2026]["n_estimated"] - 592 / 0.8) < 1e-6)
    check("config defaults", config.LAST_FULL_YEAR == 2025 and config.PARTIAL_YEAR_SHARE == {2026: 0.739})


def test_sample_run(spark) -> None:
    """Run ``build_release_credits`` on at most 20,000 staged releases."""
    print("sample run")
    user = os.environ.get("USER", "")
    candidates = sorted(glob.glob(f"/tmp/{user}/discogs_sample*/parquet/*/releases_staged"))
    candidates.append(config.parquet_path(config.MAIN_SNAPSHOT, "releases_staged"))
    path = next((p for p in candidates if os.path.isdir(p)), None)
    if path is None:
        print("  [SKIP] no staged releases found")
        return
    print(f"  staged: {path}")
    staged = spark.read.parquet(path).limit(20000)
    out = tables.build_release_credits(staged).cache()
    n = out.count()
    levels = {r["level"]: r["count"] for r in out.groupBy("level").count().collect()}
    print(f"  rows: {n}, by level: {levels}")
    check("has release-level rows", levels.get("release", 0) > 0)
    check("has track-level rows", levels.get("track", 0) > 0)
    dup = out.groupBy("release_id", "artist_id", "artist_name", "role").count().where("count > 1").count()
    check("no duplicate keys", dup == 0, f"{dup} duplicated")
    out.unpersist()


def main() -> int:
    """Run all checks and return the exit code."""
    spark = ingest.get_spark("test-t04", shuffle_partitions=8)
    try:
        test_label_roots(spark)
        test_credits(spark)
        test_undercount(spark)
        test_sample_run(spark)
    finally:
        spark.stop()
    if FAILURES:
        print(f"\nFAILED: {FAILURES}")
        return 1
    print("\nall checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
