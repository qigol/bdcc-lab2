"""Self-test for the T06 additions to ``vinyl.formats`` (no pytest needed).

Covers each new function on tiny hand-built DataFrames, then runs every
function once on the 2026 tables restricted to the years 2016 and 2025.
Exit code 0 only if every check passes.

Usage, from ``lab2/``:
``DISCOGS_DATA_DIR=$PWD/data python scripts/test_t06.py``
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--driver-memory 4g pyspark-shell")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from vinyl import config, formats, ingest  # noqa: E402

FAILURES: list[str] = []


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


def test_main_formats() -> None:
    """Threshold on full years only, mean-share ordering, ties."""
    print("main_formats")
    rows = [
        (2016, "Vinyl", 0.2), (2017, "Vinyl", 0.3), (2025, "Vinyl", 0.4),
        (2016, "CD", 0.6), (2017, "CD", 0.5), (2025, "CD", 0.3),
        # exactly at the threshold in one year -> kept
        (2016, "Cassette", 0.01), (2017, "Cassette", 0.004),
        # reaches the threshold only in the partial year -> dropped
        (2016, "Flexi", 0.002), (2026, "Flexi", 0.05),
        # below the threshold in every year -> dropped
        (2016, "DVD", 0.009), (2025, "DVD", 0.0099),
    ]
    pdf = pd.DataFrame(rows, columns=["year", "format_name", "share"])
    out = formats.main_formats(pdf)
    check("threshold and order", out == ["CD", "Vinyl", "Cassette"], str(out))
    check("min_share raises the bar", formats.main_formats(pdf, min_share=0.35) == ["CD", "Vinyl"])
    check("empty input", formats.main_formats(pdf.iloc[0:0]) == [])
    only_partial = pd.DataFrame([(2026, "X", 0.9)], columns=["year", "format_name", "share"])
    check("partial years ignored", formats.main_formats(only_partial) == [])
    tie = pd.DataFrame([(2016, "B", 0.5), (2016, "A", 0.5)], columns=["year", "format_name", "share"])
    check("tie broken by name", formats.main_formats(tie) == ["A", "B"])


def test_group_minor(spark) -> None:
    """Relabelling and row count."""
    print("group_minor_formats")
    df = spark.createDataFrame(
        [(1, 2020, "Vinyl"), (2, 2020, "Lathe Cut"), (2, 2020, "Flexi-disc"), (3, 2020, None)],
        "release_id long, year int, format_name string",
    )
    out = formats.group_minor_formats(df, ["Vinyl", "CD"])
    names = sorted(r["format_name"] for r in out.collect())
    check("relabelled", names == ["Other", "Other", "Other", "Vinyl"], str(names))
    check("row count kept", out.count() == 4)
    custom = formats.group_minor_formats(df, ["Vinyl"], other="Rest")
    check("custom label", custom.where("format_name = 'Rest'").count() == 3)
    check("empty keep", formats.group_minor_formats(df, []).where("format_name = 'Other'").count() == 4)
    shares = formats.format_shares_by_year(out).where("format_name = 'Other'").collect()[0]
    check("release counted once under Other", shares["n_releases"] == 2 and shares["n_year"] == 3,
          str(shares))


def test_age(spark) -> None:
    """Age bins, labels, exclusions, format filter and the future-master count."""
    print("reissue age")
    check(
        "labels",
        formats.age_bin_labels() == ["1", "2-4", "5-9", "10-19", "20-29", "30-39", "40-49", "50+"],
        str(formats.age_bin_labels()),
    )
    check("custom labels", formats.age_bin_labels((1, 3, 6)) == ["1-2", "3-5", "6+"])
    for bad in [(), (2, 5), (1, 5, 5), (1, 5, 2)]:
        try:
            formats.age_bin_labels(bad)
            check(f"rejects {bad}", False)
        except ValueError:
            check(f"rejects {bad}", True)

    rows = [  # release_id, year, master_year, format_name
        (1, 2020, 2019, "Vinyl"),    # age 1
        (2, 2020, 2018, "Vinyl"),    # age 2
        (3, 2020, 2016, "Vinyl"),    # age 4
        (4, 2020, 2015, "Vinyl"),    # age 5
        (5, 2020, 2000, "Vinyl"),    # age 20
        (6, 2020, 1970, "Vinyl"),    # age 50 -> "50+"
        (7, 2020, 1900, "Vinyl"),    # age 120 -> "50+"
        (8, 2020, 2020, "Vinyl"),    # age 0 -> excluded
        (9, 2020, 2022, "Vinyl"),    # master later than release -> excluded
        (10, 2020, None, "Vinyl"),   # no master -> excluded
        (11, 2020, 2019, "CD"),      # CD only
        (1, 2020, 2019, "Vinyl"),    # duplicate row of release 1 -> counted once
        (12, 2021, 2020, "Vinyl"),   # other year
        (13, 2021, 2023, "CD"),      # master later than release, CD
    ]
    flagged = spark.createDataFrame(
        rows, "release_id long, year int, master_year int, format_name string"
    )
    out = formats.reissue_age_distribution(flagged, "Vinyl").toPandas()
    got = {(r.year, r.age_bin): r.n_releases for r in out.itertuples()}
    want = {
        (2020, "1"): 1, (2020, "2-4"): 2, (2020, "5-9"): 1, (2020, "20-29"): 1,
        (2020, "50+"): 2, (2021, "1"): 1,
    }
    check("counts per bin", got == want, str(got))
    order = {r.age_bin: r.age_bin_order for r in out.itertuples()}
    check("bin order", order["1"] == 0 and order["2-4"] == 1 and order["50+"] == 7, str(order))
    med = out.groupby("year")["median_age"].first().to_dict()
    check("median age", med == {2020: 5, 2021: 1}, str(med))  # ages 1,2,4,5,20,50,120
    check("median repeated within a year", out.groupby("year")["median_age"].nunique().max() == 1)

    both = formats.reissue_age_distribution(flagged).toPandas()
    n_2020 = both[both.year == 2020].n_releases.sum()
    check("all formats: release counted once", n_2020 == 8, str(n_2020))
    cd = formats.reissue_age_distribution(flagged, "CD").toPandas()
    check("format filter", set(cd.year) == {2020} and cd.n_releases.sum() == 1, str(cd))

    check("future master count (Vinyl)", formats.count_master_after_release(flagged, "Vinyl") == 1)
    check("future master count (CD)", formats.count_master_after_release(flagged, "CD") == 1)
    check("future master count (all)", formats.count_master_after_release(flagged) == 2)


def test_pressing(spark) -> None:
    """Shares, denominators, ranking window, coverage and role filter."""
    print("pressing plants")
    rf = spark.createDataFrame(
        [
            (1, 2020, "Vinyl"), (2, 2020, "Vinyl"), (3, 2020, "Vinyl"), (4, 2020, "Vinyl"),
            (5, 2021, "Vinyl"), (6, 2021, "Vinyl"),
            (7, 2020, "CD"),
            (8, 2026, "Vinyl"), (9, 2026, "Vinyl"), (10, 2026, "Vinyl"),
            (1, 2020, "Vinyl"),  # duplicate format row
        ],
        "release_id long, year int, format_name string",
    )
    rc = spark.createDataFrame(
        [
            (1, 100, "Plant A", "Pressed By"),
            (1, 100, "Plant A", "Pressed By"),   # duplicate
            (2, 100, "Plant A", "Pressed By"),
            (2, 200, "Plant B", "Pressed By"),   # release with two plants
            (3, 200, "Plant B", "Pressed By"),
            (4, 300, "Distro", "Distributed By"),  # release 4 has no plant
            (5, 100, "Plant A", "Pressed By"),
            (6, 200, "Plant B", "Pressed By"),
            (7, 100, "Plant A", "Pressed By"),   # CD, must not count for Vinyl
            (8, 400, "Plant C", "Pressed By"),   # 2026 only, outside the ranking window
            (9, None, "No id", "Pressed By"),    # no id: in the denominator, not ranked
        ],
        "release_id long, company_id long, company_name string, entity_type_name string",
    )
    out = formats.top_pressing_plants(rc, rf).toPandas()
    key = {(r.year, r.company_name): r for r in out.itertuples()}
    a20 = key[(2020, "Plant A")]
    check("n_releases (duplicates counted once)", a20.n_releases == 2, str(a20))
    check("denominator = releases with any plant", a20.n_year_with_role == 3, str(a20))
    check("share_of_format_year", close(a20.share_of_format_year, 2 / 3))
    check("second plant", key[(2020, "Plant B")].n_releases == 2)
    check("other year", close(key[(2021, "Plant A")].share_of_format_year, 0.5))
    check("role filter", "Distro" not in set(out.company_name))
    check("total over full years", a20.total_full_years == 3
          and key[(2020, "Plant B")].total_full_years == 3)
    ranks = out.drop_duplicates("company_name").set_index("company_name")["rank"].to_dict()
    check("ranks", ranks == {"Plant A": 1, "Plant B": 2, "Plant C": 3}, str(ranks))
    check("null id not ranked", "No id" not in set(out.company_name))
    check("2026 denominator includes null-id plant",
          key[(2026, "Plant C")].n_year_with_role == 2, str(key[(2026, "Plant C")]))
    top1 = formats.top_pressing_plants(rc, rf, top_n=1).toPandas()
    check("top_n", set(top1.company_name) == {"Plant A"}, str(set(top1.company_name)))
    other_role = formats.top_pressing_plants(rc, rf, role="Distributed By").toPandas()
    check("other role", set(other_role.company_name) == {"Distro"})
    cd = formats.top_pressing_plants(rc, rf, format_name="CD").toPandas()
    check("other format", len(cd) == 1 and cd.iloc[0].n_releases == 1)

    cov = {r.year: r for r in formats.pressing_plant_coverage(rc, rf).collect()}
    check("coverage 2020", cov[2020]["n_format"] == 4 and cov[2020]["n_with_role"] == 3
          and close(cov[2020]["coverage"], 0.75), str(cov[2020]))
    check("coverage 2021", close(cov[2021]["coverage"], 1.0))
    check("coverage 2026", close(cov[2026]["coverage"], 2 / 3))


def test_format_coverage(spark) -> None:
    """Share, description coverage and top descriptions."""
    print("format coverage")
    rf = spark.createDataFrame(
        [
            (1, 2020, "File", ["MP3", "Album"]),
            (2, 2020, "File", ["FLAC", "Album"]),
            (3, 2020, "File", []),
            (4, 2020, "File", None),
            (5, 2020, "File", ["", " "]),          # blank only: no description
            (6, 2020, "Vinyl", ["LP"]),
            (1, 2020, "File", ["MP3"]),            # second File row of release 1
            (7, 2021, "File", ["MP3"]),
            (8, 2021, "CD", ["Album"]),
        ],
        "release_id long, year int, format_name string, descriptions array<string>",
    )
    releases = spark.createDataFrame(
        [(i, 2020) for i in range(1, 11)] + [(11, 2021), (12, 2021), (13, 2021), (14, 2021)],
        "release_id long, year int",
    )
    rows = {r.year: r for r in formats.format_coverage(rf, releases).collect()}
    y20 = rows[2020]
    check("n_format", y20["n_format"] == 5, str(y20))
    check("n_releases", y20["n_releases"] == 10)
    check("share", close(y20["share"], 0.5))
    check("desc_coverage", y20["n_with_desc"] == 2 and close(y20["desc_coverage"], 0.4), str(y20))
    top = [(d["description"], d["n"]) for d in y20["top_descriptions"]]
    check("top descriptions ordered by count then name",
          top == [("Album", 2), ("FLAC", 1), ("MP3", 1)], str(top))
    check("2021", rows[2021]["n_format"] == 1 and close(rows[2021]["share"], 0.25))
    short = formats.format_coverage(rf, releases, top_n_descriptions=1).where("year = 2020").collect()[0]
    check("top_n_descriptions", len(short["top_descriptions"]) == 1)
    cd = {r.year: r for r in formats.format_coverage(rf, releases, "CD").collect()}
    check("other format", set(cd) == {2021} and cd[2021]["n_format"] == 1)


def test_growth(spark) -> None:
    """Growth explained, per flag, with the no-growth case returning null."""
    print("growth explained")
    df = spark.createDataFrame(
        [
            ("tag", 2016, 100, 20), ("tag", 2025, 300, 120), ("tag", 2026, 250, 100),
            ("master", 2016, 100, 10), ("master", 2025, 300, 50),
            ("flat", 2016, 100, 10), ("flat", 2025, 100, 40),
            ("shrink", 2016, 100, 10), ("shrink", 2025, 80, 40),
            ("gap", 2016, 100, 10),
        ],
        "flag string, year int, n_releases long, n_reissues long",
    )
    out = {r.flag: r for r in formats.growth_explained_by_reissues(df, 2016, 2025).collect()}
    check("tag", close(out["tag"]["growth_explained"], 100 / 200), str(out["tag"]))
    check("master", close(out["master"]["growth_explained"], 40 / 200))
    check("columns", out["tag"]["releases_start"] == 100 and out["tag"]["releases_end"] == 300
          and out["tag"]["reissues_start"] == 20 and out["tag"]["reissues_end"] == 120)
    check("no growth -> null", out["flat"]["growth_explained"] is None)
    check("decline -> null", out["shrink"]["growth_explained"] is None)
    check("missing end year -> null", out["gap"]["growth_explained"] is None)
    single = df.where("flag = 'tag'").drop("flag")
    one = formats.growth_explained_by_reissues(single, 2016, 2025).collect()
    check("no flag column", len(one) == 1 and close(one[0]["growth_explained"], 0.5))
    rf = spark.createDataFrame(
        [(1, 2016, "Vinyl", False), (2, 2016, "Vinyl", False),
         (3, 2025, "Vinyl", False), (4, 2025, "Vinyl", True), (5, 2025, "Vinyl", True),
         (6, 2025, "Vinyl", True)],
        "release_id long, year int, format_name string, is_reissue boolean",
    )
    share = formats.reissue_share(rf, by=["year"], flag_col="is_reissue")
    res = formats.growth_explained_by_reissues(share, 2016, 2025).collect()[0]
    check("with reissue_share output", close(res["growth_explained"], 3 / 2), str(res))


def test_real_data(spark) -> None:
    """Run every new function on the 2026 tables, years 2016 and 2025 only."""
    print("real-data smoke run (2016, 2025)")
    snap = config.MAIN_SNAPSHOT
    years = [2016, 2025]
    _T0[0] = time.time()
    # The filtered tables are small (two years); cache them so each function
    # does not re-scan the Parquet files.
    rf = ingest.read_table(spark, snap, "release_formats").where(F.col("year").isin(years)).cache()
    rc = ingest.read_table(spark, snap, "release_companies").where(F.col("year").isin(years)).cache()
    rel = ingest.read_table(spark, snap, "releases").where(F.col("year").isin(years)).cache()
    masters = ingest.read_table(spark, snap, "masters")
    lap("load and cache inputs")
    print(f"  rows: release_formats={rf.count():,} release_companies={rc.count():,} "
          f"releases={rel.count():,}")
    lap("materialise the cached two-year tables")

    shares = formats.format_shares_by_year(rf).toPandas()
    main = formats.main_formats(shares)
    print(f"  main formats: {main}")
    check("main_formats finds Vinyl and CD", "Vinyl" in main and "CD" in main, str(main))
    lap("format shares + main_formats")
    grouped = formats.group_minor_formats(rf, main)
    gshares = formats.format_shares_by_year(grouped).toPandas()
    check("grouped formats are main + Other only",
          set(gshares.format_name) <= set(main) | {"Other"}, str(set(gshares.format_name)))

    lap("group_minor_formats")
    flagged = formats.add_master_reissue_flag(rf, rel, masters).cache()
    flagged.count()
    lap("add_master_reissue_flag (cached)")
    for fmt in ("Vinyl", "CD"):
        age = formats.reissue_age_distribution(flagged, fmt).toPandas()
        check(f"age distribution {fmt}", len(age) > 0 and set(age.year) <= set(years)
              and age.age_bin.isin(formats.age_bin_labels()).all())
        print(f"  {fmt} median age: {age.groupby('year').median_age.first().to_dict()}")
    n_future = formats.count_master_after_release(flagged, "Vinyl")
    check("future-master count", n_future >= 0)
    print(f"  vinyl releases with master later than release: {n_future}")

    lap("reissue_age_distribution x2 + count_master_after_release")
    plants = formats.top_pressing_plants(rc, rf, top_n=5).toPandas()
    check("plants: 5 companies", plants.company_name.nunique() == 5, str(plants.company_name.nunique()))
    check("plants: shares in (0, 1]", plants.share_of_format_year.between(0, 1).all())
    cov = formats.pressing_plant_coverage(rc, rf).toPandas().sort_values("year")
    print(cov.to_string(index=False))
    check("coverage in [0, 1]", cov.coverage.between(0, 1).all() and set(cov.year) == set(years))

    lap("top_pressing_plants + pressing_plant_coverage")
    fc = formats.format_coverage(rf, rel).toPandas().sort_values("year")
    check("format coverage rows", set(fc.year) == set(years) and fc.share.between(0, 1).all())
    check("desc_coverage in [0, 1]", fc.desc_coverage.between(0, 1).all())
    check("top descriptions present", all(len(t) > 0 for t in fc.top_descriptions))

    lap("format_coverage")
    vinyl = flagged.where(F.col("format_name") == "Vinyl")
    parts = [
        formats.reissue_share(vinyl, by=["year"], flag_col=flag).withColumn("flag", F.lit(flag))
        for flag in ["is_reissue", "is_reissue_master", "is_reissue_any"]
    ]
    union = parts[0].unionByName(parts[1]).unionByName(parts[2])
    growth = formats.growth_explained_by_reissues(union, 2016, 2025).toPandas()
    print(growth.to_string(index=False))
    check("growth: three flags", len(growth) == 3)
    lap("growth_explained_by_reissues")
    for df in (rf, rc, rel, flagged):
        df.unpersist()


def main() -> int:
    """Run all checks and return the exit code."""
    test_main_formats()
    spark = ingest.get_spark("test-t06", shuffle_partitions=8)
    try:
        for test in (test_group_minor, test_age, test_pressing, test_format_coverage,
                     test_growth, test_real_data):
            start = time.time()
            test(spark)
            print(f"  ({time.time() - start:.0f} s)")
    finally:
        spark.stop()
    if FAILURES:
        print(f"\nFAILED: {FAILURES}")
        return 1
    print("\nall checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
