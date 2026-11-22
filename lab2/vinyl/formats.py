"""RQ2 - How did the format mix change, and how much of the vinyl boom was reissues?

Two definitions of "reissue" are available and should be compared in the
explorer notebook before one is chosen for the report:

* **Tag-based** - the format descriptions include Reissue, Repress or
  Remastered (``release_formats.is_reissue``).
* **Master-based** - the release year is later than the year of its
  master release (``add_master_reissue_flag``).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def format_shares_by_year(release_formats: DataFrame) -> DataFrame:
    """Compute each format's share of releases in every year.

    Parameters
    ----------
    release_formats : DataFrame
        Columns ``release_id``, ``year``, ``format_name``.

    Returns
    -------
    DataFrame
        One row per (year, format_name) with ``n_releases`` (distinct
        releases in that format), ``n_year`` (distinct releases with any
        format) and ``share``.

    Notes
    -----
    A release that lists two formats counts once under each, so shares
    within a year can add up to slightly more than 1.
    """
    per_format = release_formats.groupBy("year", "format_name").agg(
        F.countDistinct("release_id").alias("n_releases")
    )
    per_year = release_formats.groupBy("year").agg(
        F.countDistinct("release_id").alias("n_year")
    )
    return per_format.join(per_year, "year").withColumn(
        "share", F.col("n_releases") / F.col("n_year")
    )


def add_master_reissue_flag(
    release_formats: DataFrame,
    releases: DataFrame,
    masters: DataFrame,
    min_gap_years: int = 1,
) -> DataFrame:
    """Add a reissue flag based on the age of the master release.

    Parameters
    ----------
    release_formats : DataFrame
        Columns ``release_id``, ``year``, ``format_name``, ``is_reissue``.
    releases : DataFrame
        Columns ``release_id``, ``master_id``.
    masters : DataFrame
        Columns ``master_id``, ``master_year``.
    min_gap_years : int
        A release counts as a reissue when
        ``year - master_year >= min_gap_years``.

    Returns
    -------
    DataFrame
        ``release_formats`` plus:

        * ``master_year`` - null when the release has no master.
        * ``is_reissue_master`` - True, False, or null when unknown
          (no master, or master year missing).
        * ``is_reissue_any`` - True if either definition says reissue.

    Notes
    -----
    A release with no master is usually the only version of that music,
    so treating null as "not a reissue" is a reasonable reading. That
    choice is left to the caller.
    """
    lookup = releases.select("release_id", "master_id").join(
        masters.select("master_id", "master_year"), "master_id", "left"
    )
    return (
        release_formats.join(lookup.select("release_id", "master_year"), "release_id", "left")
        .withColumn(
            "is_reissue_master",
            (F.col("year") - F.col("master_year")) >= min_gap_years,
        )
        .withColumn(
            "is_reissue_any",
            F.col("is_reissue") | F.coalesce(F.col("is_reissue_master"), F.lit(False)),
        )
    )


def reissue_share(
    release_formats: DataFrame,
    by: list[str] | None = None,
    flag_col: str = "is_reissue",
) -> DataFrame:
    """Compute the share of releases that are reissues, per group.

    Parameters
    ----------
    release_formats : DataFrame
        Must contain ``release_id``, the columns in ``by`` and ``flag_col``.
    by : list of str, optional
        Grouping columns. Defaults to ``["year", "format_name"]``. Add
        ``"genre"`` after joining ``release_genres`` to split by genre.
    flag_col : str
        ``"is_reissue"``, ``"is_reissue_master"`` or ``"is_reissue_any"``.

    Returns
    -------
    DataFrame
        One row per group with ``n_releases``, ``n_reissues``,
        ``n_new`` and ``reissue_share`` (``n_reissues / n_releases``).
        A null flag counts as not a reissue.
    """
    by = by or ["year", "format_name"]
    is_reissue = F.coalesce(F.col(flag_col), F.lit(False))
    return (
        release_formats.groupBy(*by)
        .agg(
            F.countDistinct("release_id").alias("n_releases"),
            F.countDistinct(F.when(is_reissue, F.col("release_id"))).alias("n_reissues"),
        )
        .withColumn("n_new", F.col("n_releases") - F.col("n_reissues"))
        .withColumn("reissue_share", F.col("n_reissues") / F.col("n_releases"))
    )
