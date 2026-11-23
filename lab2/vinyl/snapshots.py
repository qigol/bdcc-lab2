"""Snapshot comparison: January 2020 versus October 2026.

Two questions are answered by diffing the snapshots:

* Which labels changed parent (ownership) between the two dates?
* How late do releases get catalogued, and therefore how undercounted
  are the most recent years in the October 2026 snapshot?

Two snapshots show the net change only. They cannot say when between the
two dates a change happened, or whether something changed and changed
back.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from . import config


def label_parent_changes(labels_old: DataFrame, labels_new: DataFrame) -> DataFrame:
    """Compare every label's parent between the two snapshots.

    Parameters
    ----------
    labels_old : DataFrame
        ``labels`` table of the earlier snapshot (``label_id``,
        ``label_name``, ``parent_id``, ``parent_name``).
    labels_new : DataFrame
        ``labels`` table of the later snapshot, same columns.

    Returns
    -------
    DataFrame
        One row per label id found in either snapshot with
        ``label_id``, ``label_name`` (later name if available),
        ``parent_id_old``, ``parent_name_old``, ``parent_id_new``,
        ``parent_name_new`` and ``change``, one of:

        * ``"unchanged"`` - same parent, or no parent, in both.
        * ``"gained_parent"`` - no parent before, has one now.
        * ``"lost_parent"`` - had a parent before, none now.
        * ``"changed_parent"`` - a different parent now.
        * ``"new_label"`` - only in the later snapshot.
        * ``"removed_label"`` - only in the earlier snapshot.

    Notes
    -----
    A parent can appear to change because a contributor corrected the
    data, not because a company was sold. Check the biggest cases by
    hand before reporting them as acquisitions.
    """
    old = labels_old.select(
        "label_id",
        F.col("label_name").alias("label_name_old"),
        F.col("parent_id").alias("parent_id_old"),
        F.col("parent_name").alias("parent_name_old"),
        F.lit(True).alias("in_old"),
    )
    new = labels_new.select(
        "label_id",
        F.col("label_name").alias("label_name_new"),
        F.col("parent_id").alias("parent_id_new"),
        F.col("parent_name").alias("parent_name_new"),
        F.lit(True).alias("in_new"),
    )
    had, has = F.col("parent_id_old").isNotNull(), F.col("parent_id_new").isNotNull()
    change = (
        F.when(F.col("in_old").isNull(), "new_label")
        .when(F.col("in_new").isNull(), "removed_label")
        .when(~had & has, "gained_parent")
        .when(had & ~has, "lost_parent")
        .when(had & has & (F.col("parent_id_old") != F.col("parent_id_new")), "changed_parent")
        .otherwise("unchanged")
    )
    return (
        old.join(new, "label_id", "outer")
        .withColumn("change", change)
        .select(
            "label_id",
            F.coalesce("label_name_new", "label_name_old").alias("label_name"),
            "parent_id_old",
            "parent_name_old",
            "parent_id_new",
            "parent_name_new",
            "change",
        )
    )


def cataloguing_lag(releases_old: DataFrame, releases_new: DataFrame) -> DataFrame:
    """Count, per release year, how many releases were added after the old snapshot.

    Parameters
    ----------
    releases_old : DataFrame
        ``releases`` table of the earlier snapshot (``release_id``, ``year``).
    releases_new : DataFrame
        ``releases`` table of the later snapshot, same columns.

    Returns
    -------
    DataFrame
        One row per release year (as dated in the later snapshot) with:

        * ``n_new`` - releases of that year in the later snapshot.
        * ``n_already`` - of those, how many were already in the earlier
          snapshot (matched on ``release_id``).
        * ``n_added_later`` - ``n_new - n_already``.
        * ``completeness`` - ``n_already / n_new``: the fraction of the
          year's eventual catalogue that existed at the earlier date.

    Notes
    -----
    Matching is on ``release_id`` only, so a release whose date was
    edited between snapshots is counted under its later year. Releases
    deleted or merged after the earlier snapshot are ignored.
    """
    old_ids = releases_old.select("release_id").distinct().withColumn("already", F.lit(1))
    return (
        releases_new.select("release_id", "year")
        .join(old_ids, "release_id", "left")
        .groupBy("year")
        .agg(
            F.count("*").alias("n_new"),
            F.coalesce(F.sum("already"), F.lit(0)).alias("n_already"),
        )
        .withColumn("n_added_later", F.col("n_new") - F.col("n_already"))
        .withColumn("completeness", F.col("n_already") / F.col("n_new"))
        .orderBy("year")
    )


def estimate_undercount(
    lag: DataFrame,
    yearly_counts: DataFrame,
    old_snapshot_year: int = 2020,
    new_snapshot_year: int = 2027,
    partial_year_share: dict[int, float] | None = None,
) -> DataFrame:
    """Scale up recent years using the completeness seen in the old snapshot.

    The idea: if only 60% of releases dated 2019 had been catalogued by
    January 2020, then a year of the same "age" in the October 2026
    snapshot is probably also only about 60% complete.

    Parameters
    ----------
    lag : DataFrame
        Output of ``cataloguing_lag`` (``year``, ``completeness``).
    yearly_counts : DataFrame
        Columns ``year`` and ``n_releases`` from the later snapshot.
    old_snapshot_year : int
        Year used to compute age in the earlier snapshot. January 2020
        means 2019 has age 1.
    new_snapshot_year : int
        Year used to compute age in the later snapshot. The default of
        2027 treats 1 October 2026 as roughly the start of 2027, so 2026
        has age 1. This is reasonable because 2026 is also scaled for
        its missing months through ``partial_year_share``: after that
        scaling it is treated like a year that has just ended, the same
        as 2019 in January 2020.
    partial_year_share : dict of int to float, optional
        Year -> expected share of a full year's releases that fall in
        the months the dump covers (for 2026, January to September).
        Defaults to ``config.PARTIAL_YEAR_SHARE``. Pass ``{}`` to switch
        the correction off.

    Returns
    -------
    DataFrame
        ``yearly_counts`` plus ``age``, ``completeness`` (null when the
        year is old enough to have no matching age, which is read as
        complete), ``partial_year`` (boolean, True for years in
        ``partial_year_share``) and ``n_estimated``. For full years
        ``n_estimated = n_releases / completeness``; for partial years
        ``n_estimated = n_releases / share / completeness``.

    Notes
    -----
    Assumes contributors catalogue at the same pace now as in 2016-2019.
    This is an estimate for the limitations section, not a correction to
    apply to the main results.

    The seasonal share (``config.PARTIAL_YEAR_SHARE``) assumes releases
    are spread over the months as in 2016-2025, and that month coverage
    in 2026 is like earlier years.
    """
    if partial_year_share is None:
        partial_year_share = config.PARTIAL_YEAR_SHARE
    share = F.lit(None).cast("double")
    for year, value in partial_year_share.items():
        share = F.when(F.col("year") == year, F.lit(float(value))).otherwise(share)
    by_age = lag.select(
        (F.lit(old_snapshot_year) - F.col("year")).alias("age"), "completeness"
    ).where(F.col("age") >= 1)
    return (
        yearly_counts.withColumn("age", F.lit(new_snapshot_year) - F.col("year"))
        .join(by_age, "age", "left")
        .withColumn("_share", share)
        .withColumn("partial_year", F.col("_share").isNotNull())
        .withColumn(
            "n_estimated",
            F.col("n_releases")
            / F.coalesce(F.col("_share"), F.lit(1.0))
            / F.coalesce(F.col("completeness"), F.lit(1.0)),
        )
        .drop("_share")
        .orderBy("year")
    )
