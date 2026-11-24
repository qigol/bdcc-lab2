"""Snapshot comparison: January 2020 versus October 2026.

Two questions are answered by diffing the snapshots:

* Which labels changed parent (ownership) between the two dates?
* How late do releases get catalogued, and therefore how undercounted
  are the most recent years in the October 2026 snapshot? Does that lag
  differ by format or genre?

Two snapshots show the net change only. They cannot say when between the
two dates a change happened, or whether something changed and changed
back.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from . import config, labels

if TYPE_CHECKING:  # only for type hints; not needed at run time
    import pandas as pd


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


def major_root_check(
    labels_old: DataFrame,
    labels_new: DataFrame,
    major_root_ids: dict[str, int] | None = None,
) -> "pd.DataFrame":
    """Check that each major's root label exists, with the same parent, in both snapshots.

    ``config.MAJOR_ROOT_IDS`` was checked against the 2026 labels table.
    The same ids are used on the 2020 table, so a root that is missing
    there, or has another parent, would make the comparison of ``major``
    between snapshots unreliable.

    Parameters
    ----------
    labels_old : DataFrame
        ``labels`` table of the earlier snapshot (``label_id``,
        ``label_name``, ``parent_id``).
    labels_new : DataFrame
        ``labels`` table of the later snapshot, same columns.
    major_root_ids : dict of str to int, optional
        Major name -> root ``label_id``. Defaults to
        ``config.MAJOR_ROOT_IDS``.

    Returns
    -------
    pandas.DataFrame
        One row per major with ``major``, ``root_id``, ``in_old``,
        ``in_new``, ``label_name_old``, ``label_name_new``,
        ``parent_id_old``, ``parent_id_new`` and ``ok``. ``ok`` is True
        when the root is in both snapshots and has the same parent id
        (or no parent) in both.
    """
    import pandas as pd

    major_root_ids = major_root_ids or config.MAJOR_ROOT_IDS
    ids = list(major_root_ids.values())

    def fetch(table: DataFrame, suffix: str) -> "pd.DataFrame":
        pdf = (
            table.where(F.col("label_id").isin(ids))
            .select(
                F.col("label_id").alias("root_id"),
                F.col("label_name").alias(f"label_name_{suffix}"),
                F.col("parent_id").alias(f"parent_id_{suffix}"),
            )
            .toPandas()
        )
        pdf[f"in_{suffix}"] = True
        return pdf

    base = pd.DataFrame({"major": list(major_root_ids), "root_id": ids})
    out = base.merge(fetch(labels_old, "old"), on="root_id", how="left").merge(
        fetch(labels_new, "new"), on="root_id", how="left"
    )
    out["in_old"] = out["in_old"].eq(True)
    out["in_new"] = out["in_new"].eq(True)
    # Compare parents with None == None, and as nullable ints (no float NaN).
    for col in ("parent_id_old", "parent_id_new"):
        out[col] = out[col].astype("Int64")
    same_parent = (out["parent_id_old"] == out["parent_id_new"]) | (
        out["parent_id_old"].isna() & out["parent_id_new"].isna()
    )
    out["ok"] = out["in_old"] & out["in_new"] & same_parent.fillna(False).astype(bool)
    return out[
        [
            "major",
            "root_id",
            "in_old",
            "in_new",
            "label_name_old",
            "label_name_new",
            "parent_id_old",
            "parent_id_new",
            "ok",
        ]
    ]


def major_moves(
    labels_old: DataFrame,
    labels_new: DataFrame,
    release_labels: DataFrame | None = None,
) -> DataFrame:
    """List labels that moved into, out of, or between major label trees.

    Roots are resolved and majors tagged separately in each snapshot with
    ``labels.resolve_label_roots`` and ``labels.tag_major_labels``. The
    two results are joined on ``label_id``.

    Parameters
    ----------
    labels_old : DataFrame
        ``labels`` table of the earlier snapshot (``label_id``,
        ``label_name``, ``parent_id``).
    labels_new : DataFrame
        ``labels`` table of the later snapshot, same columns.
    release_labels : DataFrame, optional
        Columns ``release_id`` and ``label_id``. Pass the already
        scoped table (for example the later snapshot's) to count releases.

    Returns
    -------
    DataFrame
        One row per label present in both snapshots whose ``major``
        differs, with ``label_id``, ``label_name`` (later snapshot),
        ``major_old``, ``major_new``, ``root_id_old``, ``root_id_new``,
        ``parent_id_old``, ``parent_id_new`` and ``move``, one of:

        * ``"into_major"`` - Independent before, a major now.
        * ``"out_of_major"`` - a major before, Independent now.
        * ``"between_majors"`` - a different major now.

        If ``release_labels`` is given there is also ``n_releases``, the
        number of distinct releases on the label in that table (0 if
        none), and rows are ordered by it, largest first. Ties and the
        no-``release_labels`` case are ordered by ``label_id``.

    Notes
    -----
    Labels in only one snapshot are left out, and so are labels whose
    parent changed but whose major did not. The change in ``major`` is
    the net change between the two dates. Whether it is an acquisition
    or a correction of the data needs a hand check; see ``explain_move``.
    """

    def tag(table: DataFrame, suffix: str) -> DataFrame:
        # Names are not needed for the walk, and carrying them through the
        # self-joins is slow, so the walk gets a null name and the name is
        # joined back from the table afterwards.
        slim = table.select(
            "label_id", F.lit(None).cast("string").alias("label_name"), "parent_id"
        )
        tagged = labels.tag_major_labels(labels.resolve_label_roots(slim))
        names = table.select("label_id", F.col("label_name").alias(f"label_name_{suffix}"))
        return tagged.join(names, "label_id", "left").select(
            "label_id",
            f"label_name_{suffix}",
            F.col("major").alias(f"major_{suffix}"),
            F.col("root_id").alias(f"root_id_{suffix}"),
            F.col("parent_id").alias(f"parent_id_{suffix}"),
        )

    moves = (
        tag(labels_old, "old")
        .join(tag(labels_new, "new"), "label_id", "inner")
        .where(F.col("major_old") != F.col("major_new"))
        .withColumn(
            "move",
            F.when(F.col("major_old") == "Independent", "into_major")
            .when(F.col("major_new") == "Independent", "out_of_major")
            .otherwise("between_majors"),
        )
        .select(
            "label_id",
            F.col("label_name_new").alias("label_name"),
            "major_old",
            "major_new",
            "root_id_old",
            "root_id_new",
            "parent_id_old",
            "parent_id_new",
            "move",
        )
    )
    if release_labels is None:
        return moves.orderBy("label_id")
    volume = release_labels.groupBy("label_id").agg(
        F.countDistinct("release_id").alias("n_releases")
    )
    return (
        moves.join(volume, "label_id", "left")
        .withColumn("n_releases", F.coalesce(F.col("n_releases"), F.lit(0)))
        .orderBy(F.desc("n_releases"), F.asc("label_id"))
    )


def explain_move(
    label_id: int,
    labels_old: DataFrame,
    labels_new: DataFrame,
    max_depth: int = 12,
    stop_ids: tuple[int, ...] = config.PLACEHOLDER_LABEL_IDS,
) -> "pd.DataFrame":
    """Show a label's chain of parents in both snapshots, for a hand check.

    Parameters
    ----------
    label_id : int
        Label to explain, for example a row of ``major_moves``.
    labels_old : DataFrame
        ``labels`` table of the earlier snapshot (``label_id``,
        ``label_name``, ``parent_id``, ``parent_name``).
    labels_new : DataFrame
        ``labels`` table of the later snapshot, same columns.
    max_depth : int
        Maximum number of parent hops to follow, as in
        ``labels.resolve_label_roots``.
    stop_ids : tuple of int
        Placeholder parent ids. They are shown in ``parent_id`` but not
        followed. Defaults to ``config.PLACEHOLDER_LABEL_IDS``.

    Returns
    -------
    pandas.DataFrame
        One row per label on the chain, for ``snapshot`` ``"old"`` then
        ``"new"``. Columns: ``snapshot``, ``hop`` (0 = the label itself),
        ``label_id``, ``label_name``, ``parent_id``, ``parent_name`` and
        ``note``. ``note`` is empty except on the last row of a chain:

        * ``"root"`` - no parent.
        * ``"placeholder parent, not followed"``.
        * ``"parent missing from labels table"``.
        * ``"cycle"`` - the parent is already on the chain.
        * ``"stopped at max_depth"``.
        * ``"label not in this snapshot"`` - the only row, with empty
          names.

    Notes
    -----
    Each hop is one small query on the labels table, so at most
    ``2 * (max_depth + 1)`` queries per call. Cache the tables first if
    you call this for several labels.
    """
    import pandas as pd

    columns = ["snapshot", "hop", "label_id", "label_name", "parent_id", "parent_name", "note"]
    rows: list[dict] = []
    for snapshot, table in (("old", labels_old), ("new", labels_new)):
        chain: list[dict] = []
        seen: set[int] = set()
        current = int(label_id)
        for hop in range(max_depth + 1):
            found = (
                table.where(F.col("label_id") == current)
                .select("label_id", "label_name", "parent_id", "parent_name")
                .limit(1)
                .collect()
            )
            if not found:
                if hop == 0:
                    chain.append(
                        {
                            "snapshot": snapshot,
                            "hop": 0,
                            "label_id": current,
                            "label_name": None,
                            "parent_id": None,
                            "parent_name": None,
                            "note": "label not in this snapshot",
                        }
                    )
                else:
                    chain[-1]["note"] = "parent missing from labels table"
                break
            row = found[0]
            seen.add(current)
            step = {
                "snapshot": snapshot,
                "hop": hop,
                "label_id": row["label_id"],
                "label_name": row["label_name"],
                "parent_id": row["parent_id"],
                "parent_name": row["parent_name"],
                "note": "",
            }
            chain.append(step)
            parent = row["parent_id"]
            if parent is None:
                step["note"] = "root"
                break
            if parent in stop_ids:
                step["note"] = "placeholder parent, not followed"
                break
            if parent in seen:
                step["note"] = "cycle"
                break
            if hop == max_depth:
                step["note"] = "stopped at max_depth"
                break
            current = parent
        rows.extend(chain)
    return pd.DataFrame(rows, columns=columns)


def cataloguing_lag_by(
    releases_old: DataFrame,
    releases_new: DataFrame,
    groups_new: DataFrame,
    group_col: str,
) -> DataFrame:
    """Measure the cataloguing lag separately for each format, genre or other group.

    Same measure as ``cataloguing_lag``, split by a group column of the
    later snapshot.

    Parameters
    ----------
    releases_old : DataFrame
        ``releases`` table of the earlier snapshot (``release_id``).
    releases_new : DataFrame
        ``releases`` table of the later snapshot (``release_id``,
        ``year``). Filter it to the years of interest first, for example
        ``year <= 2019``.
    groups_new : DataFrame
        Table of the later snapshot with ``release_id`` and ``group_col``,
        for example ``release_formats`` (``format_name``) or
        ``release_genres`` (``genre``). Filter it to the groups of
        interest first. Its own ``year`` column is not used.
    group_col : str
        Name of the group column in ``groups_new``. It is kept under the
        same name in the result.

    Returns
    -------
    DataFrame
        One row per (``year``, ``group_col``) with ``n_new``,
        ``n_already``, ``n_added_later`` and ``completeness``, defined as
        in ``cataloguing_lag``. A release with two groups is counted once
        in each; the same release listed twice for one group is counted
        once.

    Notes
    -----
    Only the later snapshot's groups are used, because the earlier
    snapshot has no formats or genres tables. Releases with a null group
    value, and group rows whose release is not in ``releases_new``, are
    dropped. ``year`` is the later snapshot's year, as in
    ``cataloguing_lag``.
    """
    old_ids = releases_old.select("release_id").distinct().withColumn("already", F.lit(1))
    groups = (
        groups_new.select("release_id", group_col)
        .where(F.col(group_col).isNotNull())
        .distinct()
    )
    return (
        releases_new.select("release_id", "year")
        .join(groups, "release_id", "inner")
        .join(old_ids, "release_id", "left")
        .groupBy("year", group_col)
        .agg(
            F.countDistinct("release_id").alias("n_new"),
            F.countDistinct(F.when(F.col("already") == 1, F.col("release_id"))).alias("n_already"),
        )
        .withColumn("n_added_later", F.col("n_new") - F.col("n_already"))
        .withColumn("completeness", F.col("n_already") / F.col("n_new"))
        .orderBy("year", group_col)
    )
