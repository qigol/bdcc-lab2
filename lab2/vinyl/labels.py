"""RQ3 - How concentrated is release activity in the three majors' label trees?

Steps:

1. ``resolve_label_roots`` walks every label up its ``parent_id`` chain
   to the top of its tree. Placeholder parents such as the unnamed label
   212 are not followed, because Universal and Warner both sit under it.
2. ``tag_major_labels`` marks the labels whose root id is that of
   Universal, Sony or Warner (``config.MAJOR_ROOT_IDS``).
3. ``major_share_by_year`` splits yearly releases into major versus
   independent.
4. ``artist_concentration`` and ``top_contributors`` describe who
   dominates each label's output.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from . import config


def resolve_label_roots(
    labels: DataFrame,
    max_depth: int = 12,
    stop_ids: tuple[int, ...] = config.PLACEHOLDER_LABEL_IDS,
) -> DataFrame:
    """Find the top-level ancestor (root) of every label.

    Parameters
    ----------
    labels : DataFrame
        Columns ``label_id``, ``label_name``, ``parent_id``.
    max_depth : int
        Maximum number of parent hops to follow. Raise it if the result
        still has ``reached_root == False`` rows.
    stop_ids : tuple of int
        Placeholder label ids. A ``parent_id`` in this tuple is read as
        "no parent", so a label whose parent is a placeholder is its own
        root, and so is the placeholder label itself. Defaults to
        ``config.PLACEHOLDER_LABEL_IDS``.

    Returns
    -------
    DataFrame
        One row per label with:

        * ``label_id``, ``label_name``, ``parent_id``
        * ``root_id``, ``root_name`` - the top of the label's tree (the
          label itself if it has no parent or only a placeholder parent).
        * ``depth`` - number of hops from the label to its root.
        * ``reached_root`` - False if the walk stopped at ``max_depth``
          (a deeper tree, or a cycle in the data) or hit a parent id
          that is missing from the labels file.

    Notes
    -----
    Implemented as ``max_depth`` self-joins on the labels table, one hop
    per join. The table is small next to releases, so this is cheap.

    In the 2026 labels table, label 212 has no name and is the parent of
    both Universal Music Group (38404) and Warner Music Group (2345).
    Without ``stop_ids`` all its 32,175 labels would share one root.
    ``parent_id`` in the output is the original value, including any
    placeholder.
    """
    # A parent id in stop_ids counts as "no parent", both for the first
    # hop and for every later hop through the parents table.
    up = F.when(F.col("parent_id").isin(list(stop_ids)), F.lit(None)).otherwise(
        F.col("parent_id")
    )
    parents = labels.select(F.col("label_id").alias("_pid"), up.alias("_next"))
    walk = labels.select(
        "label_id",
        "label_name",
        "parent_id",
        F.col("label_id").alias("root_id"),
        up.alias("_up"),
        F.lit(0).alias("depth"),
    )
    for _ in range(max_depth):
        walk = (
            walk.join(parents, walk["_up"] == parents["_pid"], "left")
            .select(
                "label_id",
                "label_name",
                "parent_id",
                F.coalesce(F.col("_pid"), F.col("root_id")).alias("root_id"),
                F.when(F.col("_pid").isNotNull(), F.col("depth") + 1)
                .otherwise(F.col("depth"))
                .alias("depth"),
                # Keep the pointer if the parent was not found, so the
                # label is reported as not having reached a root.
                F.when(F.col("_pid").isNotNull(), F.col("_next"))
                .otherwise(F.col("_up"))
                .alias("_up"),
            )
        )
    names = labels.select(
        F.col("label_id").alias("root_id"), F.col("label_name").alias("root_name")
    )
    return (
        walk.withColumn("reached_root", F.col("_up").isNull())
        .drop("_up")
        .join(names, "root_id", "left")
    )


def tag_major_labels(
    label_roots: DataFrame,
    major_root_ids: dict[str, int] | None = None,
) -> DataFrame:
    """Mark each label as Universal, Sony, Warner or Independent.

    Parameters
    ----------
    label_roots : DataFrame
        Output of ``resolve_label_roots``.
    major_root_ids : dict of str to int, optional
        Major name -> ``label_id`` of its root label. Defaults to
        ``config.MAJOR_ROOT_IDS``.

    Returns
    -------
    DataFrame
        ``label_roots`` plus ``major`` (``"Universal"``, ``"Sony"``,
        ``"Warner"`` or ``"Independent"``) and ``is_major`` (boolean).

    Notes
    -----
    Matching is on ``root_id``. "Independent" here means "not linked to
    one of the three majors' trees in Discogs", which depends on how
    completely contributors have filled in parent labels. State this
    limitation next to any result.
    """
    major_root_ids = major_root_ids or config.MAJOR_ROOT_IDS
    major = F.lit("Independent")
    for name, root_id in major_root_ids.items():
        major = F.when(F.col("root_id") == root_id, F.lit(name)).otherwise(major)
    return label_roots.withColumn("major", major).withColumn(
        "is_major", F.col("major") != "Independent"
    )


def major_share_by_year(release_labels: DataFrame, tagged_labels: DataFrame) -> DataFrame:
    """Split each year's releases between the majors and independents.

    Parameters
    ----------
    release_labels : DataFrame
        Columns ``release_id``, ``year``, ``label_id``.
    tagged_labels : DataFrame
        Output of ``tag_major_labels`` (needs ``label_id``, ``major``).

    Returns
    -------
    DataFrame
        One row per (year, major) with ``n_releases``, ``n_year`` and
        ``share``. A release on labels of two groups counts once in each.
        Releases whose label id is not in the labels file count as
        Independent.
    """
    joined = release_labels.join(
        tagged_labels.select("label_id", "major"), "label_id", "left"
    ).fillna({"major": "Independent"})
    per_major = joined.groupBy("year", "major").agg(
        F.countDistinct("release_id").alias("n_releases")
    )
    per_year = joined.groupBy("year").agg(F.countDistinct("release_id").alias("n_year"))
    return per_major.join(per_year, "year").withColumn(
        "share", F.col("n_releases") / F.col("n_year")
    )


def artist_concentration(
    release_labels: DataFrame,
    release_artists: DataFrame,
    min_releases: int = 50,
) -> DataFrame:
    """Measure how much each label depends on its top artists.

    Parameters
    ----------
    release_labels : DataFrame
        Columns ``release_id``, ``label_id``, ``label_name``.
    release_artists : DataFrame
        Columns ``release_id``, ``artist_id``, ``artist_name``.
    min_releases : int
        Drop labels with fewer (release, artist) pairs than this; the
        measures are not meaningful on a handful of releases.

    Returns
    -------
    DataFrame
        One row per label with:

        * ``n_pairs`` - (release, artist) pairs on the label.
        * ``n_artists`` - distinct artists on the label.
        * ``top_artist_id``, ``top_artist_name``, ``top_artist_share`` -
          the most released artist and their share of ``n_pairs``.
        * ``hhi`` - Herfindahl index, the sum of squared artist shares.
          It runs from ``1 / n_artists`` (all artists equal) to 1 (a
          single artist). Above 0.25 is usually called highly
          concentrated.

    Notes
    -----
    Remove the placeholder artist "Various" from ``release_artists``
    before calling, otherwise compilation labels look like one-artist
    labels.
    """
    per_artist = (
        release_labels.select("release_id", "label_id", "label_name")
        .distinct()
        .join(release_artists.select("release_id", "artist_id", "artist_name"), "release_id")
        .groupBy("label_id", "label_name", "artist_id", "artist_name")
        .agg(F.count("*").alias("n"))
    )
    by_label = Window.partitionBy("label_id")
    ranked = (
        per_artist.withColumn("n_pairs", F.sum("n").over(by_label))
        .withColumn("artist_share", F.col("n") / F.col("n_pairs"))
        .withColumn(
            "rank", F.row_number().over(by_label.orderBy(F.desc("n"), F.asc("artist_id")))
        )
    )
    return (
        ranked.groupBy("label_id", "label_name", "n_pairs")
        .agg(
            F.count("*").alias("n_artists"),
            F.max(F.when(F.col("rank") == 1, F.col("artist_id"))).alias("top_artist_id"),
            F.max(F.when(F.col("rank") == 1, F.col("artist_name"))).alias("top_artist_name"),
            F.max(F.when(F.col("rank") == 1, F.col("artist_share"))).alias("top_artist_share"),
            F.sum(F.col("artist_share") ** 2).alias("hhi"),
        )
        .where(F.col("n_pairs") >= min_releases)
    )


def top_contributors(
    release_labels: DataFrame,
    release_credits: DataFrame,
    roles: tuple[str, ...],
    top_n: int = 10,
) -> DataFrame:
    """List the most credited people in a set of roles, per label.

    Use with ``config.PRODUCTION_ROLES`` for producers and engineers and
    ``config.WRITING_ROLES`` for writers.

    Parameters
    ----------
    release_labels : DataFrame
        Columns ``release_id``, ``label_id``, ``label_name``.
    release_credits : DataFrame
        Columns ``release_id``, ``artist_id``, ``artist_name``, ``role``.
    roles : tuple of str
        Role names to keep (exact match on ``role``).
    top_n : int
        Number of people to keep per label.

    Returns
    -------
    DataFrame
        One row per (label, person) with ``n_releases`` (distinct
        releases credited), ``rank`` (1 = most credited) and ``share``
        (of the label's releases that have any credit in ``roles``).
    """
    credited = (
        release_credits.where(F.col("role").isin(list(roles)))
        .select("release_id", "artist_id", "artist_name")
        .distinct()
        .join(release_labels.select("release_id", "label_id", "label_name").distinct(), "release_id")
    )
    per_person = credited.groupBy("label_id", "label_name", "artist_id", "artist_name").agg(
        F.countDistinct("release_id").alias("n_releases")
    )
    per_label = credited.groupBy("label_id").agg(
        F.countDistinct("release_id").alias("n_label")
    )
    rank = F.row_number().over(
        Window.partitionBy("label_id").orderBy(F.desc("n_releases"), F.asc("artist_id"))
    )
    return (
        per_person.withColumn("rank", rank)
        .where(F.col("rank") <= top_n)
        .join(per_label, "label_id")
        .withColumn("share", F.col("n_releases") / F.col("n_label"))
        .drop("n_label")
    )
