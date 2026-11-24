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
   dominates each label's output. ``group_concentration`` does the same
   for a whole group such as a major's tree.
5. ``parent_coverage`` shows how many releases sit on labels with no
   parent information (``self_released_label_ids`` finds the "Not On
   Label" placeholders it sets apart), and ``credit_level_coverage``
   compares release- and track-level credits.
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


def group_concentration(
    release_labels: DataFrame,
    release_artists: DataFrame,
    tagged_labels: DataFrame,
    group_col: str = "major",
    min_releases: int = 50,
    by_year: bool = False,
) -> DataFrame:
    """Measure how much a whole group of labels depends on its top artists.

    Works like ``artist_concentration``, but pools every label of a group
    (for example the Universal tree) instead of looking at one label.

    Parameters
    ----------
    release_labels : DataFrame
        Columns ``release_id``, ``label_id`` and, if ``by_year`` is true,
        ``year``.
    release_artists : DataFrame
        Columns ``release_id``, ``artist_id``, ``artist_name``.
    tagged_labels : DataFrame
        Columns ``label_id`` and ``group_col``, for example the output of
        ``tag_major_labels``. One row per label.
    group_col : str
        Column of ``tagged_labels`` that defines the groups.
    min_releases : int
        Drop groups (or group-years) with fewer (release, artist) pairs
        than this.
    by_year : bool
        If true, compute the measures separately for each ``year`` of
        ``release_labels``.

    Returns
    -------
    DataFrame
        One row per group (per group and ``year`` if ``by_year``) with:

        * ``n_pairs`` - distinct (release, artist) pairs in the group.
        * ``n_artists`` - distinct artists in the group.
        * ``top_artist_id``, ``top_artist_name``, ``top_artist_share`` -
          the most released artist and their share of ``n_pairs``.
        * ``hhi`` - Herfindahl index, the sum of squared artist shares.
          It runs from ``1 / n_artists`` to 1. Above 0.25 is usually
          called highly concentrated.

    Notes
    -----
    A release on two labels of the same group counts once for that group,
    because (release, group) is deduplicated before the artists are
    joined. A release on labels of two different groups counts once in
    each. Labels missing from ``tagged_labels`` are dropped.

    Unlike ``artist_concentration``, artists are identified by
    ``artist_id`` alone and (release, artist) duplicates are removed, so
    spelling variants of one artist's name do not split that artist in
    two. ``top_artist_name`` is the alphabetically first name seen.

    Remove placeholder artists (``networks.PLACEHOLDER_ARTIST_IDS``)
    from ``release_artists`` before calling.
    """
    keys = [group_col, "year"] if by_year else [group_col]
    members = (
        release_labels.select(
            "release_id", "label_id", *(["year"] if by_year else [])
        )
        .join(tagged_labels.select("label_id", group_col), "label_id")
        .select("release_id", *keys)
        .distinct()
    )
    # One row per (release, artist); min() makes the kept name deterministic.
    pairs = release_artists.groupBy("release_id", "artist_id").agg(
        F.min("artist_name").alias("artist_name")
    )
    per_artist = (
        members.join(pairs, "release_id")
        .groupBy(*keys, "artist_id")
        .agg(F.count("*").alias("n"), F.min("artist_name").alias("artist_name"))
    )
    by_group = Window.partitionBy(*keys)
    ranked = (
        per_artist.withColumn("n_pairs", F.sum("n").over(by_group))
        .withColumn("artist_share", F.col("n") / F.col("n_pairs"))
        .withColumn(
            "rank", F.row_number().over(by_group.orderBy(F.desc("n"), F.asc("artist_id")))
        )
    )
    return (
        ranked.groupBy(*keys, "n_pairs")
        .agg(
            F.count("*").alias("n_artists"),
            F.max(F.when(F.col("rank") == 1, F.col("artist_id"))).alias("top_artist_id"),
            F.max(F.when(F.col("rank") == 1, F.col("artist_name"))).alias("top_artist_name"),
            F.max(F.when(F.col("rank") == 1, F.col("artist_share"))).alias("top_artist_share"),
            F.sum(F.col("artist_share") ** 2).alias("hhi"),
        )
        .where(F.col("n_pairs") >= min_releases)
    )


def self_released_label_ids(labels: DataFrame) -> DataFrame:
    """List the labels that stand for "released by the artist".

    Parameters
    ----------
    labels : DataFrame
        Columns ``label_id`` and ``label_name``.

    Returns
    -------
    DataFrame
        One column, ``label_id`` (long), with one row per id: every id in
        ``config.SELF_RELEASED_LABEL_IDS`` plus every label whose
        ``label_name`` matches ``config.SELF_RELEASED_LABEL_PATTERN``.

    Notes
    -----
    The fixed ids are returned even if they are missing from ``labels``.
    Pass the result to ``parent_coverage(..., self_released=...)``.
    """
    spark = labels.sparkSession
    fixed = spark.createDataFrame(
        [(int(i),) for i in config.SELF_RELEASED_LABEL_IDS], "label_id long"
    )
    by_name = labels.where(
        F.col("label_name").rlike(config.SELF_RELEASED_LABEL_PATTERN)
    ).select(F.col("label_id").cast("long").alias("label_id"))
    return fixed.unionByName(by_name).where(F.col("label_id").isNotNull()).distinct()


def parent_coverage(
    release_labels: DataFrame,
    label_roots: DataFrame,
    stop_ids: tuple[int, ...] = config.PLACEHOLDER_LABEL_IDS,
    self_released: DataFrame | None = None,
) -> DataFrame:
    """Count releases by how much parent information their labels have.

    Parameters
    ----------
    release_labels : DataFrame
        Columns ``release_id``, ``year``, ``label_id``.
    label_roots : DataFrame
        Output of ``resolve_label_roots`` or ``tag_major_labels``. Needs
        ``label_id`` and ``parent_id`` (the original parent, which may be
        a placeholder). Children are found from this table, so no
        separate ``labels`` table is needed.
    stop_ids : tuple of int
        Placeholder label ids. A ``parent_id`` in this tuple counts as
        "no parent", and a label that only has such children does not
        count as having children. Defaults to
        ``config.PLACEHOLDER_LABEL_IDS``.
    self_released : DataFrame, optional
        Column ``label_id``: the labels that mean "released by the
        artist", for example the output of ``self_released_label_ids``.
        Such a label goes to ``self_released``, whatever its parent or
        children, and even if it is missing from ``label_roots``. If
        None, no label gets that class.

    Returns
    -------
    DataFrame
        One row per ``year``, ordered by year, with distinct-release
        counts ``on_root_alone``, ``in_tree``, ``self_released``,
        ``unknown_label``, their total ``n_releases`` and the shares
        ``share_on_root_alone``, ``share_in_tree``,
        ``share_self_released``, ``share_unknown_label``. The
        ``self_released`` columns are always present and are 0 when
        ``self_released`` is None.

        * ``in_tree`` - the label has a parent, or some label names it
          as parent.
        * ``on_root_alone`` - the label has no parent and no children.
        * ``self_released`` - the label is in ``self_released``.
        * ``unknown_label`` - the label id (or none) is not in the labels
          table.

    Notes
    -----
    A release on several labels goes to its best class, in the order
    ``in_tree`` > ``on_root_alone`` > ``self_released`` >
    ``unknown_label``, so the four counts add up to ``n_releases``. A
    release on "Not On Label" and on a real label therefore takes the
    real label's class. ``on_root_alone`` releases are the ones that
    could hide major-owned output that Discogs does not link to the
    major's tree.

    Labels in ``self_released`` do not count as children of their listed
    parent. In the 2026 data "Not On Label" (1818) names Stolen Riff
    Records (205754) as parent, and that link alone does not put Stolen
    Riff in a tree.
    """
    stops = list(stop_ids)
    real_parent = F.col("parent_id").isNotNull() & ~F.col("parent_id").isin(stops)
    child_rows = label_roots.where(real_parent)
    if self_released is not None:
        child_rows = child_rows.join(
            self_released.select("label_id").distinct(), "label_id", "left_anti"
        )
    children = (
        child_rows.select(F.col("parent_id").alias("label_id"))
        .distinct()
        .withColumn("_has_child", F.lit(True))
    )
    classes = (
        label_roots.select("label_id", real_parent.alias("_has_parent"))
        .dropDuplicates(["label_id"])
        .join(children, "label_id", "left")
        .select(
            "label_id",
            F.when(F.col("_has_parent") | F.coalesce(F.col("_has_child"), F.lit(False)), 4)
            .otherwise(3)
            .alias("_rank"),
        )
    )
    # Rank 1 is the class of a label id that is not in the labels table.
    # Rank 2 is a self-released label. It is applied before the table
    # lookup, so it overrides the label's own parent and children.
    flagged = release_labels.select("release_id", "year", "label_id")
    if self_released is not None:
        flagged = flagged.join(
            F.broadcast(
                self_released.select("label_id")
                .distinct()
                .withColumn("_is_self", F.lit(True))
            ),
            "label_id",
            "left",
        )
    else:
        flagged = flagged.withColumn("_is_self", F.lit(False))
    per_release = (
        flagged.join(classes, "label_id", "left")
        .withColumn(
            "_label_rank",
            F.when(F.coalesce(F.col("_is_self"), F.lit(False)), 2).otherwise(
                F.coalesce(F.col("_rank"), F.lit(1))
            ),
        )
        .groupBy("year", "release_id")
        .agg(F.max("_label_rank").alias("_rank"))
    )
    names = {3: "on_root_alone", 4: "in_tree", 2: "self_released", 1: "unknown_label"}
    counts = per_release.groupBy("year").agg(
        *[
            F.sum(F.when(F.col("_rank") == rank, 1).otherwise(0)).alias(name)
            for rank, name in names.items()
        ]
    )
    out = counts.withColumn(
        "n_releases",
        F.col("on_root_alone")
        + F.col("in_tree")
        + F.col("self_released")
        + F.col("unknown_label"),
    )
    for name in names.values():
        out = out.withColumn(f"share_{name}", F.col(name) / F.col("n_releases"))
    return out.select(
        "year",
        "on_root_alone",
        "in_tree",
        "self_released",
        "unknown_label",
        "n_releases",
        "share_on_root_alone",
        "share_in_tree",
        "share_self_released",
        "share_unknown_label",
    ).orderBy("year")


def credit_level_coverage(
    release_credits: DataFrame,
    roles: tuple[str, ...],
) -> DataFrame:
    """Count releases with a credit in ``roles``, by credit level and year.

    Parameters
    ----------
    release_credits : DataFrame
        Columns ``release_id``, ``year``, ``role`` and ``level``
        (``"release"`` for credits on the whole release, ``"track"`` for
        credits on single tracks).
    roles : tuple of str
        Role names to keep (exact match on ``role``), for example
        ``config.WRITING_ROLES``.

    Returns
    -------
    DataFrame
        One row per (``year``, ``level``) with ``n_releases``, the
        distinct releases that have at least one credit in ``roles`` at
        that level. A release with credits at both levels appears in
        both rows. A row with level ``"any"`` per year gives the distinct
        releases with a credit at either level, which is what
        ``top_contributors`` uses.
    """
    kept = release_credits.where(F.col("role").isin(list(roles))).select(
        "year", "release_id", "level"
    )
    by_level = kept.groupBy("year", "level").agg(
        F.countDistinct("release_id").alias("n_releases")
    )
    either = (
        kept.groupBy("year")
        .agg(F.countDistinct("release_id").alias("n_releases"))
        .withColumn("level", F.lit("any"))
        .select("year", "level", "n_releases")
    )
    return by_level.unionByName(either).orderBy("year", "level")
