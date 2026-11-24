"""RQ1 - How did genre and style composition shift, and is music more fragmented?

All functions take the ``release_styles`` (or ``release_genres``) table
and return small aggregated DataFrames that are safe to ``.toPandas()``.
``drop_prolific_releases`` is the exception: it returns a filtered copy of
the tag table, not a small aggregate.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from . import config, networks

if TYPE_CHECKING:  # only for type hints; not needed at run time
    import pandas as pd

#: Seed for the random steps in this module. ``config.SEED`` if the project
#: defines one, else 42.
_SEED: int = getattr(config, "SEED", 42)


def tag_shares_by_year(release_tags: DataFrame, tag_col: str = "style") -> DataFrame:
    """Compute each tag's share of releases in every year.

    Parameters
    ----------
    release_tags : DataFrame
        ``release_styles`` or ``release_genres``: columns ``release_id``,
        ``year`` and ``tag_col``.
    tag_col : str
        ``"style"`` or ``"genre"``.

    Returns
    -------
    DataFrame
        One row per (year, tag) with:

        * ``n_releases`` - distinct releases carrying the tag that year.
        * ``n_year`` - distinct releases with at least one tag that year.
        * ``share`` - ``n_releases / n_year``.

    Notes
    -----
    A release can carry several tags, so shares within a year add up to
    more than 1. Read ``share`` as "the fraction of that year's releases
    tagged X", not as a slice of a pie.
    """
    per_tag = release_tags.groupBy("year", tag_col).agg(
        F.countDistinct("release_id").alias("n_releases")
    )
    per_year = release_tags.groupBy("year").agg(
        F.countDistinct("release_id").alias("n_year")
    )
    return per_tag.join(per_year, "year").withColumn(
        "share", F.col("n_releases") / F.col("n_year")
    )


def top_movers(
    shares: DataFrame,
    tag_col: str = "style",
    start_year: int = 2016,
    end_year: int = 2024,
    min_releases: int = 500,
) -> DataFrame:
    """Rank tags by how much their share changed between two years.

    Parameters
    ----------
    shares : DataFrame
        Output of ``tag_shares_by_year``.
    tag_col : str
        ``"style"`` or ``"genre"``.
    start_year, end_year : int
        Years to compare. The default end year stops before the most
        recent years because those are still being catalogued (see
        ``snapshots.cataloguing_lag``).
    min_releases : int
        Drop tags with fewer releases than this in both years, so tiny
        styles do not dominate the ranking.

    Returns
    -------
    DataFrame
        One row per tag with ``share_start``, ``share_end``,
        ``change`` (percentage-point difference as a fraction) and
        ``n_start``, ``n_end``, sorted by ``change`` descending. A tag
        absent in one of the years gets share 0 for that year.
    """
    def one_year(year: int, suffix: str) -> DataFrame:
        return shares.where(F.col("year") == year).select(
            tag_col,
            F.col("share").alias(f"share_{suffix}"),
            F.col("n_releases").alias(f"n_{suffix}"),
        )

    return (
        one_year(start_year, "start")
        .join(one_year(end_year, "end"), tag_col, "outer")
        .fillna(0)
        .where((F.col("n_start") >= min_releases) | (F.col("n_end") >= min_releases))
        .withColumn("change", F.col("share_end") - F.col("share_start"))
        .orderBy(F.desc("change"))
    )


def shannon_entropy_by_year(release_tags: DataFrame, tag_col: str = "style") -> DataFrame:
    """Measure how spread out releases are across tags, per year.

    Shannon entropy is ``H = -sum(p * log2(p))`` where ``p`` is each
    tag's share of all (release, tag) pairs in the year. Higher ``H``
    means releases are spread over more tags more evenly, which is the
    report's measure of fragmentation.

    Parameters
    ----------
    release_tags : DataFrame
        ``release_styles`` or ``release_genres``.
    tag_col : str
        ``"style"`` or ``"genre"``.

    Returns
    -------
    DataFrame
        One row per year with:

        * ``n_tags`` - number of distinct tags used that year.
        * ``entropy`` - Shannon entropy in bits.
        * ``max_entropy`` - ``log2(n_tags)``, the value if every tag
          were equally common.
        * ``evenness`` - ``entropy / max_entropy`` (0 to 1). Compare
          this across years, because raw entropy rises simply when more
          tags exist.

    Notes
    -----
    Here ``p`` is taken over (release, tag) pairs so that it sums to 1,
    unlike ``share`` in ``tag_shares_by_year``.
    """
    counts = release_tags.groupBy("year", tag_col).agg(F.count("*").alias("n"))
    p = F.col("n") / F.sum("n").over(Window.partitionBy("year"))
    return (
        counts.withColumn("p", p)
        .groupBy("year")
        .agg(
            F.count("*").alias("n_tags"),
            (-F.sum(F.col("p") * F.log2("p"))).alias("entropy"),
        )
        .withColumn("max_entropy", F.log2("n_tags"))
        .withColumn(
            "evenness",
            F.when(F.col("max_entropy") > 0, F.col("entropy") / F.col("max_entropy")),
        )
        .orderBy("year")
    )


def style_cooccurrence(
    release_styles: DataFrame,
    min_count: int = 50,
) -> DataFrame:
    """Build the per-year style co-occurrence edge list.

    Two styles co-occur when the same release carries both.

    Parameters
    ----------
    release_styles : DataFrame
        Columns ``release_id``, ``year``, ``style``.
    min_count : int
        Drop pairs seen on fewer releases than this in a year.

    Returns
    -------
    DataFrame
        One row per (year, style_a, style_b) with ``style_a < style_b``
        (each pair appears once) and:

        * ``n_both`` - releases carrying both styles that year.
        * ``n_a``, ``n_b`` - releases carrying each style that year.
        * ``jaccard`` - ``n_both / (n_a + n_b - n_both)``, a 0-to-1
          strength that does not favor very common styles.
    """
    styles = release_styles.select("release_id", "year", "style").distinct()
    a = styles.withColumnRenamed("style", "style_a")
    b = styles.withColumnRenamed("style", "style_b")
    pairs = (
        a.join(b, ["release_id", "year"])
        .where(F.col("style_a") < F.col("style_b"))
        .groupBy("year", "style_a", "style_b")
        .agg(F.count("*").alias("n_both"))
        .where(F.col("n_both") >= min_count)
    )
    totals = styles.groupBy("year", "style").agg(F.count("*").alias("n"))
    return (
        pairs.join(
            totals.select("year", F.col("style").alias("style_a"), F.col("n").alias("n_a")),
            ["year", "style_a"],
        )
        .join(
            totals.select("year", F.col("style").alias("style_b"), F.col("n").alias("n_b")),
            ["year", "style_b"],
        )
        .withColumn("jaccard", F.col("n_both") / (F.col("n_a") + F.col("n_b") - F.col("n_both")))
        .select("year", "style_a", "style_b", "n_both", "n_a", "n_b", "jaccard")
    )


def cooccurrence_graph_stats(cooc: DataFrame) -> "pd.DataFrame":
    """Describe the structure of each year's style co-occurrence network.

    Parameters
    ----------
    cooc : DataFrame
        Output of ``style_cooccurrence``: columns ``year``, ``style_a``,
        ``style_b`` and ``jaccard``. It is collected to the driver, so
        build it with a ``min_count`` that keeps it to a few thousand
        edges per year.

    Returns
    -------
    pandas.DataFrame
        One row per year that has at least one edge, sorted by year, with:

        * ``n_nodes`` - styles that have at least one edge.
        * ``n_edges`` - style pairs kept.
        * ``density`` - ``n_edges`` divided by the possible number of
          edges among ``n_nodes``.
        * ``n_components`` - connected components.
        * ``largest_component_share`` - nodes in the largest component
          divided by ``n_nodes``.
        * ``modularity`` - modularity of the Louvain communities, with
          ``jaccard`` as the edge weight (seed ``_SEED``). It is 0 to 1
          for a clear community structure and can be slightly negative.

    Notes
    -----
    Nodes are only the styles that pass the ``min_count`` filter in
    ``style_cooccurrence``. Because that filter is an absolute count,
    years with fewer releases keep fewer edges, which lowers ``n_nodes``
    and ``density``. Compare years with similar release counts.
    """
    import networkx as nx
    import pandas as pd

    edges = (
        cooc.select("year", "style_a", "style_b", "jaccard")
        .toPandas()
        .sort_values(["year", "style_a", "style_b"])  # Spark row order is arbitrary; Louvain depends on it
    )
    rows = []
    for year, group in edges.groupby("year", sort=True):
        graph = nx.Graph()
        graph.add_weighted_edges_from(
            zip(group["style_a"], group["style_b"], group["jaccard"].astype(float))
        )
        components = list(nx.connected_components(graph))
        communities = nx.community.louvain_communities(graph, weight="weight", seed=_SEED)
        rows.append(
            {
                "year": int(year),
                "n_nodes": graph.number_of_nodes(),
                "n_edges": graph.number_of_edges(),
                "density": nx.density(graph),
                "n_components": len(components),
                "largest_component_share": max(len(c) for c in components)
                / graph.number_of_nodes(),
                "modularity": nx.community.modularity(graph, communities, weight="weight"),
            }
        )
    columns = [
        "year",
        "n_nodes",
        "n_edges",
        "density",
        "n_components",
        "largest_component_share",
        "modularity",
    ]
    return pd.DataFrame(rows, columns=columns)


def entropy_decomposition(
    release_tags: DataFrame,
    tag_col: str = "style",
    base_year: int = 2016,
) -> DataFrame:
    """Separate new tags from evener use of old tags in the entropy trend.

    Entropy can rise because new tags appear, or because tags that
    already existed are used more evenly. This function recomputes the
    entropy using only the tags that were used in ``base_year``.

    Parameters
    ----------
    release_tags : DataFrame
        ``release_styles`` or ``release_genres``.
    tag_col : str
        ``"style"`` or ``"genre"``.
    base_year : int
        Year that defines the set of "old" tags. Must be present in
        ``release_tags``.

    Returns
    -------
    DataFrame
        One row per year with:

        * ``entropy``, ``evenness`` - as in ``shannon_entropy_by_year``.
        * ``entropy_base_tags`` - Shannon entropy in bits over the
          (release, tag) pairs whose tag was used in ``base_year``,
          renormalized so those pairs sum to 1. Null if a year uses no
          base tag.
        * ``n_new_tags`` - tags used that year and absent in
          ``base_year``. It is 0 in ``base_year``.
        * ``new_tag_pair_share`` - share of that year's (release, tag)
          pairs that carry a new tag.

    Raises
    ------
    ValueError
        If ``release_tags`` has no rows for ``base_year``.

    Notes
    -----
    If ``entropy_base_tags`` rises like ``entropy``, the change comes
    from evener use of old tags. If it stays flat while ``entropy``
    rises, it comes from new tags. The entropy of a fixed set of tags is
    capped at ``log2`` of the set size, so a flat line can also mean the
    old tags were already close to even.
    """
    counts = release_tags.groupBy("year", tag_col).agg(F.count("*").alias("n"))
    base_tags = (
        counts.where(F.col("year") == base_year)
        .select(tag_col)
        .distinct()
        .withColumn("is_base", F.lit(True))
    )
    if base_tags.limit(1).count() == 0:
        raise ValueError(f"No rows for base_year={base_year}")

    by_year = Window.partitionBy("year")
    is_base = F.col("is_base")
    flagged = (
        counts.join(F.broadcast(base_tags), tag_col, "left")
        .withColumn("is_base", F.coalesce("is_base", F.lit(False)))
        .withColumn("total", F.sum("n").over(by_year))
        .withColumn("base_total", F.sum(F.when(is_base, F.col("n")).otherwise(0)).over(by_year))
        .withColumn("p", F.col("n") / F.col("total"))
        .withColumn("p_base", F.when(is_base, F.col("n") / F.col("base_total")))
    )
    return (
        flagged.groupBy("year")
        .agg(
            F.count("*").alias("n_tags"),
            (-F.sum(F.col("p") * F.log2("p"))).alias("entropy"),
            (-F.sum(F.col("p_base") * F.log2("p_base"))).alias("entropy_base_tags"),
            F.sum(F.when(is_base, 0).otherwise(1)).alias("n_new_tags"),
            (
                F.sum(F.when(is_base, 0).otherwise(F.col("n"))) / F.sum("n")
            ).alias("new_tag_pair_share"),
        )
        .withColumn(
            "evenness",
            F.when(F.col("n_tags") > 1, F.col("entropy") / F.log2("n_tags")),
        )
        .select(
            "year",
            "entropy",
            "evenness",
            "entropy_base_tags",
            "n_new_tags",
            "new_tag_pair_share",
        )
        .orderBy("year")
    )


def entropy_sensitivity(
    release_tags: DataFrame,
    releases: DataFrame,
    tag_col: str = "style",
    n_draws: int = 5,
    seed: int = _SEED,
) -> DataFrame:
    """Test whether evenness depends on how many releases a year has.

    Recent years have fewer releases because they are still being
    catalogued (and 2026 is partial). Evenness over fewer releases can
    differ only because of the smaller sample. This function draws, for
    every year, random samples of the same size and recomputes evenness
    on them.

    Parameters
    ----------
    release_tags : DataFrame
        ``release_styles`` or ``release_genres``.
    releases : DataFrame
        The ``releases`` table. Only ``release_id`` and ``year`` are
        used. All releases, tagged or not, define the sample size and
        the population that is sampled.
    tag_col : str
        ``"style"`` or ``"genre"``.
    n_draws : int
        Number of random samples per year.
    seed : int
        Seed for the random ordering of releases.

    Returns
    -------
    DataFrame
        One row per year with:

        * ``evenness_full`` - evenness on all releases, as in
          ``shannon_entropy_by_year``.
        * ``evenness_sub_mean``, ``evenness_sub_min``,
          ``evenness_sub_max`` - mean, minimum and maximum evenness over
          the ``n_draws`` samples.

    Raises
    ------
    ValueError
        If ``n_draws`` is below 1, or ``releases`` has no rows in the
        full years ``config.YEAR_MIN``-``config.LAST_FULL_YEAR``.

    Notes
    -----
    The sample size ``m`` is the number of distinct releases in the
    smallest full year in ``config.YEAR_MIN``-``config.LAST_FULL_YEAR``,
    counted over all of ``releases``. Each sample takes ``m`` releases
    per year, then keeps only their (release, tag) pairs, so releases
    without tags count towards ``m`` but add no pairs. A year with
    fewer than ``m`` releases is used whole, so its subsampled values
    equal ``evenness_full``. All draws share one Spark plan: distinct
    ``(release_id, year)`` rows are cross-joined with the draw ids
    ``0..n_draws-1``, given one ``F.rand(seed)`` value per row, and the
    ``m`` lowest values per ``(draw, year)`` are kept. The tags are
    joined and counted once for all draws. No ids reach the driver.
    """
    if n_draws < 1:
        raise ValueError("n_draws must be at least 1")

    ids = releases.where(F.col("year").isNotNull()).select("release_id", "year").distinct()
    smallest = (
        ids.where(F.col("year").between(config.YEAR_MIN, config.LAST_FULL_YEAR))
        .groupBy("year")
        .count()
        .agg(F.min("count"))
        .first()[0]
    )
    if smallest is None:
        raise ValueError("No releases in the full years of the study window")

    draw_ids = releases.sparkSession.range(n_draws).select(F.col("id").cast("int").alias("draw"))
    ranked = (
        ids.crossJoin(F.broadcast(draw_ids))
        .withColumn("_r", F.rand(seed))
        .withColumn(
            "_rank",
            F.row_number().over(Window.partitionBy("draw", "year").orderBy("_r", "release_id")),
        )
        .where(F.col("_rank") <= smallest)
        .select("draw", "release_id", "year")
    )
    counts = (
        ranked.join(release_tags.select("release_id", "year", tag_col), ["release_id", "year"])
        .groupBy("draw", "year", tag_col)
        .agg(F.count("*").alias("n"))
    )
    p = F.col("n") / F.sum("n").over(Window.partitionBy("draw", "year"))
    per_draw = (
        counts.withColumn("p", p)
        .groupBy("draw", "year")
        .agg(
            F.count("*").alias("n_tags"),
            (-F.sum(F.col("p") * F.log2("p"))).alias("entropy"),
        )
        .withColumn("max_entropy", F.log2("n_tags"))
        .withColumn(
            "evenness",
            F.when(F.col("max_entropy") > 0, F.col("entropy") / F.col("max_entropy")),
        )
    )
    sub = per_draw.groupBy("year").agg(
        F.avg("evenness").alias("evenness_sub_mean"),
        F.min("evenness").alias("evenness_sub_min"),
        F.max("evenness").alias("evenness_sub_max"),
    )
    full = shannon_entropy_by_year(release_tags, tag_col).select(
        "year", F.col("evenness").alias("evenness_full")
    )
    return full.join(sub, "year", "left").orderBy("year")


def prolific_artist_years(
    release_artists: DataFrame,
    min_per_year: int = config.PROLIFIC_ARTIST_MIN_PER_YEAR,
    exclude_ids: tuple[int, ...] = networks.PLACEHOLDER_ARTIST_IDS,
) -> DataFrame:
    """Find the (artist, year) pairs with a very large number of releases.

    A few real artists self-release thousands of albums a year, which can
    make up a large part of a small style.

    Parameters
    ----------
    release_artists : DataFrame
        Columns ``release_id``, ``year``, ``artist_id``, ``artist_name``
        (one row per release and main artist).
    min_per_year : int
        Keep pairs with at least this many distinct releases. Defaults
        to ``config.PROLIFIC_ARTIST_MIN_PER_YEAR``.
    exclude_ids : tuple of int
        Artist ids to ignore, such as "Various". Defaults to
        ``networks.PLACEHOLDER_ARTIST_IDS``.

    Returns
    -------
    DataFrame
        One row per prolific (artist, year) with ``artist_id``,
        ``artist_name`` (the alphabetically first name seen), ``year``
        and ``n_releases`` (distinct releases), sorted by ``n_releases``
        descending.
    """
    return (
        release_artists.where(
            F.col("artist_id").isNotNull()
            & F.col("year").isNotNull()
            & ~F.col("artist_id").isin(list(exclude_ids))
        )
        .groupBy("artist_id", "year")
        .agg(
            F.min("artist_name").alias("artist_name"),
            F.countDistinct("release_id").alias("n_releases"),
        )
        .where(F.col("n_releases") >= min_per_year)
        .select("artist_id", "artist_name", "year", "n_releases")
        .orderBy(F.desc("n_releases"), F.asc("artist_id"), F.asc("year"))
    )


def drop_prolific_releases(
    release_tags: DataFrame,
    release_artists: DataFrame,
    prolific: DataFrame,
) -> DataFrame:
    """Remove the releases of prolific (artist, year) pairs from a tag table.

    This is a robustness check, not the main definition. Run the
    analysis on the full table first, then on this one. If a style's
    trend shrinks a lot without these releases, it reflects one artist's
    output and not a scene.

    Parameters
    ----------
    release_tags : DataFrame
        ``release_styles`` or ``release_genres``: columns ``release_id``
        and ``year``, plus any others.
    release_artists : DataFrame
        Columns ``release_id``, ``year``, ``artist_id``. Main artists
        only, as in the ``release_artists`` table.
    prolific : DataFrame
        Output of ``prolific_artist_years``: columns ``artist_id`` and
        ``year``.

    Returns
    -------
    DataFrame
        ``release_tags`` without the rows of any release whose main
        artists include a pair in ``prolific``. A release is matched on
        ``release_id`` and ``year``, and the artist's year must be the
        pair's year. The same artist's releases in other years are kept.
        Columns are unchanged.
    """
    pairs = prolific.select("artist_id", "year").distinct()
    dropped = (
        release_artists.select("release_id", "year", "artist_id")
        .join(pairs, ["artist_id", "year"])
        .select("release_id", "year")
        .distinct()
    )
    return release_tags.join(dropped, ["release_id", "year"], "left_anti")
