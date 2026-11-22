"""RQ1 - How did genre and style composition shift, and is music more fragmented?

All functions take the ``release_styles`` (or ``release_genres``) table
and return small aggregated DataFrames that are safe to ``.toPandas()``.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


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
