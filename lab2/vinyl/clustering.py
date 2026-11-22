"""RQ4 - Cluster artists on what they release and who they work with.

Pipeline::

    artist_profile  --+
    style_tfidf     --+--> combine_features --> reduce_umap --> cluster_hdbscan
    node2vec        --+

``artist_profile`` and ``style_tfidf`` run in Spark. The rest runs on the
driver with pandas / NumPy after the artist set has been cut down (for
example to artists with at least N releases in the window).

Status: ``node2vec_embeddings`` and ``combine_features`` are stubs. Their
contracts are documented so the notebooks can be written against them.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

if TYPE_CHECKING:  # only for type hints; not needed at run time
    import numpy as np
    import pandas as pd


# --------------------------------------------------------------------------
# Spark side: features
# --------------------------------------------------------------------------

def artist_profile(
    release_artists: DataFrame,
    release_formats: DataFrame,
    release_labels: DataFrame,
    min_releases: int = 5,
    formats: tuple[str, ...] = ("Vinyl", "CD", "Cassette", "File"),
) -> DataFrame:
    """Build one row of summary features per artist.

    Parameters
    ----------
    release_artists : DataFrame
        Columns ``release_id``, ``year``, ``artist_id``, ``artist_name``.
    release_formats : DataFrame
        Columns ``release_id``, ``format_name``.
    release_labels : DataFrame
        Columns ``release_id``, ``label_id``.
    min_releases : int
        Drop artists with fewer releases than this in the study window.
    formats : tuple of str
        Formats that each get a share column.

    Returns
    -------
    DataFrame
        One row per artist with:

        * ``artist_id``, ``artist_name``
        * ``n_releases`` - distinct releases in the window.
        * ``first_year``, ``last_year``, ``career_span`` (years active in
          the window, ``last_year - first_year + 1``).
        * ``n_labels`` - distinct labels released on.
        * ``share_<format>`` - fraction of the artist's releases in each
          format of ``formats`` (lower-cased, e.g. ``share_vinyl``).

    Notes
    -----
    Career span only covers the study window (2016-2026), not the
    artist's whole career.
    """
    artists = release_artists.where(F.col("artist_id").isNotNull()).select(
        "release_id", "year", "artist_id", "artist_name"
    ).distinct()

    base = (
        artists.groupBy("artist_id")
        .agg(
            F.first("artist_name", ignorenulls=True).alias("artist_name"),
            F.countDistinct("release_id").alias("n_releases"),
            F.min("year").alias("first_year"),
            F.max("year").alias("last_year"),
        )
        .where(F.col("n_releases") >= min_releases)
        .withColumn("career_span", F.col("last_year") - F.col("first_year") + 1)
    )

    n_labels = (
        artists.join(release_labels.select("release_id", "label_id"), "release_id")
        .groupBy("artist_id")
        .agg(F.countDistinct("label_id").alias("n_labels"))
    )

    with_formats = artists.join(
        release_formats.select("release_id", "format_name").distinct(), "release_id"
    )
    format_counts = with_formats.groupBy("artist_id").agg(
        *[
            F.countDistinct(
                F.when(F.col("format_name") == name, F.col("release_id"))
            ).alias(f"_n_{name.lower()}")
            for name in formats
        ]
    )

    result = base.join(n_labels, "artist_id", "left").join(format_counts, "artist_id", "left")
    for name in formats:
        key = name.lower()
        result = result.withColumn(
            f"share_{key}",
            F.coalesce(F.col(f"_n_{key}"), F.lit(0)) / F.col("n_releases"),
        ).drop(f"_n_{key}")
    return result.fillna({"n_labels": 0})


def style_tfidf(
    release_artists: DataFrame,
    release_styles: DataFrame,
    min_artists: int = 20,
) -> DataFrame:
    """Compute TF-IDF weights of styles per artist (long format).

    Each artist is treated as a "document" and each style as a "word".
    A style gets a high weight for an artist when the artist uses it a
    lot (TF) and few other artists use it at all (IDF).

    Parameters
    ----------
    release_artists : DataFrame
        Columns ``release_id``, ``artist_id``. Filter it to the artists
        of interest first (for example those kept by ``artist_profile``).
    release_styles : DataFrame
        Columns ``release_id``, ``style``.
    min_artists : int
        Drop styles used by fewer artists than this.

    Returns
    -------
    DataFrame
        One row per (artist, style) with:

        * ``tf`` - share of the artist's (release, style) pairs with
          this style.
        * ``idf`` - ``ln(n_artists / artists_using_style)``.
        * ``tfidf`` - ``tf * idf``.

        Pivot or convert to a sparse matrix on the driver before
        clustering.
    """
    pairs = (
        release_artists.where(F.col("artist_id").isNotNull())
        .select("release_id", "artist_id")
        .distinct()
        .join(release_styles.select("release_id", "style").distinct(), "release_id")
    )
    counts = pairs.groupBy("artist_id", "style").agg(F.count("*").alias("n"))
    totals = counts.groupBy("artist_id").agg(F.sum("n").alias("n_total"))
    n_artists = totals.count()
    doc_freq = (
        counts.groupBy("style")
        .agg(F.count("*").alias("df"))
        .where(F.col("df") >= min_artists)
        .withColumn("idf", F.log(F.lit(float(n_artists)) / F.col("df")))
    )
    return (
        counts.join(totals, "artist_id")
        .join(doc_freq, "style")
        .withColumn("tf", F.col("n") / F.col("n_total"))
        .withColumn("tfidf", F.col("tf") * F.col("idf"))
        .select("artist_id", "style", "tf", "idf", "tfidf")
    )


# --------------------------------------------------------------------------
# Driver side: embeddings, reduction, clustering
# --------------------------------------------------------------------------

def node2vec_embeddings(
    edges: "pd.DataFrame",
    dimensions: int = 64,
    walk_length: int = 30,
    num_walks: int = 10,
    p: float = 1.0,
    q: float = 1.0,
    seed: int = 42,
) -> "pd.DataFrame":
    """Learn a vector for every node of a graph with node2vec. (STUB)

    node2vec runs random walks over the graph and trains word2vec on
    them, so nodes that sit in the same neighbourhood get similar
    vectors.

    Parameters
    ----------
    edges : pandas.DataFrame
        Edge list with columns ``src``, ``dst``, ``weight``.
    dimensions : int
        Length of each vector.
    walk_length : int
        Steps per random walk.
    num_walks : int
        Walks started from every node.
    p, q : float
        Return and in-out parameters. ``q < 1`` favours exploring
        outward (community structure); ``q > 1`` stays local (role).
    seed : int
        Random seed.

    Returns
    -------
    pandas.DataFrame
        One row per node: ``node`` plus columns ``n2v_0`` ...
        ``n2v_<dimensions-1>``.

    Raises
    ------
    NotImplementedError
        Always, until the library is chosen. TODO: pick between
        ``pecanpy`` (fast, driver side), ``node2vec`` (simple, slow) or
        Spark ``Word2Vec`` over walks generated in Spark (scales out).
    """
    raise NotImplementedError("node2vec_embeddings: choose a library and implement.")


def combine_features(
    profile: "pd.DataFrame",
    tfidf: "pd.DataFrame",
    embeddings: "pd.DataFrame",
    weights: tuple[float, float, float] = (1.0, 1.0, 1.0),
) -> tuple["np.ndarray", "pd.Series"]:
    """Join the three feature blocks into one matrix. (STUB)

    Parameters
    ----------
    profile : pandas.DataFrame
        Collected ``artist_profile`` output.
    tfidf : pandas.DataFrame
        Collected ``style_tfidf`` output (long format).
    embeddings : pandas.DataFrame
        Output of ``node2vec_embeddings`` with ``node`` = artist id.
    weights : tuple of float
        Relative weight of the (profile, tfidf, embedding) blocks after
        each block has been scaled.

    Returns
    -------
    tuple of (numpy.ndarray, pandas.Series)
        The feature matrix (one row per artist) and the matching artist
        ids in row order.

    Raises
    ------
    NotImplementedError
        Always, for now. TODO: decide scaling (standardize the profile
        block, L2-normalize TF-IDF and embeddings), whether to reduce
        the TF-IDF block with truncated SVD first, and the block weights.
    """
    raise NotImplementedError("combine_features: decide scaling and block weights.")


def reduce_umap(
    features: "np.ndarray",
    n_components: int = 2,
    n_neighbors: int = 30,
    min_dist: float = 0.0,
    metric: str = "cosine",
    seed: int = 42,
) -> "np.ndarray":
    """Reduce a feature matrix to a few dimensions with UMAP.

    Parameters
    ----------
    features : numpy.ndarray
        Matrix of shape (n_artists, n_features).
    n_components : int
        Output dimensions. Use 2 for plotting; 5 to 15 usually clusters
        better.
    n_neighbors : int
        Size of the local neighbourhood. Larger values keep more of the
        global structure.
    min_dist : float
        Minimum spacing of points. 0 packs clusters tightly, which is
        what HDBSCAN wants.
    metric : str
        Distance used in the input space.
    seed : int
        Random seed.

    Returns
    -------
    numpy.ndarray
        Matrix of shape (n_artists, n_components).

    Notes
    -----
    Requires the ``umap-learn`` package.
    """
    import umap

    reducer = umap.UMAP(
        n_components=n_components,
        n_neighbors=n_neighbors,
        min_dist=min_dist,
        metric=metric,
        random_state=seed,
    )
    return reducer.fit_transform(features)


def cluster_hdbscan(
    embedding: "np.ndarray",
    min_cluster_size: int = 50,
    min_samples: int | None = None,
) -> "np.ndarray":
    """Cluster points with HDBSCAN.

    HDBSCAN finds dense groups of any shape, picks the number of
    clusters itself, and leaves sparse points unassigned.

    Parameters
    ----------
    embedding : numpy.ndarray
        Usually the output of ``reduce_umap``.
    min_cluster_size : int
        Smallest group that counts as a cluster.
    min_samples : int, optional
        How conservative the clustering is; higher values mark more
        points as noise. Defaults to ``min_cluster_size``.

    Returns
    -------
    numpy.ndarray
        One integer label per row. ``-1`` means noise (no cluster).

    Notes
    -----
    Uses ``sklearn.cluster.HDBSCAN`` (scikit-learn 1.3 or newer).
    """
    from sklearn.cluster import HDBSCAN

    return HDBSCAN(min_cluster_size=min_cluster_size, min_samples=min_samples).fit_predict(
        embedding
    )
