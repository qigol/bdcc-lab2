"""RQ4 - What communities of artists, producers, and labels emerge?

Two halves:

* **Spark side** - ``*_edges`` functions build weighted edge lists from
  the normalized tables. These stay distributed.
* **Driver side** - once an edge list is filtered small enough to
  collect, ``to_igraph`` turns it into an ``igraph`` graph for
  centrality and Leiden community detection.

Driver-side functions need ``python-igraph`` (Leiden is built in) and
``scikit-learn``. They are imported inside the functions so the Spark
side works without them.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from . import config

if TYPE_CHECKING:  # only for type hints; not needed at run time
    import igraph
    import pandas as pd

#: Discogs placeholder artist ids that are not real collaborators:
#: 194 = "Various", 355 = "Unknown Artist". TODO: confirm in the artists table.
PLACEHOLDER_ARTIST_IDS: tuple[int, ...] = (194, 355)


# --------------------------------------------------------------------------
# Spark side: edge lists
# --------------------------------------------------------------------------

def collaboration_edges(
    release_artists: DataFrame,
    min_weight: int = 2,
    max_artists_per_release: int = 20,
    exclude_ids: tuple[int, ...] = PLACEHOLDER_ARTIST_IDS,
) -> DataFrame:
    """Build the artist-artist collaboration edge list.

    Two artists are linked when they are both main artists of the same
    release.

    Parameters
    ----------
    release_artists : DataFrame
        Columns ``release_id``, ``artist_id``.
    min_weight : int
        Keep only pairs that share at least this many releases.
    max_artists_per_release : int
        Skip releases with more main artists than this. A release with
        k artists creates k*(k-1)/2 pairs, so very large compilations
        would swamp the graph.
    exclude_ids : tuple of int
        Artist ids to drop before pairing.

    Returns
    -------
    DataFrame
        One row per undirected pair: ``src``, ``dst`` (artist ids with
        ``src < dst``) and ``weight`` (shared releases).
    """
    artists = (
        release_artists.where(F.col("artist_id").isNotNull())
        .where(~F.col("artist_id").isin(list(exclude_ids)))
        .select("release_id", "artist_id")
        .distinct()
    )
    small = (
        artists.groupBy("release_id")
        .agg(F.count("*").alias("k"))
        .where(F.col("k").between(2, max_artists_per_release))
        .select("release_id")
    )
    artists = artists.join(small, "release_id")
    a = artists.withColumnRenamed("artist_id", "src")
    b = artists.withColumnRenamed("artist_id", "dst")
    return (
        a.join(b, "release_id")
        .where(F.col("src") < F.col("dst"))
        .groupBy("src", "dst")
        .agg(F.count("*").alias("weight"))
        .where(F.col("weight") >= min_weight)
    )


def credit_edges(
    release_artists: DataFrame,
    release_credits: DataFrame,
    roles: tuple[str, ...] = config.PRODUCTION_ROLES,
    min_weight: int = 2,
    exclude_ids: tuple[int, ...] = PLACEHOLDER_ARTIST_IDS,
) -> DataFrame:
    """Build the artist-to-producer/engineer credit edge list.

    A main artist is linked to a credited person when that person holds
    one of ``roles`` on the artist's release.

    Parameters
    ----------
    release_artists : DataFrame
        Columns ``release_id``, ``artist_id``.
    release_credits : DataFrame
        Columns ``release_id``, ``artist_id``, ``role``.
    roles : tuple of str
        Roles to keep (exact match).
    min_weight : int
        Keep only pairs that share at least this many releases.
    exclude_ids : tuple of int
        Artist ids to drop on both sides.

    Returns
    -------
    DataFrame
        One row per directed pair: ``src`` (main artist id), ``dst``
        (credited person's artist id) and ``weight`` (releases). Self
        credits (an artist producing their own release) are removed.
    """
    excluded = list(exclude_ids)
    mains = (
        release_artists.where(F.col("artist_id").isNotNull())
        .where(~F.col("artist_id").isin(excluded))
        .select("release_id", F.col("artist_id").alias("src"))
        .distinct()
    )
    credited = (
        release_credits.where(F.col("role").isin(list(roles)))
        .where(F.col("artist_id").isNotNull())
        .where(~F.col("artist_id").isin(excluded))
        .select("release_id", F.col("artist_id").alias("dst"))
        .distinct()
    )
    return (
        mains.join(credited, "release_id")
        .where(F.col("src") != F.col("dst"))
        .groupBy("src", "dst")
        .agg(F.count("*").alias("weight"))
        .where(F.col("weight") >= min_weight)
    )


def artist_label_edges(
    release_artists: DataFrame,
    release_labels: DataFrame,
    min_weight: int = 2,
    exclude_ids: tuple[int, ...] = PLACEHOLDER_ARTIST_IDS,
) -> DataFrame:
    """Build the bipartite artist-label edge list.

    Parameters
    ----------
    release_artists : DataFrame
        Columns ``release_id``, ``artist_id``.
    release_labels : DataFrame
        Columns ``release_id``, ``label_id``.
    min_weight : int
        Keep only pairs with at least this many releases.
    exclude_ids : tuple of int
        Artist ids to drop.

    Returns
    -------
    DataFrame
        One row per pair: ``artist_id``, ``label_id`` and ``weight``
        (distinct releases by the artist on the label).

    Notes
    -----
    Artist ids and label ids overlap as numbers. Prefix them
    (``"a123"``, ``"l123"``) before putting both in one graph.
    """
    artists = release_artists.where(
        F.col("artist_id").isNotNull() & ~F.col("artist_id").isin(list(exclude_ids))
    ).select("release_id", "artist_id")
    labels = release_labels.where(F.col("label_id").isNotNull()).select(
        "release_id", "label_id"
    )
    return (
        artists.join(labels, "release_id")
        .groupBy("artist_id", "label_id")
        .agg(F.countDistinct("release_id").alias("weight"))
        .where(F.col("weight") >= min_weight)
    )


# --------------------------------------------------------------------------
# Driver side: graph analysis
# --------------------------------------------------------------------------

def to_igraph(
    edges: "pd.DataFrame",
    source: str = "src",
    target: str = "dst",
    weight: str = "weight",
    directed: bool = False,
) -> "igraph.Graph":
    """Turn a collected edge list into an igraph graph.

    Parameters
    ----------
    edges : pandas.DataFrame
        Edge list, for example ``collaboration_edges(...).toPandas()``.
        Check ``.count()`` first; aim for a few million edges at most.
    source, target, weight : str
        Column names in ``edges``.
    directed : bool
        Whether edges have a direction.

    Returns
    -------
    igraph.Graph
        Vertex attribute ``name`` holds the original id; edge attribute
        ``weight`` holds the weight.
    """
    import igraph as ig

    return ig.Graph.DataFrame(
        edges[[source, target, weight]].rename(columns={weight: "weight"}),
        directed=directed,
        use_vids=False,
    )


def centrality_table(graph: "igraph.Graph", betweenness: bool = False) -> "pd.DataFrame":
    """Compute per-node centrality measures.

    Parameters
    ----------
    graph : igraph.Graph
        Output of ``to_igraph``.
    betweenness : bool
        Also compute betweenness. It is slow on large graphs, so it is
        off by default.

    Returns
    -------
    pandas.DataFrame
        One row per node with:

        * ``node`` - original id.
        * ``degree`` - number of neighbours.
        * ``strength`` - sum of edge weights.
        * ``pagerank`` - weighted PageRank.
        * ``betweenness`` - only if requested (unweighted).
    """
    import pandas as pd

    table = pd.DataFrame(
        {
            "node": graph.vs["name"],
            "degree": graph.degree(),
            "strength": graph.strength(weights="weight"),
            "pagerank": graph.pagerank(weights="weight"),
        }
    )
    if betweenness:
        table["betweenness"] = graph.betweenness()
    return table


def leiden_communities(
    graph: "igraph.Graph",
    resolution: float = 1.0,
    seed: int = 42,
    n_iterations: int = -1,
) -> "pd.DataFrame":
    """Detect communities with the Leiden algorithm (modularity).

    Parameters
    ----------
    graph : igraph.Graph
        Undirected graph from ``to_igraph``.
    resolution : float
        Higher values give more, smaller communities.
    seed : int
        Random seed, so the report is reproducible.
    n_iterations : int
        Number of Leiden passes; -1 runs until the result stops changing.

    Returns
    -------
    pandas.DataFrame
        One row per node with ``node``, ``community`` (integer label)
        and ``community_size``. The overall modularity is stored in
        ``result.attrs["modularity"]``.
    """
    import random

    import pandas as pd

    random.seed(seed)  # igraph draws from Python's random module
    partition = graph.community_leiden(
        objective_function="modularity",
        weights="weight",
        resolution=resolution,
        n_iterations=n_iterations,
    )
    table = pd.DataFrame({"node": graph.vs["name"], "community": partition.membership})
    table["community_size"] = table.groupby("community")["node"].transform("size")
    table.attrs["modularity"] = partition.modularity
    return table


def community_tag_agreement(
    communities: "pd.DataFrame",
    node_tags: "pd.DataFrame",
    tag_col: str = "genre",
) -> dict[str, float]:
    """Score how well detected communities line up with genre tags.

    Parameters
    ----------
    communities : pandas.DataFrame
        Output of ``leiden_communities`` (``node``, ``community``).
    node_tags : pandas.DataFrame
        One row per node with ``node`` and its single dominant tag in
        ``tag_col`` (for example each artist's most frequent genre).
    tag_col : str
        Name of the tag column.

    Returns
    -------
    dict
        * ``n_nodes`` - nodes present in both inputs.
        * ``nmi`` - normalized mutual information, 0 (unrelated) to 1
          (communities and tags are the same grouping).
        * ``ari`` - adjusted Rand index, about 0 for random agreement,
          1 for identical groupings.
        * ``purity`` - share of nodes whose tag is their community's
          most common tag.
    """
    from sklearn.metrics import adjusted_rand_score, normalized_mutual_info_score

    merged = communities.merge(node_tags[["node", tag_col]], on="node").dropna(
        subset=[tag_col]
    )
    majority = merged.groupby("community")[tag_col].agg(lambda s: s.value_counts().iloc[0])
    return {
        "n_nodes": int(len(merged)),
        "nmi": float(normalized_mutual_info_score(merged[tag_col], merged["community"])),
        "ari": float(adjusted_rand_score(merged[tag_col], merged["community"])),
        "purity": float(majority.sum() / len(merged)) if len(merged) else float("nan"),
    }
