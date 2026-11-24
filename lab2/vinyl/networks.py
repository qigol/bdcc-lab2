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

from typing import TYPE_CHECKING, Iterable

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from . import config

if TYPE_CHECKING:  # only for type hints; not needed at run time
    import igraph
    import pandas as pd

#: Discogs placeholder artist ids that are not real collaborators:
#: 194 = "Various", 355 = "Unknown Artist". Confirmed in T04: neither id is
#: in the artists file; their names come from ``release_artists``.
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
    exclude_label_ids: "Iterable[int] | DataFrame" = (),
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
    exclude_label_ids : iterable of int or DataFrame
        Label ids to drop, for example the self-released placeholders.
        Without this, "Not On Label" becomes one giant hub linked to most
        artists. Pass a DataFrame with a ``label_id`` column (such as
        ``labels.self_released_label_ids(labels)``) for long lists: it is
        applied as a left anti join. A Python list goes into ``isin``,
        which is fine for a few ids but very slow for the 312,527
        self-released labels in 2026 (30 minutes instead of 2).

    Returns
    -------
    DataFrame
        One row per pair: ``artist_id``, ``label_id`` and ``weight``
        (distinct releases by the artist on the label).

    Notes
    -----
    Artist ids and label ids overlap as numbers. Prefix them
    (``"a123"``, ``"l123"``) before putting both in one graph; see
    ``prefixed_bipartite``.
    """
    artists = release_artists.where(
        F.col("artist_id").isNotNull() & ~F.col("artist_id").isin(list(exclude_ids))
    ).select("release_id", "artist_id")
    labels = release_labels.where(F.col("label_id").isNotNull()).select(
        "release_id", "label_id"
    )
    if isinstance(exclude_label_ids, DataFrame):
        labels = labels.join(
            exclude_label_ids.select("label_id").distinct(), "label_id", "left_anti"
        )
    else:
        excluded_labels = [int(i) for i in exclude_label_ids]
        if excluded_labels:
            labels = labels.where(~F.col("label_id").isin(excluded_labels))
    return (
        artists.join(labels, "release_id")
        .groupBy("artist_id", "label_id")
        .agg(F.countDistinct("release_id").alias("weight"))
        .where(F.col("weight") >= min_weight)
    )


def prefixed_bipartite(edges: DataFrame) -> DataFrame:
    """Give the artist-label edges one id space, ready for ``to_igraph``.

    Artist ids and label ids overlap as integers, so a plain edge list
    would merge artist 5 and label 5 into one node.

    Parameters
    ----------
    edges : DataFrame
        Output of ``artist_label_edges``: ``artist_id``, ``label_id``,
        ``weight``.

    Returns
    -------
    DataFrame
        ``src`` (``"a<artist_id>"``), ``dst`` (``"l<label_id>"``) and
        ``weight``. Collect it with ``.toPandas()`` for ``to_igraph``.
    """
    return edges.select(
        F.concat(F.lit("a"), F.col("artist_id").cast("string")).alias("src"),
        F.concat(F.lit("l"), F.col("label_id").cast("string")).alias("dst"),
        F.col("weight"),
    )


def _names_of(
    table: DataFrame,
    id_col: str,
    name_col: str,
    prefix: str | None,
    ids: list[int] | None,
) -> DataFrame:
    """Return ``node`` and ``name`` for one entity table (see ``node_names``)."""
    named = (
        table.where(F.col(id_col).isNotNull())
        .select(F.col(id_col).cast("long").alias("_id"), F.col(name_col).alias("name"))
    )
    if ids is not None:
        keys = table.sparkSession.createDataFrame([(i,) for i in set(ids)], "_id long")
        named = named.join(keys, "_id")
    named = named.dropDuplicates(["_id"])
    node = F.col("_id") if prefix is None else F.concat(F.lit(prefix), F.col("_id").cast("string"))
    return named.select(node.alias("node"), "name")


def node_names(
    artists: DataFrame,
    labels: DataFrame | None = None,
    prefixed: bool = False,
    ids: Iterable[int | str] | None = None,
) -> "pd.DataFrame":
    """Collect display names for graph nodes.

    Parameters
    ----------
    artists : DataFrame
        Columns ``artist_id`` and ``artist_name``. The artists table
        works; so does ``release_artists`` (rows are deduplicated by id).
    labels : DataFrame, optional
        Columns ``label_id`` and ``label_name``. Use it with
        ``prefixed=True`` for the artist-label graph.
    prefixed : bool
        If True, nodes are ``"a<artist_id>"`` and ``"l<label_id>"``
        (strings). If False they are the plain integer ids; then artist
        and label ids with the same number share one ``node`` value, so
        pass ``labels`` only with ``prefixed=True``.
    ids : iterable of int or str, optional
        The nodes you need, as they appear in the graph
        (``graph.vs["name"]``): prefixed strings when ``prefixed`` is
        True, integers otherwise. Only these names are collected. Pass
        it for any real table; without it every artist name is collected
        (millions of rows).

    Returns
    -------
    pandas.DataFrame
        One row per node found, with ``node`` and ``name``. Nodes missing
        from the tables are absent; ``describe_communities`` falls back to
        the id for them.
    """
    import pandas as pd

    artist_ids = label_ids = None
    if ids is not None:
        listed = list(ids)
        if prefixed:
            artist_ids = [int(i[1:]) for i in listed if str(i).startswith("a")]
            label_ids = [int(i[1:]) for i in listed if str(i).startswith("l")]
        else:
            artist_ids = label_ids = [int(i) for i in listed]
    frames = [
        _names_of(artists, "artist_id", "artist_name", "a" if prefixed else None, artist_ids)
    ]
    if labels is not None:
        frames.append(
            _names_of(labels, "label_id", "label_name", "l" if prefixed else None, label_ids)
        )
    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame)
    out = combined.toPandas()
    return out if len(out) else pd.DataFrame({"node": [], "name": []})


def artist_dominant_tag(
    release_artists: DataFrame,
    release_tags: DataFrame,
    tag_col: str,
) -> DataFrame:
    """Find the tag that sits on most of each artist's releases.

    Works for genres and for styles.

    Parameters
    ----------
    release_artists : DataFrame
        Columns ``release_id``, ``artist_id``.
    release_tags : DataFrame
        Columns ``release_id`` and ``tag_col`` (for example
        ``release_genres`` or ``release_styles``).
    tag_col : str
        Name of the tag column, ``"genre"`` or ``"style"``.

    Returns
    -------
    DataFrame
        One row per artist that has at least one tagged release:

        * ``artist_id``.
        * ``<tag_col>`` - the tag on the most releases of the artist.
          Ties go to the alphabetically first tag.
        * ``tag_share`` - releases of the artist carrying that tag,
          divided by ``n_releases``.
        * ``n_releases`` - the artist's distinct releases in
          ``release_artists`` (tagged or not).

    Notes
    -----
    A release counts once per artist and tag, even if it is listed
    twice. Releases without a tag count in ``n_releases`` but give no
    tag, so for styles (many releases have none) ``tag_share`` can be
    well below 1 for an artist whose tagged releases all agree.
    """
    pairs = (
        release_artists.where(F.col("artist_id").isNotNull())
        .select("release_id", "artist_id")
        .distinct()
    )
    totals = pairs.groupBy("artist_id").agg(F.count("*").alias("n_releases"))
    tags = (
        release_tags.where(F.col(tag_col).isNotNull())
        .select("release_id", tag_col)
        .distinct()
    )
    counts = pairs.join(tags, "release_id").groupBy("artist_id", tag_col).agg(
        F.count("*").alias("n_tagged")
    )
    rank = Window.partitionBy("artist_id").orderBy(F.desc("n_tagged"), F.asc(tag_col))
    top = counts.withColumn("_rank", F.row_number().over(rank)).where(F.col("_rank") == 1)
    return top.join(totals, "artist_id").select(
        "artist_id",
        tag_col,
        (F.col("n_tagged") / F.col("n_releases")).alias("tag_share"),
        "n_releases",
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


def graph_artist_ids(graph: "igraph.Graph", prefixed: bool = False) -> list[int]:
    """List the artist ids that are nodes of a graph.

    Parameters
    ----------
    graph : igraph.Graph
        Output of ``to_igraph``.
    prefixed : bool
        True for the artist-label graph (nodes ``"a123"`` / ``"l123"``);
        only the ``"a"`` nodes are returned. False when every node is an
        artist id.

    Returns
    -------
    list of int
        Artist ids, in vertex order.
    """
    names = graph.vs["name"]
    if prefixed:
        return [int(n[1:]) for n in names if str(n).startswith("a")]
    return [int(n) for n in names]


def describe_communities(
    communities: "pd.DataFrame",
    centrality: "pd.DataFrame",
    names: "pd.DataFrame",
    node_tags: "pd.DataFrame | None" = None,
    top_n: int = 5,
    min_size: int = 50,
    tag_col: str | None = None,
) -> "pd.DataFrame":
    """Summarize each large community with its key nodes and tags.

    Parameters
    ----------
    communities : pandas.DataFrame
        Output of ``leiden_communities`` (``node``, ``community``).
    centrality : pandas.DataFrame
        Output of ``centrality_table`` (``node``, ``pagerank``).
    names : pandas.DataFrame
        ``node`` and ``name``, for example from ``node_names``. Nodes
        without a name are shown by their id.
    node_tags : pandas.DataFrame, optional
        ``node`` and one tag column (for example the dominant genre).
        Nodes without a tag are ignored in the tag columns.
    top_n : int
        Number of nodes listed in ``top_nodes``.
    min_size : int
        Smallest community (in nodes) to describe.
    tag_col : str, optional
        Name of the tag column in ``node_tags``. Needed only when
        ``node_tags`` has more than one column besides ``node``.

    Returns
    -------
    pandas.DataFrame
        One row per community with at least ``min_size`` nodes, largest
        first (ties by community label):

        * ``community`` and ``size`` (nodes).
        * ``top_nodes`` - names of the ``top_n`` nodes with the highest
          PageRank, joined by ``", "``.
        * With ``node_tags``: ``n_tagged`` (nodes with a tag),
          ``top_tags`` (the three most common tags with their share of
          the tagged nodes, for example ``"Rock (0.62)"``) and
          ``tag_purity`` (share of the most common tag; NaN when no node
          is tagged). Tag ties go to the alphabetically first tag.

    Raises
    ------
    ValueError
        If ``node_tags`` has no single tag column and ``tag_col`` is not
        given.
    """
    import pandas as pd

    if node_tags is not None and tag_col is None:
        extra = [c for c in node_tags.columns if c != "node"]
        if len(extra) != 1:
            raise ValueError(f"Pass tag_col; node_tags has columns {list(node_tags.columns)}")
        tag_col = extra[0]

    label = dict(zip(names["node"], names["name"]))
    ranked = communities[["node", "community"]].merge(
        centrality[["node", "pagerank"]], on="node", how="left"
    )
    ranked = ranked.sort_values(["pagerank", "node"], ascending=[False, True])
    sizes = ranked.groupby("community").size()
    keep = sorted(sizes[sizes >= min_size].index, key=lambda c: (-sizes[c], c))

    tagged = None
    if node_tags is not None:
        tagged = ranked.merge(node_tags[["node", tag_col]], on="node").dropna(subset=[tag_col])
        tagged_by_comm = dict(tuple(tagged.groupby("community")))

    rows = []
    for comm in keep:
        members = ranked[ranked["community"] == comm]
        top = members["node"].head(top_n)
        row = {
            "community": comm,
            "size": int(sizes[comm]),
            "top_nodes": ", ".join(str(label.get(n, n)) for n in top),
        }
        if tagged is not None:
            sub = tagged_by_comm.get(comm)
            if sub is None or len(sub) == 0:
                row.update(n_tagged=0, top_tags="", tag_purity=float("nan"))
            else:
                counts = (
                    sub[tag_col].value_counts().rename_axis("tag").reset_index(name="n")
                    .sort_values(["n", "tag"], ascending=[False, True])
                )
                n = len(sub)
                best = counts.head(3)
                row.update(
                    n_tagged=int(n),
                    top_tags=", ".join(f"{t} ({k / n:.2f})" for t, k in zip(best["tag"], best["n"])),
                    tag_purity=float(best["n"].iloc[0] / n),
                )
        rows.append(row)
    columns = ["community", "size", "top_nodes"]
    if node_tags is not None:
        columns += ["n_tagged", "top_tags", "tag_purity"]
    return pd.DataFrame(rows, columns=columns)


def resolution_sweep(
    graph: "igraph.Graph",
    resolutions: tuple[float, ...] = (0.5, 1.0, 2.0),
    seed: int = 42,
    reference: float = 1.0,
) -> "pd.DataFrame":
    """Run Leiden at several resolutions and compare the results.

    Parameters
    ----------
    graph : igraph.Graph
        Undirected graph from ``to_igraph``.
    resolutions : tuple of float
        Resolutions to run. Higher gives more, smaller communities.
    seed : int
        Random seed, the same for every run.
    reference : float
        Resolution whose partition the others are compared with. It is
        run even when it is not in ``resolutions``.

    Returns
    -------
    pandas.DataFrame
        One row per resolution, in the order given:

        * ``resolution``.
        * ``n_communities`` and ``n_communities_ge50`` (communities with
          at least 50 nodes).
        * ``largest_share`` - share of all nodes in the largest
          community.
        * ``modularity`` - standard modularity (resolution 1) of the
          partition, so rows can be compared with each other.
        * ``nmi_vs_reference`` - normalized mutual information with the
          partition at ``reference`` (1 means the same grouping).
    """
    import pandas as pd
    from sklearn.metrics import normalized_mutual_info_score

    memberships: dict[float, list[int]] = {}
    for res in dict.fromkeys([float(r) for r in resolutions] + [float(reference)]):
        found = leiden_communities(graph, resolution=res, seed=seed)
        memberships[res] = found["community"].tolist()
    ref = memberships[float(reference)]
    rows = []
    for res in resolutions:
        member = memberships[float(res)]
        sizes = pd.Series(member).value_counts()
        rows.append(
            {
                "resolution": float(res),
                "n_communities": int(len(sizes)),
                "n_communities_ge50": int((sizes >= 50).sum()),
                "largest_share": float(sizes.iloc[0] / len(member)),
                "modularity": float(graph.modularity(member, weights="weight")),
                "nmi_vs_reference": float(normalized_mutual_info_score(ref, member)),
            }
        )
    return pd.DataFrame(rows)
