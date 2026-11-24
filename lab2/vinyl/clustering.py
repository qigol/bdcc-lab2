"""RQ4 - Cluster artists on what they release and who they work with.

Pipeline::

    artist_profile  --+
    style_tfidf     --+--> combine_features --> reduce_umap --> cluster_hdbscan
    node2vec        --+

``artist_profile`` and ``style_tfidf`` run in Spark. The rest runs on the
driver with pandas / NumPy after the artist set has been cut down (for
example to artists with at least N releases in the window).

``node2vec_embeddings`` generates its random walks with NumPy over a
``scipy.sparse`` CSR adjacency and trains ``gensim`` Word2Vec on them.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Iterator

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

if TYPE_CHECKING:  # only for type hints; not needed at run time
    import numpy as np
    import pandas as pd
    from matplotlib.figure import Figure


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

def _build_csr(edges: "pd.DataFrame") -> tuple[object, "pd.Index"]:
    """Build an undirected, weighted CSR adjacency from an edge list.

    Parameters
    ----------
    edges : pandas.DataFrame
        Columns ``src``, ``dst``, ``weight``. Node ids can be ints or
        strings. Repeated pairs (in either direction) have their weights
        summed. Edges with weight 0 are dropped, but their end nodes are
        kept (as isolated nodes if they have no other edge).

    Returns
    -------
    tuple of (scipy.sparse.csr_matrix, pandas.Index)
        The ``n x n`` symmetric adjacency, with sorted column indices in
        every row, and the original node ids: row ``i`` is ``nodes[i]``.

    Raises
    ------
    ValueError
        If a column is missing, a node id is null, or a weight is
        negative or not finite.
    """
    import numpy as np
    import pandas as pd
    from scipy import sparse

    missing = {"src", "dst", "weight"} - set(edges.columns)
    if missing:
        raise ValueError(f"edges is missing columns: {sorted(missing)}")

    m = len(edges)
    codes, nodes = pd.factorize(
        pd.concat([edges["src"], edges["dst"]], ignore_index=True), sort=True
    )
    if (codes < 0).any():
        raise ValueError("edges has null node ids in src or dst")
    weight = edges["weight"].to_numpy(dtype=np.float64)
    if not np.isfinite(weight).all() or (weight < 0).any():
        raise ValueError("edge weights must be finite and non-negative")

    n = len(nodes)
    src, dst = codes[:m].astype(np.int64), codes[m:].astype(np.int64)
    keep = weight > 0
    src, dst, weight = src[keep], dst[keep], weight[keep]
    loop = src == dst  # self-loops are stored once, not twice
    rows = np.concatenate([src, dst[~loop]])
    cols = np.concatenate([dst, src[~loop]])
    vals = np.concatenate([weight, weight[~loop]])
    adj = sparse.csr_matrix((vals, (rows, cols)), shape=(n, n))
    adj.sum_duplicates()  # also sorts the indices of every row
    adj.sort_indices()
    return adj, nodes


def _simulate_walks(
    indptr: "np.ndarray",
    indices: "np.ndarray",
    data: "np.ndarray",
    num_walks: int,
    walk_length: int,
    p: float,
    q: float,
    rng: "np.random.Generator",
) -> "np.ndarray":
    """Generate node2vec random walks on a CSR graph with NumPy.

    All walkers of one round (one walk per node, in a random order) move
    together, so the Python loop runs once per step, not once per walker.

    Parameters
    ----------
    indptr, indices, data : numpy.ndarray
        CSR arrays of a symmetric adjacency with positive weights and
        sorted column indices in every row (as built by ``_build_csr``).
    num_walks : int
        Walks started from every node.
    walk_length : int
        Nodes per walk, including the start node.
    p, q : float
        node2vec return and in-out parameters. ``p == q == 1`` gives
        plain first-order weighted walks.
    rng : numpy.random.Generator
        Source of randomness. The same generator state gives the same
        walks.

    Returns
    -------
    numpy.ndarray
        Integer array of shape ``(n_nodes * num_walks, walk_length)``.
        Rows are grouped by round. A walk from an isolated node holds
        only the start node, and the rest of its row is ``-1``.

    Notes
    -----
    First-order step: draw ``u`` uniformly in the current row's slice of
    the global cumulative weight array and find it with
    ``np.searchsorted``, so the next node is drawn proportional to edge
    weight.

    Second-order step (``p`` or ``q`` not 1): rejection sampling, as in
    KnightKing and pecanpy's SparseOTF. Propose ``x`` from the first-order
    distribution and accept it with probability ``alpha / max_alpha``,
    where ``alpha`` is ``1/p`` if ``x`` is the previous node, ``1`` if
    ``x`` is a neighbour of the previous node and ``1/q`` otherwise. The
    neighbour test searches the key ``prev * n + x`` in the array
    ``row * n + col`` of all CSR entries. That array is sorted because
    rows are in order and each row's indices are sorted, so one
    ``np.searchsorted`` call does the test for all walkers at once.
    """
    import numpy as np

    indptr = np.asarray(indptr, dtype=np.int64)
    indices = np.asarray(indices, dtype=np.int64)
    n = len(indptr) - 1
    deg = np.diff(indptr)
    cw = np.cumsum(np.asarray(data, dtype=np.float64))
    cw0 = np.concatenate([[0.0], cw])
    base = cw0[indptr[:-1]]
    total = cw0[indptr[1:]] - base

    def draw(cur: "np.ndarray") -> "np.ndarray":
        u = base[cur] + rng.random(cur.size) * total[cur]
        pos = np.searchsorted(cw, u, side="right")
        pos = np.clip(pos, indptr[cur], indptr[cur + 1] - 1)
        return indices[pos]

    second_order = not (p == 1.0 and q == 1.0)
    if second_order:
        inv_p, inv_q = 1.0 / p, 1.0 / q
        max_alpha = max(inv_p, 1.0, inv_q)
        keys = np.repeat(np.arange(n, dtype=np.int64), deg) * n + indices
        last = max(len(keys) - 1, 0)

    def step(prev: "np.ndarray", cur: "np.ndarray") -> "np.ndarray":
        if not second_order:
            return draw(cur)
        nxt = np.empty_like(cur)
        todo = np.arange(cur.size)
        while todo.size:
            c, pv = cur[todo], prev[todo]
            x = draw(c)
            k = pv * n + x
            pos = np.minimum(np.searchsorted(keys, k), last)
            alpha = np.where(keys[pos] == k, 1.0, inv_q)
            alpha[x == pv] = inv_p
            accept = rng.random(x.size) * max_alpha < alpha
            nxt[todo[accept]] = x[accept]
            todo = todo[~accept]
        return nxt

    dtype = np.int32 if n < np.iinfo(np.int32).max else np.int64
    walks = np.full((n * num_walks, walk_length), -1, dtype=dtype)
    for r in range(num_walks):
        block = walks[r * n:(r + 1) * n]
        starts = rng.permutation(n).astype(np.int64)
        block[:, 0] = starts
        active = np.flatnonzero(deg[starts] > 0)
        if walk_length < 2 or active.size == 0:
            continue
        prev = starts[active]
        cur = draw(prev)
        block[active, 1] = cur
        for t in range(2, walk_length):
            nxt = step(prev, cur)
            block[active, t] = nxt
            prev, cur = cur, nxt
    return walks


class _WalkCorpus:
    """Restartable iterable that turns walk rows into lists of tokens.

    gensim reads the corpus once to build the vocabulary and once per
    epoch. Converting one walk at a time avoids holding every walk as a
    Python list of strings at once.

    Parameters
    ----------
    walks : numpy.ndarray
        Output of ``_simulate_walks``.
    tokens : numpy.ndarray
        Object array of strings; ``tokens[i]`` is the word for node ``i``.
    """

    def __init__(self, walks: "np.ndarray", tokens: "np.ndarray") -> None:
        self.walks = walks
        self.tokens = tokens

    def __iter__(self):
        tokens = self.tokens
        for row in self.walks:
            if row[-1] >= 0:
                yield tokens[row].tolist()
            else:  # isolated start node: the walk is just that node
                yield tokens[row[row >= 0]].tolist()


def node2vec_embeddings(
    edges: "pd.DataFrame",
    dimensions: int = 64,
    walk_length: int = 30,
    num_walks: int = 10,
    p: float = 1.0,
    q: float = 1.0,
    seed: int = 42,
    *,
    workers: int = 4,
    window: int = 10,
    epochs: int = 1,
) -> "pd.DataFrame":
    """Learn a vector for every node of a graph with node2vec.

    node2vec runs random walks over the graph and trains word2vec on
    them, so nodes that sit in the same neighbourhood get similar
    vectors.

    Parameters
    ----------
    edges : pandas.DataFrame
        Edge list with columns ``src``, ``dst``, ``weight``. The graph
        is treated as undirected; repeated pairs have their weights
        summed. Node ids can be ints or strings.
    dimensions : int
        Length of each vector.
    walk_length : int
        Nodes per random walk, including the start node.
    num_walks : int
        Walks started from every node.
    p, q : float
        Return and in-out parameters. ``q < 1`` favours exploring
        outward (community structure); ``q > 1`` stays local (role).
        Both must be positive.
    seed : int
        Random seed for the walks and for word2vec.
    workers : int
        word2vec training threads.
    window : int
        word2vec context window.
    epochs : int
        word2vec passes over the walks.

    Returns
    -------
    pandas.DataFrame
        One row per node: ``node`` (same type as ``src`` / ``dst``) plus
        columns ``n2v_0`` ... ``n2v_<dimensions-1>``. Empty if ``edges``
        is empty.

    Raises
    ------
    ValueError
        If a parameter is out of range, or if ``edges`` is malformed
        (see ``_build_csr``).

    Notes
    -----
    Implementation choice: walks are generated here with NumPy over a
    ``scipy.sparse`` CSR adjacency, and the vectors are trained with
    ``gensim.models.Word2Vec`` (skip-gram, 5 negative samples). The
    ``node2vec`` package was ruled out because it precomputes every
    second-order transition table in pure Python, which is too slow and
    too large for the ~114 K node collaboration graph. pecanpy is not
    installed; its SparseOTF idea (rejection sampling, no precomputed
    tables) is reproduced in ``_simulate_walks``. Spark Word2Vec would
    add a cluster round trip for a graph that fits on the driver.

    * ``p == q == 1`` uses first-order weighted walks. Otherwise each
      step uses rejection sampling against the node2vec bias.
    * The walks are reproducible given ``seed``: they come from
      ``np.random.default_rng(seed)``.
    * gensim with ``workers > 1`` is not bit-reproducible because thread
      scheduling changes the update order. Pass ``workers=1`` for exact
      reproducibility. Across processes, gensim also needs the same
      ``PYTHONHASHSEED``, because it seeds each word vector from
      Python's string hash.
    * Memory for the walks is about ``n_nodes * num_walks * walk_length``
      ints (4 bytes each): about 140 MB for 114 K nodes with the
      defaults. Walks are turned into token lists one at a time while
      gensim reads them.
    """
    import numpy as np
    import pandas as pd
    from gensim.models import Word2Vec

    if dimensions < 1 or walk_length < 1 or num_walks < 1:
        raise ValueError("dimensions, walk_length and num_walks must be >= 1")
    if not (p > 0 and q > 0):
        raise ValueError("p and q must be positive")
    if workers < 1 or window < 1 or epochs < 1:
        raise ValueError("workers, window and epochs must be >= 1")

    columns = [f"n2v_{i}" for i in range(dimensions)]
    adj, nodes = _build_csr(edges)
    n = len(nodes)
    if n == 0:
        return pd.DataFrame(
            {"node": pd.Series([], dtype=edges["src"].dtype),
             **{c: pd.Series([], dtype=np.float32) for c in columns}}
        )

    rng = np.random.default_rng(seed)
    walks = _simulate_walks(
        adj.indptr, adj.indices, adj.data, num_walks, walk_length, p, q, rng
    )
    # Tokens are the integer positions, so ids like 5 and "5" cannot clash.
    tokens = np.array([str(i) for i in range(n)], dtype=object)
    model = Word2Vec(
        sentences=_WalkCorpus(walks, tokens),
        vector_size=dimensions,
        window=window,
        min_count=0,
        sg=1,
        negative=5,
        workers=workers,
        seed=seed,
        epochs=epochs,
    )
    order = [model.wv.key_to_index[t] for t in tokens]
    vectors = model.wv.vectors[order]
    result = pd.DataFrame(vectors, columns=columns)
    result.insert(0, "node", np.asarray(nodes))
    return result


def combine_features(
    profile: "pd.DataFrame",
    tfidf: "pd.DataFrame",
    embeddings: "pd.DataFrame",
    weights: tuple[float, float, float] = (1.0, 1.0, 1.0),
    *,
    svd_components: int = 50,
    seed: int = 42,
) -> tuple["np.ndarray", "pd.Series"]:
    """Join the three feature blocks into one matrix.

    Parameters
    ----------
    profile : pandas.DataFrame
        Collected ``artist_profile`` output. Defines the rows.
    tfidf : pandas.DataFrame
        Collected ``style_tfidf`` output (long format: ``artist_id``,
        ``style``, ``tfidf``). Rows for artists not in ``profile`` are
        ignored.
    embeddings : pandas.DataFrame
        Output of ``node2vec_embeddings`` with ``node`` = artist id.
    weights : tuple of float
        Relative weight of the (profile, tfidf, embedding) blocks after
        each block has been scaled. A weight of 0 zeroes that block.
    svd_components : int
        Upper bound on the TF-IDF block's truncated SVD components.
    seed : int
        Random seed for the truncated SVD.

    Returns
    -------
    tuple of (numpy.ndarray, pandas.Series)
        The float32 feature matrix (one row per artist, sorted by
        ``artist_id``) and the matching artist ids in row order. Columns
        are the profile block, then the TF-IDF block, then the embedding
        block.

    Raises
    ------
    ValueError
        If ``profile`` is empty or ``weights`` does not have 3 entries.

    Notes
    -----
    * Profile block: ``n_releases``, ``career_span``, ``n_labels``, every
      ``share_*`` column and an ``in_graph`` 0/1 column (1 if the artist
      has an embedding). ``artist_name`` is text; ``first_year`` and
      ``last_year`` are dropped because ``career_span`` carries their
      difference and the absolute years mostly reflect where the study
      window cuts careers. ``n_releases`` and ``n_labels`` get
      ``log1p`` because they are heavy-tailed. Then ``StandardScaler``.
    * TF-IDF block: the long table is pivoted to a sparse artist x style
      matrix (artists with no styles get an all-zero row), reduced with
      ``TruncatedSVD(n_components=min(svd_components, n_styles - 1,
      n_artists))`` and L2-normalized by row; zero rows stay zero. With
      fewer than 2 styles the block has no columns.
    * Embedding block: ``embeddings`` is matched on ``node == artist_id``
      (both cast to ``str`` when their dtypes differ). Artists not in the
      graph get a zero vector, and ``in_graph`` tells them apart. Rows are
      L2-normalized.
    * Block scaling: each block is scaled to a mean squared row norm of
      about 1 before weighting, so the blocks contribute equally whatever
      their width. The standardized profile block has a mean squared row
      norm of ``n_columns``, so it is multiplied by
      ``weight / sqrt(n_columns)``. The L2-normalized blocks already have
      row norm 1, so they are multiplied by ``weight`` only. A block with
      zero columns is skipped.
    """
    import numpy as np
    import pandas as pd
    from scipy import sparse
    from sklearn.decomposition import TruncatedSVD
    from sklearn.preprocessing import StandardScaler, normalize

    if len(weights) != 3:
        raise ValueError(f"weights must have 3 entries, got {len(weights)}")
    if profile is None or len(profile) == 0:
        raise ValueError("profile is empty")

    prof = profile.sort_values("artist_id", kind="mergesort").reset_index(drop=True)
    artist_ids = prof["artist_id"].rename("artist_id")
    n = len(prof)

    # Embedding block (computed first: in_graph feeds the profile block).
    emb_cols = sorted(
        (c for c in embeddings.columns if str(c).startswith("n2v_")),
        key=lambda c: int(str(c)[4:]),
    ) if len(embeddings) else []
    emb_block = np.zeros((n, len(emb_cols)), dtype=np.float64)
    in_graph = np.zeros(n, dtype=np.float64)
    if emb_cols:
        emb = embeddings.drop_duplicates("node")
        left, right = artist_ids, emb["node"]
        if left.dtype != right.dtype:
            left, right = left.astype(str), right.astype(str)
        pos = pd.Index(right).get_indexer(left)
        hit = pos >= 0
        emb_block[hit] = emb[emb_cols].to_numpy(dtype=np.float64)[pos[hit]]
        in_graph[hit] = 1.0
        emb_block = normalize(emb_block)

    # Profile block.
    num_cols = ["n_releases", "career_span", "n_labels"] + [
        c for c in prof.columns if str(c).startswith("share_")
    ]
    prof_block = prof[num_cols].astype(np.float64).fillna(0.0)
    prof_block["n_releases"] = np.log1p(prof_block["n_releases"])
    prof_block["n_labels"] = np.log1p(prof_block["n_labels"])
    prof_block["in_graph"] = in_graph
    prof_block = StandardScaler().fit_transform(prof_block.to_numpy())

    # TF-IDF block.
    tfidf_block = np.zeros((n, 0), dtype=np.float64)
    if len(tfidf):
        rows = pd.Index(artist_ids).get_indexer(tfidf["artist_id"])
        keep = rows >= 0
        sub = tfidf.loc[keep]
        style_codes, styles = pd.factorize(sub["style"], sort=True)
        k = min(svd_components, len(styles) - 1, n)
        if k >= 1:
            mat = sparse.csr_matrix(
                (sub["tfidf"].to_numpy(dtype=np.float64), (rows[keep], style_codes)),
                shape=(n, len(styles)),
            )
            svd = TruncatedSVD(n_components=k, random_state=seed).fit(mat)
            # transform() is X @ components.T, so zero rows stay exactly zero.
            tfidf_block = normalize(svd.transform(mat))

    parts = []
    for block, weight, unit_rows in (
        (prof_block, weights[0], False),
        (tfidf_block, weights[1], True),
        (emb_block, weights[2], True),
    ):
        if block.shape[1] == 0:
            continue
        scale = float(weight) if unit_rows else float(weight) / np.sqrt(block.shape[1])
        parts.append(block * scale)
    return np.hstack(parts).astype(np.float32), artist_ids


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


# --------------------------------------------------------------------------
# Inspecting the clusters (explorer 07)
# --------------------------------------------------------------------------

@contextmanager
def timed(label: str) -> Iterator[None]:
    """Print how long the ``with`` block took.

    Parameters
    ----------
    label : str
        Name of the step, shown in the printed line.

    Yields
    ------
    None
        The block runs between the two timestamps. The line
        ``[label] 12.3 s`` is printed even if the block raises.
    """
    start = time.perf_counter()
    try:
        yield
    finally:
        print(f"[{label}] {time.perf_counter() - start:.1f} s", flush=True)


def describe_clusters(
    cluster_labels: "np.ndarray",
    artist_ids: "pd.Series | np.ndarray",
    profile: "pd.DataFrame",
    tfidf: "pd.DataFrame",
    top_styles: int = 3,
    examples: int = 5,
) -> "pd.DataFrame":
    """Summarize each cluster of artists in one row.

    Parameters
    ----------
    cluster_labels : numpy.ndarray
        One label per artist, ``-1`` for noise (output of
        ``cluster_hdbscan``).
    artist_ids : pandas.Series or numpy.ndarray
        Artist id of each label, in the same order (second output of
        ``combine_features``). Must be unique.
    profile : pandas.DataFrame
        Collected ``artist_profile`` output. Needs ``artist_id``,
        ``artist_name``, ``n_releases``, ``career_span``, ``first_year``
        and the ``share_*`` columns.
    tfidf : pandas.DataFrame
        Collected ``style_tfidf`` output (``artist_id``, ``style``,
        ``tfidf``).
    top_styles : int
        Number of styles to list per cluster.
    examples : int
        Number of example artists to list per cluster.

    Returns
    -------
    pandas.DataFrame
        One row per cluster, noise (``-1``) included as its own row,
        sorted by ``size`` (largest first). Columns:

        * ``cluster``, ``size``, ``share`` (of all labelled artists) and
          ``is_noise``.
        * ``top_styles`` - list of ``(style, mean_tfidf)`` pairs, best
          first. The mean is over all artists of the cluster, so an
          artist without the style counts as 0.
        * ``mean_share_<format>`` - mean of each ``share_*`` column of
          ``profile``.
        * ``median_career_span``, ``median_n_releases``,
          ``median_first_year``.
        * ``examples`` - names of the artists with the most releases
          (ties: lower artist id first). The id is used when the name is
          missing.

    Raises
    ------
    ValueError
        If the lengths of ``cluster_labels`` and ``artist_ids`` differ,
        or ``artist_ids`` has duplicates.
    """
    import numpy as np
    import pandas as pd

    labels = np.asarray(cluster_labels).astype(np.int64)
    ids = np.asarray(artist_ids)
    if len(labels) != len(ids):
        raise ValueError(f"{len(labels)} labels but {len(ids)} artist ids")
    members = pd.DataFrame({"artist_id": ids, "cluster": labels})
    if members["artist_id"].duplicated().any():
        raise ValueError("artist_ids must be unique")

    merged = members.merge(profile, on="artist_id", how="left")
    sizes = members.groupby("cluster").size()
    out = sizes.rename("size").reset_index()
    out["share"] = out["size"] / max(len(members), 1)
    out["is_noise"] = out["cluster"] == -1

    # Styles: mean TF-IDF over every member, so absent styles count as 0.
    sums = (
        tfidf[["artist_id", "style", "tfidf"]]
        .merge(members, on="artist_id")
        .groupby(["cluster", "style"])["tfidf"].sum()
        .rename("total")
        .reset_index()
    )
    sums["mean_tfidf"] = sums["total"] / sums["cluster"].map(sizes)
    sums = sums.sort_values(
        ["cluster", "mean_tfidf", "style"], ascending=[True, False, True]
    )
    best = {
        c: list(zip(g["style"], g["mean_tfidf"]))
        for c, g in sums.groupby("cluster").head(top_styles).groupby("cluster")
    }
    out["top_styles"] = [best.get(c, []) for c in out["cluster"]]

    share_cols = [c for c in profile.columns if str(c).startswith("share_")]
    means = merged.groupby("cluster")[share_cols].mean()
    means.columns = [f"mean_{c}" for c in means.columns]
    medians = merged.groupby("cluster")[["career_span", "n_releases", "first_year"]].median()
    medians.columns = [f"median_{c}" for c in medians.columns]
    out = out.merge(means, left_on="cluster", right_index=True, how="left")
    out = out.merge(medians, left_on="cluster", right_index=True, how="left")

    ranked = merged.sort_values(
        ["cluster", "n_releases", "artist_id"], ascending=[True, False, True]
    )
    ranked["_name"] = ranked["artist_name"].where(
        ranked["artist_name"].notna(), ranked["artist_id"].astype(str)
    )
    names = {
        c: g["_name"].tolist() for c, g in ranked.groupby("cluster").head(examples).groupby("cluster")
    }
    out["examples"] = [names.get(c, []) for c in out["cluster"]]

    return out.sort_values(["size", "cluster"], ascending=[False, True]).reset_index(drop=True)


def min_cluster_size_sweep(
    embedding: "np.ndarray",
    sizes: tuple[int, ...] = (25, 50, 100, 200),
    min_samples: int | None = None,
) -> "pd.DataFrame":
    """Run HDBSCAN for several ``min_cluster_size`` values and compare.

    Parameters
    ----------
    embedding : numpy.ndarray
        Points to cluster, usually the 10-dimensional UMAP output.
    sizes : tuple of int
        Values of ``min_cluster_size`` to try.
    min_samples : int, optional
        Passed to ``cluster_hdbscan``; ``None`` uses each size.

    Returns
    -------
    pandas.DataFrame
        One row per size with ``min_cluster_size``, ``n_clusters``
        (noise not counted), ``noise_share``, ``largest_cluster_share``
        (0 if there is no cluster) and ``seconds``. It only reports; it
        does not choose a size.
    """
    import numpy as np
    import pandas as pd

    rows = []
    n = len(embedding)
    for size in sizes:
        start = time.perf_counter()
        labels = cluster_hdbscan(embedding, min_cluster_size=size, min_samples=min_samples)
        seconds = time.perf_counter() - start
        counts = np.bincount(labels[labels >= 0]) if (labels >= 0).any() else np.array([])
        rows.append(
            {
                "min_cluster_size": size,
                "n_clusters": int((counts > 0).sum()),
                "noise_share": float((labels == -1).sum() / n) if n else float("nan"),
                "largest_cluster_share": float(counts.max() / n) if len(counts) else 0.0,
                "seconds": seconds,
            }
        )
    return pd.DataFrame(rows)


def compare_partitions(
    labels_a: "np.ndarray",
    labels_b: "np.ndarray",
    ids_a: "pd.Series | np.ndarray",
    ids_b: "pd.Series | np.ndarray",
    drop_noise: bool = True,
) -> dict:
    """Compare two groupings of (partly) the same items.

    Parameters
    ----------
    labels_a, labels_b : numpy.ndarray
        One label per item in each grouping. Labels can be ints or
        strings.
    ids_a, ids_b : pandas.Series or numpy.ndarray
        Item id of each label. Ids must be unique within each side.
    drop_noise : bool
        Drop items labelled ``-1`` in ``labels_a`` (HDBSCAN noise)
        before scoring. ``labels_b`` is left as it is.

    Returns
    -------
    dict
        * ``n_common`` - ids present on both sides, before dropping noise.
        * ``n_compared`` - items used for the scores, after dropping
          noise.
        * ``nmi`` - normalized mutual information (1 = same grouping).
        * ``ari`` - adjusted Rand index (1 = same grouping, about 0 =
          random). Both are NaN if no items are left.
        * ``contingency`` - ``pandas.DataFrame`` of counts for the 10
          largest groups of ``a`` (rows) by the 10 largest of ``b``
          (columns), by size among the compared items.

    Raises
    ------
    ValueError
        If labels and ids differ in length, or ids repeat within a side.
    """
    import pandas as pd
    from sklearn.metrics import adjusted_rand_score, normalized_mutual_info_score

    sides = []
    for name, labels, ids in (("a", labels_a, ids_a), ("b", labels_b, ids_b)):
        side = pd.DataFrame({"id": list(ids), name: list(labels)})
        if len(side) != len(labels):
            raise ValueError(f"labels_{name} and ids_{name} differ in length")
        if side["id"].duplicated().any():
            raise ValueError(f"ids_{name} must be unique")
        sides.append(side)
    common = sides[0].merge(sides[1], on="id", how="inner")
    n_common = len(common)
    if drop_noise:
        common = common[common["a"] != -1]

    if common.empty:
        nmi = ari = float("nan")
        table = pd.DataFrame()
    else:
        nmi = float(normalized_mutual_info_score(common["a"], common["b"]))
        ari = float(adjusted_rand_score(common["a"], common["b"]))
        top_a = common["a"].value_counts().index[:10]
        top_b = common["b"].value_counts().index[:10]
        sub = common[common["a"].isin(top_a) & common["b"].isin(top_b)]
        table = pd.crosstab(sub["a"], sub["b"]).reindex(
            index=list(top_a), columns=list(top_b), fill_value=0
        )
    return {
        "n_common": int(n_common),
        "n_compared": int(len(common)),
        "nmi": nmi,
        "ari": ari,
        "contingency": table,
    }


def plot_clusters_2d(
    embedding_2d: "np.ndarray",
    cluster_labels: "np.ndarray",
    n_colored: int = 20,
    n_labelled: int = 10,
    max_points: int = 200_000,
    seed: int = 42,
    title: str = "Artist clusters (UMAP, 2 components)",
) -> "Figure":
    """Scatter plot of a 2-D embedding coloured by cluster.

    Parameters
    ----------
    embedding_2d : numpy.ndarray
        Matrix of shape (n_artists, 2).
    cluster_labels : numpy.ndarray
        One label per row, ``-1`` for noise.
    n_colored : int
        The largest clusters get their own colour; the other clusters
        are mid grey and noise is light grey.
    n_labelled : int
        The largest clusters get their id written at the median of their
        points.
    max_points : int
        If there are more points, draw a random sample of this size.
        Cluster sizes and medians still use every point.
    seed : int
        Seed of the sample.
    title : str
        Plot title.

    Returns
    -------
    matplotlib.figure.Figure
        Figure to pass to ``viz.save_figure``.
    """
    import matplotlib.pyplot as plt
    import numpy as np

    xy = np.asarray(embedding_2d)
    labels = np.asarray(cluster_labels)
    ids, counts = np.unique(labels[labels >= 0], return_counts=True)
    ranked = ids[np.lexsort((ids, -counts))]  # largest first, ties by id

    keep = np.arange(len(xy))
    if len(xy) > max_points:
        keep = np.sort(np.random.default_rng(seed).choice(len(xy), max_points, replace=False))
    xs, ls = xy[keep], labels[keep]

    fig, ax = plt.subplots(figsize=(10, 8))
    draw = dict(s=1, alpha=0.3, linewidths=0, rasterized=True)
    noise = ls == -1
    ax.scatter(xs[noise, 0], xs[noise, 1], c="#d9d9d9", **draw)
    colored = ranked[:n_colored]
    rest = ~noise & ~np.isin(ls, colored)
    ax.scatter(xs[rest, 0], xs[rest, 1], c="#9a9a9a", **draw)
    cmap = plt.get_cmap("tab20")
    for i, cluster in enumerate(colored):
        sel = ls == cluster
        ax.scatter(xs[sel, 0], xs[sel, 1], color=cmap(i % 20), **draw)
    for cluster in ranked[:n_labelled]:
        x, y = np.median(xy[labels == cluster], axis=0)
        ax.text(x, y, str(cluster), ha="center", va="center", fontsize=9, weight="bold",
                bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.7))
    ax.set_title(title, loc="left")
    ax.set_xticks([])
    ax.set_yticks([])
    ax.spines[:].set_visible(False)
    return fig
