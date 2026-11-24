"""Self-test for the T10 functions (no pytest needed, no Spark session).

Covers ``clustering.node2vec_embeddings`` (and its walk helper
``clustering._simulate_walks``) and ``clustering.combine_features`` on
tiny hand-built data, then a timed smoke run on a random graph with
100 K nodes and 150 K edges. Exit code 0 only if every check passes.

Usage, from ``lab2/``:
``timeout 900 ../.venv/bin/python scripts/test_t10.py``
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from vinyl import clustering  # noqa: E402

FAILURES: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    """Record and print one check result."""
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" - {detail}" if detail and not ok else ""))
    if not ok:
        FAILURES.append(name)


def two_cliques(isolated: bool = False) -> pd.DataFrame:
    """Two 10-node cliques (0-9, 10-19) joined by the edge 9-10.

    With ``isolated``, add a zero-weight edge 20-21, so nodes 20 and 21
    exist but have no neighbours.
    """
    rows = []
    for lo in (0, 10):
        for a in range(lo, lo + 10):
            for b in range(a + 1, lo + 10):
                rows.append((a, b, 1.0))
    rows.append((9, 10, 1.0))
    if isolated:
        rows.append((20, 21, 0.0))
    return pd.DataFrame(rows, columns=["src", "dst", "weight"])


def walks_for(edges: pd.DataFrame, p: float, q: float, num_walks: int, walk_length: int,
              seed: int = 0) -> tuple[np.ndarray, object]:
    """Build the CSR and run the walk helper."""
    adj, _ = clustering._build_csr(edges)
    rng = np.random.default_rng(seed)
    walks = clustering._simulate_walks(
        adj.indptr, adj.indices, adj.data, num_walks, walk_length, p, q, rng
    )
    return walks, adj


def steps(walks: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Return the (from, to) node pairs of every step taken."""
    a, b = walks[:, :-1].ravel(), walks[:, 1:].ravel()
    ok = b >= 0
    return a[ok], b[ok]


def bridge_share(walks: np.ndarray) -> float:
    """Share of steps that cross between the two cliques."""
    a, b = steps(walks)
    return float(np.mean((a < 10) != (b < 10)))


def test_walks() -> None:
    print("walks")
    edges = two_cliques(isolated=True)
    walks, adj = walks_for(edges, 1.0, 1.0, num_walks=5, walk_length=12)
    n = adj.shape[0]
    check("shape is (n * num_walks, walk_length)", walks.shape == (22 * 5, 12), str(walks.shape))
    a, b = steps(walks)
    check("p=q=1: every step follows an edge", bool(np.all(adj[a, b].A1 > 0)))
    iso = np.isin(walks[:, 0], [20, 21])
    check("isolated nodes give length-1 walks",
          iso.sum() == 10 and bool(np.all(walks[iso, 1:] == -1)))
    check("other walks have full length", bool(np.all(walks[~iso] >= 0)))
    check("every node starts num_walks walks",
          bool(np.all(np.bincount(walks[:, 0], minlength=n) == 5)))

    w1, _ = walks_for(edges, 1.0, 1.0, 1, 1)
    check("walk_length=1 gives start nodes only", w1.shape == (22, 1))

    local, _ = walks_for(two_cliques(), 0.25, 4.0, num_walks=50, walk_length=30)
    explore, _ = walks_for(two_cliques(), 4.0, 0.25, num_walks=50, walk_length=30)
    a, b = steps(local)
    check("second order: every step follows an edge", bool(np.all(adj[a, b].A1 > 0)))
    s_local, s_explore = bridge_share(local), bridge_share(explore)
    print(f"    bridge-crossing share: stay local {s_local:.4f}, explore {s_explore:.4f}")
    check("p=0.25,q=4 crosses the bridge less than p=4,q=0.25", s_local < s_explore,
          f"{s_local} vs {s_explore}")
    ret_local = float(np.mean(local[:, 2:] == local[:, :-2]))
    ret_explore = float(np.mean(explore[:, 2:] == explore[:, :-2]))
    check("p=0.25 returns more often than p=4", ret_local > ret_explore,
          f"{ret_local} vs {ret_explore}")

    r1, _ = walks_for(edges, 0.5, 2.0, 3, 10, seed=7)
    r2, _ = walks_for(edges, 0.5, 2.0, 3, 10, seed=7)
    r3, _ = walks_for(edges, 0.5, 2.0, 3, 10, seed=8)
    check("same seed gives identical walks", bool(np.array_equal(r1, r2)))
    check("different seed gives different walks", not np.array_equal(r1, r3))


def mean_cos(vecs: np.ndarray, ia: list[int], ib: list[int]) -> float:
    """Mean cosine similarity between rows ``ia`` and ``ib`` (no self pairs)."""
    u = vecs / np.linalg.norm(vecs, axis=1, keepdims=True)
    sims = u[ia] @ u[ib].T
    if ia == ib:
        sims = sims[~np.eye(len(ia), dtype=bool)]
    return float(sims.mean())


def test_embeddings() -> None:
    print("embeddings")
    kwargs = dict(dimensions=16, walk_length=20, num_walks=20, seed=3, workers=1,
                  window=5, epochs=5)
    for label, mapper in (("int ids", None), ("str ids", lambda i: f"a{i}")):
        edges = two_cliques(isolated=True)
        if mapper is not None:
            edges["src"] = edges["src"].map(mapper)
            edges["dst"] = edges["dst"].map(mapper)
        emb = clustering.node2vec_embeddings(edges, **kwargs)
        cols = ["node"] + [f"n2v_{i}" for i in range(16)]
        check(f"{label}: columns", list(emb.columns) == cols, str(list(emb.columns)))
        check(f"{label}: one row per node", len(emb) == 22 and emb["node"].is_unique)
        if mapper is None:
            check(f"{label}: node dtype is integer", pd.api.types.is_integer_dtype(emb["node"]),
                  str(emb["node"].dtype))
            ids = list(range(22))
        else:
            check(f"{label}: nodes are str", all(isinstance(x, str) for x in emb["node"]))
            ids = [mapper(i) for i in range(22)]
        emb = emb.set_index("node").loc[ids]
        vecs = emb.to_numpy()
        within = (mean_cos(vecs, list(range(10)), list(range(10)))
                  + mean_cos(vecs, list(range(10, 20)), list(range(10, 20)))) / 2
        between = mean_cos(vecs, list(range(10)), list(range(10, 20)))
        print(f"    {label}: mean cosine within {within:.3f}, between {between:.3f}")
        check(f"{label}: within-clique cosine > between", within > between)

    e1 = clustering.node2vec_embeddings(two_cliques(), **kwargs)
    e2 = clustering.node2vec_embeddings(two_cliques(), **kwargs)
    check("workers=1: same seed gives identical vectors (same process)", e1.equals(e2))

    second = clustering.node2vec_embeddings(two_cliques(), p=0.5, q=2.0, **kwargs)
    check("second-order run returns all nodes", len(second) == 20)

    empty = clustering.node2vec_embeddings(
        pd.DataFrame({"src": pd.Series([], dtype="int64"), "dst": pd.Series([], dtype="int64"),
                      "weight": pd.Series([], dtype="float64")}), dimensions=4)
    check("empty edges give an empty frame", len(empty) == 0
          and list(empty.columns) == ["node", "n2v_0", "n2v_1", "n2v_2", "n2v_3"])

    for bad in (dict(p=0), dict(q=-1), dict(dimensions=0), dict(walk_length=0)):
        try:
            clustering.node2vec_embeddings(two_cliques(), **bad)
            check(f"ValueError for {bad}", False, "no error")
        except ValueError:
            check(f"ValueError for {bad}", True)


def tiny_frames() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Four artists in shuffled order; 40 has no styles, 30 is not in the graph."""
    profile = pd.DataFrame({
        "artist_id": [30, 10, 40, 20],
        "artist_name": ["C", "A", "D", "B"],
        "n_releases": [5, 50, 8, 12],
        "first_year": [2016, 2016, 2020, 2018],
        "last_year": [2020, 2026, 2021, 2024],
        "career_span": [5, 11, 2, 7],
        "n_labels": [1, 9, 2, 3],
        "share_vinyl": [0.2, 0.6, 1.0, 0.0],
        "share_cd": [0.8, 0.4, 0.0, 1.0],
    })
    tfidf = pd.DataFrame({
        "artist_id": [10, 10, 20, 20, 30, 30, 99],
        "style": ["House", "Techno", "Techno", "Ambient", "Jazz", "House", "Jazz"],
        "tf": [0.5, 0.5, 0.7, 0.3, 0.9, 0.1, 1.0],
        "idf": [0.3, 0.3, 0.3, 1.0, 1.0, 0.3, 1.0],
    })
    tfidf["tfidf"] = tfidf["tf"] * tfidf["idf"]
    embeddings = pd.DataFrame({
        "node": [40, 10, 20, 77],
        "n2v_0": [1.0, 0.0, 3.0, 9.0],
        "n2v_1": [0.0, 2.0, 4.0, 9.0],
        "n2v_2": [1.0, 0.0, 0.0, 9.0],
    })
    return profile, tfidf, embeddings


def test_combine() -> None:
    print("combine_features")
    profile, tfidf, embeddings = tiny_frames()
    X, ids = clustering.combine_features(profile, tfidf, embeddings)
    # profile: n_releases, career_span, n_labels, share_vinyl, share_cd, in_graph = 6
    # tfidf: 4 styles among kept artists -> min(50, 3, 4) = 3; embeddings: 3
    check("ids sorted by artist_id", ids.tolist() == [10, 20, 30, 40], str(ids.tolist()))
    check("ids is a Series", isinstance(ids, pd.Series))
    check("dtype float32", X.dtype == np.float32, str(X.dtype))
    check("shape (4, 12)", X.shape == (4, 12), str(X.shape))
    prof, tf, emb = X[:, :6], X[:, 6:9], X[:, 9:]
    check("no-style artist (40) has a zero TF-IDF block", bool(np.all(tf[3] == 0)))
    check("styled artists have unit-norm TF-IDF rows",
          np.allclose(np.linalg.norm(tf[:3], axis=1), 1.0, atol=1e-5))
    check("artist not in graph (30) has a zero embedding block", bool(np.all(emb[2] == 0)))
    check("graph artists have unit-norm embedding rows",
          np.allclose(np.linalg.norm(emb[[0, 1, 3]], axis=1), 1.0, atol=1e-5))
    in_graph = prof[:, 5]
    check("in_graph is lowest for artist 30 and equal for the others",
          in_graph[2] < 0 and np.allclose(in_graph[[0, 1, 3]], in_graph[0]) and in_graph[0] > 0)
    # Standardized profile block scaled by 1/sqrt(6): mean squared row norm 1.
    check("profile block mean squared row norm is 1",
          np.isclose(np.mean(np.sum(prof.astype(np.float64) ** 2, axis=1)), 1.0, atol=1e-5))

    X1, _ = clustering.combine_features(profile, tfidf, embeddings, weights=(1, 0, 0))
    check("weights=(1,0,0): TF-IDF and embedding columns are zero",
          bool(np.all(X1[:, 6:] == 0)) and bool(np.any(X1[:, :6] != 0)))
    X2, _ = clustering.combine_features(profile, tfidf, embeddings, weights=(2, 1, 1))
    check("weight 2 doubles the profile block", np.allclose(X2[:, :6], 2 * X[:, :6]))

    str_emb = embeddings.assign(node=embeddings["node"].astype(str))
    X3, _ = clustering.combine_features(profile, tfidf, str_emb)
    check("str node ids match int artist ids", np.allclose(X3, X))

    X4, _ = clustering.combine_features(profile, tfidf, pd.DataFrame({"node": []}))
    check("empty embeddings: block skipped", X4.shape == (4, 9), str(X4.shape))

    X5, _ = clustering.combine_features(profile, tfidf, embeddings, svd_components=2)
    check("svd_components caps the TF-IDF block", X5.shape == (4, 11), str(X5.shape))

    X6, _ = clustering.combine_features(profile, tfidf.iloc[0:0], embeddings)
    check("empty tfidf: block skipped", X6.shape == (4, 9), str(X6.shape))

    for label, call in (
        ("empty profile", lambda: clustering.combine_features(profile.iloc[0:0], tfidf, embeddings)),
        ("2 weights", lambda: clustering.combine_features(profile, tfidf, embeddings, (1, 1))),
        ("4 weights", lambda: clustering.combine_features(profile, tfidf, embeddings, (1, 1, 1, 1))),
    ):
        try:
            call()
            check(f"ValueError for {label}", False, "no error")
        except ValueError:
            check(f"ValueError for {label}", True)


def test_scale() -> None:
    print("scale smoke (100 K nodes, 150 K edges)")
    rng = np.random.default_rng(0)
    n, m = 100_000, 150_000
    edges = pd.DataFrame({
        "src": rng.integers(0, n, m),
        "dst": rng.integers(0, n, m),
        "weight": rng.integers(1, 5, m).astype(float),
    })
    t0 = time.perf_counter()
    emb = clustering.node2vec_embeddings(edges, dimensions=32, num_walks=2, walk_length=20,
                                         workers=4)
    elapsed = time.perf_counter() - t0
    print(f"    node2vec_embeddings (p=q=1): {elapsed:.1f} s, {len(emb)} nodes")
    n_nodes = pd.concat([edges["src"], edges["dst"]]).nunique()
    check("one row per node", len(emb) == n_nodes and emb.shape[1] == 33)
    check("finished within 10 minutes", elapsed < 600, f"{elapsed:.0f} s")

    adj, _ = clustering._build_csr(edges)
    t0 = time.perf_counter()
    clustering._simulate_walks(adj.indptr, adj.indices, adj.data, 2, 20, 0.5, 2.0,
                               np.random.default_rng(0))
    print(f"    second-order walks only (p=0.5, q=2): {time.perf_counter() - t0:.1f} s")


def main() -> int:
    test_walks()
    test_embeddings()
    test_combine()
    test_scale()
    print()
    if FAILURES:
        print(f"FAILED: {len(FAILURES)} check(s): {FAILURES}")
        return 1
    print("ALL PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
