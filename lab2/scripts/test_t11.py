"""Self-test for the T11 functions (no pytest needed, no Spark, synthetic data only).

Covers ``clustering.describe_clusters``, ``clustering.min_cluster_size_sweep``,
``clustering.compare_partitions``, ``clustering.timed`` and
``clustering.plot_clusters_2d``. Exit code 0 only if every check passes.

Usage, from ``lab2/``:
``DISCOGS_DATA_DIR=$PWD/data timeout 900 ../.venv/bin/python scripts/test_t11.py``
"""

from __future__ import annotations

import contextlib
import io
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from vinyl import clustering  # noqa: E402

FAILURES: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    """Record and print one check result."""
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" - {detail}" if detail and not ok else ""))
    if not ok:
        FAILURES.append(name)


def test_describe() -> None:
    """Two clusters plus noise, built by hand."""
    print("describe_clusters")
    # Cluster 0: artists 1-4, cluster 1: artists 5-7, noise: artist 8.
    artist_ids = pd.Series([1, 2, 3, 4, 5, 6, 7, 8], name="artist_id")
    labels = np.array([0, 0, 0, 0, 1, 1, 1, -1])
    profile = pd.DataFrame(
        {
            "artist_id": [8, 7, 6, 5, 4, 3, 2, 1],  # shuffled on purpose
            "artist_name": ["H", "G", "F", "E", "D", "C", None, "A"],
            "n_releases": [5, 9, 20, 6, 8, 30, 12, 12],
            "first_year": [2020, 2016, 2018, 2019, 2017, 2016, 2018, 2021],
            "career_span": [3, 11, 9, 8, 10, 11, 5, 4],
            "share_vinyl": [0.0, 1.0, 0.5, 0.0, 1.0, 0.5, 0.0, 0.5],
            "share_cd": [1.0, 0.0, 0.5, 1.0, 0.0, 0.5, 1.0, 0.5],
        }
    )
    # Cluster 0 (size 4): Dub 0.4+0.4+0.4+0.0 -> 0.3; Ambient 0.1 each on 2 -> 0.05;
    # Techno 0.04 on 1 -> 0.01. Cluster 1 (size 3): Punk 0.9 on 1 -> 0.3; Dub 0.6 on 1 -> 0.2.
    tfidf = pd.DataFrame(
        {
            "artist_id": [1, 2, 3, 1, 2, 4, 5, 6, 99],
            "style": ["Dub", "Dub", "Dub", "Ambient", "Ambient", "Techno", "Punk", "Dub", "Dub"],
            "tfidf": [0.4, 0.4, 0.4, 0.1, 0.1, 0.04, 0.9, 0.6, 5.0],
        }
    )
    out = clustering.describe_clusters(labels, artist_ids, profile, tfidf, top_styles=2, examples=2)
    check("one row per cluster, noise included", sorted(out["cluster"]) == [-1, 0, 1])
    check("sorted by size", out["size"].tolist() == [4, 3, 1], str(out["size"].tolist()))
    check("cluster order 0, 1, -1", out["cluster"].tolist() == [0, 1, -1])
    check("shares", np.allclose(out["share"], [0.5, 0.375, 0.125]))
    check("noise flag", out["is_noise"].tolist() == [False, False, True])
    row0 = out.set_index("cluster").loc[0]
    top0 = row0["top_styles"]
    check("top styles of cluster 0", [s for s, _ in top0] == ["Dub", "Ambient"], str(top0))
    check("top style values are means over members",
          np.allclose([v for _, v in top0], [0.3, 0.05]), str(top0))
    top1 = out.set_index("cluster").loc[1, "top_styles"]
    check("top styles of cluster 1", [s for s, _ in top1] == ["Punk", "Dub"], str(top1))
    check("artist outside the clusters is ignored",
          all(v < 1 for _, v in top1) and out.set_index("cluster").loc[-1, "top_styles"] == [])
    check("examples ordered by n_releases (C=30, A=12)", row0["examples"] == ["C", "A"],
          str(row0["examples"]))
    check("missing name falls back to id (cluster 0 has 2 with 12: id 1 first)",
          out.set_index("cluster").loc[1, "examples"] == ["F", "G"],
          str(out.set_index("cluster").loc[1, "examples"]))
    check("mean_share_vinyl", np.isclose(row0["mean_share_vinyl"], (0.5 + 0.0 + 0.5 + 1.0) / 4))
    check("mean_share_cd column present", "mean_share_cd" in out.columns)
    check("median_career_span", row0["median_career_span"] == np.median([4, 5, 11, 10]))
    check("median_n_releases", row0["median_n_releases"] == np.median([12, 12, 30, 8]))
    check("median_first_year", row0["median_first_year"] == np.median([2021, 2018, 2016, 2017]))
    noise = out.set_index("cluster").loc[-1]
    check("noise row stats", noise["examples"] == ["H"] and noise["median_career_span"] == 3)

    fallback = profile.assign(artist_name=None)
    out2 = clustering.describe_clusters(labels, artist_ids, fallback, tfidf, examples=1)
    check("id used when name missing",
          out2.set_index("cluster").loc[0, "examples"] == ["3"],
          str(out2.set_index("cluster").loc[0, "examples"]))
    try:
        clustering.describe_clusters(labels[:-1], artist_ids, profile, tfidf)
        check("length mismatch raises", False)
    except ValueError:
        check("length mismatch raises", True)


def test_sweep() -> None:
    """Three well separated blobs."""
    print("min_cluster_size_sweep")
    from sklearn.datasets import make_blobs

    x, _ = make_blobs(n_samples=1500, centers=[(0, 0), (10, 10), (-10, 10)],
                      cluster_std=0.8, random_state=0)
    table = clustering.min_cluster_size_sweep(x, sizes=(25, 50, 100, 200))
    print(table.to_string(index=False))
    check("one row per size", table["min_cluster_size"].tolist() == [25, 50, 100, 200])
    check("columns", {"n_clusters", "noise_share", "largest_cluster_share", "seconds"}
          <= set(table.columns))
    small = table[table["min_cluster_size"] <= 50]
    check("small sizes find 3 clusters", (small["n_clusters"] == 3).all(),
          str(small["n_clusters"].tolist()))
    check("noise share in [0, 1)", table["noise_share"].between(0, 0.2).all())
    check("largest cluster share about 1/3", table["largest_cluster_share"].between(0.3, 0.4).all(),
          str(table["largest_cluster_share"].tolist()))
    check("seconds are positive", (table["seconds"] > 0).all())


def test_compare() -> None:
    """Identical, noisy and partly overlapping partitions."""
    print("compare_partitions")
    ids = np.arange(100)
    labels = np.repeat([0, 1, 2, 3], 25)
    same = clustering.compare_partitions(labels, labels.copy(), ids, ids)
    check("identical: NMI 1", np.isclose(same["nmi"], 1.0))
    check("identical: ARI 1", np.isclose(same["ari"], 1.0))
    check("identical: n_common 100", same["n_common"] == 100 and same["n_compared"] == 100)
    check("identical: diagonal contingency",
          same["contingency"].shape == (4, 4)
          and np.array_equal(np.diag(same["contingency"].to_numpy()), [25] * 4)
          and same["contingency"].to_numpy().sum() == 100)

    renamed = clustering.compare_partitions(labels, np.array(list("wxyz"))[labels], ids, ids)
    check("renamed groups still NMI 1 / ARI 1",
          np.isclose(renamed["nmi"], 1.0) and np.isclose(renamed["ari"], 1.0))

    # Noise: the first 10 items are -1 in a and put in a wrong group in b.
    noisy_a = labels.copy()
    noisy_a[:10] = -1
    wrong_b = labels.copy()
    wrong_b[:10] = 3
    dropped = clustering.compare_partitions(noisy_a, wrong_b, ids, ids)
    check("noise dropped: scores 1", np.isclose(dropped["nmi"], 1.0) and np.isclose(dropped["ari"], 1.0))
    check("noise dropped: counts", dropped["n_common"] == 100 and dropped["n_compared"] == 90)
    check("noise dropped: no -1 row", -1 not in dropped["contingency"].index)
    kept = clustering.compare_partitions(noisy_a, wrong_b, ids, ids, drop_noise=False)
    check("noise kept: scores below 1", kept["nmi"] < 0.999 and kept["ari"] < 0.999)
    check("noise kept: n_compared 100", kept["n_compared"] == 100)

    # Different id sets and order: only ids 50-99 are shared.
    ids_b = np.arange(50, 150)[::-1]
    labels_b = np.where(ids_b < 100, labels[np.minimum(ids_b, 99)], 0)
    common = clustering.compare_partitions(labels, labels_b, ids, ids_b)
    check("only common ids used", common["n_common"] == 50 and common["n_compared"] == 50,
          str(common["n_common"]))
    check("ids aligned, not positions", np.isclose(common["nmi"], 1.0) and np.isclose(common["ari"], 1.0),
          f"nmi={common['nmi']}")

    a = np.array([0, 0, 1, 1, 2, 2])
    b = np.array([0, 1, 0, 1, 0, 1])
    unrelated = clustering.compare_partitions(a, b, np.arange(6), np.arange(6))
    check("unrelated: ARI <= 0", unrelated["ari"] <= 1e-9 and unrelated["nmi"] < 0.5)
    check("different ids give empty result",
          np.isnan(clustering.compare_partitions(a, b, np.arange(6), np.arange(10, 16))["nmi"]))
    check("top 10 x top 10 contingency", clustering.compare_partitions(
        np.arange(30), np.arange(30), np.arange(30), np.arange(30))["contingency"].shape == (10, 10))
    try:
        clustering.compare_partitions(a, b, np.zeros(6), np.arange(6))
        check("duplicate ids raise", False)
    except ValueError:
        check("duplicate ids raise", True)


def test_helpers() -> None:
    """``timed`` prints, and the scatter plot builds on a tiny example."""
    print("timed / plot_clusters_2d")
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        with clustering.timed("step"):
            pass
    check("timed prints label and seconds", buffer.getvalue().startswith("[step] ")
          and buffer.getvalue().strip().endswith(" s"), buffer.getvalue())

    rng = np.random.default_rng(0)
    xy = rng.normal(size=(500, 2))
    labels = rng.integers(-1, 25, size=500)
    fig = clustering.plot_clusters_2d(xy, labels, max_points=300)
    ax = fig.axes[0]
    check("plot has 10 cluster labels", len(ax.texts) == 10, str(len(ax.texts)))
    check("plot is subsampled to max_points",
          sum(len(c.get_offsets()) for c in ax.collections) <= 300)
    check("noise only plot works", clustering.plot_clusters_2d(xy, np.full(500, -1)) is not None)


def main() -> int:
    test_describe()
    test_sweep()
    test_compare()
    test_helpers()
    print()
    if FAILURES:
        print(f"FAILED: {len(FAILURES)} check(s): {FAILURES}")
        return 1
    print("ALL PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
