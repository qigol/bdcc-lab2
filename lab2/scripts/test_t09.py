"""Self-test for the T09 functions in ``vinyl/networks.py`` (no pytest needed).

Covers ``prefixed_bipartite``, ``node_names``, ``describe_communities``,
``resolution_sweep``, ``artist_dominant_tag``, ``graph_artist_ids``,
``artist_label_edges(exclude_label_ids=...)`` and ``config.GRAPH_MIN_WEIGHT``
on tiny hand-built data only. Exit code 0 only if every check passes.

Needs the repo venv (python-igraph, scikit-learn). Usage, from ``lab2/``:
``DISCOGS_DATA_DIR=$PWD/data ../.venv/bin/python scripts/test_t09.py``
"""

from __future__ import annotations

import math
import os
import sys
from pathlib import Path

os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--driver-memory 2g pyspark-shell")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd  # noqa: E402

from vinyl import config, ingest, networks  # noqa: E402

FAILURES: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    """Record and print one check result."""
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" - {detail}" if detail and not ok else ""))
    if not ok:
        FAILURES.append(name)


def close(a: float, b: float, tol: float = 1e-9) -> bool:
    """Return True if two floats are equal within ``tol``."""
    return a is not None and b is not None and abs(a - b) <= tol


def test_config() -> None:
    """The per-graph minimum weights have the specified values."""
    print("config")
    check(
        "GRAPH_MIN_WEIGHT",
        config.GRAPH_MIN_WEIGHT == {"collaboration": 2, "credit": 3, "artist_label": 3},
        str(config.GRAPH_MIN_WEIGHT),
    )


def test_prefixed_bipartite(spark) -> None:
    """Artist 5 and label 5 stay distinct; the graph has 3 nodes."""
    print("prefixed_bipartite")
    edges = spark.createDataFrame(
        [(5, 5, 4), (5, 7, 2), (9, 5, 3)], "artist_id long, label_id long, weight long"
    )
    out = networks.prefixed_bipartite(edges)
    check("columns", out.columns == ["src", "dst", "weight"], str(out.columns))
    pdf = out.toPandas().sort_values(["src", "dst"]).reset_index(drop=True)
    got = list(zip(pdf.src, pdf.dst, pdf.weight))
    check("ids", got == [("a5", "l5", 4), ("a5", "l7", 2), ("a9", "l5", 3)], str(got))
    graph = networks.to_igraph(pdf)
    check("a5 and l5 are distinct nodes", sorted(graph.vs["name"]) == ["a5", "a9", "l5", "l7"], str(graph.vs["name"]))
    check("edge count", graph.ecount() == 3)
    check("graph_artist_ids prefixed", sorted(networks.graph_artist_ids(graph, prefixed=True)) == [5, 9])


def test_node_names(spark) -> None:
    """Names with and without prefixes, restricted to ``ids``, with duplicates removed."""
    print("node_names")
    artists = spark.createDataFrame(
        [(5, "Artist Five"), (6, "Artist Six"), (9, "Artist Nine"), (9, "Artist Nine")],
        "artist_id long, artist_name string",
    )
    labels = spark.createDataFrame(
        [(5, "Label Five"), (7, "Label Seven")], "label_id long, label_name string"
    )
    plain = networks.node_names(artists)
    got = dict(zip(plain.node, plain.name))
    check("plain: all artists, one row per id", got == {5: "Artist Five", 6: "Artist Six", 9: "Artist Nine"} and len(plain) == 3, str(got))
    check("plain: columns", list(plain.columns) == ["node", "name"])
    sel = networks.node_names(artists, ids=[6, 9, 123])
    check("plain with ids", dict(zip(sel.node, sel.name)) == {6: "Artist Six", 9: "Artist Nine"}, str(sel))
    pref = networks.node_names(artists, labels, prefixed=True)
    got = dict(zip(pref.node, pref.name))
    expected = {
        "a5": "Artist Five", "a6": "Artist Six", "a9": "Artist Nine",
        "l5": "Label Five", "l7": "Label Seven",
    }
    check("prefixed: artist 5 and label 5 differ", got == expected, str(got))
    sel = networks.node_names(artists, labels, prefixed=True, ids=["a5", "l5", "l7", "a42"])
    check(
        "prefixed with ids",
        dict(zip(sel.node, sel.name)) == {"a5": "Artist Five", "l5": "Label Five", "l7": "Label Seven"},
        str(sel),
    )
    none = networks.node_names(artists, ids=[])
    check("empty ids give an empty table", len(none) == 0 and list(none.columns) == ["node", "name"], str(none))


def test_describe_communities() -> None:
    """Order, fields, names, tag shares, purity and ``min_size``."""
    print("describe_communities")
    nodes = list(range(1, 11))
    communities = pd.DataFrame(
        {
            "node": nodes,
            "community": [0, 0, 0, 0, 1, 1, 1, 2, 2, 3],  # sizes 4, 3, 2, 1
        }
    )
    centrality = pd.DataFrame(
        {"node": nodes, "pagerank": [0.1, 0.4, 0.3, 0.2, 0.9, 0.5, 0.7, 0.2, 0.3, 1.0]}
    )
    names = pd.DataFrame({"node": nodes[:-1], "name": [f"N{n}" for n in nodes[:-1]]})  # node 10 unnamed
    tags = pd.DataFrame(
        {
            "node": [1, 2, 3, 5, 6, 7, 8],
            "genre": ["Rock", "Rock", "Jazz", "Jazz", "Jazz", "Jazz", "Pop"],
        }
    )
    out = networks.describe_communities(communities, centrality, names, node_tags=tags, top_n=2, min_size=2)
    check("columns", list(out.columns) == ["community", "size", "top_nodes", "n_tagged", "top_tags", "tag_purity"], str(list(out.columns)))
    check("sorted by size, small ones dropped", out["community"].tolist() == [0, 1, 2] and out["size"].tolist() == [4, 3, 2], str(out))
    check("top_nodes by pagerank", out.loc[0, "top_nodes"] == "N2, N3" and out.loc[1, "top_nodes"] == "N5, N7", str(out["top_nodes"].tolist()))
    check("top_tags with share", out.loc[0, "top_tags"] == "Rock (0.67), Jazz (0.33)", out.loc[0, "top_tags"])
    check("purity", close(out.loc[0, "tag_purity"], 2 / 3) and close(out.loc[1, "tag_purity"], 1.0), str(out["tag_purity"].tolist()))
    check("n_tagged", out["n_tagged"].tolist() == [3, 3, 1], str(out["n_tagged"].tolist()))
    check("single-tag community", out.loc[2, "top_tags"] == "Pop (1.00)" and close(out.loc[2, "tag_purity"], 1.0))

    plain = networks.describe_communities(communities, centrality, names, top_n=1, min_size=1)
    check("without tags: only 3 fields", list(plain.columns) == ["community", "size", "top_nodes"], str(list(plain.columns)))
    check("size order with min_size=1", plain["community"].tolist() == [0, 1, 2, 3], str(plain["community"].tolist()))
    check("unnamed node falls back to its id", plain.loc[3, "top_nodes"] == "10", plain.loc[3, "top_nodes"])

    tied = pd.DataFrame({"node": [1, 2], "genre": ["B", "A"]})
    one = pd.DataFrame({"node": [1, 2], "community": [0, 0]})
    pr = pd.DataFrame({"node": [1, 2], "pagerank": [0.5, 0.5]})
    out = networks.describe_communities(one, pr, names, node_tags=tied, min_size=1)
    check("tag ties go to the alphabetically first tag", out.loc[0, "top_tags"] == "A (0.50), B (0.50)", out.loc[0, "top_tags"])

    untagged = networks.describe_communities(
        communities, centrality, names, node_tags=tags[tags["node"] <= 3], min_size=3
    )
    check("community without tags: n_tagged 0, purity NaN", untagged.loc[1, "n_tagged"] == 0 and math.isnan(untagged.loc[1, "tag_purity"]), str(untagged))

    two_cols = tags.assign(extra=1)
    try:
        networks.describe_communities(communities, centrality, names, node_tags=two_cols)
        check("ambiguous tag column raises", False)
    except ValueError:
        check("ambiguous tag column raises", True)
    ok = networks.describe_communities(communities, centrality, names, node_tags=two_cols, tag_col="genre", min_size=3)
    check("tag_col picks the column", ok["community"].tolist() == [0, 1])


def two_cliques():
    """Two 5-cliques joined by one edge (nodes 0-4 and 5-9)."""
    rows = []
    for base in (0, 5):
        for i in range(5):
            for j in range(i + 1, 5):
                rows.append((base + i, base + j, 1))
    rows.append((4, 5, 1))
    return networks.to_igraph(pd.DataFrame(rows, columns=["src", "dst", "weight"]))


def test_resolution_sweep() -> None:
    """Two cliques joined by one edge: 2 communities at resolution 1."""
    print("resolution_sweep")
    graph = two_cliques()
    out = networks.resolution_sweep(graph, resolutions=(0.5, 1.0, 2.0), seed=42, reference=1.0)
    check(
        "columns",
        list(out.columns)
        == ["resolution", "n_communities", "n_communities_ge50", "largest_share", "modularity", "nmi_vs_reference"],
        str(list(out.columns)),
    )
    check("one row per resolution", out["resolution"].tolist() == [0.5, 1.0, 2.0], str(out["resolution"].tolist()))
    ref = out[out["resolution"] == 1.0].iloc[0]
    check("2 communities at resolution 1", ref.n_communities == 2, str(ref.to_dict()))
    check("nmi_vs_reference == 1 at the reference", close(ref.nmi_vs_reference, 1.0), str(ref.nmi_vs_reference))
    check("largest_share is 0.5", close(ref.largest_share, 0.5), str(ref.largest_share))
    check("no community has 50 nodes", bool((out["n_communities_ge50"] == 0).all()))
    check("modularity of the two-clique split", close(ref.modularity, 0.4523809523809524, 1e-6), str(ref.modularity))
    check("nmi in [0, 1]", bool(out["nmi_vs_reference"].between(0, 1 + 1e-9).all()))
    check("coarser resolution has no more communities", out.loc[0, "n_communities"] <= ref.n_communities <= out.loc[2, "n_communities"], str(out))
    off = networks.resolution_sweep(graph, resolutions=(2.0,), reference=1.0)
    check("reference outside resolutions: one row", len(off) == 1 and off.loc[0, "resolution"] == 2.0)
    again = networks.resolution_sweep(graph, resolutions=(0.5, 1.0, 2.0), seed=42)
    check("same seed, same result", again.equals(out))


def test_artist_dominant_tag(spark) -> None:
    """Most frequent tag, alphabetical tie-break, tag_share, duplicates."""
    print("artist_dominant_tag")
    release_artists = spark.createDataFrame(
        [
            (1, 10), (2, 10), (3, 10), (3, 10),  # artist 10: 3 releases (1 listed twice)
            (4, 20), (5, 20),  # artist 20: tie
            (6, 30), (7, 30),  # artist 30: one untagged release
            (8, 40),  # artist 40: no tags at all
            (9, None),  # null artist
        ],
        "release_id long, artist_id long",
    )
    release_genres = spark.createDataFrame(
        [
            (1, "Rock"), (2, "Rock"), (2, "Pop"), (3, "Pop"), (3, "Pop"),
            (4, "Jazz"), (5, "Blues"),
            (6, "Funk"),
            (9, "Rock"),
        ],
        "release_id long, genre string",
    )
    out = networks.artist_dominant_tag(release_artists, release_genres, "genre")
    check("columns", out.columns == ["artist_id", "genre", "tag_share", "n_releases"], str(out.columns))
    got = {r.artist_id: (r.genre, r.tag_share, r.n_releases) for r in out.collect()}
    check("rows: untagged and null artists absent", sorted(got) == [10, 20, 30], str(got))
    check("tie between Pop and Rock (2 releases each) goes to Pop", got[10][0] == "Pop", str(got[10]))
    check("tag_share 2 of 3 releases", close(got[10][1], 2 / 3) and got[10][2] == 3, str(got[10]))
    check("tie goes to the alphabetically first tag", got[20][0] == "Blues" and close(got[20][1], 0.5), str(got[20]))
    check("untagged release counts in the denominator", got[30][0] == "Funk" and close(got[30][1], 0.5), str(got[30]))

    release_styles = spark.createDataFrame(
        [(1, "Techno"), (2, "Techno"), (3, "House")], "release_id long, style string"
    )
    by_style = networks.artist_dominant_tag(release_artists, release_styles, "style")
    row = by_style.where("artist_id = 10").collect()[0]
    check("works for styles", row.style == "Techno" and close(row.tag_share, 2 / 3), str(row))


def test_exclude_label_ids(spark) -> None:
    """``exclude_label_ids`` drops self-released labels before counting."""
    print("artist_label_edges exclude_label_ids")
    release_artists = spark.createDataFrame(
        [(1, 10), (2, 10), (3, 10), (1, 194), (4, 20), (5, 20)],
        "release_id long, artist_id long",
    )
    release_labels = spark.createDataFrame(
        [(1, 1818), (2, 1818), (3, 1818), (1, 7), (2, 7), (4, 1818), (5, 8), (3, 7)],
        "release_id long, label_id long",
    )
    base = networks.artist_label_edges(release_artists, release_labels, min_weight=1)
    got = {(r.artist_id, r.label_id): r.weight for r in base.collect()}
    check(
        "default keeps 1818",
        got == {(10, 1818): 3, (10, 7): 3, (20, 1818): 1, (20, 8): 1},
        str(got),
    )
    out = networks.artist_label_edges(release_artists, release_labels, min_weight=1, exclude_label_ids=(1818,))
    got = {(r.artist_id, r.label_id): r.weight for r in out.collect()}
    check("1818 removed", got == {(10, 7): 3, (20, 8): 1}, str(got))
    out = networks.artist_label_edges(release_artists, release_labels, min_weight=2, exclude_label_ids=(1818,))
    got = {(r.artist_id, r.label_id): r.weight for r in out.collect()}
    check("works with min_weight", got == {(10, 7): 3}, str(got))
    out = networks.artist_label_edges(release_artists, release_labels, min_weight=1, exclude_label_ids=[1818, 8])
    check("any iterable of ids", {(r.artist_id, r.label_id) for r in out.collect()} == {(10, 7)})
    ids_df = spark.createDataFrame([(1818,), (1818,)], "label_id long")
    out = networks.artist_label_edges(release_artists, release_labels, min_weight=1, exclude_label_ids=ids_df)
    got = {(r.artist_id, r.label_id): r.weight for r in out.collect()}
    check("DataFrame of ids (anti join)", got == {(10, 7): 3, (20, 8): 1}, str(got))


def main() -> int:
    """Run all checks and return the process exit code."""
    spark = ingest.get_spark(
        app_name="vinyl-test-t09", shuffle_partitions=4, extra_conf={"spark.master": "local[2]"}
    )
    spark.sparkContext.setLogLevel("ERROR")
    test_config()
    test_prefixed_bipartite(spark)
    test_node_names(spark)
    test_describe_communities()
    test_resolution_sweep()
    test_artist_dominant_tag(spark)
    test_exclude_label_ids(spark)
    if FAILURES:
        print(f"\nFAILED: {len(FAILURES)} check(s): {FAILURES}")
        return 1
    print("\nAll checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
