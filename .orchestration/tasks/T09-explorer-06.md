# T09 - explorer 06 (networks and communities, RQ4)

## Goal
Make `lab2/explorers/06_networks_and_communities.ipynb` ready for the orchestrator to run on the full 2026 data. Fill its TODOs with code, with any non-trivial logic in `lab2/vinyl/networks.py`. The orchestrator runs the notebook and fills in the "Decisions" checklist. Leave that checklist as is.

## Facts (2026, measured by the orchestrator with the current edge builders)
| graph | min_weight 1 | 2 | 3 | 5 | 10 |
|---|---|---|---|---|---|
| collaboration (`max_artists_per_release=20`) | 637 K edges / 338 K nodes | 152 K / 114 K | 61 K / 54 K | 20 K / 21 K | 4.4 K / 5.3 K |
| credit (`PRODUCTION_ROLES`) | 1.73 M / 811 K | 641 K / 358 K | 334 K / 209 K | 142 K / 102 K | 41 K / 35 K |
| artist-label | 1.93 M / 1.27 M | 721 K / 576 K | 386 K / 350 K | 180 K / 185 K | 63 K / 74 K |

- All of these fit on the driver with igraph. Defaults: collaboration `min_weight=2`, credit `min_weight=3`, artist-label `min_weight=3`. Put these in config as `GRAPH_MIN_WEIGHT: dict[str, int]`.
- Artist ids and label ids overlap as integers. The artist-label graph needs prefixed string ids ("a123" / "l123"). The credit graph links main artists to credited people, and both are artist ids, so no prefix is needed there.
- Placeholder artists are `PLACEHOLDER_ARTIST_IDS` (194, 355). Self-released labels are `config.SELF_RELEASED_LABEL_IDS` and `labels.self_released_label_ids(labels)`, from T08b. Drop self-released labels from the artist-label graph, otherwise "Not On Label" becomes one giant hub.
- Pod: 16 GiB and 4 CPUs. Spark driver 4g max for your runs.

## Files in scope
- `lab2/vinyl/networks.py`: new functions, plus an optional `exclude_label_ids` parameter added at the end of `artist_label_edges`'s signature with default `()`. Do not change other existing signatures.
- `lab2/vinyl/config.py`: `GRAPH_MIN_WEIGHT` only.
- `lab2/explorers/06_networks_and_communities.ipynb`: code cells and section headers. Keep the intro markdown and the "Decisions" cell.
- New test script `lab2/scripts/test_t09.py`.

## Requirements
1. **`networks.node_names(artists, labels=None, prefixed=False) -> pandas.DataFrame`.**
   - Returns `node` and `name`, collected from the artists table and optionally the labels table.
   - With `prefixed=True`, nodes are "a<id>" / "l<id>".
   - Only the ids actually needed should be collected. Accept an optional `ids` list, or document a join-then-collect pattern. Do not collect all 10 M artist names.
2. **`networks.prefixed_bipartite(edges) -> DataFrame`.** Turns `artist_label_edges` output into `src` = "a<artist_id>", `dst` = "l<label_id>", `weight`, ready for `to_igraph`.
3. **`networks.describe_communities(communities, centrality, names, node_tags=None, top_n=5, min_size=50) -> pandas.DataFrame`.** One row per community of at least `min_size` nodes, with:
   - `community` and `size`;
   - `top_nodes`: names of the top `top_n` nodes by pagerank, as a comma-joined string;
   - and, when `node_tags` is given, `top_tags` (top 3 tags with share) and `tag_purity`.
   - Sorted by size.
4. **`networks.resolution_sweep(graph, resolutions=(0.5, 1.0, 2.0), seed=42, reference=1.0) -> pandas.DataFrame`.** One row per resolution with:
   - `n_communities`;
   - `n_communities_ge50`;
   - `largest_share`;
   - `modularity`;
   - `nmi_vs_reference`: NMI of the membership against the run at `reference`.
5. **`networks.artist_dominant_tag(release_artists, release_tags, tag_col) -> DataFrame` (Spark).** Move the "dominant genre per artist" code from the explorer into this function. Each artist gets the tag on most of their releases, with ties broken alphabetically. Also return `tag_share`, the dominant tag's share of the artist's releases. Use it for both genre and style.
6. **Explorer.**
   - Build all three graphs with `config.GRAPH_MIN_WEIGHT`, and print node and edge counts.
   - For each graph:
     - centrality top 20 with names;
     - Leiden at resolution 1.0, seed 42, with modularity;
     - `describe_communities` for the top 10 communities;
     - agreement against dominant genre and against dominant style (NMI, ARI, purity).
   - For the credit graph, dominant tags are only defined for main artists, so report agreement on the nodes that have tags and say so.
   - For the artist-label graph, compute agreement on artist nodes only.
   - Run a resolution sweep on the collaboration graph, and on the credit graph if it takes under 5 minutes. Print the timing.
   - Gather the per-graph agreement numbers in one summary table at the end. The report will quote it.
   - Replace the TODO cell. Keep `betweenness=False` (too slow).
7. Numpy docstrings and no new TODOs. Notebook cells only call `vinyl` functions and short display code.

## Acceptance checks
Spark 4g at most. Use `timeout`.

1. `cd lab2 && DISCOGS_DATA_DIR=$PWD/data timeout 600 python scripts/test_t09.py` exits 0. **Use tiny hand-built data only, with no real-data section.** It covers:
   - `prefixed_bipartite`, including a collision case: artist 5 and label 5 stay distinct nodes;
   - `node_names` with and without prefixes;
   - `describe_communities` ordering and fields;
   - `resolution_sweep` on a small graph of two cliques joined by one edge, which at resolution 1 finds 2 communities, with `nmi_vs_reference == 1` at the reference;
   - `artist_dominant_tag` tie-breaking and `tag_share`;
   - `artist_label_edges(..., exclude_label_ids=(1818,))`.
2. **Do not run the notebook on full data.** The orchestrator does that. Check the notebook is valid JSON and every code cell passes `ast.parse`, skipping `%` lines.
3. `git diff --stat` touches only files in scope.

## Out of scope
The "Decisions" checklist, `main.ipynb`, other explorers, clustering (T10), and Parquet changes.
