# T11 - explorer 07 (artist clustering): analysis cells

## Goal
Make `lab2/explorers/07_artist_clustering.ipynb` ready for the orchestrator to run on the full 2026 data. Fill the TODO cell (cell 12) with code. Put any non-trivial logic in `lab2/vinyl/clustering.py`. The orchestrator runs the notebook on full data after the merge and fills in the "Decisions" checklist from its outputs. Leave that checklist as is.

## Context
- T10 implemented `node2vec_embeddings` (numpy walks plus gensim) and `combine_features`.
- The collaboration graph at `min_weight=2` over all artists has about 114 K nodes and 152 K edges. Explorer 07 builds it only over profiled artists (`min_releases=5`), so it is smaller.
- On this pod, node2vec with the default 10 walks × 30 steps over about 100 K nodes is estimated at 10-30 minutes, most of it Word2Vec. UMAP and HDBSCAN on about 100-200 K rows also take minutes.
- `config.GRAPH_MIN_WEIGHT["collaboration"] = 2`. Leiden lives in `networks.leiden_communities(graph, resolution, seed)`, and `networks.to_igraph(edges_pd)` builds the graph. Explorer 06 does not save its communities, so recompute Leiden here on the same collaboration edges used for node2vec. It is cheap.
- Pod: 16 GiB and 4 CPUs.

## Files in scope
- `lab2/vinyl/clustering.py`: new functions only.
- `lab2/explorers/07_artist_clustering.ipynb`: the status markdown (cell 1), the code cells and section headers. Keep the intro markdown and the "Decisions" cell.
- New test script `lab2/scripts/test_t11.py`.

## Requirements
1. **Status cell.** Cell 1 still says the functions are stubs. Replace it with a one-line note that they were implemented in T10, plus the expected runtime.
2. **Timing and caching.**
   - Wrap the slow steps (node2vec, the two UMAP calls and HDBSCAN) with simple elapsed-time prints.
   - After node2vec, save the embeddings to `$DISCOGS_DATA_DIR/derived/2026/artist_n2v.parquet` with pandas and pyarrow.
   - Add a flag `REUSE_EMBEDDINGS = True` that loads the file if it exists, so tuning reruns skip node2vec. Do the same for `features` and `embedding_10d`/`embedding_2d` as `.npy` files in that folder, behind the same flag.
   - Use `config.parquet_path`'s base or `os.environ["DISCOGS_DATA_DIR"]` for the folder. Never write under `/mnt/data`.
3. **Use `config.GRAPH_MIN_WEIGHT["collaboration"]`** for the node2vec edges, instead of the literal 2.
4. **`clustering.describe_clusters(cluster_labels, artist_ids, profile, tfidf, top_styles=3, examples=5) -> pandas.DataFrame`.** One row per cluster, with noise `-1` as its own row flagged `is_noise`, sorted by size. Columns:
   - `cluster`, `size` and `share`;
   - `top_styles`: the top styles by mean TF-IDF inside the cluster, with values;
   - `format mix`: mean of each `share_*` column, as separate columns `mean_share_vinyl` and so on;
   - `median_career_span`, `median_n_releases` and `median_first_year`;
   - `examples`: names of the `examples` artists with the most releases.
5. **`clustering.min_cluster_size_sweep(embedding, sizes=(25, 50, 100, 200), min_samples=None) -> pandas.DataFrame`.** One row per size with `n_clusters`, `noise_share`, `largest_cluster_share` and elapsed seconds. In the explorer, run it on `embedding_10d`, print it, and keep the default `min_cluster_size=50` for the main labels unless the orchestrator changes it after the run. Do not pick automatically.
6. **`clustering.compare_partitions(labels_a, labels_b, ids_a, ids_b, drop_noise=True) -> dict`.**
   - Aligns the two labelings on common ids and drops `-1` from `labels_a` when `drop_noise`.
   - Returns `n_common`, `nmi`, `ari` and a pandas contingency table of the top 10 × top 10 groups.
   - In the explorer, compare the HDBSCAN clusters with Leiden communities (resolution 1.0, seed 42) on the same collaboration graph, covering artists in both.
   - Also compare clusters with each artist's dominant genre via `networks.artist_dominant_tag(real_artists, release_genres, "genre")`. Load `release_genres` in the data cell.
6b. **Scatter plot.**
   - Plot `embedding_2d` coloured by cluster, with noise in light grey and small points (`s=1`, `alpha=0.3`).
   - Label the 10 largest clusters at their medians with the cluster id.
   - Use the top-20 clusters for colors and draw the rest grey. Save it with `viz.save_figure(fig, "07_umap_clusters")`.
   - If there are more than 200 K points, subsample 200 K for the plot only (seed 42).
7. Replace the TODO cell. Numpy docstrings and no new TODOs.

## Acceptance checks
1. `cd lab2 && DISCOGS_DATA_DIR=$PWD/data timeout 900 ../.venv/bin/python scripts/test_t11.py` exits 0. **Use tiny synthetic data only, with no real-data section and no Spark.** It covers:
   - `describe_clusters` on a hand-built example with 2 clusters and noise: sizes, shares, noise flag, top styles and examples order;
   - `min_cluster_size_sweep` on `sklearn.datasets.make_blobs` with 3 blobs, 1,500 points, where the small sizes find 3 clusters;
   - `compare_partitions`: identical partitions give NMI 1 and ARI 1, noise is dropped, and only common ids are used.
2. The notebook is valid JSON, every code cell passes `ast.parse` with `%` lines skipped, and no cell has a TODO. **Do not run the notebook.** The orchestrator runs it on full data.
3. `git diff --stat` touches only files in scope.

## Out of scope
Running explorer 07 on full data, changing `node2vec_embeddings` or `combine_features`, the Decisions checklist, and `main.ipynb`.
