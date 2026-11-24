# T10 - clustering stubs: node2vec_embeddings and combine_features

## Goal
Implement the two `NotImplementedError` stubs in `lab2/vinyl/clustering.py`, keeping their signatures, so explorer 07 (T11) can run end to end. The orchestrator runs explorer 07 on full data in T11.

## Context
- Installed in `.venv`: `gensim` 4.4, `numpy`, `scipy`, `scikit-learn` 1.8, `umap-learn` 0.5.12, `hdbscan`, `python-igraph`, and the `node2vec` package 0.5. **Do not install anything.**
- The `node2vec` package precomputes second-order transition probabilities in pure Python. That is too slow and memory-heavy for the graph explorer 07 uses: the collaboration graph at `min_weight=2` has about 114 K nodes and 152 K edges. Do not use it.
- Pod: 16 GiB and 4 CPUs.
- Inputs:
  - `artist_profile` output: `artist_id`, `artist_name`, `n_releases`, `first_year`, `last_year`, `career_span`, `n_labels`, `share_<format>`.
  - `style_tfidf` output, long format: `artist_id`, `style`, `tf`, `idf`, `tfidf`.
  - The edge list from `networks.collaboration_edges`: `src`, `dst`, `weight`.

## Files in scope
- `lab2/vinyl/clustering.py`: the two stubs, plus private helpers. Do not change the signatures of other functions.
- `lab2/explorers/07_artist_clustering.ipynb`: only the two `# TODO: implement` comments on the stub calls. Remove them. Leave the other TODO cell for T11.
- New test script `lab2/scripts/test_t10.py`.

## Requirements
1. **`node2vec_embeddings(edges, dimensions=64, walk_length=30, num_walks=10, p=1.0, q=1.0, seed=42)`.** Keep the signature. You may add keyword-only parameters at the end with defaults, for example `workers: int = 4`, `window: int = 10` and `epochs: int = 1`.
   - Build a CSR adjacency, `scipy.sparse`, undirected and weighted, from `src`, `dst`, `weight`. Node ids can be ints or strings; map them to 0..n-1 and back.
   - Generate walks with numpy:
     - When `p == q == 1`, use first-order weighted walks: the next node is sampled proportional to edge weight, using the CSR row and cumulative weights with `searchsorted`.
     - Otherwise use the node2vec second-order bias by **rejection sampling**, as in KnightKing or pecanpy's "SparseOTF". Propose from the weighted first-order distribution, then accept with probability `alpha / max_alpha`, where `alpha = 1/p` for returning to the previous node, `1` for a neighbor of the previous node, and `1/q` otherwise.
     - Neighbor checks use the sorted CSR indices of the previous node with `np.searchsorted`.
     - Vectorize across walkers where reasonable. A per-step Python loop over all walkers at once is fine; a Python loop per walker per step is too slow.
   - A walk from an isolated node is just that node.
   - Train `gensim.models.Word2Vec(sentences=walks_as_str_lists, vector_size=dimensions, window=window, min_count=0, sg=1, negative=5, workers=workers, seed=seed, epochs=epochs)`.
   - Return a pandas DataFrame with `node`, in the original id type, plus `n2v_0` .. `n2v_{d-1}`, one row per node.
   - Document in Notes:
     - gensim with `workers > 1` is not bit-reproducible, so pass `workers=1` for exact reproducibility;
     - the walks are reproducible given `seed`, using `np.random.default_rng(seed)`;
     - memory is about `n_nodes * num_walks * walk_length` ints.
   - Replace the TODO in the docstring with the choice made and why.
2. **`combine_features(profile, tfidf, embeddings, weights=(1.0, 1.0, 1.0))`.** Keep the signature. You may add keyword-only parameters at the end with defaults, for example `svd_components: int = 50` and `seed: int = 42`.
   - **Rows:** the artists in `profile`, in a stable order sorted by `artist_id`. Return the matrix and the matching `pd.Series` of artist ids.
   - **Profile block:**
     - numeric columns are `n_releases`, `career_span`, `n_labels` and every `share_*`. Drop `artist_name`, `first_year` and `last_year`, or document the choice;
     - apply `log1p` to the count columns `n_releases` and `n_labels`;
     - then standardize with `StandardScaler`.
   - **TF-IDF block:**
     - pivot the long table to a `scipy.sparse` matrix, rows aligned to `profile`, so artists with no styles get an all-zero row;
     - reduce with `TruncatedSVD(n_components=min(svd_components, n_styles - 1), random_state=seed)`;
     - then L2-normalize rows. Zero rows stay zero.
   - **Embedding block:**
     - align `embeddings` on `node == artist_id`, after casting both to the same type;
     - artists not in the graph get a zero vector;
     - L2-normalize rows;
     - also append a 0/1 column `in_graph` to the profile block before scaling, so the zero vectors are distinguishable.
   - **Weights:** scale each block so it contributes equally before weighting. Multiply block `b` by `weights[b] / sqrt(n_columns_b)`, then `hstack`. A weight of 0 drops that block's influence. A block with zero columns, such as empty embeddings, is skipped.
   - Return `(np.ndarray float32, pd.Series)`.
   - Replace the TODO in the docstring with these choices.
   - Raise `ValueError` if `profile` is empty or if `weights` does not have 3 entries.
3. Numpy docstrings and no new TODOs in `clustering.py`.

## Acceptance checks
Spark 2g at most, and only if needed. These functions are driver-side pandas/numpy. Use `timeout`.

1. `cd lab2 && timeout 900 python scripts/test_t10.py` exits 0. It covers:
   - **Walks:** on a hand-built graph of two 10-node cliques joined by one edge:
     - with `p=q=1`, every step follows an existing edge;
     - walk lengths are right;
     - an isolated node gives a length-1 walk;
     - with `p=0.25, q=4` (stay local), the share of steps that cross the bridge is lower than with `p=4, q=0.25` (explore). Test this on walk statistics, through a private helper exposed for testing.
   - **Embeddings:** same graph, `dimensions=16`, `workers=1`. The mean cosine similarity within a clique is greater than between cliques, and the output has one row per node with the right columns and id type. Run it once with string node ids too.
   - **Reproducibility:** two calls with the same seed and `workers=1` return identical walks.
   - **`combine_features`:** with tiny profile, tfidf and embeddings frames:
     - row order and ids;
     - an artist with no styles gets a zero TF-IDF block, and one not in the graph gets a zero embedding block with `in_graph == 0`;
     - `weights=(1,0,0)` gives zeros in the TF-IDF and embedding columns;
     - the output dtype is float32;
     - `ValueError` cases.
   - **Scale smoke:** a random graph with 100 K nodes and 150 K edges (`numpy` random, seed 0), `dimensions=32`, `num_walks=2`, `walk_length=20`, `workers=4`. Print the elapsed time. It must finish within the timeout. If it takes over 10 minutes, report it as a problem.
2. The explorer 07 JSON is valid, and the edited cell parses with `ast.parse` (skip `%` lines). Do not run the explorer.
3. `git diff --stat` touches only files in scope.

## Out of scope
Running explorer 07, choosing the final parameters (T11), other modules, and Parquet changes.
