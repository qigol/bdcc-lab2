# T05 - explorer 02 (genres and styles, RQ1)

## Goal
Make `lab2/explorers/02_genres_and_styles.ipynb` ready for the orchestrator to run on the full 2026 data. Fill its TODOs with code. Put any non-trivial logic in `lab2/vinyl/trends.py`, so `main.ipynb` can reuse it. The orchestrator runs the notebook on full data after the merge and fills in the "Decisions" checklist from its outputs. Leave that checklist as is.

## Context
- Tables, all small enough for Spark with a 4g heap:
  - `release_genres`: 4.79 M rows.
  - `release_styles`: 6.49 M rows.
  - `releases`: 3.83 M rows.
  - Schema of `release_styles`: `release_id` long, `year` int, `style` string.
- The 2026 snapshot (`config.MAIN_SNAPSHOT`) is dated 1 Oct 2026:
  - `config.LAST_FULL_YEAR = 2025`.
  - `config.PARTIAL_YEAR_SHARE = {2026: 0.739}`.
  - Recent years are also undercounted because cataloguing lags. `snapshots.estimate_undercount` covers this.
- `networkx` 3.6 and `python-igraph` are installed in the venv.

## Files in scope
- `lab2/vinyl/trends.py`: new functions only. Do not change existing signatures.
- `lab2/explorers/02_genres_and_styles.ipynb`: code cells and section headers. Keep the intro markdown and the "Decisions" cell.
- New test script `lab2/scripts/test_t05.py`.

## Requirements
1. **Titles.** Replace the `"TODO: ..."` plot titles with real titles. Mark 2026 as a partial year in the title or with a note. Trend comparisons stop at `config.LAST_FULL_YEAR`, not at a hard-coded year.
   - Change the `top_movers` call to `end_year=config.LAST_FULL_YEAR`.
   - Also show the `end_year=2024` version, as a check of whether the lag in 2025 changes the ranking.
2. **Network structure per year** (TODO 1). Add `trends.cooccurrence_graph_stats(cooc: DataFrame) -> pandas.DataFrame`.
   - Collect the per-year edge list. It is small: a few thousand edges per year at `min_count=50`.
   - Build one undirected `networkx` graph per year, with weight `jaccard`.
   - Return one row per year: `n_nodes`, `n_edges`, `density`, `n_components`, `largest_component_share` (nodes in the largest component / nodes), and `modularity`.
   - Compute `modularity` with `networkx.community.louvain_communities(G, weight="weight", seed=config.SEED if it exists, else 42)` and `networkx.community.modularity`.
   - In the explorer, show the table and plot `density` and `modularity` by year.
3. **New styles vs evener use** (TODO 2). Add `trends.entropy_decomposition(release_tags, tag_col="style", base_year=2016) -> DataFrame` to separate the two effects.
   - Return one row per year with:
     - `entropy` and `evenness`, the same as `shannon_entropy_by_year`;
     - `entropy_base_tags`: entropy computed only over tags that already existed in `base_year`, renormalized;
     - `n_new_tags`: tags used that year that were absent in `base_year`;
     - `new_tag_pair_share`: the share of (release, tag) pairs that carry those new tags.
   - If `entropy_base_tags` rises like `entropy`, the change is evener use of old styles. If it stays flat, the change comes from new styles appearing.
   - Add a short markdown cell that states this reading rule. Do not write conclusions, since the numbers do not exist yet.
4. **Sensitivity to undercounted years** (TODO 3). Add `trends.entropy_sensitivity(release_tags, releases, tag_col="style", n_draws=5, seed=42) -> DataFrame`. It tests whether entropy could rise or fall only because the last years have fewer releases.
   - For every year, draw `n_draws` random samples of release ids. Each sample is the size of the smallest full year in 2016-`config.LAST_FULL_YEAR`.
   - Compute evenness on each sample.
   - Return per year `evenness_full`, `evenness_sub_mean`, `evenness_sub_min` and `evenness_sub_max`.
   - Use `releases` only for the release ids per year. Sample with `F.rand(seed)` ordering or `sampleBy`; do not collect release ids to the driver.
   - The explorer plots full and subsampled evenness together.
5. Each new function gets a numpy docstring in the module's style. It returns a small aggregated DataFrame, or a pandas DataFrame where the docstring says so. No new TODOs. Remove the old TODO comment cell once its three items are covered.
6. Keep the explorer pattern: notebook cells only call `vinyl` functions, `viz` plots and short display code.

## Acceptance checks
Spark driver memory is 4g at most (`PYSPARK_SUBMIT_ARGS="--driver-memory 4g pyspark-shell"`). The orchestrator may be running Spark at the same time, and the pod limit is 16 GiB. Use `timeout`.

1. `cd lab2 && DISCOGS_DATA_DIR=$PWD/data timeout 900 python scripts/test_t05.py` exits 0. It covers:
   - **`cooccurrence_graph_stats`:** a tiny hand-built edge list with two years, where one year has two components. Check `n_nodes`, `n_edges`, `n_components` and `largest_component_share`, and that `modularity` is in [-0.5, 1].
   - **`entropy_decomposition`:** a tiny DataFrame with a new tag appearing in year 2.
     - `n_new_tags` is 0 in the base year and 1 in year 2.
     - `entropy_base_tags` equals the hand-computed value.
   - **`entropy_sensitivity`:** a tiny DataFrame. The output has one row per year, and `min <= mean <= max`.
   - **Real-data run:** every new function on `release_styles` from `lab2/data/parquet/20261001`, limited to years 2016 and 2025 via `where`. Print the outputs.
2. Run the edited notebook top to bottom with `jupyter nbconvert --execute` and write the output to `.orchestration/runs/T05/02.worker.ipynb`. Do not overwrite the tracked notebook's outputs.
   - Use the `vinyl` kernel, with `DISCOGS_DATA_DIR=$PWD/data` and the 4g heap.
   - The tables are small, so the full run is allowed. If a cell takes over 10 minutes, stop it and report.
   - The run must have no errors. Paste the entropy, decomposition and graph-stats tables in your summary.
3. `git diff --stat` touches only the files in scope.

## Out of scope
Filling the "Decisions" checklist, `main.ipynb`, other explorers, and any change to the Parquet tables.
