# T12a - main.ipynb code cells: final parameters, explorer functions, dashboard exports

## Goal
Bring the code cells of `lab2/main.ipynb` (the report) in line with what explorers 02-07 settled. After this task the orchestrator runs `main.ipynb` top to bottom on full data. T12b then writes the prose from those outputs. **This task does not write findings prose.**

## Context
- Executed explorers with full-data outputs are in `lab2/explorers/executed/` (02-06). Explorer 07 is being run by the orchestrator now. Read the executed copies to see which functions and parameters each explorer used.
- Functions added since main.ipynb was written:
  - `trends`: `entropy_decomposition`, `entropy_sensitivity`, `cooccurrence_graph_stats`, `prolific_artist_years`, `drop_prolific_releases`.
  - `formats`: `main_formats`, `group_minor_formats`, `reissue_age_distribution`, `top_pressing_plants`, `pressing_plant_coverage`, `format_coverage`, `growth_explained_by_reissues`.
  - `labels`: `group_concentration`, `parent_coverage` (with `self_released`), `self_released_label_ids`, `credit_level_coverage`.
  - `snapshots`: `major_root_check`, `major_moves`, `explain_move`, `cataloguing_lag_by`; `estimate_undercount` has `partial_year_share`.
  - `networks`: `GRAPH_MIN_WEIGHT`, `artist_dominant_tag`, `describe_communities`, `resolution_sweep`, `node_names`, `prefixed_bipartite`, and `artist_label_edges(exclude_label_ids=<DataFrame>)`.
  - `clustering`: real `node2vec_embeddings` and `combine_features`, plus `describe_clusters`, `min_cluster_size_sweep`, `compare_partitions`, `plot_clusters_2d`, `timed`.
- Config: `LAST_FULL_YEAR = 2025`, `PARTIAL_YEAR_SHARE`, `GRAPH_MIN_WEIGHT`, `SELF_RELEASED_LABEL_*`, `PROLIFIC_ARTIST_MIN_PER_YEAR`, `MAJOR_ROOT_IDS`.
- **Performance traps already found:**
  - Never pass the 312,527 self-released ids as a Python list. Pass the DataFrame from `labels.self_released_label_ids`.
  - `resolve_label_roots` on 2.4 M labels is slow, so cache the roots once and reuse them.
  - `entropy_sensitivity` takes about 6-8 min.
  - The credit-graph resolution sweep takes 7 min, so skip it in main; the collaboration sweep is enough.
- Explorer 07 caches node2vec, features and UMAP output under `$DISCOGS_DATA_DIR/derived/2026/`. main must reuse those files when present, with the same id checks as explorer 07, so the report run does not repeat 30+ minutes of node2vec.

## Files in scope
- `lab2/main.ipynb`: code cells, and the markdown cells listed below only.
- `lab2/vinyl/config.py`: add `HDBSCAN_MIN_CLUSTER_SIZE: int = 50` and `UMAP_N_NEIGHBORS: int = 30`. The orchestrator may change the values after explorer 07.
- New module `lab2/vinyl/export.py`.
- New test script `lab2/scripts/test_t12a.py`.

## Requirements
1. **`vinyl/export.py`.**
   - Add `DASHBOARD_DATA_DIR = config.PROJECT_ROOT / "dashboard" / "data"`.
   - Add `export_table(df, name, index=False) -> Path`. It writes a pandas DataFrame as CSV, UTF-8, with floats rounded to 6 significant digits. It creates the folder and returns the path.
   - Add `export_manifest(entries: dict[str, str]) -> Path`, which writes `manifest.json` mapping each table name to a one-line description.
   - Numpy docstrings. These tables are small aggregates and are tracked in git.
2. **Every result section of main.ipynb** uses the final parameters and exports its key table for the dashboard. One `export.export_table` call per table below:
   - **5.1:**
     - genre shares (`rq1_genre_shares`);
     - style movers 2016 vs `LAST_FULL_YEAR` (`rq1_style_movers`);
     - entropy and evenness with the decomposition columns (`rq1_entropy`);
     - the sensitivity band (`rq1_entropy_sensitivity`);
     - co-occurrence graph stats per year (`rq1_style_network`);
     - prolific-artist robustness, as the Spearman value plus the top movers without prolific artists (`rq1_movers_no_prolific`).
     - Replace the `TODO` plot titles with neutral descriptive titles; findings go in T12b. Mark 2026 as partial.
   - **5.2:**
     - shares of `formats.main_formats`, minor formats grouped (`rq2_format_shares`);
     - vinyl reissue share for all three flags (`rq2_vinyl_reissues`);
     - growth explained (`rq2_growth_explained`);
     - reissue share by genre for vinyl (`rq2_reissue_by_genre`);
     - reissue age distribution and median age, vinyl and CD (`rq2_reissue_age`);
     - pressing-plant coverage and top plants (`rq2_pressing_plants`).
     - Set `REISSUE_FLAG = "is_reissue_any"` with a comment: the broadest definition, reported alongside the other two.
   - **5.3:**
     - cache the label roots once;
     - major share (`rq3_major_share`);
     - group concentration overall and by year (`rq3_group_concentration`);
     - label concentration by major (median HHI and top share);
     - parent coverage with `self_released` (`rq3_parent_coverage`);
     - writer credit levels (`rq3_writer_levels`).
     - For "top producers/writers", show the top 5 for a fixed list of flagship labels: Columbia 1866, Atlantic 681, Interscope 2311, Island 8377, Def Jam 4241, Warp Records 23528, Sub Pop 77343, Domino 26465 (ids checked against the 2026 labels table). Put the list in a cell constant.
   - **5.4:**
     - `major_root_check`;
     - parent-change counts;
     - `major_moves` counts and the top 20 (`cmp_major_moves`);
     - `explain_move` for the top 3;
     - cataloguing lag and completeness by age (`cmp_lag`);
     - undercount with and without the seasonal factor (`cmp_undercount`);
     - lag by format and by top 8 genres (`cmp_lag_by_format`, `cmp_lag_by_genre`).
   - **5.5:**
     - build the three graphs with `GRAPH_MIN_WEIGHT`, excluding self-released labels with the DataFrame;
     - node and edge counts;
     - Leiden for each graph;
     - agreement with dominant genre and style for each graph, gathered in one table (`rq4_graph_summary`);
     - `describe_communities` for the top 10 of the collaboration and artist-label graphs (`rq4_communities_collab`, `rq4_communities_artist_label`);
     - collaboration resolution sweep only (`rq4_resolution_sweep`);
     - top 15 by PageRank with names for each graph (`rq4_top_pagerank`).
   - **5.6:**
     - profile, TF-IDF and node2vec, reusing the cached files as explorer 07 does;
     - `combine_features`;
     - UMAP with `config.UMAP_N_NEIGHBORS`;
     - HDBSCAN with `config.HDBSCAN_MIN_CLUSTER_SIZE`;
     - the scatter via `plot_clusters_2d`, saved as `rq4_clusters`;
     - `describe_clusters` (`rq4_clusters`);
     - comparison with Leiden and with dominant genre (`rq4_cluster_agreement`);
     - the noise share;
     - and a 2-D sample for the dashboard: at most 20,000 points with `x`, `y`, `cluster` and `artist_name`, seed 42 (`rq4_cluster_points`).
   - **Data section (cell 8 area):** after the size table, add one cell that computes the numbers the Data finding needs: releases in scope, distinct linked artists and labels, the share of releases with at least one style, and the share with a usable year. The year share is 1.0 by construction, so instead print the splitter's stats from `ingest.read_stage_stats` if available. Otherwise state "year filter applied at ingest". Export it as `data_summary`.
   - At the end of section 5, write `export.export_manifest(...)` listing every exported table.
3. **Markdown cells to update. Keep the meaning and do not invent results.**
   - Cell 0: replace the "Status: skeleton" line with "Status: code final; results run on full data on <date of run>". Use the placeholder `<RUN DATE>` for T12b to fill.
   - Cell 10, section 4.3:
     - replace the two TODOs with the parameter values now in config and code: the reissue definition, `GRAPH_MIN_WEIGHT`, the Leiden resolution and seed, node2vec (64 dims, 10 walks × 30 steps, p = q = 1), UMAP neighbours, the HDBSCAN size, the self-released class, placeholder label 212, and majors by root id;
     - add a sentence on the 2026 seasonal factor (0.739).
   - Cell 54, Appendix A: add `vinyl/export.py`, `explorers/executed/` and `dashboard/`.
   - Leave every `> **Finding (TODO)**` cell and sections 1 and 6-10 for T12b.
4. No `TODO` left in **code** cells. Keep `betweenness=False`. Use `clustering.timed` around the slow steps. Notebook cells only call `vinyl` functions and short display code.

## Acceptance checks
1. `cd lab2 && timeout 600 ../.venv/bin/python scripts/test_t12a.py` exits 0. Use tiny data only.
   - `export_table` writes the CSV with rounding and creates the folder; point it at a temp dir with monkeypatching or an argument.
   - `export_manifest` writes valid JSON.
   - `main.ipynb` checks:
     - it is valid JSON;
     - every code cell passes `ast.parse` with `%` lines skipped;
     - no code cell contains `TODO`;
     - every `export_table` name listed above appears exactly once;
     - the `Finding (TODO)` markdown cells are still present.
2. **Do not run main.ipynb or any Spark on full data.** The orchestrator runs it.
3. `git diff --stat` touches only files in scope.

## Out of scope
Findings prose and sections 1 and 6-10 (T12b), the dashboard HTML (T13), and changes to analysis functions in `vinyl/` other than `export.py` and the two config constants.
