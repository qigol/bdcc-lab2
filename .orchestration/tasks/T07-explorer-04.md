# T07 - explorer 04 (labels and the majors, RQ3)

## Goal
Make `lab2/explorers/04_labels_and_majors.ipynb` ready for the orchestrator to run on the full 2026 data. Fill its TODOs with code. Put any non-trivial logic in `lab2/vinyl/labels.py`. The orchestrator runs the notebook on full data and fills in the "Decisions" checklist. Leave that checklist as is.

## Context
- Majors are tagged by root id, from T04. Label 212 is a placeholder parent and is not followed.
- Full 2026 labels: Universal 8,080, Sony 3,633, Warner 3,087, Independent 2,411,275. 978 labels do not reach a root at `max_depth=12`.
- Tables (2026):
  - `labels`: 2.43 M rows.
  - `release_labels`: 4.26 M rows. Columns `release_id`, `year`, `label_id`, `label_name`, `catno`.
  - `release_artists`: 4.49 M rows.
  - `release_credits`: 22.4 M rows, with a new `level` column ("release"/"track").
- Placeholder artists are `networks.PLACEHOLDER_ARTIST_IDS = (194, 355)`.
- Pod limit is 16 GiB. Use a 4g heap. `release_credits` is the largest table here. Joins on it are fine with 4g, but `.cache()` only small results.

## Files in scope
- `lab2/vinyl/labels.py`: new functions only. Do not change existing signatures.
- `lab2/explorers/04_labels_and_majors.ipynb`: code cells and section headers. Keep the intro markdown and the "Decisions" cell.
- New test script `lab2/scripts/test_t07.py`.

## Requirements
1. **Small fixes in the notebook.**
   - Replace the hard-coded `artist_id != 194` with `~F.col("artist_id").isin(list(networks.PLACEHOLDER_ARTIST_IDS))`. Import `networks` in the setup cell.
   - Section 1 should show the three root rows by id (`labels.where(label_id in MAJOR_ROOT_IDS.values())`), plus the name-regex view that is there today.
   - Replace the `"TODO: ..."` plot title with a real one, and mark 2026 as partial.
   - Add a plot of major share by year (Universal, Sony, Warner, Independent) using `viz.plot_stacked_share` if it fits, otherwise `plot_lines`.
2. **Concentration for the whole major tree** (TODO 1). Add `labels.group_concentration(release_labels, release_artists, tagged_labels, group_col="major", min_releases=50) -> DataFrame`. It works like `artist_concentration`, but it groups by `group_col` from `tagged_labels` instead of by `label_id`.
   - One row per group with `n_pairs`, `n_artists`, `top_artist_id`, `top_artist_name`, `top_artist_share` and `hhi`.
   - A release on two labels of the same group counts once for that group. Deduplicate (release, group) before joining artists.
   - Optionally accept `by_year: bool = False`. When true, add `year` to the grouping so the explorer can show HHI per major per year.
3. **Releases on labels with no parent information** (TODO 2). Add `labels.parent_coverage(release_labels, label_roots) -> DataFrame`, one row per year, with distinct-release counts for each class:
   - `on_root_alone`: the label has no parent (or only a placeholder parent) and no children in the labels table;
   - `in_tree`: the label has a parent, or has children;
   - `unknown_label`: the label id is not in the labels table;
   - and the share of each.
   - "Children" means some label's `parent_id` (after the placeholder mask) points to it. Compute that inside the function from `label_roots`, or take `labels` as an extra parameter if that is cleaner, and document it.
   - A release on several labels goes to its "best" class, in the order `in_tree` > `on_root_alone` > `unknown_label`.
   - The explorer prints this table. It is what tells the reader how much "Independent" might be hidden major output.
4. **Contributors.** Keep the existing `top_contributors` calls.
   - Add one cell that compares release-level and track-level credit coverage for writers: distinct releases with a writer role, by `level`, per year.
   - Add a short markdown note: `top_contributors` counts both levels.
5. Numpy docstrings in the module's style. No new TODOs. Remove the TODO comment cell once covered. Notebook cells only call `vinyl` functions, `viz` plots and short display code.

## Acceptance checks
Spark driver memory 4g at most. Use `timeout`.

1. `cd lab2 && DISCOGS_DATA_DIR=$PWD/data timeout 900 python scripts/test_t07.py` exits 0. It covers, on tiny hand-built DataFrames:
   - **`group_concentration`:** two majors, one release on two labels of the same major counted once, and HHI checked by hand. Also the `by_year` variant.
   - **`parent_coverage`:** a release on a label with a parent, a label with a child, a lone label, an unknown label id, and a release on both a lone label and a tree label that resolves to `in_tree`.
   - **Real-data smoke run:** both functions on the 2026 tables, limited to years 2016 and 2025.
2. Run the edited notebook top to bottom with `jupyter nbconvert --execute` and write the output to `.orchestration/runs/T07/04.worker.ipynb`. Do not overwrite the tracked notebook.
   - Use the `vinyl` kernel, with `DISCOGS_DATA_DIR=$PWD/data` and the 4g heap.
   - If a cell takes over 15 minutes, stop it and report which cell.
   - Paste the major share 2016 and 2025, group concentration, and parent coverage tables.
3. `git diff --stat` touches only files in scope.

## Out of scope
The "Decisions" checklist, `main.ipynb`, other explorers, and Parquet changes.
