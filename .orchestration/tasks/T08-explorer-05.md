# T08 - explorer 05 (snapshot comparison, 2020 vs 2026)

## Goal
Make `lab2/explorers/05_snapshot_comparison.ipynb` ready for the orchestrator to run on the full data of both snapshots. Fill its TODOs with code, with any non-trivial logic in `lab2/vinyl/snapshots.py`. The orchestrator runs the notebook and fills in the "Decisions" checklist. Leave that checklist as is.

## Context
- Snapshots: `config.BASE_SNAPSHOT = "2020"` (1 Jan 2020) and `config.MAIN_SNAPSHOT = "2026"` (1 Oct 2026).
- 2020 tables:
  - `releases`: 1,082,351 rows, years 2016-2020;
  - `release_labels`: 1.21 M rows;
  - `labels`: 1.46 M rows;
  - `artists` and `masters`.
  - It has **no** `release_formats` or `release_genres`.
- 2026 has all tables. `releases`: 3.83 M rows.
- 7,390 release ids in the 2020 releases table are missing from 2026, deleted or merged.
- T04 already handles the partial year: `estimate_undercount(..., partial_year_share=config.PARTIAL_YEAR_SHARE)` adds a `partial_year` column, and `config.LAST_FULL_YEAR = 2025`.
- Majors are tagged by root id. Label 212 is a placeholder. Both work the same on 2020 labels, but **check that 38404, 353657 and 2345 exist in the 2020 labels table**. If one is missing or has a different parent in 2020, the explorer must print it.
- Use a 4g heap.

## Files in scope
- `lab2/vinyl/snapshots.py`: new functions only. Do not change existing signatures.
- `lab2/explorers/05_snapshot_comparison.ipynb`: code cells and section headers. Keep the intro markdown and the "Decisions" cell.
- New test script `lab2/scripts/test_t08.py`.

## Requirements
1. **Moves in and out of a major tree** (TODO 1). Add `snapshots.major_moves(labels_old, labels_new, release_labels=None) -> DataFrame`.
   - Resolve roots and tag majors in each snapshot with `labels.resolve_label_roots` and `labels.tag_major_labels`. Join on `label_id`.
   - Keep labels present in both snapshots whose `major` differs. Return `label_id`, `label_name` (new), `major_old`, `major_new`, `root_id_old`, `root_id_new`, `parent_id_old`, `parent_id_new` and `move`.
   - `move` is one of "into_major", "out_of_major" and "between_majors".
   - If `release_labels` is given, add `n_releases`: distinct in-scope releases on the label in that table.
   - Order by `n_releases` desc if present.
   - The explorer shows:
     - counts by (`major_old`, `major_new`);
     - the top 30 moves by releases.
2. **Hand-check aid** (TODO 2). Add `snapshots.explain_move(label_id, labels_old, labels_new) -> pandas.DataFrame`, the parent chain in both snapshots. The chain is a small loop of collects; label tables are small and the chain is at most 12 hops.
   - The explorer calls it for the top 5 moves.
   - Add a markdown cell with the reading rule. A real acquisition is a new parent that is a known corporate entity, often with an `(n)` profile, for example a Warner company. A data correction is a parent that flips between placeholder, duplicate or similar-name profiles, or a chain fix.
   - Do not write conclusions.
3. **Cataloguing lag.**
   - Replace the TODO plot title with a real one.
   - Keep `year <= 2019` for completeness, since 2020 in the old snapshot is a stub of a few days.
   - Print the completeness-by-age table, `age = 2020 - year`.
4. **Partial 2026** (TODO 3). Already handled in T04. Make the `estimate_undercount` cell show the `partial_year` column and plot `n_releases` vs `n_estimated` by year, with 2026 marked.
   - Add one cell that also prints the same estimate with `partial_year_share={}`, so the size of the seasonal correction is visible.
5. **Lag by format and genre** (TODO 4). Add `snapshots.cataloguing_lag_by(releases_old, releases_new, groups_new, group_col) -> DataFrame`. `groups_new` is a 2026 table with `release_id` and `group_col`, such as `release_formats` (`format_name`) or `release_genres` (`genre`).
   - Match on `release_id` as in `cataloguing_lag`. Return one row per (`year`, `group_col`) with `n_new`, `n_already` and `completeness`.
   - Count distinct releases per group, so a release with two formats counts once in each.
   - Only 2026 groups are needed, because the 2020 snapshot has no formats or genres tables.
   - The explorer runs it for formats (Vinyl, CD, Cassette, File) and for the top 8 genres, `year <= 2019`.
   - Add a markdown reading rule: if completeness for the same age differs by group, recent-year shares are biased toward the faster-catalogued groups.
6. Numpy docstrings in the module's style. No new TODOs. Remove the TODO comment cells once covered. Notebook cells only call `vinyl` functions, `viz` plots and short display code.

## Acceptance checks
Spark driver memory 4g at most. Use `timeout`.

1. `cd lab2 && DISCOGS_DATA_DIR=$PWD/data timeout 900 python scripts/test_t08.py` exits 0. It covers, on tiny hand-built DataFrames:
   - **`major_moves`:** a label moving into Warner, one moving out of Sony, one between majors, an unchanged label (excluded), and a label only in one snapshot (excluded); plus `n_releases`.
   - **`explain_move`:** the chains for both snapshots.
   - **`cataloguing_lag_by`:** two formats, a release with both formats counted in each, and completeness checked by hand.
   - **Real-data smoke run:** `major_moves` on both full labels tables (small) and `cataloguing_lag_by` for formats, limited to years 2016-2017.
2. Run the edited notebook top to bottom with `jupyter nbconvert --execute` and write the output to `.orchestration/runs/T08/05.worker.ipynb`. Do not overwrite the tracked notebook.
   - Use the `vinyl` kernel, with `DISCOGS_DATA_DIR=$PWD/data` and the 4g heap.
   - If a cell takes over 15 minutes, stop it and report which cell.
   - Paste the (`major_old`, `major_new`) counts, the top 10 moves, the completeness-by-age table and the lag-by-format table.
3. `git diff --stat` touches only files in scope.

## Out of scope
The "Decisions" checklist, `main.ipynb`, other explorers, and Parquet changes.
