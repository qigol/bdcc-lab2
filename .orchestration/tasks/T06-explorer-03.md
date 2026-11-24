# T06 - explorer 03 (formats and reissues, RQ2)

## Goal
Make `lab2/explorers/03_formats_and_reissues.ipynb` ready for the orchestrator to run on the full 2026 data. Fill its TODOs with code. Put any non-trivial logic in `lab2/vinyl/formats.py`, so `main.ipynb` can reuse it. The orchestrator runs the notebook on full data after the merge and fills in the "Decisions" checklist from its outputs. Leave that checklist as is.

## Context
- Tables (2026):
  - `release_formats`: 3.99 M rows. Columns `release_id`, `year`, `format_name`, `qty`, `text`, `descriptions` (array of string) and `is_reissue` (bool).
  - `release_companies`: 4.60 M rows. Columns `release_id`, `year`, `company_id`, `company_name`, `entity_type`, `entity_type_name` (e.g. "Pressed By") and `catno`.
  - `masters`: 2.60 M rows. Columns `master_id`, `title`, `main_release`, `master_year`.
  - `releases`: 3.83 M rows. Includes `master_id`.
  - `release_genres`: 4.79 M rows.
- These tables are small enough for Spark with a 4g heap.
- The snapshot is dated 1 Oct 2026:
  - `config.LAST_FULL_YEAR = 2025`.
  - `config.PARTIAL_YEAR_SHARE = {2026: 0.739}`.
- `config.REISSUE_TAGS` holds the tags that mark a reissue: Reissue, Repress and Remastered.

## Files in scope
- `lab2/vinyl/formats.py`: new functions only. Do not change existing signatures.
- `lab2/explorers/03_formats_and_reissues.ipynb`: code cells and section headers. Keep the intro markdown and the "Decisions" cell.
- New test script `lab2/scripts/test_t06.py`.

## Requirements
1. **Main formats.**
   - Replace the hard-coded `main_formats` list and its TODO. Add `formats.main_formats(format_shares, min_share=0.01) -> list[str]`, returning the formats whose share is at least `min_share` in at least one full year (`<= config.LAST_FULL_YEAR`), ordered by mean share.
   - Add `formats.group_minor_formats(release_formats, keep: list[str], other="Other") -> DataFrame`, which relabels every other `format_name` as `other`.
   - Plot the shares with the minor formats grouped. Use a real title and mark 2026 as partial.
2. **Reissue age** (TODO 1). Add `formats.reissue_age_distribution(flagged, format_name=None, bins=(1, 2, 5, 10, 20, 30, 40, 50)) -> DataFrame`.
   - Input is the output of `add_master_reissue_flag`.
   - Take releases with `master_year` not null and `year - master_year >= 1`. Count distinct releases per (`year`, age bin).
   - Label age bins as strings like `"1"`, `"2-4"`, `"5-9"`, ..., `"50+"`.
   - Also return a `median_age` per year. `percentile_approx` is fine.
   - Optionally filter by `format_name`.
   - Ignore a `master_year` that is later than the release year. Count those rows and print the count once in the explorer.
   - In the explorer, show vinyl and CD side by side.
3. **Pressing plants** (TODO 2). Add `formats.top_pressing_plants(release_companies, release_formats, format_name="Vinyl", role="Pressed By", top_n=15) -> DataFrame`.
   - Count distinct releases per (`year`, `company_id`, `company_name`) for releases that have that format and a company with `entity_type_name == role`.
   - Rank by the 2016-`LAST_FULL_YEAR` total, and keep the top `top_n`.
   - Add `share_of_format_year`: the denominator is the distinct releases of that format in that year that have any `role` company.
   - Also return, or print in the explorer, the share of vinyl releases per year that name any pressing plant at all. That is the coverage figure.
   - Before choosing the role, check the actual spelling in `entity_type_name` with a groupBy in the explorer.
4. **Digital releases** (TODO 3). Add `formats.format_coverage(release_formats, releases, format_name="File") -> DataFrame`. It returns, per year:
   - `n_format`: distinct releases in that format;
   - `share`;
   - `desc_coverage`: share of them with non-empty `descriptions`;
   - top descriptions such as MP3, FLAC and WAV. A small companion DataFrame or a list column is fine.
   - Add a markdown cell stating the reading rule: if the File share jumps or collapses in step with catalogue policy rather than smoothly, File is not comparable across years. Do not write conclusions.
5. **Reissue share.**
   - The existing reissue-share loop should also print `config.LAST_FULL_YEAR` as the end year.
   - Add one cell computing the headline candidate, "share of vinyl growth explained by reissues": `(reissues_end - reissues_start) / (releases_end - releases_start)` for vinyl, 2016 vs `LAST_FULL_YEAR`, for each of the three flags.
   - Put this in `formats.growth_explained_by_reissues(reissue_share_df, start_year, end_year) -> DataFrame`.
   - If vinyl releases did not grow (the denominator is 0 or negative), return null and say so in the docstring.
6. Numpy docstrings in the module's style. No new TODOs. Remove the old TODO comment cell once its three items are covered. Notebook cells only call `vinyl` functions, `viz` plots and short display code.

## Acceptance checks
Spark driver memory is 4g at most (`PYSPARK_SUBMIT_ARGS="--driver-memory 4g pyspark-shell"`). The orchestrator may be running Spark at the same time, and the pod limit is 16 GiB. Use `timeout`.

1. `cd lab2 && DISCOGS_DATA_DIR=$PWD/data timeout 900 python scripts/test_t06.py` exits 0. It covers each new function on tiny hand-built DataFrames:
   - `main_formats` threshold and ordering;
   - `group_minor_formats` relabelling;
   - age bins and labels, including a `master_year` later than the release year, which is excluded;
   - pressing-plant shares;
   - format coverage;
   - growth explained, including the no-growth case that returns null;
   - plus a real-data smoke run of every function on the 2026 tables with `where(year in (2016, 2025))`.
2. Run the edited notebook top to bottom with `jupyter nbconvert --execute` and write the output to `.orchestration/runs/T06/03.worker.ipynb`. Do not overwrite the tracked notebook's outputs.
   - Use the `vinyl` kernel, with `DISCOGS_DATA_DIR=$PWD/data` and the 4g heap.
   - If a cell takes over 10 minutes, stop it and report.
   - The run must have no errors. Paste the main formats list, the pressing-plant coverage and the growth-explained table in your summary.
3. `git diff --stat` touches only the files in scope.

## Out of scope
Filling the "Decisions" checklist, `main.ipynb`, other explorers, and any change to the Parquet tables.
