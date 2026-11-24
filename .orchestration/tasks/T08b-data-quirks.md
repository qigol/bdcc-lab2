# T08b - two data quirks: self-released labels and prolific artists

## Goal
Fix two issues found while running the explorers. They are listed in `.orchestration/FOLLOWUPS.md`, items 1 and 2.

## Facts (2026 snapshot, checked by the orchestrator)
1. **"Not On Label" (label 1818)** is Discogs' self-released placeholder.
   - In 2026 it has `parent_id = 205754` (Stolen Riff Records), and 205754 has parent 212. This is a data-entry error. 1818 has no child labels.
   - It holds 4-6% of each year's in-scope releases: 24,562 in 2016, 11,613 in 2025.
   - There are also many labels named like "Not On Label (X Self-released)", for example 3550708.
   - `labels.parent_coverage` counts 1818 as `in_tree` because it has a parent. Major tagging is unaffected, because its root is not a major.
2. **Prolific artists.** "This Is What I Hear When You Talk" (6351861) is a real harsh-noise-wall project that self-releases thousands of digital albums. It is not a placeholder.
   - 35 artist-years have 300 or more releases (16,346 releases in all, 0.4% of 3.83 M). Examples: 6351861 with 1,233 in 2019 and 957 in 2023; Nosferatu 1922 (6391827) with 962 in 2018; Merzlux (4607669) with 906 in 2024.
   - They barely affect aggregates, but they distort niche styles. 6351861 alone carries 22% of the 2019 "Harsh Noise Wall" releases (1,233 of 5,626) and 27% of 2023.

## Files in scope
- `lab2/vinyl/config.py`: new constants only.
- `lab2/vinyl/labels.py`: `parent_coverage`, plus one new helper.
- `lab2/vinyl/trends.py`: new functions only.
- `lab2/explorers/04_labels_and_majors.ipynb`: the parent-coverage cell and its markdown.
- `lab2/explorers/02_genres_and_styles.ipynb`: one new robustness cell, plus markdown after the top-movers cells.
- New test script `lab2/scripts/test_t08b.py`.

## Requirements
1. **Config.**
   - Add `SELF_RELEASED_LABEL_IDS: tuple[int, ...] = (1818,)`.
   - Add `SELF_RELEASED_LABEL_PATTERN: str = r"(?i)^not on label\b"`.
   - Document both with the facts above.
   - Add `PROLIFIC_ARTIST_MIN_PER_YEAR: int = 300`.
2. **`labels.self_released_label_ids(labels) -> DataFrame`.** Return one column, `label_id`: the ids in `SELF_RELEASED_LABEL_IDS` plus every label whose `label_name` matches `SELF_RELEASED_LABEL_PATTERN`.
3. **`parent_coverage`.**
   - Add the parameter `self_released: DataFrame | None = None`, a DataFrame of `label_id`s.
   - When it is given, a label in it goes to the new class `self_released`, whatever its parent or children.
   - Class order for a release on several labels: `in_tree` > `on_root_alone` > `self_released` > `unknown_label`. A release on Not On Label plus a real label takes the real label's class.
   - Add the `self_released` count and `share_self_released` columns, always present (0 when the parameter is None). The shares still sum to 1.
   - Keep the default call working.
   - Update the docstring.
4. **`trends.prolific_artist_years(release_artists, min_per_year=config.PROLIFIC_ARTIST_MIN_PER_YEAR, exclude_ids=networks.PLACEHOLDER_ARTIST_IDS) -> DataFrame`.** Return `artist_id`, `artist_name` (min), `year` and `n_releases` (distinct releases) for (artist, year) pairs at or above the threshold.
5. **`trends.drop_prolific_releases(release_tags, release_artists, prolific) -> DataFrame`.**
   - Remove from `release_tags` every release whose main artists include an (artist, year) pair in `prolific`. Match on `release_id` and `year`.
   - Document that this is a robustness check, not the main definition.
6. **Explorer 02.**
   - After the top-movers cells, add one cell that recomputes `tag_shares_by_year` and `top_movers` (2016 vs `LAST_FULL_YEAR`) on `drop_prolific_releases(...)`.
   - Print the top 15 gainers and losers side by side with the main ranking, plus the Spearman correlation of `change` between the two rankings.
   - Also print `prolific_artist_years(...)` sorted by `n_releases`.
   - Add a markdown reading rule: if a style's change shrinks a lot once prolific artists are dropped, that style's trend is one artist's output, not a scene.
   - The explorer needs `release_artists`, so load it in the data cell.
7. **Explorer 04.** Call `parent_coverage(release_labels, tagged, self_released=label_power.self_released_label_ids(labels))`. Update the markdown note.
8. Numpy docstrings and no new TODOs. Existing tests (`scripts/test_t07.py`) must still pass on their tiny-data parts.

## Acceptance checks
Spark driver memory 4g at most. Use `timeout`.

1. `cd lab2 && DISCOGS_DATA_DIR=$PWD/data timeout 600 python scripts/test_t08b.py` exits 0. It covers, on tiny hand-built DataFrames **only, with no real-data section**:
   - `self_released_label_ids`: the fixed id, plus the name pattern, which must not match "Not On Labelle".
   - `parent_coverage` with `self_released`: a release on 1818 only gives `self_released`; 1818 plus a tree label gives `in_tree`; the shares sum to 1; and the default call gives `self_released == 0`.
   - `prolific_artist_years` threshold, and that placeholder ids are excluded.
   - `drop_prolific_releases`: it removes only the releases of the matching (artist, year).
2. The tiny-data functions of `scripts/test_t07.py` still pass. Import them and call them, skipping `test_real_data`.
3. **Do not run the notebooks.** The orchestrator runs explorers 02 and 04 on full data. Only check that both notebooks are valid JSON and that the edited cells are syntactically valid Python (`ast.parse`).
4. `git diff --stat` touches only files in scope.

## Out of scope
`main.ipynb`, other explorers, Parquet changes, and the Decisions checklists.
