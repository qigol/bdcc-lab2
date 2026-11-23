# T04 - real config values (majors, roles, placeholders, credits, partial year)

## Goal
Replace the placeholder values and TODOs in `lab2/vinyl/` with values checked against the full 2026 Parquet. The facts below were pulled by the orchestrator with Spark on the full data. Use them; do not re-derive them on full data.

## Facts (2026 snapshot, from `lab2/data/parquet/20261001`)

### A. Label trees: the root-name match cannot work
`labels.resolve_label_roots` walks `parent_id` to the top of each tree:
- **Label 212 has no name** (`label_name` NULL, no parent). It is a placeholder parent. Its subtree holds 32,175 labels and 674,305 in-scope releases, including unrelated small labels (Dirty Snowball Records, Condesco, ...).
- Universal Music Group (38404) and Warner Music Group (2345) both have `parent_id = 212`, so with today's code their root is 212 with `root_name` NULL. Universal and Warner can't be told apart, and both match nothing in `MAJOR_ROOTS`.
- Sony's tree tops out at **Sony Corporation (353657)**: 83,733 releases, 639 labels. Its chain is Columbia (1866) / Epic (1005) / RCA (895) → Sony Music Entertainment (29073) → 19795 → 353657.
- Examples resolving through 212 today: Atlantic 681, Capitol 654, Def Jam 4241, Elektra 651, Interscope 2311, Island 8377, Parlophone 2294, Polydor 1610 (parent 38404), Universal Music 23694 (parent 38404), Republic 38017, Virgin 750, Warner Records 134882.
- 978 labels have `reached_root == False` at `max_depth=12`.
- Bertelsmann AG (353658, 31,432 releases) is BMG, not one of the three majors. It stays Independent.

### B. Credit roles: top 70 by row count
The role names are already split and bracket-stripped by `build_release_credits`.
- Configured production roles are all present: Producer 1,057,817 · Mastered By 792,933 · Mixed By 748,821 · Recorded By 542,931 · Engineer 502,529 · Executive-Producer 120,847 · Co-producer 61,844.
- Configured writing roles are all present: Written-By 451,610 · Composed By 296,271 · Lyrics By 270,064 · Music By 248,391 · Written By 104,335 · Songwriter 73,812.
- Unconfigured technical roles that are frequent: Lacquer Cut By 102,159 · Programmed By 66,157 · Edited By 56,765 · Remastered By 56,330.
- Performance and art roles (Vocals, Guitar, Artwork, Design, ...) stay out of both lists.

### C. Placeholder artists
- Ids 194 and 355 are **not in the artists file**. In `release_artists` they are named "Various" (150,093 releases) and "Unknown Artist" (3,031 releases). This confirms `networks.PLACEHOLDER_ARTIST_IDS = (194, 355)`.

### D. Track-level credits
- Of 3,829,109 in-scope releases, 714,389 have a release-level writing credit and 361,504 have a track-level one (`tracklist.track.extraartists`).
- **290,488 releases have track-level writing credits only.**
- There are 11,678,616 track-level credit rows in total, against about 15.2 M release-level role rows.

### E. 2026 is a partial year
- The dump is from 1 Oct 2026. Of the 2026 releases dated to a month, 99.7% are Jan-Sep.
- In 2016-2025, the share of a year's month-dated releases falling in Jan-Sep is steady: 0.728-0.747, **mean 0.739**.
- Month coverage (share of releases with a month) is 0.70-0.83.

## Files in scope
- `lab2/vinyl/config.py`, `lab2/vinyl/labels.py`, `lab2/vinyl/tables.py`, `lab2/vinyl/networks.py`, `lab2/vinyl/snapshots.py`
- Notebooks, **only these cells/lines** (edit the JSON source strings, keep outputs and metadata as they are):
  - `lab2/main.ipynb`: the cell with `tagged_labels = labels.tag_major_labels(label_roots)` (drop the "TODO: confirm names" comment).
  - `lab2/explorers/04_labels_and_majors.ipynb`: the markdown line "`config.MAJOR_ROOTS` holds placeholder names. Confirm them here first." and the code cell with "# TODO: most common credit roles". Reword both as checks of the now-confirmed values.
- New test script `lab2/scripts/test_t04.py`.

## Requirements
1. **Majors by label id.**
   - Replace `config.MAJOR_ROOTS` (names) with `MAJOR_ROOT_IDS: dict[str, int] = {"Universal": 38404, "Sony": 353657, "Warner": 2345}`. In the comment, name each root label and give the fact source (T04, 2026 labels).
   - Add `PLACEHOLDER_LABEL_IDS: tuple[int, ...] = (212,)`, documented as an unnamed placeholder parent that is not a real owner.
   - `resolve_label_roots(labels, max_depth=12, stop_ids=config.PLACEHOLDER_LABEL_IDS)` treats a `parent_id` in `stop_ids` as no parent. A label whose parent is 212 is then its own root, and so is label 212 itself. Keep the output columns.
   - `tag_major_labels(label_roots, major_root_ids=None)` matches on `root_id`. Update the module docstring and parameter docs.
   - The Notes keep the limitation that "Independent" means "not linked to a major's tree in Discogs".
2. **Roles.** Add "Remastered By" and "Lacquer Cut By" to `PRODUCTION_ROLES` (mastering engineers, same craft as "Mastered By"). Leave `WRITING_ROLES` as is. Replace the TODO with a one-line note that the roles were checked against the top credit roles in T04.
3. **Placeholder artists.** In `networks.py`, replace the TODO with the confirmed fact: the ids are not in the artists file, and their names come from `release_artists`.
4. **Track-level credits.** `tables.build_release_credits` also takes `tracklist.track.extraartists.artist`, with the same role split and bracket strip.
   - Add a column `level`, either `"release"` or `"track"`.
   - Deduplicate to one row per (`release_id`, `artist_id`, `artist_name`, `role`). Keep `year`, and take `level = "release"` if the person has that role at release level, else `"track"`.
   - Handle a null or missing `tracklist`, a null `track`, and a null `extraartists`.
   - Update the docstring (drop the TODO) and `TABLE_DESCRIPTIONS["release_credits"]`. Keep the existing consumers working: `labels.top_contributors` and `networks.credit_edges` filter on `role` only.
5. **Partial year.**
   - Add `config.LAST_FULL_YEAR: int = 2025`.
   - Add `config.PARTIAL_YEAR_SHARE: dict[int, float] = {2026: 0.739}`, the expected share of a full year's releases that fall in the months the dump covers (Jan-Sep, mean of 2016-2025).
   - `snapshots.estimate_undercount` gets `partial_year_share: dict[int, float] | None = None` (default `config.PARTIAL_YEAR_SHARE`). It adds a boolean `partial_year` column. For those years, `n_estimated = n_releases / share / completeness`, where null completeness is read as 1.
   - Replace the TODO with this rule. Explain why the default `new_snapshot_year=2027` is then reasonable: after the seasonal scaling, 2026 is treated like a year that has just ended.
6. Numpy docstrings in the existing style. No new TODOs.

## Acceptance checks
Use samples for everything except check 2. Use `timeout`. Do not run anything on full release tables.
1. `cd lab2 && timeout 900 python scripts/test_t04.py` exits 0. It covers:
   - **Root walk with stop ids:** a tiny in-memory labels DataFrame with an id-212 placeholder, two groups under it, a sub-label chain and a missing parent. The two groups get different roots, the sub-label takes its group's root, the missing parent gives `reached_root == False`, and `tag_major_labels` tags by id.
   - **Credits:** `build_release_credits` on a tiny hand-built staged DataFrame with release- and track-level credits, including a duplicate person/role at both levels, a null tracklist and a null extraartists. Check the row count and `level` values.
   - **Undercount:** `estimate_undercount` on a tiny DataFrame, where the 2026 row gets `partial_year == True` and the expected `n_estimated`.
   - **Sample run:** `build_release_credits` on the T02 sample stage (`/tmp/jsandiego/discogs_sample*` if present; skip with a message if not, since /tmp may have been wiped) or on `lab2/data/parquet/20261001/releases_staged` **with `.limit(20000)` before any explode**. It runs and has both levels.
2. Labels check on the full labels table (small, allowed):
   ```bash
   cd lab2 && DISCOGS_DATA_DIR=$PWD/data PYSPARK_SUBMIT_ARGS="--driver-memory 4g pyspark-shell" timeout 900 python -c "<resolve roots, tag majors, print label count per major>"
   ```
   Universal, Sony and Warner each get more than 100 labels, and labels 681, 1866 and 134882 are tagged Warner, Sony and Warner respectively. Paste the counts. Spark driver memory: 4g at most. The pod limit is 16 GiB and the orchestrator may be running Spark too.
3. `git diff --stat` touches only the files in scope.

## Out of scope
Rebuilding `release_credits` on full data (the orchestrator does that after merge), analysis results, explorers other than the two named cells, and the `.ipynb_checkpoints` folder.
