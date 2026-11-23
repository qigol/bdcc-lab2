# T03 - full ingest (run by Claude, no Kiro)

## What ran
`lab2/scripts/run_full_ingest.sh`, detached with `nohup setsid`. It does two things:
1. Pre-splits the raw gz files with `ingest.split_raw_xml`, using the same parameters as `build_all(fast=True)`. The two releases files are split concurrently.
2. Runs `explorers/01_ingest_and_parse.ipynb` headless on the `vinyl` kernel with a 10 GB driver heap. The executed copy is in `.orchestration/runs/T03/` (gitignored).

## First attempt lost (2026-11-23, 08:02-12:46)
- The pod restarted while the 2026 release tables were being written. Its memory limit is 16 GiB (`memory.max`), while `free` reports the host's 251 GB. The run used `--driver-memory 16g`.
- The restart wiped the container `/tmp`, which held the Parquet, the logs and the raw symlinks.
- Fixes:
  - Data dir is now `lab2/data` (gitignored, on /home2): default in `scripts/setup_env.sh` and the README; kernel.json was re-patched.
  - Driver heap is 10g.
  - Logs go to `.orchestration/runs/T03/`.
  - Staging chunks stay in `/tmp/$USER/discogs_stage`.

## Rerun (2026-11-23, 13:41-16:58, 3 h 17 min)
| Phase | Time |
|---|---|
| Splits (2026 releases is the longest) | 13:41-14:43 |
| Small tables | 14:45-14:59 |
| 2020 releases (parse 32 min, tables 2 min) | 14:59-15:33 |
| 2026 releases (parse 77 min, tables 7 min) | 15:33-16:57 |

- The anon memory peak stayed under about 7 GiB, with no OOM events.

### Split pre-filter (releases)
| Snapshot | Records seen | Kept by conservative filter | In scope after `scope_releases` |
|---|---|---|---|
| 2026 | 19,492,392 | 6,422,449 | 3,829,109 |
| 2020 | 11,955,493 | 2,606,325 | 1,082,351 |

### Row counts
| Table | 2020 | 2026 |
|---|---|---|
| releases | 1,082,351 | 3,829,109 |
| release_labels | 1,208,158 | 4,262,009 |
| release_genres | - | 4,790,607 |
| release_styles | - | 6,488,970 |
| release_formats | - | 3,985,674 |
| release_artists | - | 4,485,710 |
| release_credits | - | 15,238,351 |
| release_companies | - | 4,598,779 |
| labels | 1,461,523 | 2,426,075 |
| artists | - | 10,243,337 |
| masters | - | 2,598,586 |

### 2026 releases per year
2016 385,281 · 2017 390,355 · 2018 396,710 · 2019 394,016 · 2020 415,671 · 2021 391,297 · 2022 360,863 · 2023 347,065 · 2024 312,179 · 2025 280,882 · 2026 154,790 (partial year, dump of 1 Oct)

The 2020 snapshot by year: 2016 295,865 · 2017 292,388 · 2018 276,507 · 2019 217,299 · 2020 292.

## Checks
- No error outputs in the executed notebook.
- `release_id` is unique and non-null in both `releases` tables. `year` is non-null and within 2016-2026 (2020 snapshot: 2016-2020).
- Entity row counts equal the splitter's `records_seen` (labels, artists, masters, 2020 labels).
- `/tmp/$USER/discogs_stage` holds no chunk files after the run.
- 2026 in-scope releases (3.83 M) match the T02b projection (about 3.83 M).
- 7,390 release ids in the 2020 table are not in the 2026 table (deleted or merged, or re-dated outside the window). For T08.
- Parquet total is about 3 GB under `lab2/data/parquet`.
- The `FileNotFoundException ... part-*.xml` and `... releases_staged does not exist` stack traces in run.log are logged warnings, not failures. The first comes from Spark's `FileStreamSink.hasMetadata` probing a glob path. The second is the expected `build_all` probe for an existing staged table.

## Open
- 2020 artists/masters are not built (lean 2020). The gameplan's "all four files" wording could be matched with `build_all(spark, "2020", tables=["artists", "masters"])`, about 15 min.

## T03b - 2020 artists and masters (2026-11-23)
These were added so that all four dump files of both snapshots are parsed, matching the gameplan's "all four files each".
- `config.LEAN_SNAPSHOT_TABLES["2020"]` now also lists `artists` and `masters`.
- Ran `build_all(spark, "2020", tables=["artists", "masters"])`: 658 s.
- Rows: artists 6,625,426, masters 1,616,539. No staging chunks are left.
- The stack traces in the logs come from Spark's `WARN FileStreamSink: Assume no metadata directory ... part-*.xml`. They are harmless.
