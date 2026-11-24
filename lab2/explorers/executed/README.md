# Executed explorers (full data)

These are copies of the explorer notebooks with outputs, from runs on the full Parquet tables: 2026 snapshot, plus 2020 where used. The notebooks one level up are the source and have no outputs. Rerun them with `jupyter nbconvert --execute` on the `vinyl` kernel to refresh these copies.

| Notebook | Run | Notes |
|---|---|---|
| 01_ingest_and_parse | T03, 2026-11-23 (3 h 17 min) | Older source: the T03b comment change and the 2020 `artists`/`masters` tables were built later by a separate script (see `.orchestration/tasks/T03-full-ingest.md`). |
| 02_genres_and_styles | T08b, 2026-11-24 | Includes the prolific-artist robustness check. |
| 03_formats_and_reissues | T06, 2026-11-24 | |
| 04_labels_and_majors | T08b, 2026-11-24 | Parent coverage with the self-released class. |
| 05_snapshot_comparison | T08, 2026-11-24 | |
| 06_networks_and_communities | T09, 2026-11-24 (41 min) | Credit-graph resolution sweep took 421 s. |
