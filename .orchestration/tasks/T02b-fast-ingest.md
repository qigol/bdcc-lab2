# T02b - fast ingest: split, year pre-filter, trimmed schema, lean 2020

## Goal
Cut the full-ingest time (T03) by parsing raw XML on all 4 CPUs and parsing less of it. Results must be **identical** to the current path for every in-window release. T02 measured the baseline: see `.orchestration/reviews/T02-ingest-sample-check.md` and the T02 summary in the PR history.

## Baseline from T02 (`/tmp/$USER/discogs_sample/summary.txt`)
- The current single-core releases parse runs at about 350 records/s. That is about 12.6 h for 2026 (about 16.3 M records, about 60 GB XML) plus about 8.9 h for 2020 (about 10.9 M records, about 47 GB XML).
- The first 7-11% of each releases file has essentially **no** 2016-2026 releases, because records are in Discogs id order. The in-window records sit late in the file, so the year pre-filter should remove most of the parse work.
- The 2020 files have fields the saved (2026) schema drops (`images`, `_status`, `artists.artist.role/tracks`, `extraartists.artist.join`, `companies.company.resource_url`, `members.id`). None of them feed any table, so this is harmless. Note it in the `read_raw_xml` docstring, replacing the TODO there.

## Facts
- The container has a 4-CPU cgroup quota (`/sys/fs/cgroup/cpu.max` = `400000 100000`) on a 32-core host. Spark's `local[*]` would start 32 threads. Use the quota, as `discogs_loader.py::_cpu_quota` does.
- `/tmp` has about 569 GB free. The 2026 releases file decompresses to roughly 80-100 GB. Staging chunks go under `/tmp/$USER/discogs_stage/` and are deleted after the Parquet is written and verified.
- A gzip file is read by one task. Spark's XML reader also doesn't split plain files, so many chunk files are needed for parallelism.
- The old `discogs_loader.py::split_xml` is a working reference. It streams `gzip -dc`, cuts only at lines starting `<{row_tag} ` or `<{row_tag}>` (safe because record text is XML-escaped), and wraps each chunk in its own root element. Port the idea into the package, not the file.
- `tables.scope_releases` derives `year` from the first four characters of `released`, if they are digits, and keeps `YEAR_MIN <= year <= YEAR_MAX`. Records with a missing or unparseable year are dropped.
- Release fields that are never used downstream: `videos`, `identifiers`, `notes`, `series`. Keep `tracklist`, because T04 may add track-level writer credits.
- The snapshot comparison (`explorers/05_snapshot_comparison.ipynb`, `vinyl/snapshots.py`) needs only `releases`, `release_labels` and `labels` from 2020.

## Files in scope
- `lab2/vinyl/ingest.py`, `lab2/vinyl/tables.py`, `lab2/vinyl/config.py`
- `lab2/scripts/compare_fast_ingest.py` (new, see below). `lab2/scripts/sample_ingest_check.py` stays as it is.
- `lab2/explorers/01_ingest_and_parse.ipynb`: only the cells that call `build_all`, so they pass the new options. Keep everything else as it is.

## Requirements
1. **Spark session.** `ingest.get_spark` sets `master` to `local[<cgroup quota>]` unless `extra_conf` or the caller sets `spark.master`. Add a small `_cpu_quota()` helper with a docstring.
2. **Splitter.** Add `ingest.split_raw_xml(snapshot, entity, chunk_bytes=256 MiB, year_window=None, stage_dir=None) -> str`. It streams the gz, writes plain XML chunks, and returns the chunk directory. Make it idempotent with a `_DONE` marker, and record stats in `_STATS.json` (records seen, records kept, bytes, seconds).
   - **Conservative year pre-filter** (releases only, when `year_window` is given): buffer one record at a time. Drop it only if it contains a `<released>` element whose first four characters are digits and fall outside the window. Keep every other record, including those with a missing or unparseable `released`, because `scope_releases` still applies the exact rule afterwards. Document this invariant in the docstring.
   - Use a streaming line loop. Don't load a whole record list into memory. The loop must keep up with about 60 GB of XML, so decompress with a `gzip -dc` subprocess (as the old loader does), not Python's `gzip` module. Check first with `zcat ... | head -c 20000` whether each record is on a single line. If it is, the filter is a cheap per-line regex. Use a precompiled `bytes` regex and don't decode lines.
3. **Reader.** `ingest.read_raw_xml` gets `split: bool = False` and `year_window`. When `split=True` it reads the chunk directory instead of the gz. It also gets `drop_fields: tuple[str, ...] = ()`, which removes those top-level fields from the schema before reading. Add `config.RELEASE_DROP_FIELDS = ("videos", "identifiers", "notes", "series")`.
4. **build_all.** Add `fast: bool = True`. When on, releases are read via split, year pre-filter and dropped fields, and the entity files via split, with no filter or drops. Stage chunks are deleted after the Parquet write succeeds (`keep_stage=False` default). Add `config.LEAN_SNAPSHOT_TABLES = {"2020": ["releases", "release_labels", "labels"]}`. With `tables=None`, `build_all` uses that list for snapshots listed there and all tables otherwise.
5. Docstrings in the existing numpy style. Update the module docstring of `ingest.py` ("Why loading is slow").

## Equivalence and speed check
Keep this small. Don't restructure the existing 40 KB script. Add a separate `lab2/scripts/compare_fast_ingest.py` (aim for under 250 lines) that reuses functions from `sample_ingest_check.py` only where that's simple.

**Test data must contain in-window releases.** The head samples have none. Build one extra "tail" sample per snapshot: stream the full releases gz once with `gzip -dc | tail -n 30000` (about 5 min per file; it writes nothing big) and wrap it in a root element. Store it as its own sample data dir (`/tmp/$USER/discogs_sample_tail/raw/...`, same file names; copy or symlink the head samples for the other three entities). The comparison runs on both the head and the tail samples.

On each sample set, build every table twice into two separate sample data dirs, once with `fast=False` and once with `fast=True`. Then assert:
- every table has the same row count;
- `exceptAll` is empty both ways on the columns both versions share;
- the pre-filter stats show `kept <= seen`, and no in-window release is missing.

Print the wall time of the releases staging for both paths, plus the projected full-run times using T02's method.

## Acceptance checks
1. `cd lab2 && timeout 2700 python scripts/compare_fast_ingest.py` exits 0, every equivalence assertion passes on the head and tail samples, the tail sample has at least 1,000 in-window releases, and the timings are printed. Paste the summary into your report.
2. Also report the splitter's own throughput (MB/s of XML through the pre-filter) on the tail extraction. It decides whether the splitter or Spark is the bottleneck in T03.
3. Budget: you have 2.5 hours. Get a minimal correct version passing first, then improve it. Don't re-run `sample_ingest_check.py`, which takes 30 min. Its samples are already on disk.
3. `python -c "from vinyl import ingest; print(ingest._cpu_quota())"` prints 4.
4. Nothing is written under `/tmp/$USER/discogs/parquet`, and the stage dirs for samples are cleaned up or reported.

## Out of scope
Running on full files (T03), schema JSON changes, and analysis modules.
