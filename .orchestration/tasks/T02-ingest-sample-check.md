# T02 - sample ingest check (schemas, builders, speed)

## Goal
Before the multi-hour full ingest (T03), prove on small samples that:
1. the saved schemas in `lab2/schemas/` parse both snapshots, especially 2020 (TODO in `lab2/vinyl/ingest.py::read_raw_xml` Notes), with no silently dropped fields;
2. every table in `lab2/vinyl/tables.py::TABLES` builds through `tables.build_all` without error and with plausible columns;
3. how fast one core parses releases XML, so we can estimate T03's run time.

## Facts
- Raw dumps: `$DISCOGS_DATA_DIR/raw/discogs_<date>_<entity>.xml.gz` (symlinks, read-only). `DISCOGS_DATA_DIR=/tmp/$USER/discogs`.
- `config.DATA_DIR` reads `DISCOGS_DATA_DIR` at import time, so pointing it at a sample folder makes `build_all` read sample files and write sample Parquet with no code changes.
- Every Discogs file is `<?xml ...?>` and one root element (`<releases>`, `<artists>`, `<labels>`, `<masters>`), with one record per `<release ...>` / `<artist>` / `<label>` / `<master ...>` element. In releases and masters, each record starts on a new line. Check this for artists and labels with `zcat ... | head -c 3000`. Text inside records is XML-escaped.
- Environment: `source .venv/bin/activate` (repo root). PySpark 4.1.1 has XML built in. `ingest.get_spark(shuffle_partitions=8)` is fine for samples.
- The machine is shared. Keep Spark to small samples, wrap long commands in `timeout`, and don't decompress whole files to disk.

## Files in scope
- `lab2/scripts/sample_ingest_check.py` (new), runnable as `cd lab2 && python scripts/sample_ingest_check.py [--records N]`.
- Nothing in `lab2/vinyl/` may change. If you find a bug there, report it in your summary with file:line and a proposed fix. Do not fix it, because the orchestrator will spec it.

## What the script does
1. **Make samples.** For each snapshot in `config.SNAPSHOTS` and each entity in `config.ROW_TAGS`, stream the first N records (default 20000) from the gz with Python's `gzip` module, line by line. Cut at a record boundary, close the root element, and write plain XML to `/tmp/$USER/discogs_sample/raw/discogs_<date>_<entity>.xml.gz`. Write it gzipped with the same file name so `config.raw_path` matches. Skip a sample file that already exists, unless `--force`.
2. **Schema drift check.** For each sample, read it with the saved schema (`ingest.read_raw_xml`) and also with schema inference (same reader options, no `.schema`). Compare the two recursively. List the field paths that are inferred but not in the saved schema (would be dropped), and the paths in the saved schema never seen in the sample (informational). Also check the `_corrupt_record`, if present, or count rows where every top-level field is null.
3. **Build all tables on the sample.** Set `os.environ["DISCOGS_DATA_DIR"]="/tmp/$USER/discogs_sample"` **before** importing `vinyl`. Run `tables.build_all(spark, snapshot)` for both snapshots, then print each table's row count, columns, and 3 example rows. Note that the year window 2016-2026 may leave few releases in the first records, so report how many sampled releases fall in the window. If fewer than 200 do, also sample records from later in the file: skip ahead line by line, streaming without storing, to about 80% of the file. Use a `--skip-records` option for this.
4. **Speed.** Time the parse of the releases sample with the saved schema (records/second, one task). Estimate the full single-core parse time for each releases file. Get the record count by multiplying sampled bytes per record by the decompressed size, estimating that size from the compression ratio of the bytes you streamed. Print the estimate.
5. Print a final summary block: drift findings per snapshot/entity, tables built OK or failed, the in-window count, and the time estimates.

## Acceptance checks
1. `cd lab2 && timeout 1800 python scripts/sample_ingest_check.py --records 20000` exits 0 and prints the summary.
2. Re-running it reuses the samples and still exits 0.
3. Nothing was written under `/tmp/$USER/discogs/parquet` (the real data dir), and nothing changed under `lab2/vinyl/` or `lab2/schemas/`.
4. Include the full summary block verbatim in your final report.

## Out of scope
Changing schemas, `vinyl/` code or notebooks, and running on full files.
