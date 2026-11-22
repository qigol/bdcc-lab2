"""Check that the fast ingest path (T02b) gives the same tables as the slow one.

Sample sets: **head** is the T02 samples in ``/tmp/$USER/discogs_sample/raw``
(almost no 2016-2026 releases). **tail** is the last ``--tail-records``
releases of each full releases file, in ``/tmp/$USER/discogs_sample_tail/raw``
(other entities are symlinks to the head samples). Building the tail streams
each full releases gz once through ``gzip -dc`` and the splitter's own
``iter_records`` + ``keep_record``, which also measures the splitter's
throughput and what the pre-filter keeps on the full file. It is cached;
build it first with ``--tail-only`` (as long as one ``gzip -dc`` of the file).

On each set, every table plus ``releases_staged`` is built with
``fast=False`` and ``fast=True`` into separate folders under
``/tmp/$USER/discogs_compare``. Row counts must match and ``exceptAll`` must
be empty both ways on the shared columns. Exit code 0 only if all checks pass.

Usage, from ``lab2/``: ``python scripts/compare_fast_ingest.py [--tail-only]``
"""

from __future__ import annotations

import argparse
import collections
import getpass
import gzip
import io
import json
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

LAB_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAB_ROOT))
USER = os.environ.get("USER") or getpass.getuser()
REAL_DIR = Path(os.environ.get("DISCOGS_DATA_DIR") or f"/tmp/{USER}/discogs")
HEAD_DIR = Path(f"/tmp/{USER}/discogs_sample")
TAIL_DIR = Path(f"/tmp/{USER}/discogs_sample_tail")
OUT_DIR = Path(f"/tmp/{USER}/discogs_compare")
os.environ["DISCOGS_DATA_DIR"] = str(OUT_DIR / "unused")  # never the real data dir
os.environ["DISCOGS_STAGE_DIR"] = str(OUT_DIR / "stage")

from vinyl import config, ingest, tables  # noqa: E402

WINDOW = (config.YEAR_MIN, config.YEAR_MAX)
MB = 1e6


def log(message: str = "") -> None:
    """Print and flush, so progress shows under ``timeout``."""
    print(message, flush=True)


def make_tail(snapshot: str, n_records: int) -> dict:
    """Write the tail sample of one releases file and return its stats.

    Streams the full gz once through ``gzip -dc``, ``ingest.iter_records``
    and ``ingest.keep_record`` (nothing big is written) and keeps the last
    ``n_records`` records in memory. Cached in ``TAIL_DIR/meta``.
    """
    name = f"discogs_{config.SNAPSHOTS[snapshot]}_releases.xml.gz"
    target, meta_file = TAIL_DIR / "raw" / name, TAIL_DIR / "meta" / f"{name}.json"
    if target.exists() and meta_file.exists():
        return json.loads(meta_file.read_text())
    source = REAL_DIR / "raw" / name
    tail: collections.deque[bytes] = collections.deque(maxlen=n_records)
    seen = kept = in_window = bytes_in = bytes_kept = 0
    started = time.perf_counter()
    proc = subprocess.Popen(["gzip", "-dc", str(source)], stdout=subprocess.PIPE, bufsize=1 << 24)
    for record in ingest.iter_records(proc.stdout, "releases"):
        seen += 1
        bytes_in += len(record)
        tail.append(record)
        if ingest.keep_record(record, WINDOW):
            kept += 1
            bytes_kept += len(record)
            match = ingest._RELEASED_YEAR.search(record)
            in_window += bool(match and WINDOW[0] <= int(match.group(1)) <= WINDOW[1])
    if proc.wait() != 0:
        raise RuntimeError(f"gzip -dc {source} failed")
    seconds = time.perf_counter() - started
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_suffix(".part")
    with gzip.open(partial, "wb", compresslevel=1) as handle:
        handle.write(b"<releases>\n" + b"".join(tail) + b"</releases>\n")
    partial.rename(target)
    meta = {"snapshot": snapshot, "source_bytes": source.stat().st_size, "seconds": seconds,
            "records_seen": seen, "records_kept": kept, "records_in_window_regex": in_window,
            "bytes_in": bytes_in, "bytes_kept": bytes_kept, "tail_records": len(tail)}
    meta_file.parent.mkdir(parents=True, exist_ok=True)
    meta_file.write_text(json.dumps(meta, indent=2))
    for entity in ("masters", "labels", "artists"):
        other = f"discogs_{config.SNAPSHOTS[snapshot]}_{entity}.xml.gz"
        if not (TAIL_DIR / "raw" / other).exists():
            (TAIL_DIR / "raw" / other).symlink_to(HEAD_DIR / "raw" / other)
    return meta


def filter_only_rate(snapshot: str) -> float:
    """MB/s of plain XML through ``iter_records`` + ``keep_record`` alone (no gzip)."""
    name = f"discogs_{config.SNAPSHOTS[snapshot]}_releases.xml.gz"
    with gzip.open(TAIL_DIR / "raw" / name, "rb") as handle:
        data = handle.read()
    started = time.perf_counter()
    for record in ingest.iter_records(io.BytesIO(data), "releases"):
        ingest.keep_record(record, WINDOW)
    return len(data) / (time.perf_counter() - started) / MB


def use_dirs(raw_dir: Path, parquet_dir: Path) -> None:
    """Point ``vinyl.config`` at one sample set and one output folder."""
    assert parquet_dir.resolve() != (REAL_DIR / "parquet").resolve()
    config.RAW_DIR, config.PARQUET_DIR = str(raw_dir), str(parquet_dir)


def build(spark, snapshot: str, fast: bool, stage_dir: str, chunk_bytes: int) -> dict:
    """Build every table of one snapshot; time the releases staging."""
    result: dict = {}
    started = time.perf_counter()
    if fast:
        chunk_dir = ingest.split_raw_xml(snapshot, "releases", chunk_bytes, WINDOW, stage_dir)
        result["split"] = ingest.read_stage_stats(chunk_dir)
    staged = tables.stage_releases(spark, snapshot, fast=fast, stage_dir=stage_dir,
                                   chunk_bytes=chunk_bytes)
    result["partitions"] = staged.rdd.getNumPartitions()
    result["stage_seconds"] = time.perf_counter() - started
    raw = ingest.read_raw_xml(spark, snapshot, "releases")  # gz, one task: the reference count
    result["raw_releases"] = None if fast else raw.count()
    tables.build_all(spark, snapshot, tables=list(tables.TABLES), fast=fast,
                     stage_dir=stage_dir, chunk_bytes=chunk_bytes)
    result["leftover_stage"] = sorted(str(p) for p in Path(stage_dir).glob("*/*"))
    return result


def compare(spark, snapshot: str, slow_dir: Path, fast_dir: Path) -> list[str]:
    """Return the failed checks between the two builds of one snapshot."""
    failures = []
    for name in ["releases_staged", *tables.TABLES]:
        use_dirs(Path(config.RAW_DIR), slow_dir)
        slow = ingest.read_table(spark, snapshot, name)
        use_dirs(Path(config.RAW_DIR), fast_dir)
        fast = ingest.read_table(spark, snapshot, name)
        shared = [c for c in slow.columns if c in fast.columns]
        slow, fast = slow.select(shared), fast.select(shared)
        n_slow, n_fast = slow.count(), fast.count()
        only_slow, only_fast = slow.exceptAll(fast).count(), fast.exceptAll(slow).count()
        ok = n_slow == n_fast and only_slow == 0 and only_fast == 0
        log(f"    {name:18s} rows {n_slow:>8,} / {n_fast:>8,}  only-slow {only_slow}  "
            f"only-fast {only_fast}  {'OK' if ok else 'FAIL'}  ({len(shared)} shared columns)")
        if not ok:
            failures.append(f"{snapshot}/{name}")
    return failures


def run_set(spark, label: str, raw_dir: Path, chunk_bytes: int) -> tuple[list[str], dict]:
    """Build and compare both snapshots of one sample set."""
    failures, results = [], {}
    base = OUT_DIR / label
    shutil.rmtree(base, ignore_errors=True)
    for snapshot in config.SNAPSHOTS:
        log(f"--- {label} / {snapshot}")
        runs = {}
        for fast in (False, True):
            use_dirs(raw_dir, base / ("fast" if fast else "slow") / "parquet")
            runs[fast] = build(spark, snapshot, fast, str(base / "stage"), chunk_bytes)
            log(f"    fast={fast}: releases staging {runs[fast]['stage_seconds']:.1f} s, "
                f"{runs[fast]['partitions']} partition(s) read back")
        split = runs[True]["split"]
        n_raw = runs[False]["raw_releases"]
        n_in = spark.read.parquet(f"{base}/slow/parquet/{config.SNAPSHOTS[snapshot]}/releases").count()
        log(f"    pre-filter: seen {split['records_seen']:,} kept {split['records_kept']:,} "
            f"(raw records via Spark {n_raw:,}; in window {n_in:,}); {split['chunks']} chunk(s)")
        if not split["records_kept"] <= split["records_seen"] == n_raw or n_in > split["records_kept"]:
            failures.append(f"{label}/{snapshot}: pre-filter counts")
        if runs[True]["leftover_stage"]:
            failures.append(f"{label}/{snapshot}: stage not cleaned {runs[True]['leftover_stage']}")
        failures += [f"{label}/{f}" for f in compare(
            spark, snapshot, base / "slow" / "parquet", base / "fast" / "parquet")]
        results[snapshot] = {"slow": runs[False], "fast": runs[True], "in_window": n_in}
    return failures, results


def main(argv: list[str] | None = None) -> int:
    """Build the tail samples, compare both paths on both sets, print the summary."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--tail-records", type=int, default=30000)
    parser.add_argument("--tail-only", action="store_true", help="only build the tail samples")
    parser.add_argument("--chunk-mib", type=int, default=8, help="chunk size for the samples")
    args = parser.parse_args(argv)

    log("=== tail samples (one full pass over each releases gz; cached) ===")
    with ProcessPoolExecutor(max_workers=len(config.SNAPSHOTS)) as pool:
        tails = dict(zip(config.SNAPSHOTS, pool.map(
            make_tail, config.SNAPSHOTS, [args.tail_records] * len(config.SNAPSHOTS))))
    for snapshot, meta in tails.items():
        log(f"  {snapshot}: {meta['records_seen']:,} records, {meta['bytes_in'] / 1e9:.1f} GB XML "
            f"in {meta['seconds'] / 60:.1f} min = {meta['bytes_in'] / MB / meta['seconds']:.1f} MB/s "
            f"(gzip -dc + pre-filter); kept {meta['records_kept']:,} "
            f"({meta['records_kept'] / meta['records_seen']:.1%}), {meta['bytes_kept'] / 1e9:.1f} GB")
    if args.tail_only:
        return 0
    filter_rates = {s: filter_only_rate(s) for s in config.SNAPSHOTS}

    spark = ingest.get_spark("compare-fast-ingest", shuffle_partitions=8, extra_conf={
        "spark.driver.memory": "6g", "spark.ui.enabled": "false",
        "spark.ui.showConsoleProgress": "false"})
    spark.sparkContext.setLogLevel("ERROR")
    log(f"Spark master: {spark.sparkContext.master}")
    failures, results = [], {}
    for label, sample in (("head", HEAD_DIR), ("tail", TAIL_DIR)):
        found, results[label] = run_set(spark, label, sample / "raw", args.chunk_mib << 20)
        failures += found
    for snapshot in config.SNAPSHOTS:
        if results["tail"][snapshot]["in_window"] < 1000:
            failures.append(f"tail/{snapshot}: fewer than 1,000 in-window releases")
    spark.stop()

    log("\n" + "=" * 72 + "\nSUMMARY\n" + "=" * 72)
    for snapshot, meta in tails.items():
        tail = results["tail"][snapshot]
        slow_s, fast_s = tail["slow"]["stage_seconds"], tail["fast"]["stage_seconds"]
        split = tail["fast"]["split"]
        slow_rate = split["records_seen"] / slow_s           # raw records/s, 1 task
        spark_s = fast_s - split["seconds"]
        fast_rate = split["records_kept"] / spark_s          # kept records/s, all cores
        full_split = meta["seconds"]
        full_spark = meta["records_kept"] / fast_rate
        log(f"{snapshot}: tail sample {split['records_seen']:,} releases, "
            f"{tail['in_window']:,} in window")
        log(f"  releases staging on the tail: slow {slow_s:.1f} s ({slow_rate:,.0f} records/s); "
            f"fast {fast_s:.1f} s = split {split['seconds']:.1f} s + Spark {spark_s:.1f} s "
            f"({fast_rate:,.0f} kept records/s, {tail['fast']['partitions']} partitions)")
        log(f"  splitter on the full file: {meta['bytes_in'] / MB / full_split:.1f} MB/s "
            f"(gzip -dc + pre-filter); pre-filter alone {filter_rates[snapshot]:.0f} MB/s")
        log(f"  full file: {meta['records_seen']:,} records, pre-filter keeps "
            f"{meta['records_kept']:,} ({meta['records_kept'] / meta['records_seen']:.1%})")
        log(f"  projected releases staging: slow {meta['records_seen'] / slow_rate / 3600:.1f} h; "
            f"fast {(full_split + full_spark) / 3600:.1f} h "
            f"(split {full_split / 3600:.2f} h + Spark {full_spark / 3600:.2f} h)")
    for label, by_snapshot in results.items():
        log(f"{label}: " + "; ".join(f"{s} staging slow {r['slow']['stage_seconds']:.1f} s / fast "
            f"{r['fast']['stage_seconds']:.1f} s, {r['in_window']:,} in window"
            for s, r in by_snapshot.items()))
    shutil.rmtree(OUT_DIR / "unused", ignore_errors=True)
    leftovers = sorted(str(p) for p in OUT_DIR.glob("*/stage/*/*"))
    log(f"stage leftovers: {leftovers or 'none'}")
    log("RESULT: " + ("PASS" if not failures else "FAIL\n  " + "\n  ".join(failures)))
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
