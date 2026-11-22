#!/usr/bin/env python
"""Check the ingest on small samples before the full (multi-hour) run.

Run from the ``lab2`` folder::

    cd lab2 && python scripts/sample_ingest_check.py [--records N]

What it does
------------
1. **Samples.** Streams the first ``--records`` records of each raw dump
   (both snapshots, all four entities) into a small gzipped XML file under
   ``<sample dir>/raw/``, with the same file name as the real dump. For
   releases, if fewer than ``--min-in-window`` sampled records fall in the
   study window, it also appends records from about 80% into the file.
2. **Schema drift.** Reads each sample with the saved schema and with
   schema inference, then compares them. Reports fields that would be
   dropped, fields never seen, and fields whose type could lose values.
3. **Tables.** Runs ``tables.build_all`` on both samples and prints row
   counts, columns and example rows for every table.
4. **Speed.** Times the parse of the releases sample with the saved schema
   and estimates the single-core parse time of the full files.

Safety
------
The raw dumps are only opened for reading. ``DISCOGS_DATA_DIR`` is pointed
at the sample folder before ``vinyl`` is imported, so ``build_all`` reads
and writes only there. The real data dir is never written to.

Environment
-----------
``DISCOGS_DATA_DIR``
    Where the real raw dumps are (``<dir>/raw``). Default
    ``/tmp/$USER/discogs``.
``DISCOGS_SAMPLE_DIR``
    Where samples and sample Parquet go. Default
    ``/tmp/$USER/discogs_sample``.
"""

from __future__ import annotations

import argparse
import getpass
import gzip
import json
import os
import re
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

# --------------------------------------------------------------------------
# Folders. This must run before ``vinyl`` is imported, because
# ``vinyl.config.DATA_DIR`` reads DISCOGS_DATA_DIR at import time.
# --------------------------------------------------------------------------

LAB_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAB_ROOT))

_USER = os.environ.get("USER") or getpass.getuser()
SOURCE_DIR = Path(os.environ.get("DISCOGS_DATA_DIR") or f"/tmp/{_USER}/discogs")
SAMPLE_DIR = Path(os.environ.get("DISCOGS_SAMPLE_DIR") or f"/tmp/{_USER}/discogs_sample")

if SOURCE_DIR.resolve() == SAMPLE_DIR.resolve():
    sys.exit(f"Sample dir and real data dir are the same ({SAMPLE_DIR}); refusing to run.")
os.environ["DISCOGS_DATA_DIR"] = str(SAMPLE_DIR)

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import ArrayType, DataType, StructType  # noqa: E402

from vinyl import config, ingest, tables  # noqa: E402

if Path(config.DATA_DIR).resolve() != SAMPLE_DIR.resolve():  # pragma: no cover
    sys.exit(f"vinyl.config.DATA_DIR is {config.DATA_DIR}, expected {SAMPLE_DIR}; aborting.")

META_DIR = SAMPLE_DIR / "meta"
SKIP_CHUNK = 32 * 1024 * 1024  # bytes decompressed per step while skipping ahead
RELEASED = re.compile(rb"<released>(\d{4})")
#: Window used to stage releases when the sample has too few in-window ones.
WIDE_WINDOW = (1000, 2100)
NUMERIC_TYPES = {"int", "bigint", "smallint", "tinyint", "double", "float", "boolean"}


_LOCAL = threading.local()


def log(message: str = "") -> None:
    """Print one line and flush, so progress shows up when output is piped.

    If the calling thread has set ``_LOCAL.buffer`` (a list), the line is
    appended there instead, so output of parallel work can be printed in order.
    """
    buffer = getattr(_LOCAL, "buffer", None)
    if buffer is not None:
        buffer.append(message)
    else:
        print(message, flush=True)


def human(n_bytes: float) -> str:
    """Format a byte count, for example ``1.5 GB``."""
    value = float(n_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(value) < 1024 or unit == "TB":
            return f"{value:,.1f} {unit}"
        value /= 1024
    return f"{value:,.1f} TB"


def duration(seconds: float) -> str:
    """Format seconds as hours and minutes, for example ``3.2 h``."""
    if seconds < 120:
        return f"{seconds:,.0f} s"
    if seconds < 7200:
        return f"{seconds / 60:,.1f} min"
    return f"{seconds / 3600:,.1f} h"


# --------------------------------------------------------------------------
# 1. Sampling
# --------------------------------------------------------------------------

def source_path(snapshot: str, entity: str) -> Path:
    """Return the real (read-only) raw dump for one snapshot and entity."""
    return SOURCE_DIR / "raw" / Path(config.raw_path(snapshot, entity)).name


def sample_path(snapshot: str, entity: str) -> Path:
    """Return where the sample for one snapshot and entity is written."""
    return Path(config.raw_path(snapshot, entity))


def meta_path(snapshot: str, entity: str) -> Path:
    """Return the JSON file that stores facts measured while sampling."""
    return META_DIR / f"{sample_path(snapshot, entity).name}.json"


def _copy_records(
    stream,
    out,
    entity: str,
    n_records: int,
    keep_header: bool,
) -> dict:
    """Copy up to ``n_records`` whole records from ``stream`` to ``out``.

    A record starts at a line beginning with ``<tag `` or ``<tag>``. Text
    inside records is XML-escaped, so a line cannot start with ``<`` unless
    it is markup. The record that follows the last one copied is read but
    not written; the closing root element ends the copy early.

    Parameters
    ----------
    stream : file object
        Binary line iterator over decompressed XML.
    out : file object
        Binary destination.
    entity : str
        ``"releases"``, ``"masters"``, ``"labels"`` or ``"artists"``.
    n_records : int
        Maximum number of records to copy.
    keep_header : bool
        Copy the lines before the first record (xml declaration, root).

    Returns
    -------
    dict
        ``records``, ``bytes`` (decompressed, records only), ``in_window``
        (releases only) and ``header`` (list of header lines, bytes).
    """
    tag = config.ROW_TAGS[entity].encode()
    starts = (b"<" + tag + b" ", b"<" + tag + b">")
    root_close = b"</" + entity.encode() + b">"
    count = n_bytes = in_window = 0
    header: list[bytes] = []
    for line in stream:
        if line.startswith(root_close):
            break
        if line.startswith(starts):
            if count >= n_records:
                break
            count += 1
        elif count == 0:
            header.append(line)
            if keep_header:
                out.write(line)
            continue
        out.write(line)
        n_bytes += len(line)
        if entity == "releases":
            for match in RELEASED.finditer(line):
                if config.YEAR_MIN <= int(match.group(1)) <= config.YEAR_MAX:
                    in_window += 1
    return {"records": count, "bytes": n_bytes, "in_window": in_window, "header": header}


def _skip_ahead(
    stream,
    raw,
    entity: str,
    file_size: int,
    fraction: float,
    n_records: int | None,
    label: str = "",
    max_seconds: float | None = None,
) -> dict:
    """Read and discard decompressed data, counting records on the way.

    This streams in large chunks instead of line by line, because
    decompressing the dump is the cost and Python line splitting only
    slows it down. Nothing is stored.

    Parameters
    ----------
    stream : gzip.GzipFile
    raw : file object
        The compressed file under ``stream`` (its ``tell`` gives the
        compressed position).
    entity : str
    file_size : int
        Size of the compressed file in bytes.
    fraction : float
        Stop once this share of the compressed file has been consumed.
        Ignored when ``n_records`` is given.
    n_records : int or None
        If given, stop after at least this many records instead.
    label : str
        Name shown in the progress lines.
    max_seconds : float, optional
        Give up skipping after this many seconds and sample from wherever
        the stream has got to.

    Returns
    -------
    dict
        ``records`` and ``bytes`` skipped, ``eof`` (True if the file ended),
        ``at_line_start`` (True if the last byte read was a newline) and
        ``stopped_by`` (``"fraction"``, ``"records"``, ``"time"`` or ``"eof"``).
    """
    tag = config.ROW_TAGS[entity].encode()
    markers = (b"\n<" + tag + b" ", b"\n<" + tag + b">")
    keep = max(len(m) for m in markers) - 1
    carry = b""
    records = n_bytes = 0
    eof = False
    last_byte = b""
    began = last_report = time.monotonic()
    stopped_by = "eof"
    while True:
        if n_records is not None:
            if records >= n_records:
                stopped_by = "records"
                break
        elif raw.tell() >= fraction * file_size:
            stopped_by = "fraction"
            break
        if max_seconds is not None and time.monotonic() - began > max_seconds:
            stopped_by = "time"
            break
        chunk = stream.read(SKIP_CHUNK)
        if not chunk:
            eof = True
            break
        n_bytes += len(chunk)
        data = carry + chunk
        records += sum(data.count(marker) for marker in markers)
        carry = data[-keep:]
        last_byte = chunk[-1:]
        if time.monotonic() - last_report > 120:
            last_report = time.monotonic()
            log(
                f"  ...   {label}: skipped {human(n_bytes)} XML, {records:,} records "
                f"({raw.tell() / file_size:.0%} of the file)"
            )
    return {
        "records": records,
        "bytes": n_bytes,
        "eof": eof,
        "at_line_start": last_byte == b"\n",
        "stopped_by": stopped_by,
    }


def make_sample(
    snapshot: str,
    entity: str,
    n_records: int,
    skip_records: int | None,
    skip_fraction: float,
    min_in_window: int,
    force: bool,
    max_skip_seconds: float | None = None,
) -> dict:
    """Create (or reuse) the sample file of one raw dump.

    Parameters
    ----------
    snapshot, entity : str
    n_records : int
        Records to take from the start, and again from the later part.
    skip_records : int or None
        ``None``: take a later part only for releases whose head holds
        fewer than ``min_in_window`` in-window records, starting about
        ``skip_fraction`` into the file. ``0``: never. ``N > 0``: always
        for releases, after skipping at least ``N`` records.
    skip_fraction : float
    min_in_window : int
    force : bool
        Rebuild the sample even if it exists.
    max_skip_seconds : float, optional
        Time cap for skipping ahead (see ``_skip_ahead``).

    Returns
    -------
    dict
        Facts about the sample, also saved as JSON in ``META_DIR``.
        ``probe`` holds a (decompressed bytes, compressed bytes, records)
        point used to estimate the size of the full file.

    Raises
    ------
    FileNotFoundError
        If the raw dump is missing.
    RuntimeError
        If the dump does not have the expected root element.
    """
    src = source_path(snapshot, entity)
    dst = sample_path(snapshot, entity)
    meta_file = meta_path(snapshot, entity)
    if dst.exists() and meta_file.exists() and not force:
        meta = json.loads(meta_file.read_text())
        note = ""
        if meta["requested_records"] != n_records:
            note = f" (built with --records {meta['requested_records']}, not {n_records})"
        log(f"  reuse  {dst.name}: {meta['sample_records']:,} records{note}")
        return meta
    if not src.exists() or src.stat().st_size == 0:
        raise FileNotFoundError(f"Raw dump missing or empty: {src}")

    dst.parent.mkdir(parents=True, exist_ok=True)
    META_DIR.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(dst.name + ".tmp")
    file_size = src.stat().st_size
    started = time.perf_counter()
    meta: dict = {
        "snapshot": snapshot,
        "entity": entity,
        "requested_records": n_records,
        "source": str(src),
        "file_size": file_size,
        "head": None,
        "skip": None,
        "tail": None,
    }

    with open(src, "rb") as raw, gzip.GzipFile(fileobj=raw, mode="rb") as stream, gzip.open(
        tmp, "wb", compresslevel=3
    ) as out:
        head = _copy_records(stream, out, entity, n_records, keep_header=True)
        if f"<{entity}>".encode() not in b"".join(head["header"]):
            raise RuntimeError(f"{src.name}: no <{entity}> root element before the first record")
        head["compressed_bytes"] = raw.tell()
        head["decompressed_bytes"] = stream.tell()
        meta["head"] = {k: v for k, v in head.items() if k != "header"}
        probe = (stream.tell(), raw.tell(), head["records"])

        take_tail = False
        if entity == "releases":
            if skip_records is None:
                take_tail = head["in_window"] < min_in_window
            else:
                take_tail = skip_records > 0
        if take_tail:
            skip = _skip_ahead(
                stream, raw, entity, file_size, skip_fraction,
                skip_records, f"{snapshot}/{entity}", max_skip_seconds,
            )
            skip["compressed_bytes"] = raw.tell()
            meta["skip"] = skip
            if not skip["eof"]:
                if not skip["at_line_start"]:
                    stream.readline()  # discard the partial line
                probe = (stream.tell(), raw.tell(), head["records"] + skip["records"])
                tail = _copy_records(stream, out, entity, n_records, keep_header=False)
                tail.pop("header")
                meta["tail"] = tail
        out.write(f"</{entity}>\n".encode())

    os.replace(tmp, dst)
    meta["probe"] = {
        "decompressed_bytes": probe[0],
        "compressed_bytes": probe[1],
        "records": probe[2],
    }
    parts = [meta["head"]] + ([meta["tail"]] if meta["tail"] else [])
    meta["sample_records"] = sum(p["records"] for p in parts)
    meta["sample_bytes"] = sum(p["bytes"] for p in parts)
    meta["sample_in_window"] = sum(p["in_window"] for p in parts)
    meta["head_in_window"] = meta["head"]["in_window"]
    meta_file.write_text(json.dumps(meta, indent=2))
    log(
        f"  made   {dst.name}: {meta['sample_records']:,} records"
        f"{' (head + tail)' if meta['tail'] else ''}, {human(dst.stat().st_size)}, "
        f"{time.perf_counter() - started:,.0f} s"
    )
    return meta


# --------------------------------------------------------------------------
# 2. Schema drift
# --------------------------------------------------------------------------

def flatten(dtype: DataType, path: str = "") -> dict[str, str]:
    """Map every field path of a Spark type to ``"struct"`` or its leaf type.

    Arrays are transparent: ``artists.artist.name`` stands for the name of
    each element of the ``artist`` array.

    Parameters
    ----------
    dtype : DataType
    path : str
        Prefix for the paths (dot separated).

    Returns
    -------
    dict of str to str
    """
    out: dict[str, str] = {}
    depth = 0
    while isinstance(dtype, ArrayType):
        dtype = dtype.elementType
        depth += 1
    # "[]" per array level, so a saved scalar vs an inferred array shows up
    # as shape drift (a scalar would keep only one of the repeated values).
    prefix = "[]" * depth
    if isinstance(dtype, StructType):
        if path:
            out[path] = prefix + "struct"
        for column in dtype.fields:
            out.update(flatten(column.dataType, f"{path}.{column.name}" if path else column.name))
    elif path:
        out[path] = prefix + dtype.simpleString()
    return out


def type_risk(saved: str, inferred: str) -> str | None:
    """Say why a saved type could lose values the data really has.

    Parameters
    ----------
    saved, inferred : str
        Simple type names, for example ``"int"`` or ``"string"``.

    Returns
    -------
    str or None
        A short reason, or None if the saved type is safe.
    """
    if saved == inferred:
        return None
    if saved in NUMERIC_TYPES and inferred == "string":
        return "saved type is numeric/boolean but the data has text: values become null"
    if saved == "int" and inferred in {"bigint", "double"}:
        return f"saved int, data needs {inferred}"
    if saved == "bigint" and inferred == "double":
        return "saved bigint, data has decimals"
    return None


@dataclass
class Drift:
    """Result of comparing the saved schema with inference on one sample."""

    snapshot: str
    entity: str
    expected_rows: int
    rows: int = 0
    corrupt_rows: int | None = None
    all_null_rows: int = 0
    inferred_corrupt_rows: int | None = None
    dropped: list[str] = field(default_factory=list)
    unseen: list[str] = field(default_factory=list)
    shape: list[str] = field(default_factory=list)
    risks: list[str] = field(default_factory=list)
    other_types: list[str] = field(default_factory=list)
    lost_values: list[str] = field(default_factory=list)

    @property
    def label(self) -> str:
        """Short name such as ``2020/releases``."""
        return f"{self.snapshot}/{self.entity}"

    @property
    def clean(self) -> bool:
        """True if nothing would be dropped or nulled."""
        return not (
            self.dropped or self.shape or self.risks or self.lost_values
            or self.all_null_rows or self.corrupt_rows or self.inferred_corrupt_rows
            or self.rows != self.expected_rows
        )


def _inferred_reader(spark: SparkSession, entity: str, path: str) -> DataFrame:
    """Read a sample with the same options as ``ingest.read_raw_xml`` but no schema."""
    return (
        spark.read.format("xml")
        .option("rowTag", config.ROW_TAGS[entity])
        .option("attributePrefix", "_")
        .option("valueTag", "_VALUE")
        .option("mode", "PERMISSIVE")
        .load(path)
    )


def _non_null_counts(df: DataFrame, columns: list[str]) -> dict[str, int]:
    """Count the non-null values of each top-level column in one pass."""
    row = df.agg(*[F.count(F.col(f"`{c}`")).alias(c) for c in columns]).first()
    return {c: int(row[c]) for c in columns}


def check_drift(spark: SparkSession, snapshot: str, entity: str, expected_rows: int) -> Drift:
    """Compare the saved schema with inferred schema on one sample.

    Parameters
    ----------
    spark : SparkSession
    snapshot, entity : str
    expected_rows : int
        Records written to the sample; the reader must return as many rows.

    Returns
    -------
    Drift
    """
    path = str(sample_path(snapshot, entity))
    saved_df = ingest.read_raw_xml(spark, snapshot, entity)
    inferred_df = _inferred_reader(spark, entity, path)
    result = Drift(snapshot, entity, expected_rows)

    saved_paths = flatten(saved_df.schema)
    inferred_paths = flatten(inferred_df.schema)
    result.dropped = sorted(set(inferred_paths) - set(saved_paths))
    result.unseen = sorted(set(saved_paths) - set(inferred_paths))
    for key in sorted(set(saved_paths) & set(inferred_paths)):
        saved_t, inferred_t = saved_paths[key], inferred_paths[key]
        if saved_t.count("[]") != inferred_t.count("[]"):
            result.shape.append(f"{key}: saved {saved_t}, inferred {inferred_t} (array depth differs)")
            continue
        saved_t, inferred_t = saved_t.replace("[]", ""), inferred_t.replace("[]", "")
        if (saved_t == "struct") != (inferred_t == "struct"):
            result.shape.append(f"{key}: saved {saved_t}, inferred {inferred_t}")
        elif saved_t != inferred_t:
            reason = type_risk(saved_t, inferred_t)
            line = f"{key}: saved {saved_t}, inferred {inferred_t}"
            if reason:
                result.risks.append(f"{line} ({reason})")
            else:
                result.other_types.append(line)

    columns = saved_df.columns
    common = [c for c in columns if c in inferred_df.columns and c != "_corrupt_record"]
    all_null = F.lit(True)
    for column in columns:
        all_null = all_null & F.col(f"`{column}`").isNull()
    # One pass over each read: row counts, bad rows and per-column non-null counts.
    saved_row = saved_df.agg(
        F.count(F.lit(1)).alias("__rows"),
        F.sum(F.when(all_null, 1).otherwise(0)).alias("__all_null"),
        *[F.count(F.col(f"`{c}`")).alias(c) for c in common],
        *(
            [F.count(F.col("_corrupt_record")).alias("__corrupt")]
            if "_corrupt_record" in columns else []
        ),
    ).first()
    result.rows = int(saved_row["__rows"])
    result.all_null_rows = int(saved_row["__all_null"] or 0)
    if "_corrupt_record" in columns:
        result.corrupt_rows = int(saved_row["__corrupt"])

    inferred_row = inferred_df.agg(
        *[F.count(F.col(f"`{c}`")).alias(c) for c in common],
        *(
            [F.count(F.col("_corrupt_record")).alias("__corrupt")]
            if "_corrupt_record" in inferred_df.columns else []
        ),
    ).first()
    if "_corrupt_record" in inferred_df.columns:
        result.inferred_corrupt_rows = int(inferred_row["__corrupt"])

    for column in common:
        if saved_row[column] < inferred_row[column]:
            result.lost_values.append(
                f"{column}: {saved_row[column]:,} non-null with saved schema, "
                f"{inferred_row[column]:,} with inference"
            )
    return result


def print_drift(result: Drift) -> None:
    """Print the full drift findings of one sample."""
    log(f"--- {result.label}: {result.rows:,} rows read, {result.expected_rows:,} expected")
    log(f"    rows with every top-level field null: {result.all_null_rows:,}")
    if result.corrupt_rows is not None:
        log(f"    _corrupt_record rows (saved schema): {result.corrupt_rows:,}")
    if result.inferred_corrupt_rows is not None:
        log(f"    _corrupt_record rows (inferred): {result.inferred_corrupt_rows:,}")
    for title, items in (
        ("DROPPED (inferred, not in saved schema)", result.dropped),
        ("SHAPE differs (struct vs plain value)", result.shape),
        ("TYPE RISK", result.risks),
        ("VALUES LOST (fewer non-null with saved schema)", result.lost_values),
        ("type differs, harmless", result.other_types),
        ("never seen in sample (in saved schema only)", result.unseen),
    ):
        log(f"    {title}: {len(items)}")
        for item in items:
            log(f"        {item}")


# --------------------------------------------------------------------------
# 3. Build all tables
# --------------------------------------------------------------------------

def _short(value: object, width: int = 60) -> str:
    """Render a cell value on one line, cut to ``width`` characters."""
    text = repr(value)
    return text if len(text) <= width else text[: width - 3] + "..."


def describe_table(spark: SparkSession, snapshot: str, name: str) -> tuple[int, list[str]]:
    """Print row count, columns and 3 example rows of one built table.

    Parameters
    ----------
    spark : SparkSession
    snapshot, name : str

    Returns
    -------
    tuple
        Row count and the list of columns that are null in every row.
    """
    df = ingest.read_table(spark, snapshot, name)
    counts = _non_null_counts(df, df.columns)
    n_rows = df.count()
    empty = [c for c in df.columns if n_rows and counts[c] == 0]
    log(f"  [{snapshot}] {name}: {n_rows:,} rows")
    log(f"      columns: {', '.join(f'{c}:{t}' for c, t in df.dtypes)}")
    if empty:
        log(f"      WARNING all-null columns: {', '.join(empty)}")
    for row in df.limit(3).collect():
        log("      " + ", ".join(f"{k}={_short(v)}" for k, v in row.asDict().items()))
    return n_rows, empty


def build_and_describe(spark: SparkSession, snapshot: str, widen: bool = False) -> dict:
    """Run ``tables.build_all`` for one snapshot sample and describe the output.

    The whole build is tried first. If it raises, each table is retried on
    its own to find which ones fail.

    Parameters
    ----------
    spark : SparkSession
    snapshot : str
    widen : bool
        Use when the sample holds too few releases inside the study window.
        The releases are first staged with ``WIDE_WINDOW`` (via
        ``tables.stage_releases``) and ``build_all`` then reuses that
        staged copy, so the table builders still run on real nested data.

    Returns
    -------
    dict
        ``ok`` (list of table names), ``failed`` (name -> error text),
        ``rows`` (name -> count) and ``empty`` (name -> all-null columns).
    """
    outcome: dict = {"ok": [], "failed": {}, "rows": {}, "empty": {}, "widened": widen}
    started = time.perf_counter()
    try:
        if widen:
            log(f"  too few in-window releases: staging with window {WIDE_WINDOW} instead")
            tables.stage_releases(spark, snapshot, *WIDE_WINDOW)
        tables.build_all(spark, snapshot, tables=list(tables.TABLES), restage=not widen)
        built = list(tables.TABLES)
    except Exception as error:  # noqa: BLE001 - report and carry on
        log(f"  build_all({snapshot}) FAILED: {type(error).__name__}: {str(error)[:300]}")
        log("  retrying table by table to find the failing ones")
        built = []
        for index, name in enumerate(tables.TABLES):
            try:
                tables.build_all(spark, snapshot, tables=[name], restage=(index == 0 and not widen))
                built.append(name)
            except Exception as inner:  # noqa: BLE001
                outcome["failed"][name] = f"{type(inner).__name__}: {str(inner)[:300]}"
                log(f"    {name}: FAILED {outcome['failed'][name]}")
    log(f"  build time ({snapshot}): {time.perf_counter() - started:,.0f} s")

    for name in built:
        try:
            outcome["rows"][name], outcome["empty"][name] = describe_table(spark, snapshot, name)
            outcome["ok"].append(name)
        except Exception as error:  # noqa: BLE001
            outcome["failed"][name] = f"read back: {type(error).__name__}: {str(error)[:300]}"
            log(f"  [{snapshot}] {name}: FAILED reading back: {outcome['failed'][name]}")
    return outcome


# --------------------------------------------------------------------------
# 4. Speed
# --------------------------------------------------------------------------

def time_parse(spark: SparkSession, snapshot: str, repeats: int = 1) -> dict:
    """Time a full parse of the releases sample with the saved schema.

    The ``noop`` sink asks for every column, so Spark cannot skip fields
    it would otherwise prune. It leaves out the Parquet write that
    ``stage_releases`` also does.

    Parameters
    ----------
    spark : SparkSession
    snapshot : str
    repeats : int
        Number of timed runs. The first can include JVM warm-up, so the
        fastest run is used.

    Returns
    -------
    dict
        ``seconds`` (list), ``partitions`` (read tasks) and ``best``.
    """
    df = ingest.read_raw_xml(spark, snapshot, "releases")
    partitions = df.rdd.getNumPartitions()
    seconds = []
    for _ in range(repeats):
        started = time.perf_counter()
        df.write.format("noop").mode("overwrite").save()
        seconds.append(time.perf_counter() - started)
    return {"seconds": seconds, "partitions": partitions, "best": min(seconds)}


def estimate_full_parse(meta: dict, timing: dict) -> dict:
    """Scale the sample timing up to the whole releases file.

    The decompressed size is the compressed size times the compression
    ratio seen on the streamed part (the point reached by skipping ahead
    when there is one, otherwise the end of the head sample). The record
    count is that size divided by the bytes per record measured over the
    same streamed part.

    Parameters
    ----------
    meta : dict
        Output of ``make_sample`` for the releases file.
    timing : dict
        Output of ``time_parse``.

    Returns
    -------
    dict
        Estimated sizes, record count, rate and times.
    """
    probe = meta["probe"]
    ratio = probe["decompressed_bytes"] / probe["compressed_bytes"]
    decompressed = meta["file_size"] * ratio
    bytes_per_record = probe["decompressed_bytes"] / probe["records"]
    total_records = decompressed / bytes_per_record
    rate = meta["sample_records"] / timing["best"]
    byte_rate = meta["sample_bytes"] / timing["best"]
    return {
        "compressed": meta["file_size"],
        "ratio": ratio,
        "decompressed": decompressed,
        "bytes_per_record": bytes_per_record,
        "records": total_records,
        "rate": rate,
        "seconds": total_records / rate,
        "seconds_by_bytes": decompressed / byte_rate,
        "probe_share": probe["compressed_bytes"] / meta["file_size"],
    }


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(
        description="Check schemas, table builders and parse speed on small samples."
    )
    parser.add_argument("--records", type=int, default=20000,
                        help="records per sample, from the head (and again from the later "
                             "part of releases) (default: %(default)s)")
    parser.add_argument("--skip-records", type=int, default=None,
                        help="releases: skip at least this many records, then sample. "
                             "0 = never take a later part. Default: only when the head has "
                             "too few in-window releases, skipping to --skip-fraction.")
    parser.add_argument("--skip-fraction", type=float, default=0.8,
                        help="share of the compressed file to skip when --skip-records is "
                             "not given (default: %(default)s)")
    parser.add_argument("--max-skip-seconds", type=float, default=420,
                        help="stop skipping ahead after this long and sample from where "
                             "the stream has got to; 0 = no limit (default: %(default)s)")
    parser.add_argument("--min-in-window", type=int, default=200,
                        help="in-window releases needed in the head sample before the later "
                             "part is skipped (default: %(default)s)")
    parser.add_argument("--force", action="store_true", help="rebuild samples that already exist")
    parser.add_argument("--samples-only", action="store_true",
                        help="make the sample files and stop (no Spark)")
    args = parser.parse_args(argv)
    if args.max_skip_seconds <= 0:
        args.max_skip_seconds = None
    if args.records < 1:
        parser.error("--records must be at least 1")
    if not 0 < args.skip_fraction < 1:
        parser.error("--skip-fraction must be between 0 and 1")
    return args


def main(argv: list[str] | None = None) -> int:
    """Run the four checks and print the summary.

    Returns
    -------
    int
        0 if every table built and every sample read back with the
        expected row count, otherwise 1.
    """
    args = parse_args(argv)
    log(f"real data dir (read only): {SOURCE_DIR}")
    log(f"sample dir:                {SAMPLE_DIR}")
    log(f"vinyl.config.DATA_DIR:     {config.DATA_DIR}")

    log("\n=== 1. Samples ===")
    # Decompressing is the cost and it is CPU bound, so the files are built in
    # parallel threads (zlib releases the GIL). The wall time is that of the
    # slowest file, the releases dump.
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {
            (snapshot, entity): pool.submit(
                make_sample, snapshot, entity, args.records, args.skip_records,
                args.skip_fraction, args.min_in_window, args.force,
                args.max_skip_seconds,
            )
            for snapshot in config.SNAPSHOTS
            for entity in config.ROW_TAGS
        }
        metas: dict[tuple[str, str], dict] = {key: f.result() for key, f in futures.items()}
    if args.samples_only:
        log("--samples-only: stopping after step 1.")
        return 0

    spark = ingest.get_spark(
        "sample-ingest-check",
        shuffle_partitions=8,
        extra_conf={
            "spark.master": "local[10]",
            "spark.driver.memory": "4g",
            "spark.ui.enabled": "false",
            "spark.ui.showConsoleProgress": "false",
        },
    )
    spark.sparkContext.setLogLevel("ERROR")

    log("\n=== 2. Schema drift (saved schema vs inference) ===")
    def run_drift(key: tuple[str, str]) -> Drift:
        snapshot, entity = key
        started = time.perf_counter()
        try:
            drift = check_drift(spark, snapshot, entity, metas[key]["sample_records"])
        except Exception as error:  # noqa: BLE001
            log(f"--- {snapshot}/{entity}: drift check FAILED: {type(error).__name__}: {error}")
            traceback.print_exc()
            drift = Drift(snapshot, entity, metas[key]["sample_records"])
            drift.dropped = [f"CHECK FAILED: {type(error).__name__}"]
        log(f"  checked {snapshot}/{entity} in {time.perf_counter() - started:,.0f} s")
        return drift

    def run_build(snapshot: str) -> tuple[dict, list[str]]:
        _LOCAL.buffer = []
        too_few = metas[(snapshot, "releases")]["sample_in_window"] < args.min_in_window
        outcome = build_and_describe(spark, snapshot, widen=too_few)
        return outcome, _LOCAL.buffer

    # One gzip file is one Spark task, i.e. one core, so everything that is
    # independent runs at once: 8 drift checks and one table build per snapshot
    # (the snapshots write to different folders). Output is printed afterwards,
    # in order.
    with ThreadPoolExecutor(max_workers=10) as pool:
        drift_futures = [pool.submit(run_drift, key) for key in metas]
        build_futures = {s: pool.submit(run_build, s) for s in config.SNAPSHOTS}
        drifts: list[Drift] = [f.result() for f in drift_futures]
        build_results = {s: f.result() for s, f in build_futures.items()}

    for drift in drifts:
        print_drift(drift)

    log("\n=== 3. Build all tables on the samples ===")
    builds: dict[str, dict] = {}
    for snapshot, (outcome, lines) in build_results.items():
        builds[snapshot] = outcome
        log(f"--- snapshot {snapshot} ({config.SNAPSHOTS[snapshot]})")
        for line in lines:
            log(line)

    log("\n=== 4. Parse speed (releases sample, saved schema) ===")
    # Nothing else is running now. Both snapshots are timed together (one core
    # each). One run is enough: the JVM is already warm from steps 2 and 3.
    with ThreadPoolExecutor(max_workers=len(config.SNAPSHOTS)) as pool:
        timing_futures = {s: pool.submit(time_parse, spark, s, 1) for s in config.SNAPSHOTS}
        timings: dict[str, dict] = {s: f.result() for s, f in timing_futures.items()}
    estimates: dict[str, dict] = {}
    for snapshot in config.SNAPSHOTS:
        meta = metas[(snapshot, "releases")]
        estimates[snapshot] = estimate_full_parse(meta, timings[snapshot])
        t, e = timings[snapshot], estimates[snapshot]
        log(
            f"  [{snapshot}] {meta['sample_records']:,} records in "
            f"{', '.join(f'{x:.1f}' for x in t['seconds'])} s "
            f"({t['partitions']} read task(s)); {e['rate']:,.0f} records/s"
        )

    summary = build_summary(args, metas, drifts, builds, timings, estimates)
    log("\n" + summary)
    (SAMPLE_DIR / "summary.txt").write_text(summary + "\n")
    spark.stop()

    failed = any(b["failed"] for b in builds.values())
    wrong_rows = any(d.rows != d.expected_rows for d in drifts)
    return 1 if failed or wrong_rows else 0


def build_summary(
    args: argparse.Namespace,
    metas: dict[tuple[str, str], dict],
    drifts: list[Drift],
    builds: dict[str, dict],
    timings: dict[str, dict],
    estimates: dict[str, dict],
) -> str:
    """Assemble the final summary block as text."""
    cap = 15
    out = ["=" * 72, "SUMMARY", "=" * 72]
    out.append(f"samples: {args.records:,} records per file, in {SAMPLE_DIR}/raw")

    out.append("\nSchema drift (saved schema vs inference on the sample)")
    for d in drifts:
        status = "clean" if d.clean else "FINDINGS"
        out.append(
            f"  {d.label:<16} {status:<8} rows {d.rows:,}/{d.expected_rows:,}  "
            f"dropped {len(d.dropped)}  shape {len(d.shape)}  type-risk {len(d.risks)}  "
            f"values-lost {len(d.lost_values)}  all-null rows {d.all_null_rows}  "
            f"corrupt {d.corrupt_rows if d.corrupt_rows is not None else '-'}"
            f"/{d.inferred_corrupt_rows if d.inferred_corrupt_rows is not None else '-'}  "
            f"unseen {len(d.unseen)}"
        )
        for title, items in (
            ("dropped", d.dropped), ("shape", d.shape),
            ("type-risk", d.risks), ("values-lost", d.lost_values),
        ):
            for item in items[:cap]:
                out.append(f"      {title}: {item}")
            if len(items) > cap:
                out.append(f"      {title}: ... {len(items) - cap} more (see the log)")

    out.append("\nTables built from the samples")
    for snapshot, build in builds.items():
        total = len(tables.TABLES)
        note = (
            f" (releases staged with window {WIDE_WINDOW}: too few in-window releases)"
            if build["widened"] else ""
        )
        out.append(f"  {snapshot}: {len(build['ok'])}/{total} OK{note}")
        for name, error in build["failed"].items():
            out.append(f"      FAILED {name}: {error}")
        for name, columns in build["empty"].items():
            if columns:
                out.append(f"      all-null columns in {name}: {', '.join(columns)}")
        rows = ", ".join(f"{n} {c:,}" for n, c in build["rows"].items())
        out.append(f"      rows: {rows}")

    out.append(f"\nReleases in the {config.YEAR_MIN}-{config.YEAR_MAX} window")
    for snapshot in config.SNAPSHOTS:
        meta = metas[(snapshot, "releases")]
        staged = builds[snapshot]["rows"].get("releases")
        tail = meta["tail"]
        out.append(
            f"  {snapshot}: head {meta['head']['records']:,} records, "
            f"{meta['head_in_window']:,} in window"
            + (
                f"; later part {tail['records']:,} records, {tail['in_window']:,} in window "
                f"(skipped to record ~{meta['head']['records'] + meta['skip']['records']:,}, "
                f"{meta['probe']['compressed_bytes'] / meta['file_size']:.0%} of the file; "
                f"stopped by {meta['skip']['stopped_by']})"
                if tail else "; no later part"
            )
            + f"; releases table: {staged if staged is not None else 'n/a'}"
        )

    out.append("\nSingle-core parse estimate for the full releases files (saved schema)")
    total_seconds = 0.0
    for snapshot in config.SNAPSHOTS:
        e, t = estimates[snapshot], timings[snapshot]
        total_seconds += e["seconds"]
        out.append(
            f"  {snapshot}: {human(e['compressed'])} gz, ratio {e['ratio']:.1f}x "
            f"=> ~{human(e['decompressed'])} XML, ~{e['bytes_per_record']:,.0f} B/record "
            f"=> ~{e['records'] / 1e6:,.1f} M records (extrapolated from the first "
            f"{e['probe_share']:.0%} of the file)"
        )
        out.append(
            f"        sample rate {e['rate']:,.0f} records/s on {t['partitions']} task(s) "
            f"=> ~{duration(e['seconds'])} (by bytes: ~{duration(e['seconds_by_bytes'])})"
        )
    out.append(
        f"  both releases files: ~{duration(total_seconds)} if read one after the other. "
        "Does not include the Parquet write, and records later in the file may parse "
        "more slowly."
    )
    out.append("=" * 72)
    return "\n".join(out)


if __name__ == "__main__":
    sys.exit(main())
