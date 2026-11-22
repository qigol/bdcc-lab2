"""Spark session, raw XML reading, and Parquet I/O.

Why loading is slow, and what this module does about it
-------------------------------------------------------
1. **Schema inference.** Without a schema, Spark reads the whole file once
   just to work out the columns, then again to load it. ``read_raw_xml``
   passes the saved schemas in ``schemas/`` so that first pass is skipped.
2. **Gzip cannot be split.** A ``.xml.gz`` file is always read by a single
   task on a single core, and Spark's XML reader does not split plain XML
   files either. ``split_raw_xml`` therefore streams the gz once through
   ``gzip -dc`` and cuts it into plain-XML chunk files (256 MiB by
   default), each wrapped in its own root element, so Spark parses one
   chunk per core. ``get_spark`` sizes ``local[n]`` to the container's
   cgroup CPU quota (4 here), not the host's 32 cores.
3. **Most releases are outside the window.** Records are in Discogs id
   order, so the early part of the releases file holds almost no
   2016-2026 releases. While splitting, a conservative year pre-filter
   drops records whose ``<released>`` year is clearly outside the window,
   so Spark never parses them. ``tables.scope_releases`` still applies the
   exact rule afterwards.
4. **Unused fields.** ``read_raw_xml(drop_fields=...)`` removes top-level
   fields that no table uses (``config.RELEASE_DROP_FIELDS``) from the
   schema, so Spark does not build them.

The raw files are still read exactly once and written to Parquet
(``tables.build_all``). Everything after that reads Parquet, which is
columnar, splittable, and fast.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import time
from collections.abc import Iterable, Iterator
from pathlib import Path

from pyspark import SparkConf, SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from . import config

#: Raw data that must never be written to or deleted (see the project rules).
_READ_ONLY_ROOT = "/mnt/data/public"

#: ``<released>`` followed by four digits; the conservative pre-filter key.
_RELEASED_YEAR = re.compile(rb"<released>(\d{4})")

#: A ``spark.master`` line in ``spark-defaults.conf`` (Java properties syntax).
_DEFAULTS_MASTER = re.compile(r"^[ \t]*spark\.master[ \t]*[=: \t][ \t]*(\S.*)$", re.MULTILINE)


def _cpu_quota() -> int:
    """Return the number of CPUs this process may use.

    Containers often have a cgroup CPU quota far below the host's core
    count (here 4 CPUs on a 32-core host). ``local[*]`` would start one
    thread per host core, so the quota is used instead.

    Returns
    -------
    int
        The cgroup v2 quota (``/sys/fs/cgroup/cpu.max``), else the cgroup
        v1 quota (``cpu.cfs_quota_us / cpu.cfs_period_us``), else the
        CPUs in the process affinity mask, rounded down and at least 1.
    """
    try:
        quota, period = Path("/sys/fs/cgroup/cpu.max").read_text().split()[:2]
        if quota != "max":
            return max(1, int(quota) // int(period))
    except (OSError, ValueError):
        pass
    try:
        quota = int(Path("/sys/fs/cgroup/cpu/cpu.cfs_quota_us").read_text())
        period = int(Path("/sys/fs/cgroup/cpu/cpu.cfs_period_us").read_text())
        if quota > 0 and period > 0:
            return max(1, quota // period)
    except (OSError, ValueError):
        pass
    try:
        return max(1, len(os.sched_getaffinity(0)))
    except AttributeError:  # not on Linux
        return max(1, os.cpu_count() or 1)


def _external_master() -> str | None:
    """Return the Spark master set outside ``get_spark``, if any.

    Checked in order: a running session, Spark properties already in the
    JVM (``SparkConf``; under ``spark-submit`` the JVM is joined first), a
    ``--master`` or ``--conf spark.master=...`` in ``PYSPARK_SUBMIT_ARGS``,
    and ``spark.master`` in ``spark-defaults.conf`` (``SPARK_CONF_DIR``, else
    ``$SPARK_HOME/conf``). A master set in any of these places must win
    over the ``local[<quota>]`` default.

    Under ``spark-submit`` without ``--master`` this returns spark-submit's
    own default, ``local[*]``, because the JVM cannot tell it from an
    explicit one. Pass ``--master local[4]`` there.

    Returns
    -------
    str or None
        The master URL, or None if nothing sets it.
    """
    active = SparkSession.getActiveSession()
    if active is not None:
        return active.sparkContext.master
    if os.environ.get("PYSPARK_GATEWAY_PORT"):
        # Started by spark-submit: its JVM already holds the master.
        SparkContext._ensure_initialized()
    master = SparkConf().get("spark.master", None)
    if master:
        return master
    args = shlex.split(os.environ.get("PYSPARK_SUBMIT_ARGS", ""))
    for index, arg in enumerate(args):
        if arg == "--master" and index + 1 < len(args):
            return args[index + 1]
        if arg.startswith("--master="):
            return arg.split("=", 1)[1]
        if arg == "--conf" and index + 1 < len(args):
            key, _, value = args[index + 1].partition("=")
            if key.strip() == "spark.master" and value.strip():
                return value.strip()
    conf_dir = os.environ.get("SPARK_CONF_DIR") or (
        os.path.join(os.environ["SPARK_HOME"], "conf") if os.environ.get("SPARK_HOME") else None
    )
    if conf_dir:
        try:
            text = Path(conf_dir, "spark-defaults.conf").read_text(encoding="utf-8")
        except OSError:
            text = ""
        match = _DEFAULTS_MASTER.search(text)
        if match:
            return match.group(1).strip()
    return None


def get_spark(
    app_name: str = "vinyl-decade",
    shuffle_partitions: int = 200,
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    """Create the Spark session, or return the one already running.

    Parameters
    ----------
    app_name : str
        Name shown in the Spark UI.
    shuffle_partitions : int
        Value for ``spark.sql.shuffle.partitions``. Lower it (for example
        to 16) when working on small aggregated tables on one machine.
    extra_conf : dict of str to str, optional
        Any other Spark settings, for example
        ``{"spark.driver.memory": "8g"}`` or the ``spark-xml`` package
        coordinates on Spark 3 (see ``read_raw_xml``). A
        ``"spark.master"`` key here sets the master. Otherwise a master set
        outside this call is kept (see ``_external_master``), and only if
        there is none the master is ``local[<n>]``, where ``n`` is the
        cgroup CPU quota (``_cpu_quota``).

    Returns
    -------
    pyspark.sql.SparkSession

    Notes
    -----
    If a session is already running it is returned as is; settings such
    as the master cannot be changed on it.
    """
    extra_conf = dict(extra_conf or {})
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
    )
    if "spark.master" not in extra_conf and _external_master() is None:
        builder = builder.master(f"local[{_cpu_quota()}]")
    for key, value in extra_conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def load_schema(entity: str) -> StructType:
    """Load the saved Spark schema of one raw Discogs file.

    Parameters
    ----------
    entity : str
        One of ``"releases"``, ``"masters"``, ``"labels"``, ``"artists"``.

    Returns
    -------
    pyspark.sql.types.StructType
        Schema read from ``schemas/<entity>_schema.json``.

    Raises
    ------
    FileNotFoundError
        If the schema file does not exist.
    """
    path = config.SCHEMA_DIR / f"{entity}_schema.json"
    with open(path, encoding="utf-8") as handle:
        return StructType.fromJson(json.load(handle))


def iter_records(lines: Iterable[bytes], entity: str) -> Iterator[bytes]:
    """Yield the records of a Discogs dump, one ``bytes`` object each.

    Parameters
    ----------
    lines : iterable of bytes
        The plain XML, line by line (for example a pipe from ``gzip -dc``).
    entity : str
        One of ``"releases"``, ``"masters"``, ``"labels"``, ``"artists"``.

    Yields
    ------
    bytes
        One complete ``<{row_tag}>...</{row_tag}>`` element, ending in a
        newline. The preamble (``<?xml ...?>``, the root tags) is left out.

    Notes
    -----
    A record starts at a line beginning with ``<{row_tag} `` or
    ``<{row_tag}>``. Text inside a record is XML-escaped (``&lt;``), so a
    line of record text cannot start with ``<`` and the cut is safe. Most
    records are one line; those with newlines in free text (``notes``,
    ``profile``) span several. Only one record is buffered at a time.

    The preamble may share a line with the first record, and the closing
    root tag may share the last record's last line; both are handled
    (``scripts/test_iter_records.py``). The real Discogs dumps put the root
    tag alone on line 1, with no XML declaration.
    """
    tag = config.ROW_TAGS[entity]
    starts = (f"<{tag} ".encode(), f"<{tag}>".encode())
    root_close = f"</{entity}>".encode()
    record: bytes | list[bytes] | None = None
    finished = False  # past the closing root tag
    for line in lines:
        if finished:
            continue
        if line.startswith(starts):
            if record is not None:
                yield record if isinstance(record, bytes) else b"".join(record)
            record = line
        elif line.startswith(root_close):
            finished = True
        elif record is None:
            # Preamble (XML declaration, root tag). The first record may share
            # its line, e.g. ``<?xml ...?><releases><release id="1">``. The
            # root tag ``<releases>`` cannot match ``<release `` or
            # ``<release>``, so the first match is the record start.
            found = [index for index in (line.find(start) for start in starts) if index >= 0]
            if found:
                record = line[min(found):]
        elif isinstance(record, bytes):
            record = [record, line]
        else:
            record.append(line)
    if record is not None:
        data = record if isinstance(record, bytes) else b"".join(record)
        # The closing root tag can share the last record's last line.
        trimmed = data.rstrip()
        if trimmed.endswith(root_close):
            data = trimmed[: -len(root_close)].rstrip() + b"\n"
        elif not data.endswith(b"\n"):
            data += b"\n"
        yield data


def keep_record(data: bytes, year_window: tuple[int, int]) -> bool:
    """Decide whether the year pre-filter keeps one release record.

    Parameters
    ----------
    data : bytes
        One ``<release>`` element, as yielded by ``iter_records``.
    year_window : tuple of int
        ``(year_min, year_max)``, both inclusive.

    Returns
    -------
    bool
        False only if the record has exactly one ``<released`` tag, it is
        ``<released>`` followed directly by four digits, and that year is
        outside the window. ``tables.scope_releases`` takes the year from
        the same four characters, so it would drop the record too. True
        for everything else, including a missing, empty or unparseable
        date; ``scope_releases`` decides those.
    """
    match = _RELEASED_YEAR.search(data)
    if match is None or year_window[0] <= int(match.group(1)) <= year_window[1]:
        return True
    return data.count(b"<released") != 1


def _stage_params(
    src: str, chunk_bytes: int, year_window: tuple[int, int] | None
) -> dict:
    """Return the settings that make a staged split reusable.

    Parameters
    ----------
    src : str
        Path of the gzipped source file.
    chunk_bytes : int
        Target chunk size.
    year_window : tuple of int, or None
        Pre-filter window.

    Returns
    -------
    dict
        Resolved source path, its size and mtime, ``chunk_bytes`` and
        ``year_window``. A split is reused only if all of them match.
    """
    real = os.path.realpath(src)
    info = os.stat(real)
    return {
        "source": real,
        "source_bytes": info.st_size,
        "source_mtime": int(info.st_mtime),
        "chunk_bytes": chunk_bytes,
        "year_window": list(year_window) if year_window else None,
    }


def stage_path(snapshot: str, entity: str, stage_dir: str | None = None) -> str:
    """Return the folder that holds the plain-XML chunks of one raw file.

    Parameters
    ----------
    snapshot : str
        Snapshot key, ``"2020"`` or ``"2026"``.
    entity : str
        One of ``"releases"``, ``"masters"``, ``"labels"``, ``"artists"``.
    stage_dir : str, optional
        Root of the staging area. Defaults to ``config.STAGE_DIR``.

    Returns
    -------
    str
        For example ``<STAGE_DIR>/20261001/releases``.
    """
    return f"{stage_dir or config.STAGE_DIR}/{config.SNAPSHOTS[snapshot]}/{entity}"


def split_raw_xml(
    snapshot: str,
    entity: str,
    chunk_bytes: int = config.STAGE_CHUNK_BYTES,
    year_window: tuple[int, int] | None = None,
    stage_dir: str | None = None,
    src: str | None = None,
) -> str:
    """Decompress one raw dump into plain-XML chunks Spark can read in parallel.

    The gz is streamed through a ``gzip -dc`` subprocess (faster than
    Python's ``gzip`` module) and cut into records by ``iter_records``,
    one record at a time; the file is never held in memory. A new chunk
    is started at the first record boundary after ``chunk_bytes``, and
    every chunk is wrapped in its own ``<{entity}>...</{entity}>`` root.

    Parameters
    ----------
    snapshot : str
        Snapshot key, ``"2020"`` or ``"2026"``.
    entity : str
        One of ``"releases"``, ``"masters"``, ``"labels"``, ``"artists"``.
    chunk_bytes : int
        Approximate size of one chunk file, in bytes of plain XML.
    year_window : tuple of int, optional
        ``(year_min, year_max)``, both inclusive. Only allowed for
        ``"releases"``. When given, records are pre-filtered on their
        release year (see Notes).
    stage_dir : str, optional
        Root of the staging area. Defaults to ``config.STAGE_DIR``.
    src : str, optional
        Gzipped source file. Defaults to ``config.raw_path(snapshot, entity)``.

    Returns
    -------
    str
        The chunk folder (``stage_path``), holding ``part-NNNNN.xml``,
        ``_STATS.json`` and ``_DONE``.

    Raises
    ------
    ValueError
        If ``year_window`` is given for an entity other than releases, or
        the stage folder is under the read-only raw data.
    RuntimeError
        If ``gzip`` fails.

    Notes
    -----
    **Idempotent.** If ``_DONE`` exists and ``_STATS.json`` was written
    with the same source file (path, size, mtime), ``chunk_bytes`` and
    ``year_window``, the folder is returned unchanged. Otherwise it is
    deleted and rebuilt. ``_STATS.json`` records ``records_seen``,
    ``records_kept``, ``bytes_in`` and ``bytes_out`` (plain XML of the
    records seen and kept), ``chunks`` and ``seconds``.

    **The pre-filter only ever drops records the exact rule drops**
    (``keep_record``). A record is dropped only if it has exactly one
    ``<released`` tag, that tag is ``<released>`` followed directly by
    four digits, and that year is outside ``year_window``.
    ``tables.scope_releases`` takes the year from the same four
    characters and would drop the record too. Every other record is kept,
    including those with a missing, empty, padded or otherwise
    unparseable ``released``, and ``scope_releases`` still applies the
    exact rule afterwards. The set of in-window releases is therefore the
    same with or without the pre-filter.

    Spark ignores files whose names start with ``_``, so the marker files
    do not interfere with reading the folder.
    """
    if year_window is not None and entity != "releases":
        raise ValueError("year_window applies to releases only")
    src = src or config.raw_path(snapshot, entity)
    out = stage_path(snapshot, entity, stage_dir)
    if os.path.realpath(out).startswith(_READ_ONLY_ROOT):
        raise ValueError(f"Refusing to stage under the read-only {_READ_ONLY_ROOT}: {out}")

    params = _stage_params(src, chunk_bytes, year_window)
    done, stats_file = os.path.join(out, "_DONE"), os.path.join(out, "_STATS.json")
    if os.path.exists(done) and os.path.exists(stats_file):
        with open(stats_file, encoding="utf-8") as handle:
            if json.load(handle).get("params") == params:
                return out
    shutil.rmtree(out, ignore_errors=True)
    os.makedirs(out)

    seen = kept = bytes_in = bytes_out = 0
    chunks: list[str] = []
    handle = None
    size = chunk_bytes  # forces a new chunk before the first record
    root_open, root_close = f"<{entity}>\n".encode(), f"</{entity}>\n".encode()

    started = time.perf_counter()
    proc = subprocess.Popen(["gzip", "-dc", src], stdout=subprocess.PIPE, bufsize=1 << 24)
    try:
        for data in iter_records(proc.stdout, entity):
            seen += 1
            bytes_in += len(data)
            if year_window is not None and not keep_record(data, year_window):
                continue
            if size >= chunk_bytes:
                if handle is not None:
                    handle.write(root_close)
                    handle.close()
                name = os.path.join(out, f"part-{len(chunks):05d}.xml")
                chunks.append(name)
                handle = open(name, "wb", buffering=1 << 22)
                handle.write(root_open)
                size = 0
            handle.write(data)
            size += len(data)
            bytes_out += len(data)
            kept += 1
        if handle is None:  # nothing kept: one empty, still valid chunk
            name = os.path.join(out, "part-00000.xml")
            chunks.append(name)
            handle = open(name, "wb")
            handle.write(root_open)
        handle.write(root_close)
        handle.close()
    finally:
        if handle is not None and not handle.closed:
            handle.close()
        proc.stdout.close()
        code = proc.wait()
    if code != 0:
        raise RuntimeError(f"gzip -dc {src} failed with exit code {code}")

    stats = {
        "params": params,
        "records_seen": seen,
        "records_kept": kept,
        "bytes_in": bytes_in,
        "bytes_out": bytes_out,
        "chunks": len(chunks),
        "seconds": round(time.perf_counter() - started, 3),
    }
    with open(stats_file, "w", encoding="utf-8") as handle:
        json.dump(stats, handle, indent=2)
    Path(done).touch()
    return out


def read_stage_stats(chunk_dir: str) -> dict:
    """Read the ``_STATS.json`` written by ``split_raw_xml``.

    Parameters
    ----------
    chunk_dir : str
        Folder returned by ``split_raw_xml``.

    Returns
    -------
    dict
        See the Notes of ``split_raw_xml``.
    """
    with open(os.path.join(chunk_dir, "_STATS.json"), encoding="utf-8") as handle:
        return json.load(handle)


def remove_stage(snapshot: str, entity: str, stage_dir: str | None = None) -> None:
    """Delete the chunk folder of one raw file, if there is one.

    Parameters
    ----------
    snapshot : str
        Snapshot key, ``"2020"`` or ``"2026"``.
    entity : str
        One of ``"releases"``, ``"masters"``, ``"labels"``, ``"artists"``.
    stage_dir : str, optional
        Root of the staging area. Defaults to ``config.STAGE_DIR``.
    """
    out = stage_path(snapshot, entity, stage_dir)
    if not os.path.realpath(out).startswith(_READ_ONLY_ROOT):
        shutil.rmtree(out, ignore_errors=True)


def read_raw_xml(
    spark: SparkSession,
    snapshot: str,
    entity: str,
    path: str | None = None,
    xml_format: str = "xml",
    split: bool = False,
    year_window: tuple[int, int] | None = None,
    drop_fields: tuple[str, ...] = (),
    stage_dir: str | None = None,
    chunk_bytes: int = config.STAGE_CHUNK_BYTES,
) -> DataFrame:
    """Read one gzipped Discogs XML dump with its saved schema.

    This is the slow step (see the module notes). Call it once per file,
    from ``tables.build_all``, and never from an analysis notebook.

    Parameters
    ----------
    spark : SparkSession
    snapshot : str
        Snapshot key, ``"2020"`` or ``"2026"``.
    entity : str
        One of ``"releases"``, ``"masters"``, ``"labels"``, ``"artists"``.
    path : str, optional
        Explicit gz file path. Defaults to ``config.raw_path(snapshot, entity)``.
    xml_format : str
        Data source name. ``"xml"`` works on Spark 4 (built in) and on
        Spark 3 with the ``spark-xml`` package; use
        ``"com.databricks.spark.xml"`` if the short name is not registered.
    split : bool
        If True, first cut the gz into plain-XML chunks with
        ``split_raw_xml`` (reused if already staged) and read the chunks,
        one Spark task per chunk. If False, read the gz with one task.
    year_window : tuple of int, optional
        ``(year_min, year_max)`` for the conservative release-year
        pre-filter of ``split_raw_xml``. Releases only, and only used with
        ``split=True``. It never removes an in-window release, but the
        result still needs ``tables.scope_releases``.
    drop_fields : tuple of str
        Top-level fields removed from the schema before reading, for
        example ``config.RELEASE_DROP_FIELDS``. Spark then skips them.
    stage_dir : str, optional
        Root of the staging area for ``split=True``. Defaults to
        ``config.STAGE_DIR``.
    chunk_bytes : int
        Chunk size for ``split=True``.

    Returns
    -------
    DataFrame
        One row per ``<release>``, ``<master>``, ``<label>`` or
        ``<artist>`` element, with the nested columns of the saved schema
        minus ``drop_fields``. XML attributes carry a ``_`` prefix
        (``_id``); the text of an element that also has attributes is in
        ``_VALUE``.

    Raises
    ------
    ValueError
        If ``drop_fields`` names a field that is not in the schema, or
        ``year_window`` is given without ``split=True``.

    Notes
    -----
    The schemas were saved from the October 2026 files. T02 compared them
    with an inferred read of a 20,000-record head sample of every January
    2020 file. The 2020 files have some fields the saved schema drops:
    ``images`` (all four files), ``_status`` (the ``status`` attribute of
    a release), ``artists.artist.role`` and ``artists.artist.tracks``
    (releases, masters, and in the tracklist), ``extraartists.artist.join``,
    ``companies.company.resource_url`` and ``members.id`` (artists). No
    table uses any of them, so dropping them is harmless. No field had a
    different shape or a risky type, and no saved field was missing.
    """
    if year_window is not None and not split:
        raise ValueError("year_window needs split=True; scope_releases does the exact filter")
    schema = load_schema(entity)
    if drop_fields:
        unknown = [name for name in drop_fields if name not in schema.fieldNames()]
        if unknown:
            raise ValueError(f"drop_fields {unknown} are not top-level fields of {entity}")
        schema = StructType([f for f in schema.fields if f.name not in drop_fields])

    source = path or config.raw_path(snapshot, entity)
    if split:
        chunk_dir = split_raw_xml(snapshot, entity, chunk_bytes, year_window, stage_dir, source)
        source = f"{chunk_dir}/part-*.xml"
    return (
        spark.read.format(xml_format)
        .option("rowTag", config.ROW_TAGS[entity])
        .option("attributePrefix", "_")
        .option("valueTag", "_VALUE")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .load(source)
    )


def write_table(
    df: DataFrame,
    snapshot: str,
    table: str,
    partition_by: str | list[str] | None = None,
    mode: str = "overwrite",
) -> str:
    """Write one normalized table to Parquet.

    Parameters
    ----------
    df : DataFrame
        Table to save.
    snapshot : str
        Snapshot key, ``"2020"`` or ``"2026"``.
    table : str
        Table name; becomes the folder name.
    partition_by : str or list of str, optional
        Column(s) to partition the files by, for example ``"year"``.
    mode : str
        Spark save mode. The default replaces any earlier copy.

    Returns
    -------
    str
        Path the table was written to.
    """
    path = config.parquet_path(snapshot, table)
    writer = df.write.mode(mode)
    if partition_by:
        columns = [partition_by] if isinstance(partition_by, str) else partition_by
        writer = writer.partitionBy(*columns)
    writer.parquet(path)
    return path


def read_table(spark: SparkSession, snapshot: str, table: str) -> DataFrame:
    """Read one normalized Parquet table written by ``write_table``.

    Parameters
    ----------
    spark : SparkSession
    snapshot : str
        Snapshot key, ``"2020"`` or ``"2026"``.
    table : str
        Table name, for example ``"release_styles"``.

    Returns
    -------
    DataFrame

    Raises
    ------
    pyspark.errors.AnalysisException
        If the table has not been built yet. Run
        ``explorers/01_ingest_and_parse.ipynb`` first.
    """
    return spark.read.parquet(config.parquet_path(snapshot, table))
