"""Spark session, raw XML reading, and Parquet I/O.

Why loading is slow, and what this module does about it
-------------------------------------------------------
1. **Schema inference.** Without a schema, Spark reads the whole file once
   just to work out the columns, then again to load it. ``read_raw_xml``
   passes the saved schemas in ``schemas/`` so that first pass is skipped.
2. **Gzip cannot be split.** A ``.xml.gz`` file is always read by a single
   task on a single core, however big the cluster is. This cannot be
   fixed at read time, so the raw files should be read exactly once and
   written to Parquet (``tables.build_all``). Everything after that reads
   Parquet, which is columnar, splittable, and fast.
"""

from __future__ import annotations

import json

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from . import config


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
        coordinates on Spark 3 (see ``read_raw_xml``).

    Returns
    -------
    pyspark.sql.SparkSession
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
    )
    for key, value in (extra_conf or {}).items():
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


def read_raw_xml(
    spark: SparkSession,
    snapshot: str,
    entity: str,
    path: str | None = None,
    xml_format: str = "xml",
) -> DataFrame:
    """Read one gzipped Discogs XML dump with its saved schema.

    This is the slow step (one core per file, see the module notes). Call
    it once per file, from ``tables.build_all``, and never from an
    analysis notebook.

    Parameters
    ----------
    spark : SparkSession
    snapshot : str
        Snapshot key, ``"2020"`` or ``"2026"``.
    entity : str
        One of ``"releases"``, ``"masters"``, ``"labels"``, ``"artists"``.
    path : str, optional
        Explicit file path. Defaults to ``config.raw_path(snapshot, entity)``.
    xml_format : str
        Data source name. ``"xml"`` works on Spark 4 (built in) and on
        Spark 3 with the ``spark-xml`` package; use
        ``"com.databricks.spark.xml"`` if the short name is not registered.

    Returns
    -------
    DataFrame
        One row per ``<release>``, ``<master>``, ``<label>`` or
        ``<artist>`` element, with the nested columns of the saved schema.
        XML attributes carry a ``_`` prefix (``_id``); the text of an
        element that also has attributes is in ``_VALUE``.

    Notes
    -----
    The schemas were saved from the October 2026 files. A field that only
    exists in the January 2020 files would be silently dropped, and a
    field missing from them comes back as null. TODO: confirm with one
    inferred read of a small 2020 sample.
    """
    return (
        spark.read.format(xml_format)
        .option("rowTag", config.ROW_TAGS[entity])
        .option("attributePrefix", "_")
        .option("valueTag", "_VALUE")
        .option("mode", "PERMISSIVE")
        .schema(load_schema(entity))
        .load(path or config.raw_path(snapshot, entity))
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
