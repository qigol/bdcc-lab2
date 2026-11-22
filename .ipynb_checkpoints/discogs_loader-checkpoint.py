"""
discogs_loader.py — load the Discogs XML dumps as Spark DataFrames, via Parquet.

The first time a dump is loaded, each .xml.gz file is read (inferring a schema
if none is saved yet) and written to Parquet. Every later call reads the
Parquet copy directly, which is fast and parallel.

Usage in a notebook:

    from discogs_loader import get_spark, load_all

    spark = get_spark()
    data = load_all(spark)

    releases_2026 = data["2026"]["releases"]
    artists_2020  = data["2020"]["artists"]
"""

import json
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

DUMPS = {
    "2026": ("/mnt/data/public/discogs/2026", "20261101"),
    "2020": ("/mnt/data/public/discogs/2020", "20200101"),
}
OUT_DIR = Path("discogs/parquet")
SCHEMA_DIR = Path("discogs/schemas")

# file name part -> XML element that represents one row
ROW_TAGS = {
    "artists": "artist",
    "labels": "label",
    "masters": "master",
    "releases": "release",
}


# --------------------------------------------------------------------------- #
# Spark
# --------------------------------------------------------------------------- #
def get_spark(driver_memory: str = "8g") -> SparkSession:
    """Start (or reuse) a local Spark session with XML support."""
    import pyspark

    builder = (
        SparkSession.builder.master("local[*]")
        .appName("discogs")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    )
    # Spark 4+ has XML built in; Spark 3.x needs the spark-xml package.
    if int(pyspark.__version__.split(".")[0]) < 4:
        builder = builder.config(
            "spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0"
        )
    return builder.getOrCreate()


# --------------------------------------------------------------------------- #
# Schemas
# --------------------------------------------------------------------------- #
def _schema_path(schema_dir: Path, name: str, date: str, share_schema: bool) -> Path:
    # Shared:     artists_schema.json            (one schema used for every dump)
    # Per dump:   artists_20200101_schema.json
    suffix = "" if share_schema else f"_{date}"
    return schema_dir / f"{name}{suffix}_schema.json"


def _load_schema(path: Path):
    if not path.exists():
        return None
    return StructType.fromJson(json.loads(path.read_text()))


def _save_schema(path: Path, schema: StructType) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(json.loads(schema.json()), indent=2))


# --------------------------------------------------------------------------- #
# XML -> Parquet
# --------------------------------------------------------------------------- #
def read_xml(
    spark: SparkSession,
    dump_dir: str,
    date: str,
    name: str,
    schema_dir: Path = SCHEMA_DIR,
    share_schema: bool = True,
    sampling_ratio: float = 0.05,
    force_infer: bool = False,
) -> DataFrame:
    """Read one raw .xml.gz dump file, reusing a saved schema if there is one."""
    src = Path(dump_dir) / f"discogs_{date}_{name}.xml.gz"
    if not src.exists():
        raise FileNotFoundError(src)

    path = _schema_path(Path(schema_dir), name, date, share_schema)
    schema = None if force_infer else _load_schema(path)
    reader = spark.read.format("xml").option("rowTag", ROW_TAGS[name])

    t0 = time.time()
    if schema is not None:
        print(f"[{date} {name}] reading XML with saved schema {path.name}")
        return reader.schema(schema).load(str(src))

    print(f"[{date} {name}] inferring schema (samplingRatio={sampling_ratio}) ...")
    df = reader.option("samplingRatio", sampling_ratio).load(str(src))
    _save_schema(path, df.schema)
    print(f"[{date} {name}] schema saved to {path} ({time.time() - t0:.0f}s)")
    return df


def _parquet_ready(path: Path) -> bool:
    # Spark writes _SUCCESS only when a write finishes; a crashed write won't have it.
    return (path / "_SUCCESS").exists()


def load_table(
    spark: SparkSession,
    dump_dir: str,
    date: str,
    name: str,
    out_dir: Path = OUT_DIR,
    partitions: int = None,
    max_records_per_file: int = 500_000,
    rebuild: bool = False,
    **xml_kwargs,
) -> DataFrame:
    """Return one table as a DataFrame, converting XML to Parquet first if needed.

    By default there is no repartition: a .gz file is one partition anyway, and
    repartitioning it means shuffling the whole decompressed dump through local
    disk and memory. Instead the output is split into files of at most
    `max_records_per_file` rows. Pass `partitions=N` to force a repartition.
    """
    dst = Path(out_dir) / date / name

    if rebuild or not _parquet_ready(dst):
        df = read_xml(spark, dump_dir, date, name, **xml_kwargs)
        if partitions:
            df = df.repartition(partitions)
        t0 = time.time()
        print(f"[{date} {name}] writing Parquet to {dst} ...")
        (df.write.mode("overwrite")
           .option("maxRecordsPerFile", max_records_per_file)
           .parquet(str(dst)))
        print(f"[{date} {name}] done in {time.time() - t0:.0f}s")

    return spark.read.parquet(str(dst))


def load_dump(
    spark: SparkSession,
    dump_dir: str,
    date: str,
    names=None,
    **kwargs,
) -> dict:
    """Load all (or some) tables of one dump: {"artists": df, "labels": df, ...}."""
    return {n: load_table(spark, dump_dir, date, n, **kwargs) for n in (names or ROW_TAGS)}


def load_all(
    spark: SparkSession,
    dumps: dict = None,
    names=None,
    **kwargs,
) -> dict:
    """
    Load every dump in `dumps` (default: 2026 and 2020).

    Returns {"2026": {"artists": df, ...}, "2020": {"artists": df, ...}}.

    Keyword options passed through:
        names           list of tables, e.g. ["artists", "labels"] (default: all four)
        out_dir         where Parquet goes (default: discogs/parquet)
        schema_dir      where schema JSON goes (default: discogs/schemas)
        share_schema    True = one schema per table reused for every dump (default)
                        False = a separate inferred schema per dump
        rebuild         True = redo the XML -> Parquet conversion
        force_infer     True = ignore saved schemas and infer again
        partitions      force a repartition before writing (default: none)
        max_records_per_file  rows per Parquet file (default: 500,000)
        sampling_ratio  fraction of records used for inference (default: 0.05)
    """
    dumps = dumps or DUMPS
    return {
        key: load_dump(spark, dump_dir, date, names=names, **kwargs)
        for key, (dump_dir, date) in dumps.items()
    }