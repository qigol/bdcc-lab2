"""Project-wide constants: where the data is and what the study covers.

Nothing in here touches Spark, so it is safe to import anywhere.

Set the environment variable ``DISCOGS_DATA_DIR`` to point at the folder
that holds the dumps if they are not in ``<project>/data``. Expected
layout::

    <DISCOGS_DATA_DIR>/
        raw/discogs_20200101_releases.xml.gz   (and artists, labels, masters)
        raw/discogs_20261001_releases.xml.gz   (and artists, labels, masters)
        parquet/<snapshot>/<table>/            (written by tables.build_all)
"""

from __future__ import annotations

import getpass
import os
from pathlib import Path

# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------

#: Folder that contains ``main.ipynb``.
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent

#: Root of all data. Override with the ``DISCOGS_DATA_DIR`` env variable.
#: Kept as a string so that non-local URIs (``s3a://``, ``hdfs://``) work.
DATA_DIR: str = os.environ.get("DISCOGS_DATA_DIR", str(PROJECT_ROOT / "data"))

#: Gzipped XML dumps exactly as downloaded from Discogs.
RAW_DIR: str = f"{DATA_DIR}/raw"

#: Normalized Parquet tables, one sub-folder per snapshot.
PARQUET_DIR: str = f"{DATA_DIR}/parquet"

#: Scratch space for the plain-XML chunks cut by ``ingest.split_raw_xml``.
#: Local disk, outside the repo; deleted after the Parquet write. Override
#: with the ``DISCOGS_STAGE_DIR`` env variable.
STAGE_DIR: str = os.environ.get(
    "DISCOGS_STAGE_DIR", f"/tmp/{os.environ.get('USER') or getpass.getuser()}/discogs_stage"
)

#: Target size of one staged chunk, in bytes of plain XML. One chunk is
#: parsed by one Spark task.
STAGE_CHUNK_BYTES: int = 256 * 1024**2

#: Spark schemas (JSON) for the four raw files, saved from a previous read.
SCHEMA_DIR: Path = PROJECT_ROOT / "schemas"

# --------------------------------------------------------------------------
# Snapshots and entities
# --------------------------------------------------------------------------

#: Snapshot key -> date stamp used in the Discogs file names.
SNAPSHOTS: dict[str, str] = {
    "2020": "20200101",  # pre-pandemic comparison snapshot
    "2026": "20261001",  # main analysis snapshot
}

#: Snapshot used for every analysis that is not a snapshot comparison.
MAIN_SNAPSHOT: str = "2026"

#: Snapshot used only for the before/after comparison.
BASE_SNAPSHOT: str = "2020"

#: Entity (file) name -> XML tag that marks one row in that file.
ROW_TAGS: dict[str, str] = {
    "releases": "release",
    "masters": "master",
    "labels": "label",
    "artists": "artist",
}

#: Snapshot key -> tables ``tables.build_all`` builds when no list is given.
#: The 2020 snapshot only feeds the snapshot comparison (``snapshots.py``),
#: which reads these three tables. Snapshots not listed get every table.
LEAN_SNAPSHOT_TABLES: dict[str, list[str]] = {
    "2020": ["releases", "release_labels", "labels"],
}

#: Top-level release fields no table uses. ``tables.build_all`` drops them
#: from the read schema. ``tracklist`` is kept for track-level credits.
RELEASE_DROP_FIELDS: tuple[str, ...] = ("videos", "identifiers", "notes", "series")

# --------------------------------------------------------------------------
# Analytical scope
# --------------------------------------------------------------------------

#: First and last release year kept in the analysis (both inclusive).
YEAR_MIN: int = 2016
YEAR_MAX: int = 2026

#: Root labels of the three majors, matched on the ``name`` field of the
#: labels file. TODO: confirm these names (or swap in label ids) against
#: the labels table once it is loaded.
MAJOR_ROOTS: dict[str, str] = {
    "Universal": "Universal Music Group",
    "Sony": "Sony Music Entertainment",
    "Warner": "Warner Music Group",
}

#: Format description tags that mark a release as a reissue of older music.
REISSUE_TAGS: tuple[str, ...] = ("Reissue", "Repress", "Remastered")

#: Credit roles counted as "producer / engineer" for the credit network.
#: TODO: review against the most common roles found in the credits table.
PRODUCTION_ROLES: tuple[str, ...] = (
    "Producer",
    "Co-producer",
    "Executive-Producer",
    "Engineer",
    "Mixed By",
    "Mastered By",
    "Recorded By",
)

#: Credit roles counted as "writer" when profiling a label's output.
WRITING_ROLES: tuple[str, ...] = (
    "Written-By",
    "Written By",
    "Composed By",
    "Lyrics By",
    "Music By",
    "Songwriter",
)


def raw_path(snapshot: str, entity: str) -> str:
    """Return the path of one raw gzipped XML dump.

    Parameters
    ----------
    snapshot : str
        Snapshot key, ``"2020"`` or ``"2026"`` (see ``SNAPSHOTS``).
    entity : str
        One of ``"releases"``, ``"masters"``, ``"labels"``, ``"artists"``.

    Returns
    -------
    str
        For example ``<DATA_DIR>/raw/discogs_20261001_releases.xml.gz``.

    Raises
    ------
    KeyError
        If the snapshot or the entity is not one of the known names.
    """
    if entity not in ROW_TAGS:
        raise KeyError(f"Unknown entity {entity!r}; expected one of {list(ROW_TAGS)}")
    return f"{RAW_DIR}/discogs_{SNAPSHOTS[snapshot]}_{entity}.xml.gz"


def parquet_path(snapshot: str, table: str) -> str:
    """Return the folder of one normalized Parquet table.

    Parameters
    ----------
    snapshot : str
        Snapshot key, ``"2020"`` or ``"2026"``.
    table : str
        Table name, for example ``"release_styles"`` (see ``tables.TABLES``).

    Returns
    -------
    str
        For example ``<DATA_DIR>/parquet/20261001/release_styles``.
    """
    return f"{PARQUET_DIR}/{SNAPSHOTS[snapshot]}/{table}"
