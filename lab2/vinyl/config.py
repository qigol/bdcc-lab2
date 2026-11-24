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
#: The 2020 snapshot feeds the snapshot comparison (``snapshots.py``), which
#: reads releases, release_labels and labels. Its artists and masters are
#: also built so that all four dump files of both snapshots are parsed, as
#: the gameplan states. The other release child tables are skipped for 2020.
#: Snapshots not listed get every table.
LEAN_SNAPSHOT_TABLES: dict[str, list[str]] = {
    "2020": ["releases", "release_labels", "labels", "artists", "masters"],
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

#: Root label id of each major's tree in the labels file (checked in T04
#: against the 2026 labels table). Universal = Universal Music Group
#: (38404), Sony = Sony Corporation (353657, the top of the Sony Music
#: Entertainment chain), Warner = Warner Music Group (2345). Universal and
#: Warner are matched on ids because both sit under the unnamed label 212.
MAJOR_ROOT_IDS: dict[str, int] = {
    "Universal": 38404,
    "Sony": 353657,
    "Warner": 2345,
}

#: Label ids that are placeholder parents, not real owners. Label 212 has
#: no name and holds 32,175 unrelated labels in 2026, including Universal
#: and Warner. ``labels.resolve_label_roots`` treats a parent id listed
#: here as "no parent", so each group below it becomes its own root.
PLACEHOLDER_LABEL_IDS: tuple[int, ...] = (212,)

#: Label ids that Discogs uses as a "released by the artist" placeholder.
#: Label 1818, "Not On Label", holds 4-6% of each year's in-scope releases
#: in 2026 (24,562 in 2016, 11,613 in 2025). It is not a company. Its
#: ``parent_id`` is 205754 (Stolen Riff Records), whose parent is 212; that
#: link is a data-entry error, and 1818 has no child labels.
#: ``labels.parent_coverage`` reports these labels in their own class.
SELF_RELEASED_LABEL_IDS: tuple[int, ...] = (1818,)

#: Regular expression (Java syntax, as used by ``Column.rlike``) for the
#: names of the other self-release placeholders, for example "Not On Label
#: (X Self-released)" (id 3550708). ``\b`` keeps it from matching a name
#: such as "Not On Labelle". ``labels.self_released_label_ids`` combines it
#: with ``SELF_RELEASED_LABEL_IDS``.
SELF_RELEASED_LABEL_PATTERN: str = r"(?i)^not on label\b"

#: An (artist, year) pair with at least this many releases counts as
#: prolific. In 2026, 35 artist-years reach 300 releases (16,346 releases,
#: 0.4% of 3.83 M). They are real artists, for example the noise project
#: 6351861 with 1,233 releases in 2019, and they barely move the totals.
#: They do distort niche styles: that project alone is 22% of the 2019
#: "Harsh Noise Wall" releases. Used by ``trends.prolific_artist_years``.
PROLIFIC_ARTIST_MIN_PER_YEAR: int = 300

#: Last release year that is complete in the main snapshot. The dump is
#: from 1 October 2026, so 2026 is a partial year.
LAST_FULL_YEAR: int = 2025

#: Year -> expected share of a full year's releases that fall in the
#: months the dump covers. For 2026 that is January-September: the mean
#: share of month-dated releases dated Jan-Sep in 2016-2025 (range
#: 0.728-0.747, checked in T04). Used by ``snapshots.estimate_undercount``.
PARTIAL_YEAR_SHARE: dict[int, float] = {2026: 0.739}

#: Format description tags that mark a release as a reissue of older music.
REISSUE_TAGS: tuple[str, ...] = ("Reissue", "Repress", "Remastered")

#: Credit roles counted as "producer / engineer" for the credit network.
#: Checked against the top credit roles in T04; mastering roles "Remastered By" and "Lacquer Cut By" added.
PRODUCTION_ROLES: tuple[str, ...] = (
    "Producer",
    "Co-producer",
    "Executive-Producer",
    "Engineer",
    "Mixed By",
    "Mastered By",
    "Recorded By",
    "Remastered By",
    "Lacquer Cut By",
)

#: Edge-weight cut-off (minimum shared releases) for each RQ4 graph, so that
#: it fits on the driver. Measured on 2026 (nodes / edges): collaboration
#: at 2 is 114 K / 152 K, credit at 3 is 209 K / 334 K, artist-label at 3
#: is 350 K / 386 K. Keys: ``"collaboration"``, ``"credit"``,
#: ``"artist_label"``. Used by ``explorers/06_networks_and_communities``.
GRAPH_MIN_WEIGHT: dict[str, int] = {
    "collaboration": 2,
    "credit": 3,
    "artist_label": 3,
}

#: Smallest group of artists that HDBSCAN accepts as a cluster in the
#: artist clustering (``clustering.cluster_hdbscan``). The sweep in
#: ``explorers/07_artist_clustering`` compares 25, 50, 100 and 200
#: (2026, 194,105 artists): 374 / 238 / 149 / 99 clusters with 24% / 20% /
#: 13% / 8% noise; the largest cluster stays near 6.7% from 50 up. 100
#: leaves far fewer artists unassigned than 50 while keeping style-level
#: clusters; 200 starts merging them.
HDBSCAN_MIN_CLUSTER_SIZE: int = 100

#: ``n_neighbors`` of the UMAP runs (``clustering.reduce_umap``) in the
#: artist clustering: how many neighbours define the local structure.
#: Same as the ``reduce_umap`` default, which explorer 07 used.
UMAP_N_NEIGHBORS: int = 30

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
