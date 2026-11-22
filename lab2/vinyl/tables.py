"""Turn the nested raw XML rows into flat, normalized Parquet tables.

Flow (run once per snapshot by ``build_all``)::

    releases.xml.gz --scope_releases--> releases_staged (nested Parquet)
                                            |
        +---------+---------+---------+-----+-----+---------+---------+
        v         v         v         v           v         v         v
    releases  release_  release_  release_    release_  release_  release_
              genres    styles    formats     labels    artists   credits ...

    labels.xml.gz  --> labels      artists.xml.gz --> artists
    masters.xml.gz --> masters

The gzipped releases file is read only once. It is first saved as nested
Parquet (``releases_staged``); every child table is then derived from that
copy, so the slow single-core XML read is never repeated.

Every ``build_*`` function is a pure DataFrame -> DataFrame transformation
with no I/O, so each can be tested on a handful of rows.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import config, ingest

#: Table name -> one-line description. Also used as the build order.
TABLES: dict[str, str] = {
    "releases": "One row per release: title, country, year, master link.",
    "release_genres": "One row per (release, genre).",
    "release_styles": "One row per (release, style).",
    "release_formats": "One row per (release, format) with a reissue flag.",
    "release_labels": "One row per (release, label) with catalog number.",
    "release_artists": "One row per (release, main artist).",
    "release_credits": "One row per (release, credited person, role).",
    "release_companies": "One row per (release, company), e.g. pressing plants.",
    "labels": "One row per label with its parent label.",
    "artists": "One row per artist.",
    "masters": "One row per master release with its original year.",
}


# --------------------------------------------------------------------------
# Releases
# --------------------------------------------------------------------------

def scope_releases(
    raw_releases: DataFrame,
    year_min: int = config.YEAR_MIN,
    year_max: int = config.YEAR_MAX,
) -> DataFrame:
    """Keep the releases inside the study window and add id and year.

    Parameters
    ----------
    raw_releases : DataFrame
        Output of ``ingest.read_raw_xml(spark, snapshot, "releases")``.
    year_min, year_max : int
        First and last release year to keep, both inclusive.

    Returns
    -------
    DataFrame
        The raw columns plus:

        * ``release_id`` (long) - copy of the ``_id`` attribute.
        * ``year`` (int) - first four digits of ``released``.

    Notes
    -----
    ``released`` is free text in Discogs (``"2019-03-00"``, ``"2019"``,
    or empty). Rows whose first four characters are not a year in the
    window are dropped, which includes all undated releases.
    """
    digits = F.regexp_extract(F.col("released"), r"^(\d{4})", 1)
    # No match gives "", which must become null before the cast: Spark 4
    # (ANSI mode) raises an error on cast("" as int) instead of returning null.
    year = F.when(digits != "", digits).cast("int")
    return (
        raw_releases.withColumn("release_id", F.col("_id"))
        .withColumn("year", year)
        .where(F.col("year").between(year_min, year_max))
    )


def build_releases(staged: DataFrame) -> DataFrame:
    """Build the flat releases table.

    Parameters
    ----------
    staged : DataFrame
        Output of ``scope_releases``.

    Returns
    -------
    DataFrame
        One row per release with columns ``release_id``, ``title``,
        ``country``, ``released``, ``year``, ``master_id``,
        ``is_main_release``, ``data_quality``.
    """
    return staged.select(
        "release_id",
        "title",
        "country",
        "released",
        "year",
        F.col("master_id._VALUE").alias("master_id"),
        F.col("master_id._is_main_release").alias("is_main_release"),
        "data_quality",
    )


def build_release_genres(staged: DataFrame) -> DataFrame:
    """Build one row per (release, genre).

    Parameters
    ----------
    staged : DataFrame
        Output of ``scope_releases``.

    Returns
    -------
    DataFrame
        Columns ``release_id``, ``year``, ``genre``. A release with no
        genre has no row.
    """
    return staged.select(
        "release_id", "year", F.explode("genres.genre").alias("genre")
    ).where(F.col("genre").isNotNull())


def build_release_styles(staged: DataFrame) -> DataFrame:
    """Build one row per (release, style).

    Parameters
    ----------
    staged : DataFrame
        Output of ``scope_releases``.

    Returns
    -------
    DataFrame
        Columns ``release_id``, ``year``, ``style``. A release with no
        style has no row.
    """
    return staged.select(
        "release_id", "year", F.explode("styles.style").alias("style")
    ).where(F.col("style").isNotNull())


def build_release_formats(
    staged: DataFrame,
    reissue_tags: tuple[str, ...] = config.REISSUE_TAGS,
) -> DataFrame:
    """Build one row per (release, format) and flag reissues.

    Parameters
    ----------
    staged : DataFrame
        Output of ``scope_releases``.
    reissue_tags : tuple of str
        Format descriptions that mark a reissue.

    Returns
    -------
    DataFrame
        Columns:

        * ``release_id``, ``year``
        * ``format_name`` - e.g. ``"Vinyl"``, ``"CD"``, ``"Cassette"``,
          ``"File"``.
        * ``qty`` - number of units (two LPs -> 2).
        * ``text`` - free-text note such as ``"Red"``.
        * ``descriptions`` - array such as ``["LP", "Album", "Reissue"]``.
        * ``is_reissue`` - True if any description is in ``reissue_tags``.

    Notes
    -----
    A release can list several formats (LP plus CD), so ``release_id`` is
    not unique here. Count distinct releases, not rows.
    """
    fmt = F.col("format")
    tags = F.array(*[F.lit(tag) for tag in reissue_tags])
    return staged.select(
        "release_id", "year", F.explode("formats.format").alias("format")
    ).select(
        "release_id",
        "year",
        fmt["_name"].alias("format_name"),
        fmt["_qty"].alias("qty"),
        fmt["_text"].alias("text"),
        fmt["descriptions"]["description"].alias("descriptions"),
        F.coalesce(
            F.arrays_overlap(fmt["descriptions"]["description"], tags),
            F.lit(False),
        ).alias("is_reissue"),
    )


def build_release_labels(staged: DataFrame) -> DataFrame:
    """Build one row per (release, label).

    Parameters
    ----------
    staged : DataFrame
        Output of ``scope_releases``.

    Returns
    -------
    DataFrame
        Columns ``release_id``, ``year``, ``label_id``, ``label_name``,
        ``catno``. ``label_id`` joins to ``labels.label_id``.
    """
    label = F.col("label")
    return staged.select(
        "release_id", "year", F.explode("labels.label").alias("label")
    ).select(
        "release_id",
        "year",
        label["_id"].alias("label_id"),
        label["_name"].alias("label_name"),
        label["_catno"].alias("catno"),
    )


def build_release_artists(staged: DataFrame) -> DataFrame:
    """Build one row per (release, main artist).

    Parameters
    ----------
    staged : DataFrame
        Output of ``scope_releases``.

    Returns
    -------
    DataFrame
        Columns ``release_id``, ``year``, ``artist_id``, ``artist_name``.

    Notes
    -----
    Compilations are credited to the placeholder artist "Various"
    (id 194 in Discogs). It is kept here; drop it before building
    networks or it becomes one giant hub.
    """
    artist = F.col("artist")
    return staged.select(
        "release_id", "year", F.explode("artists.artist").alias("artist")
    ).select(
        "release_id",
        "year",
        artist["id"].alias("artist_id"),
        artist["name"].alias("artist_name"),
    )


def build_release_credits(staged: DataFrame) -> DataFrame:
    """Build one row per (release, credited person, role).

    Parameters
    ----------
    staged : DataFrame
        Output of ``scope_releases``.

    Returns
    -------
    DataFrame
        Columns ``release_id``, ``year``, ``artist_id``, ``artist_name``,
        ``role``.

    Notes
    -----
    Discogs stores roles as one string, for example
    ``"Producer, Mixed By [Mix]"``. Bracketed detail is removed and the
    string is split on commas, giving one row per role
    (``"Producer"`` and ``"Mixed By"``).

    Only release-level credits (``extraartists``) are used. TODO: decide
    whether to add track-level credits (``tracklist.track.extraartists``).
    """
    artist = F.col("artist")
    without_brackets = F.regexp_replace(artist["role"], r"\s*\[[^\]]*\]", "")
    return (
        staged.select(
            "release_id", "year", F.explode("extraartists.artist").alias("artist")
        )
        .select(
            "release_id",
            "year",
            artist["id"].alias("artist_id"),
            artist["name"].alias("artist_name"),
            F.explode(F.split(without_brackets, r"\s*,\s*")).alias("role"),
        )
        .withColumn("role", F.trim("role"))
        .where(F.col("role") != "")
    )


def build_release_companies(staged: DataFrame) -> DataFrame:
    """Build one row per (release, company).

    Parameters
    ----------
    staged : DataFrame
        Output of ``scope_releases``.

    Returns
    -------
    DataFrame
        Columns ``release_id``, ``year``, ``company_id``,
        ``company_name``, ``entity_type``, ``entity_type_name`` (for
        example ``"Pressed By"`` or ``"Distributed By"``), ``catno``.
    """
    company = F.col("company")
    return staged.select(
        "release_id", "year", F.explode("companies.company").alias("company")
    ).select(
        "release_id",
        "year",
        company["id"].alias("company_id"),
        company["name"].alias("company_name"),
        company["entity_type"].alias("entity_type"),
        company["entity_type_name"].alias("entity_type_name"),
        company["catno"].alias("catno"),
    )


# --------------------------------------------------------------------------
# Labels, artists, masters
# --------------------------------------------------------------------------

def build_labels(raw_labels: DataFrame) -> DataFrame:
    """Build the flat labels table.

    Parameters
    ----------
    raw_labels : DataFrame
        Output of ``ingest.read_raw_xml(spark, snapshot, "labels")``.

    Returns
    -------
    DataFrame
        Columns ``label_id``, ``label_name``, ``parent_id``,
        ``parent_name``, ``data_quality``. ``parent_id`` is null for a
        label with no parent (a root of a label tree).
    """
    return raw_labels.select(
        F.col("id").alias("label_id"),
        F.col("name").alias("label_name"),
        F.col("parentLabel._id").alias("parent_id"),
        F.col("parentLabel._VALUE").alias("parent_name"),
        "data_quality",
    )


def build_artists(raw_artists: DataFrame) -> DataFrame:
    """Build the flat artists table.

    Parameters
    ----------
    raw_artists : DataFrame
        Output of ``ingest.read_raw_xml(spark, snapshot, "artists")``.

    Returns
    -------
    DataFrame
        Columns ``artist_id``, ``artist_name``, ``realname``,
        ``n_members`` (0 for a solo artist), ``n_groups``, ``n_aliases``,
        ``data_quality``.
    """
    def count_of(column: str):
        return F.coalesce(F.size(F.col(column)), F.lit(0))

    return raw_artists.select(
        F.col("id").alias("artist_id"),
        F.col("name").alias("artist_name"),
        "realname",
        F.greatest(count_of("members.name"), F.lit(0)).alias("n_members"),
        F.greatest(count_of("groups.name"), F.lit(0)).alias("n_groups"),
        F.greatest(count_of("aliases.name"), F.lit(0)).alias("n_aliases"),
        "data_quality",
    )


def build_masters(raw_masters: DataFrame) -> DataFrame:
    """Build the flat masters table.

    Parameters
    ----------
    raw_masters : DataFrame
        Output of ``ingest.read_raw_xml(spark, snapshot, "masters")``.

    Returns
    -------
    DataFrame
        Columns ``master_id``, ``title``, ``main_release``,
        ``master_year``. ``master_year`` is the year of the earliest
        version, which is what makes a later pressing a reissue. Discogs
        uses 0 for "unknown"; it is converted to null here.
    """
    return raw_masters.select(
        F.col("_id").alias("master_id"),
        "title",
        "main_release",
        F.when(F.col("year") > 0, F.col("year")).cast("int").alias("master_year"),
    )


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------

#: Child tables derived from the staged releases: name -> (builder, partition).
_RELEASE_BUILDERS = {
    "releases": (build_releases, None),
    "release_genres": (build_release_genres, None),
    "release_styles": (build_release_styles, None),
    "release_formats": (build_release_formats, None),
    "release_labels": (build_release_labels, None),
    "release_artists": (build_release_artists, None),
    "release_credits": (build_release_credits, None),
    "release_companies": (build_release_companies, None),
}

#: Tables built straight from one of the three small raw files.
_ENTITY_BUILDERS = {
    "labels": build_labels,
    "artists": build_artists,
    "masters": build_masters,
}


def stage_releases(
    spark: SparkSession,
    snapshot: str,
    year_min: int = config.YEAR_MIN,
    year_max: int = config.YEAR_MAX,
) -> DataFrame:
    """Read the raw releases once and save the in-scope rows as Parquet.

    This is the only function that reads the big gzipped releases file.

    Parameters
    ----------
    spark : SparkSession
    snapshot : str
        Snapshot key, ``"2020"`` or ``"2026"``.
    year_min, year_max : int
        Study window, both inclusive.

    Returns
    -------
    DataFrame
        The staged table read back from Parquet (nested columns intact,
        plus ``release_id`` and ``year``).
    """
    raw = ingest.read_raw_xml(spark, snapshot, "releases")
    staged = scope_releases(raw, year_min, year_max)
    ingest.write_table(staged, snapshot, "releases_staged")
    return ingest.read_table(spark, snapshot, "releases_staged")


def build_all(
    spark: SparkSession,
    snapshot: str,
    tables: list[str] | None = None,
    restage: bool = False,
) -> dict[str, str]:
    """Build and save the normalized tables of one snapshot.

    Parameters
    ----------
    spark : SparkSession
    snapshot : str
        Snapshot key, ``"2020"`` or ``"2026"``.
    tables : list of str, optional
        Names from ``TABLES`` to build. Defaults to all of them.
    restage : bool
        If False (default), reuse ``releases_staged`` when it already
        exists, so a failed or partial run does not re-read the XML. Set
        to True after changing the year window.

    Returns
    -------
    dict of str to str
        Table name -> path written.

    Raises
    ------
    KeyError
        If ``tables`` contains a name that is not in ``TABLES``.
    """
    wanted = list(TABLES) if tables is None else tables
    unknown = [name for name in wanted if name not in TABLES]
    if unknown:
        raise KeyError(f"Unknown table(s) {unknown}; expected names from {list(TABLES)}")

    written: dict[str, str] = {}

    release_tables = [name for name in wanted if name in _RELEASE_BUILDERS]
    if release_tables:
        staged = None
        if not restage:
            try:
                staged = ingest.read_table(spark, snapshot, "releases_staged")
            except Exception:  # not staged yet
                staged = None
        if staged is None:
            staged = stage_releases(spark, snapshot)
        for name in release_tables:
            builder, partition = _RELEASE_BUILDERS[name]
            written[name] = ingest.write_table(builder(staged), snapshot, name, partition)

    for name in wanted:
        if name in _ENTITY_BUILDERS:
            raw = ingest.read_raw_xml(spark, snapshot, name)
            written[name] = ingest.write_table(_ENTITY_BUILDERS[name](raw), snapshot, name)

    return written
