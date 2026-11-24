"""RQ2 - How did the format mix change, and how much of the vinyl boom was reissues?

Two definitions of "reissue" are available and should be compared in the
explorer notebook before one is chosen for the report:

* **Tag-based** - the format descriptions include Reissue, Repress or
  Remastered (``release_formats.is_reissue``).
* **Master-based** - the release year is later than the year of its
  master release (``add_master_reissue_flag``).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F

from . import config

#: Default lower edges (in years) of the reissue-age bins. The last bin is
#: open-ended.
AGE_BINS: tuple[int, ...] = (1, 2, 5, 10, 20, 30, 40, 50)


def format_shares_by_year(release_formats: DataFrame) -> DataFrame:
    """Compute each format's share of releases in every year.

    Parameters
    ----------
    release_formats : DataFrame
        Columns ``release_id``, ``year``, ``format_name``.

    Returns
    -------
    DataFrame
        One row per (year, format_name) with ``n_releases`` (distinct
        releases in that format), ``n_year`` (distinct releases with any
        format) and ``share``.

    Notes
    -----
    A release that lists two formats counts once under each, so shares
    within a year can add up to slightly more than 1.
    """
    per_format = release_formats.groupBy("year", "format_name").agg(
        F.countDistinct("release_id").alias("n_releases")
    )
    per_year = release_formats.groupBy("year").agg(
        F.countDistinct("release_id").alias("n_year")
    )
    return per_format.join(per_year, "year").withColumn(
        "share", F.col("n_releases") / F.col("n_year")
    )


def add_master_reissue_flag(
    release_formats: DataFrame,
    releases: DataFrame,
    masters: DataFrame,
    min_gap_years: int = 1,
) -> DataFrame:
    """Add a reissue flag based on the age of the master release.

    Parameters
    ----------
    release_formats : DataFrame
        Columns ``release_id``, ``year``, ``format_name``, ``is_reissue``.
    releases : DataFrame
        Columns ``release_id``, ``master_id``.
    masters : DataFrame
        Columns ``master_id``, ``master_year``.
    min_gap_years : int
        A release counts as a reissue when
        ``year - master_year >= min_gap_years``.

    Returns
    -------
    DataFrame
        ``release_formats`` plus:

        * ``master_year`` - null when the release has no master.
        * ``is_reissue_master`` - True, False, or null when unknown
          (no master, or master year missing).
        * ``is_reissue_any`` - True if either definition says reissue.

    Notes
    -----
    A release with no master is usually the only version of that music,
    so treating null as "not a reissue" is a reasonable reading. That
    choice is left to the caller.
    """
    lookup = releases.select("release_id", "master_id").join(
        masters.select("master_id", "master_year"), "master_id", "left"
    )
    return (
        release_formats.join(lookup.select("release_id", "master_year"), "release_id", "left")
        .withColumn(
            "is_reissue_master",
            (F.col("year") - F.col("master_year")) >= min_gap_years,
        )
        .withColumn(
            "is_reissue_any",
            F.col("is_reissue") | F.coalesce(F.col("is_reissue_master"), F.lit(False)),
        )
    )


def reissue_share(
    release_formats: DataFrame,
    by: list[str] | None = None,
    flag_col: str = "is_reissue",
) -> DataFrame:
    """Compute the share of releases that are reissues, per group.

    Parameters
    ----------
    release_formats : DataFrame
        Must contain ``release_id``, the columns in ``by`` and ``flag_col``.
    by : list of str, optional
        Grouping columns. Defaults to ``["year", "format_name"]``. Add
        ``"genre"`` after joining ``release_genres`` to split by genre.
    flag_col : str
        ``"is_reissue"``, ``"is_reissue_master"`` or ``"is_reissue_any"``.

    Returns
    -------
    DataFrame
        One row per group with ``n_releases``, ``n_reissues``,
        ``n_new`` and ``reissue_share`` (``n_reissues / n_releases``).
        A null flag counts as not a reissue.
    """
    by = by or ["year", "format_name"]
    is_reissue = F.coalesce(F.col(flag_col), F.lit(False))
    return (
        release_formats.groupBy(*by)
        .agg(
            F.countDistinct("release_id").alias("n_releases"),
            F.countDistinct(F.when(is_reissue, F.col("release_id"))).alias("n_reissues"),
        )
        .withColumn("n_new", F.col("n_releases") - F.col("n_reissues"))
        .withColumn("reissue_share", F.col("n_reissues") / F.col("n_releases"))
    )


def main_formats(format_shares, min_share: float = 0.01) -> list[str]:
    """List the formats that are big enough to show on their own.

    Parameters
    ----------
    format_shares : pandas.DataFrame or DataFrame
        Output of ``format_shares_by_year`` (columns ``year``,
        ``format_name``, ``share``). A Spark DataFrame is converted with
        ``toPandas``; it is small.
    min_share : float
        A format is kept when its share reaches this value in at least
        one full year (``year <= config.LAST_FULL_YEAR``).

    Returns
    -------
    list of str
        Kept formats, largest mean share first. The mean runs over all
        full years in ``format_shares``; a year in which the format does
        not appear counts as 0. Ties are broken by name.

    Notes
    -----
    Partial years are ignored on purpose, so a format cannot qualify on
    the basis of the 2026 months alone.
    """
    pdf = format_shares.toPandas() if hasattr(format_shares, "toPandas") else format_shares
    full = pdf[(pdf["year"] <= config.LAST_FULL_YEAR) & pdf["format_name"].notna()]
    if full.empty:
        return []
    wide = full.pivot_table(
        index="format_name", columns="year", values="share", aggfunc="sum"
    ).fillna(0.0)
    summary = wide.assign(_max=wide.max(axis=1), _mean=wide.mean(axis=1))[["_max", "_mean"]]
    summary = summary[summary["_max"] >= min_share].reset_index()
    summary = summary.sort_values(["_mean", "format_name"], ascending=[False, True])
    return summary["format_name"].tolist()


def group_minor_formats(
    release_formats: DataFrame, keep: list[str], other: str = "Other"
) -> DataFrame:
    """Relabel every format that is not in ``keep`` as one group.

    Parameters
    ----------
    release_formats : DataFrame
        Must contain ``format_name``.
    keep : list of str
        Formats that keep their name (usually the output of
        ``main_formats``).
    other : str
        Label given to all other formats, including a null
        ``format_name``.

    Returns
    -------
    DataFrame
        ``release_formats`` with ``format_name`` replaced. Row count is
        unchanged. Pass the result to ``format_shares_by_year``, which
        counts a release once per label.
    """
    if keep:
        label = F.when(F.col("format_name").isin(list(keep)), F.col("format_name"))
        label = label.otherwise(F.lit(other))
    else:
        label = F.lit(other)
    return release_formats.withColumn("format_name", label)


def age_bin_labels(bins: tuple[int, ...] = AGE_BINS) -> list[str]:
    """Name the age bins used by ``reissue_age_distribution``.

    Parameters
    ----------
    bins : tuple of int
        Lower edges in years, strictly increasing, starting at 1.

    Returns
    -------
    list of str
        One label per bin, youngest first. For the default edges:
        ``"1"``, ``"2-4"``, ``"5-9"``, ``"10-19"``, ``"20-29"``,
        ``"30-39"``, ``"40-49"``, ``"50+"``.

    Raises
    ------
    ValueError
        If the edges are empty, do not start at 1 or do not increase.
    """
    edges = tuple(int(b) for b in bins)
    if not edges or edges[0] != 1 or any(b <= a for a, b in zip(edges, edges[1:])):
        raise ValueError(f"bins must be strictly increasing and start at 1, got {bins!r}")
    labels = []
    for low, high in zip(edges, edges[1:]):
        labels.append(str(low) if high - low == 1 else f"{low}-{high - 1}")
    labels.append(f"{edges[-1]}+")
    return labels


def count_master_after_release(flagged: DataFrame, format_name: str | None = None) -> int:
    """Count releases whose master is dated after the release itself.

    Parameters
    ----------
    flagged : DataFrame
        Output of ``add_master_reissue_flag`` (needs ``release_id``,
        ``year``, ``master_year``, ``format_name``).
    format_name : str, optional
        Count only releases that have this format.

    Returns
    -------
    int
        Distinct releases with ``master_year > year``. These rows are
        left out of ``reissue_age_distribution``.
    """
    df = flagged if format_name is None else flagged.where(F.col("format_name") == format_name)
    return (
        df.where(F.col("master_year") > F.col("year"))
        .select("release_id")
        .distinct()
        .count()
    )


def reissue_age_distribution(
    flagged: DataFrame,
    format_name: str | None = None,
    bins: tuple[int, ...] = AGE_BINS,
) -> DataFrame:
    """Show how old the original music is on releases that have a master.

    Parameters
    ----------
    flagged : DataFrame
        Output of ``add_master_reissue_flag``.
    format_name : str, optional
        Keep only releases with this format (for example ``"Vinyl"``).
        All formats if omitted.
    bins : tuple of int
        Lower edges of the age bins in years, strictly increasing,
        starting at 1. The last bin is open-ended.

    Returns
    -------
    DataFrame
        One row per (``year``, ``age_bin``) that has releases:
        ``age_bin`` (label from ``age_bin_labels``), ``age_bin_order``
        (0 for the youngest bin), ``n_releases`` (distinct releases) and
        ``median_age`` (approximate median of ``year - master_year`` over
        all releases of that year in the table, repeated on every row of
        that year).

    Raises
    ------
    ValueError
        If ``bins`` is not valid (see ``age_bin_labels``).

    Notes
    -----
    Only releases with a non-null ``master_year`` and
    ``year - master_year >= 1`` are used. A ``master_year`` later than
    the release year is dropped; ``count_master_after_release`` counts
    those. Empty bins are not returned.
    """
    labels = age_bin_labels(bins)
    edges = [int(b) for b in bins]
    df = flagged if format_name is None else flagged.where(F.col("format_name") == format_name)
    age = F.col("year") - F.col("master_year")
    df = (
        df.where(F.col("master_year").isNotNull() & (age >= 1))
        .select("release_id", "year", "master_year")
        .distinct()
        .withColumn("age", age)
    )
    order = F.lit(len(edges) - 1)
    chain = None
    for i, upper in enumerate(edges[1:]):
        chain = (F.when(F.col("age") < upper, F.lit(i)) if chain is None
                 else chain.when(F.col("age") < upper, F.lit(i)))
    if chain is not None:
        order = chain.otherwise(order)
    df = df.withColumn("age_bin_order", order.cast("int")).withColumn(
        "age_bin", F.element_at(F.array(*[F.lit(x) for x in labels]), F.col("age_bin_order") + 1)
    )
    counts = df.groupBy("year", "age_bin", "age_bin_order").agg(
        F.countDistinct("release_id").alias("n_releases")
    )
    medians = df.groupBy("year").agg(F.percentile_approx("age", 0.5).alias("median_age"))
    return counts.join(medians, "year")


def pressing_plant_coverage(
    release_companies: DataFrame,
    release_formats: DataFrame,
    format_name: str = "Vinyl",
    role: str = "Pressed By",
) -> DataFrame:
    """Show how many releases of a format name any company in a role.

    Parameters
    ----------
    release_companies : DataFrame
        Columns ``release_id``, ``entity_type_name``.
    release_formats : DataFrame
        Columns ``release_id``, ``year``, ``format_name``.
    format_name : str
        Format to count.
    role : str
        Value of ``entity_type_name``, for example ``"Pressed By"``.

    Returns
    -------
    DataFrame
        One row per ``year``: ``n_format`` (distinct releases with the
        format), ``n_with_role`` (those with at least one company in the
        role) and ``coverage`` (``n_with_role / n_format``).
    """
    fmt = (
        release_formats.where(F.col("format_name") == format_name)
        .select("release_id", "year")
        .distinct()
    )
    with_role = (
        release_companies.where(F.col("entity_type_name") == role)
        .select("release_id")
        .distinct()
        .withColumn("_has_role", F.lit(True))
    )
    return (
        fmt.join(with_role, "release_id", "left")
        .groupBy("year")
        .agg(
            F.count("*").alias("n_format"),
            F.count("_has_role").alias("n_with_role"),
        )
        .withColumn("coverage", F.col("n_with_role") / F.col("n_format"))
    )


def top_pressing_plants(
    release_companies: DataFrame,
    release_formats: DataFrame,
    format_name: str = "Vinyl",
    role: str = "Pressed By",
    top_n: int = 15,
) -> DataFrame:
    """Find the companies that handle most of a format's releases.

    Parameters
    ----------
    release_companies : DataFrame
        Columns ``release_id``, ``company_id``, ``company_name``,
        ``entity_type_name``.
    release_formats : DataFrame
        Columns ``release_id``, ``year``, ``format_name``.
    format_name : str
        Format to count.
    role : str
        Value of ``entity_type_name``. Check the spelling with a
        ``groupBy`` first; in the 2026 table it is ``"Pressed By"``.
    top_n : int
        Number of companies to keep.

    Returns
    -------
    DataFrame
        One row per (``year``, ``company_id``, ``company_name``) for the
        ``top_n`` companies, with:

        * ``n_releases`` - distinct releases of the format in that year
          that name the company in the role.
        * ``n_year_with_role`` - distinct releases of the format in that
          year that name any company in the role.
        * ``share_of_format_year`` - ``n_releases / n_year_with_role``.
        * ``total_full_years`` - ``n_releases`` summed over
          ``config.YEAR_MIN`` to ``config.LAST_FULL_YEAR``. The ranking
          uses it.
        * ``rank`` - 1 for the largest total.

    Notes
    -----
    Companies with a null ``company_id`` are not ranked, but their
    releases count in ``n_year_with_role``. A release that names several
    companies in the role counts once under each, so the shares of all
    companies in a year can add up to more than 1. Use
    ``pressing_plant_coverage`` for the share of releases that name any
    company in the role at all.
    """
    fmt = (
        release_formats.where(F.col("format_name") == format_name)
        .select("release_id", "year")
        .distinct()
    )
    plants = (
        release_companies.where(F.col("entity_type_name") == role)
        .select("release_id", "company_id", "company_name")
        .distinct()
    )
    joined = fmt.join(plants, "release_id")
    per_year = joined.groupBy("year").agg(F.countDistinct("release_id").alias("n_year_with_role"))
    per_company = (
        joined.where(F.col("company_id").isNotNull())
        .groupBy("year", "company_id", "company_name")
        .agg(F.countDistinct("release_id").alias("n_releases"))
    )
    in_window = (F.col("year") >= config.YEAR_MIN) & (F.col("year") <= config.LAST_FULL_YEAR)
    totals = (
        per_company.groupBy("company_id", "company_name")
        .agg(F.sum(F.when(in_window, F.col("n_releases")).otherwise(0)).alias("total_full_years"))
    )
    rank_window = Window.orderBy(F.desc("total_full_years"), F.asc("company_id"))
    top = (
        totals.withColumn("rank", F.row_number().over(rank_window))
        .where(F.col("rank") <= top_n)
    )
    return (
        per_company.join(top, ["company_id", "company_name"])
        .join(per_year, "year")
        .withColumn("share_of_format_year", F.col("n_releases") / F.col("n_year_with_role"))
        .select(
            "year", "company_id", "company_name", "n_releases", "n_year_with_role",
            "share_of_format_year", "total_full_years", "rank",
        )
    )


def format_coverage(
    release_formats: DataFrame,
    releases: DataFrame,
    format_name: str = "File",
    top_n_descriptions: int = 8,
) -> DataFrame:
    """Check how consistently one format is catalogued over the years.

    Parameters
    ----------
    release_formats : DataFrame
        Columns ``release_id``, ``year``, ``format_name``,
        ``descriptions`` (array of string).
    releases : DataFrame
        Columns ``release_id``, ``year``. Gives the number of releases
        per year that ``share`` is measured against.
    format_name : str
        Format to check, ``"File"`` for digital releases.
    top_n_descriptions : int
        Length of ``top_descriptions``.

    Returns
    -------
    DataFrame
        One row per ``year`` with:

        * ``n_format`` - distinct releases that have the format.
        * ``n_releases`` - distinct releases in ``releases``.
        * ``share`` - ``n_format / n_releases``.
        * ``n_with_desc`` and ``desc_coverage`` - releases with at least
          one non-blank description on the format, and their share of
          ``n_format``.
        * ``top_descriptions`` - array of structs
          (``description``, ``n``), most common first, where ``n`` is the
          number of distinct releases carrying it (MP3, FLAC, WAV,
          Album, ...).

    Notes
    -----
    A release with several rows in the format counts once.
    """
    fmt = release_formats.where(F.col("format_name") == format_name)
    clean = F.filter(F.col("descriptions"), lambda d: d.isNotNull() & (F.trim(d) != ""))
    per_release = fmt.select("release_id", "year", clean.alias("descriptions"))
    base = per_release.groupBy("year").agg(
        F.countDistinct("release_id").alias("n_format"),
        F.countDistinct(F.when(F.size("descriptions") > 0, F.col("release_id"))).alias("n_with_desc"),
    )
    n_all = releases.groupBy("year").agg(F.countDistinct("release_id").alias("n_releases"))

    desc_counts = (
        per_release.select("release_id", "year", F.explode("descriptions").alias("description"))
        .groupBy("year", "description")
        .agg(F.countDistinct("release_id").alias("n"))
    )
    ranked = desc_counts.withColumn(
        "rn", F.row_number().over(Window.partitionBy("year").orderBy(F.desc("n"), F.asc("description")))
    ).where(F.col("rn") <= top_n_descriptions)
    top = ranked.groupBy("year").agg(
        F.transform(
            F.sort_array(F.collect_list(F.struct("rn", "description", "n"))),
            lambda s: F.struct(s["description"].alias("description"), s["n"].alias("n")),
        ).alias("top_descriptions")
    )
    return (
        base.join(n_all, "year", "left")
        .join(top, "year", "left")
        .withColumn("share", F.col("n_format") / F.col("n_releases"))
        .withColumn("desc_coverage", F.col("n_with_desc") / F.col("n_format"))
        .select(
            "year", "n_format", "n_releases", "share", "n_with_desc", "desc_coverage",
            "top_descriptions",
        )
    )


def growth_explained_by_reissues(
    reissue_share_df: DataFrame, start_year: int, end_year: int
) -> DataFrame:
    """Measure how much of the growth in releases is reissues.

    Parameters
    ----------
    reissue_share_df : DataFrame
        Output of ``reissue_share`` with ``by=["year"]``, with columns
        ``year``, ``n_releases`` and ``n_reissues``. To compare several
        flags, add a ``flag`` column to each result and union them; the
        figure is then computed per flag.
    start_year : int
        First year, for example ``config.YEAR_MIN``.
    end_year : int
        Last year, for example ``config.LAST_FULL_YEAR``.

    Returns
    -------
    DataFrame
        One row per ``flag`` (a single row if there is no ``flag``
        column) with ``releases_start``, ``releases_end``,
        ``reissues_start``, ``reissues_end`` and
        ``growth_explained = (reissues_end - reissues_start) /
        (releases_end - releases_start)``.

        ``growth_explained`` is null when releases did not grow (the
        denominator is 0 or negative), and when either year is missing.
        A value above 1 means reissues grew by more than all releases,
        so new releases fell.
    """
    keep = ["year", "n_releases", "n_reissues"]
    by = ["flag"] if "flag" in reissue_share_df.columns else []

    def pick(col: str, year: int):
        return F.max(F.when(F.col("year") == year, F.col(col)))

    out = (
        reissue_share_df.select(*by, *keep)
        .groupBy(*by)
        .agg(
            pick("n_releases", start_year).alias("releases_start"),
            pick("n_releases", end_year).alias("releases_end"),
            pick("n_reissues", start_year).alias("reissues_start"),
            pick("n_reissues", end_year).alias("reissues_end"),
        )
    )
    growth = F.col("releases_end") - F.col("releases_start")
    explained = F.when(
        growth > 0, (F.col("reissues_end") - F.col("reissues_start")) / growth
    )
    return out.withColumn("growth_explained", explained)
