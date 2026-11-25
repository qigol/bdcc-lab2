"""Chart and table builders for the offline Plotly dashboard.

Every chart builder takes one or two small pandas DataFrames (the CSV
tables that ``main.ipynb`` exports to ``dashboard/data/``) and returns a
``plotly.graph_objects.Figure``. Table builders return an HTML string.
Nothing here touches Spark, the network or the file system, except
``load_tables``, which reads the CSV files.

Column names come from the exported CSVs. ``REQUIRED`` lists the columns
each chart needs; ``load_tables`` raises ``DashboardError`` when a CSV
that is present lacks one of them.

Text that comes from the data (artist and label names, style names) is
HTML-escaped before it reaches a table cell or a Plotly label.
"""

from __future__ import annotations

import html
import json
import sys
from pathlib import Path
from typing import Sequence

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.colors import qualitative
from plotly.subplots import make_subplots

from . import config

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

#: Last year that is complete in the main snapshot.
LAST_FULL_YEAR: int = config.LAST_FULL_YEAR

#: System fonts only, so the file needs no web fonts.
FONT_FAMILY: str = (
    "system-ui, -apple-system, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif"
)

#: Colorblind-safe qualitative palette (Plotly ``Safe``), without its grey.
PALETTE: list[str] = list(qualitative.Safe[:-1])

#: Neutral grey for "everything else" series and for HDBSCAN noise.
GREY: str = "#8c8c8c"

#: Two-colour pair (D3 blue and orange) for new-vs-reissue and gain-vs-loss.
BLUE: str = qualitative.D3[0]
ORANGE: str = qualitative.D3[1]

#: Series names that always get the neutral grey.
NEUTRAL_KEYS: frozenset[str] = frozenset({"Independent", "Other", "All Media"})

#: Dash styles used when a chart has more series than palette colours.
DASHES: tuple[str, ...] = ("solid", "dash", "dot", "dashdot")

#: Labels of the three reissue definitions in ``rq2_vinyl_reissues``.
FLAG_LABELS: dict[str, str] = {
    "is_reissue": "Format tagged Reissue, Repress or Remastered",
    "is_reissue_master": "Released after the year of its master",
    "is_reissue_any": "Either definition",
}

#: Labels of the writer-credit levels in ``rq3_writer_levels``.
LEVEL_LABELS: dict[str, str] = {
    "any": "Credit at either level",
    "release": "Release-level credit",
    "track": "Track-level credit",
}

#: Required columns of each CSV table the dashboard uses.
REQUIRED: dict[str, tuple[str, ...]] = {
    "data_summary": ("metric", "count", "share", "note"),
    "rq1_genre_shares": ("year", "genre", "share"),
    "rq1_style_movers": ("style", "share_start", "share_end", "change", "start_year", "end_year"),
    "rq1_entropy_sensitivity": (
        "year", "evenness_full", "evenness_sub_mean", "evenness_sub_min", "evenness_sub_max",
    ),
    "rq1_style_network": ("year", "density", "modularity"),
    "rq2_format_shares": ("year", "format_name", "share"),
    "rq2_vinyl_reissues": ("year", "n_reissues", "n_new", "flag"),
    "rq2_growth_explained": ("flag", "growth_explained", "start_year", "end_year"),
    "rq2_reissue_by_genre": ("year", "genre", "reissue_share"),
    "rq2_reissue_age": ("year", "age_bin", "age_bin_order", "n_releases", "format_name"),
    "rq2_pressing_plants": (
        "year", "n_format", "n_with_role", "coverage", "company_name", "share_of_format_year",
    ),
    "rq3_major_share": ("year", "major", "share"),
    "rq3_group_concentration": ("major", "hhi", "scope", "year"),
    "rq3_parent_coverage": (
        "year", "share_on_root_alone", "share_in_tree", "share_self_released",
        "share_unknown_label",
    ),
    "rq3_writer_levels": ("year", "level", "n_releases"),
    "cmp_lag": ("year", "completeness", "age"),
    "cmp_undercount": ("year", "n_releases", "n_estimated", "partial_year"),
    "cmp_lag_by_format": ("year", "format_name", "n_new", "completeness"),
    "cmp_lag_by_genre": ("year", "genre", "n_new", "completeness"),
    "cmp_major_moves": ("label_name", "major_old", "major_new", "move", "n_releases"),
    "rq4_graph_summary": ("graph", "tag", "nodes", "edges", "communities", "modularity"),
    "rq4_communities_collab": ("community", "size", "top_nodes"),
    "rq4_communities_artist_label": ("community", "size", "top_nodes"),
    "rq4_resolution_sweep": ("resolution", "n_communities_ge50", "modularity"),
    "rq4_cluster_points": ("x", "y", "cluster", "artist_name"),
    "rq4_clusters": ("cluster", "size", "share", "is_noise"),
    "rq4_cluster_agreement": ("compared_with", "nmi", "ari", "n_clusters", "noise_share"),
}


class DashboardError(ValueError):
    """Raised when a present CSV lacks a column the dashboard needs."""


def warn(message: str) -> None:
    """Print a warning to standard error.

    Parameters
    ----------
    message : str
        Text to print after the ``WARNING:`` prefix.
    """
    print(f"WARNING: {message}", file=sys.stderr)


# --------------------------------------------------------------------------
# Loading
# --------------------------------------------------------------------------


def load_tables(data_dir: str | Path) -> dict[str, pd.DataFrame | None]:
    """Read every CSV the dashboard uses from ``data_dir``.

    Parameters
    ----------
    data_dir : str or pathlib.Path
        Folder with the exported ``<table>.csv`` files.

    Returns
    -------
    dict
        Table name to DataFrame. A missing, empty or unreadable file maps
        to ``None`` and prints a warning.

    Raises
    ------
    DashboardError
        If a CSV is present but lacks a column listed in ``REQUIRED``.
    """
    folder = Path(data_dir)
    tables: dict[str, pd.DataFrame | None] = {}
    for name, columns in REQUIRED.items():
        path = folder / f"{name}.csv"
        if not path.is_file():
            warn(f"{path.name} not found in {folder}; its charts will show a note.")
            tables[name] = None
            continue
        try:
            frame = pd.read_csv(path)
        except (pd.errors.EmptyDataError, pd.errors.ParserError) as exc:
            warn(f"{path.name} could not be read ({exc}); its charts will show a note.")
            tables[name] = None
            continue
        absent = [c for c in columns if c not in frame.columns]
        if absent:
            raise DashboardError(
                f"{path.name}: missing required column(s) {absent}; "
                f"found {list(frame.columns)}"
            )
        tables[name] = frame
    return tables


def load_manifest(data_dir: str | Path) -> dict[str, str]:
    """Read ``manifest.json`` (table name to description).

    Parameters
    ----------
    data_dir : str or pathlib.Path
        Folder that holds ``manifest.json``.

    Returns
    -------
    dict
        Descriptions by table name. Empty if the file is missing or invalid.
    """
    path = Path(data_dir) / "manifest.json"
    if not path.is_file():
        warn("manifest.json not found; data sources will be listed without descriptions.")
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        warn(f"manifest.json could not be read ({exc}).")
        return {}
    if not isinstance(data, dict):
        warn("manifest.json is not a JSON object.")
        return {}
    return {str(k): str(v) for k, v in data.items()}


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def esc(value: object) -> str:
    """Escape a value for HTML text or attribute content.

    Parameters
    ----------
    value : object
        Any value. ``None`` and NaN become an empty string.

    Returns
    -------
    str
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return ""
    return html.escape(str(value), quote=True)


def label_text(value: object) -> str:
    """Escape a data value for use inside a Plotly label or hover text.

    Plotly renders a small subset of HTML in labels, so ``<`` and ``>``
    from the data must become entities. Plotly decodes them for display.

    Parameters
    ----------
    value : object
        Any value. ``None`` and NaN become an empty string.

    Returns
    -------
    str
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return ""
    return html.escape(str(value), quote=False)


def _colors(keys: Sequence[str]) -> dict[str, tuple[str, str]]:
    """Map each series name to a (colour, dash) pair.

    Neutral names such as ``Independent`` get grey. Other names take the
    palette in order; once it is used up the dash style changes.
    """
    out: dict[str, tuple[str, str]] = {}
    i = 0
    for key in keys:
        if key in NEUTRAL_KEYS:
            out[key] = (GREY, "solid")
            continue
        out[key] = (PALETTE[i % len(PALETTE)], DASHES[(i // len(PALETTE)) % len(DASHES)])
        i += 1
    return out


def _layout(
    fig: go.Figure,
    title: str,
    xtitle: str | None,
    ytitle: str | None,
    height: int = 440,
) -> go.Figure:
    """Apply the shared look: title, axis titles, fonts, white template."""
    fig.update_layout(
        template="plotly_white",
        title={"text": title, "x": 0.01, "xanchor": "left", "font": {"size": 16}},
        font={"family": FONT_FAMILY, "size": 12},
        height=height,
        margin={"l": 70, "r": 30, "t": 70, "b": 60},
        legend={"orientation": "v", "font": {"size": 11}},
        hovermode="closest",
    )
    if xtitle is not None:
        fig.update_xaxes(title_text=xtitle)
    if ytitle is not None:
        fig.update_yaxes(title_text=ytitle)
    return fig


def _year_axis(fig: go.Figure, **kwargs: object) -> None:
    """Show every year as a tick on numeric x axes."""
    fig.update_xaxes(dtick=1, tickformat="d", **kwargs)


def _mark_partial(fig: go.Figure, **kwargs: object) -> None:
    """Shade the partial year (the one after ``LAST_FULL_YEAR``)."""
    partial = LAST_FULL_YEAR + 1
    fig.add_vrect(
        x0=partial - 0.5,
        x1=partial + 0.5,
        fillcolor="#999999",
        opacity=0.15,
        line_width=0,
        annotation_text=f"{partial}: Jan-Sep",
        annotation_position="top left",
        annotation_font_size=10,
        **kwargs,
    )


def _empty(title: str, message: str, height: int = 240) -> go.Figure:
    """A figure that only says that there is nothing to plot."""
    fig = go.Figure()
    fig.add_annotation(
        text=label_text(message), x=0.5, y=0.5, xref="paper", yref="paper", showarrow=False
    )
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    return _layout(fig, title, None, None, height=height)


def _order_by_mean(df: pd.DataFrame, key: str, value: str) -> list[str]:
    """Series names ordered by their mean value, largest first."""
    means = df.groupby(key)[value].mean().sort_values(ascending=False)
    return [str(k) for k in means.index]


def _lines(
    df: pd.DataFrame,
    x: str,
    y: str,
    group: str,
    title: str,
    ytitle: str,
    percent: bool = True,
    xtitle: str = "Year",
    partial: bool = True,
    names: dict[str, str] | None = None,
    order: Sequence[str] | None = None,
) -> go.Figure:
    """One line per ``group`` value over ``x`` (shared by many charts)."""
    if df.empty:
        return _empty(title, "No rows in this table.")
    data = df.copy()
    data[group] = data[group].astype(str)
    keys = list(order) if order is not None else _order_by_mean(data, group, y)
    palette = _colors(keys)
    fmt = ".1%" if percent else ",.0f"
    fig = go.Figure()
    for key in keys:
        part = data[data[group] == key].sort_values(x)
        if part.empty:
            continue
        colour, dash = palette[key]
        shown = label_text((names or {}).get(key, key))
        fig.add_trace(
            go.Scatter(
                x=part[x],
                y=part[y],
                name=shown,
                mode="lines+markers",
                line={"color": colour, "dash": dash, "width": 2},
                marker={"size": 5},
                hovertemplate=f"%{{fullData.name}}<br>%{{x}}: %{{y:{fmt}}}<extra></extra>",
            )
        )
    _layout(fig, title, xtitle, ytitle)
    fig.update_yaxes(tickformat=".0%" if percent else ",d", rangemode="tozero")
    if x == "year":
        _year_axis(fig)
        if partial:
            _mark_partial(fig)
    return fig


# --------------------------------------------------------------------------
# HTML tables and tiles
# --------------------------------------------------------------------------


def _cell(value: object, kind: str) -> str:
    """Format and escape one table cell."""
    if value is None or (isinstance(value, (float, np.floating)) and np.isnan(value)):
        return ""
    try:
        if kind == "int":
            return esc(f"{float(value):,.0f}")
        if kind == "pct":
            return esc(f"{float(value):.1%}")
        if kind == "f2":
            return esc(f"{float(value):.2f}")
        if kind == "f3":
            return esc(f"{float(value):.3f}")
    except (TypeError, ValueError):
        pass
    return esc(value)


def html_table(
    df: pd.DataFrame,
    columns: Sequence[tuple[str, str, str]],
    title: str = "",
    scroll: bool = False,
) -> str:
    """Render a DataFrame as an HTML table with every value escaped.

    Parameters
    ----------
    df : pandas.DataFrame
        Rows to show.
    columns : sequence of (str, str, str)
        ``(column, header, kind)`` triples. ``kind`` is ``"text"``,
        ``"int"``, ``"pct"``, ``"f2"`` or ``"f3"``. Columns that are not
        in ``df`` are skipped.
    title : str
        Table heading, shown as an ``<h3>`` when not empty.
    scroll : bool
        Put the table in a scrolling box with a sticky header (long tables).

    Returns
    -------
    str
        HTML for the heading and the table.
    """
    cols = [(c, h, k) for c, h, k in columns if c in df.columns]
    def attr(kind: str) -> str:
        return "" if kind == "text" else ' class="num"'

    head = "".join(f'<th scope="col"{attr(k)}>{esc(h)}</th>' for _, h, k in cols)
    body_rows = []
    for row in df[[c for c, _, _ in cols]].itertuples(index=False, name=None):
        cells = "".join(
            f"<td{attr(k)}>{_cell(v, k)}</td>" for v, (_, _, k) in zip(row, cols)
        )
        body_rows.append(f"<tr>{cells}</tr>")
    heading = f"<h3>{esc(title)}</h3>" if title else ""
    box = "table-wrap scroll" if scroll else "table-wrap"
    return (
        f'{heading}<div class="{box}" tabindex="0" role="region" aria-label="{esc(title)}">'
        f'<table><thead><tr>{head}</tr></thead><tbody>{"".join(body_rows)}</tbody></table></div>'
    )


_TILE_LABELS: dict[str, str] = {
    "releases_in_scope": "Releases in scope",
    "linked_artists": "Linked artists",
    "linked_labels": "Linked labels",
    "releases_with_style": "Releases with a style",
}


def summary_tiles(df: pd.DataFrame) -> str:
    """Key numbers from ``data_summary`` as a grid of tiles.

    Parameters
    ----------
    df : pandas.DataFrame
        Columns ``metric``, ``count``, ``share``, ``note``. Rows with
        neither a count nor a share are left out.

    Returns
    -------
    str
        HTML for the tile grid.
    """
    tiles = []
    for row in df.to_dict("records"):
        count = pd.to_numeric(pd.Series([row["count"]]), errors="coerce").iloc[0]
        share = pd.to_numeric(pd.Series([row["share"]]), errors="coerce").iloc[0]
        if pd.isna(count) and pd.isna(share):
            continue
        metric = str(row["metric"])
        label = _TILE_LABELS.get(metric, metric.replace("_", " ").capitalize())
        value = f"{count:,.0f}" if not pd.isna(count) else f"{share:.1%}"
        note = esc(row["note"])
        if not pd.isna(count) and not pd.isna(share):
            note = f"{esc(f'{share:.1%}')}: {note}"
        tiles.append(
            f'<div class="tile"><div class="tile-label">{esc(label)}</div>'
            f'<div class="tile-value">{esc(value)}</div>'
            f'<div class="tile-note">{note}</div></div>'
        )
    return f'<div class="tiles">{"".join(tiles)}</div>'


# --------------------------------------------------------------------------
# RQ1
# --------------------------------------------------------------------------


def genre_share_lines(df: pd.DataFrame) -> go.Figure:
    """Share of each year's releases tagged with each genre.

    Parameters
    ----------
    df : pandas.DataFrame
        ``rq1_genre_shares``: ``year``, ``genre``, ``share``.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    return _lines(
        df, "year", "share", "genre",
        "Share of releases tagged with each genre, by year",
        "Share of the year's releases",
    )


def style_movers_bars(df: pd.DataFrame, n: int = 15) -> go.Figure:
    """Diverging bars of the styles that gained and lost the most share.

    Parameters
    ----------
    df : pandas.DataFrame
        ``rq1_style_movers``: ``style``, ``share_start``, ``share_end``,
        ``change``, ``start_year``, ``end_year`` (and optionally
        ``n_start``, ``n_end``).
    n : int
        Number of gains and of losses to show.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    if df.empty:
        return _empty("Styles that gained and lost the most share", "No rows in this table.")
    start, end = int(df["start_year"].iloc[0]), int(df["end_year"].iloc[0])
    title = f"Styles that gained and lost the most share, {start} to {end}"
    gains = df[df["change"] > 0].nlargest(n, "change")
    losses = df[df["change"] < 0].nsmallest(n, "change")
    both = pd.concat([gains, losses]).sort_values("change")
    if both.empty:
        return _empty(title, "No styles with a change in share.")
    fig = go.Figure()
    for name, part, colour in (("Loss", both[both["change"] < 0], ORANGE),
                               ("Gain", both[both["change"] > 0], BLUE)):
        if part.empty:
            continue
        names = [label_text(s) for s in part["style"]]
        fig.add_trace(
            go.Bar(
                x=part["change"] * 100,
                y=names,
                orientation="h",
                name=name,
                marker={"color": colour},
                customdata=np.column_stack([part["share_start"] * 100, part["share_end"] * 100]),
                hovertemplate=(
                    "%{y}<br>Change: %{x:+.2f} points"
                    f"<br>Share in {start}: %{{customdata[0]:.2f}}%"
                    f"<br>Share in {end}: %{{customdata[1]:.2f}}%<extra></extra>"
                ),
            )
        )
    _layout(fig, title, f"Change in share of releases, {start} to {end} (percentage points)",
            "Style", height=max(360, 120 + 24 * len(both)))
    fig.update_yaxes(automargin=True, categoryorder="array", categoryarray=[
        label_text(s) for s in both["style"]])
    fig.add_vline(x=0, line_width=1, line_color="#444444")
    return fig


def evenness_band(df: pd.DataFrame) -> go.Figure:
    """Style evenness by year with the equal-size subsample band.

    Parameters
    ----------
    df : pandas.DataFrame
        ``rq1_entropy_sensitivity``: ``year``, ``evenness_full``,
        ``evenness_sub_mean``, ``evenness_sub_min``, ``evenness_sub_max``.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    title = "Style evenness by year: all releases and equal-size random samples"
    if df.empty:
        return _empty(title, "No rows in this table.")
    data = df.sort_values("year")
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=data["year"], y=data["evenness_sub_max"], mode="lines",
        line={"width": 0}, showlegend=False, hoverinfo="skip"))
    fig.add_trace(go.Scatter(
        x=data["year"], y=data["evenness_sub_min"], mode="lines", line={"width": 0},
        fill="tonexty", fillcolor="rgba(255,127,14,0.25)", name="Samples: min to max",
        hovertemplate="%{x}: min %{y:.4f}<extra>Samples</extra>"))
    fig.add_trace(go.Scatter(
        x=data["year"], y=data["evenness_sub_mean"], mode="lines",
        line={"color": ORANGE, "dash": "dash", "width": 2}, name="Samples: mean",
        hovertemplate="%{x}: %{y:.4f}<extra>Sample mean</extra>"))
    fig.add_trace(go.Scatter(
        x=data["year"], y=data["evenness_full"], mode="lines+markers",
        line={"color": BLUE, "width": 2}, marker={"size": 6}, name="All releases",
        hovertemplate="%{x}: %{y:.4f}<extra>All releases</extra>"))
    _layout(fig, title, "Year", "Evenness (normalised Shannon entropy of styles)")
    _year_axis(fig)
    _mark_partial(fig)
    return fig


def style_network_dual(df: pd.DataFrame) -> go.Figure:
    """Density and modularity of the style co-occurrence network by year.

    Parameters
    ----------
    df : pandas.DataFrame
        ``rq1_style_network``: ``year``, ``density``, ``modularity``.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    title = "Style co-occurrence network: density and modularity by year"
    if df.empty:
        return _empty(title, "No rows in this table.")
    data = df.sort_values("year")
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Scatter(
        x=data["year"], y=data["density"], name="Density (left axis)",
        mode="lines+markers", line={"color": BLUE, "width": 2}, marker={"symbol": "circle"},
        hovertemplate="%{x}: density %{y:.4f}<extra></extra>"), secondary_y=False)
    fig.add_trace(go.Scatter(
        x=data["year"], y=data["modularity"], name="Modularity (right axis)",
        mode="lines+markers", line={"color": ORANGE, "width": 2, "dash": "dash"},
        marker={"symbol": "square"},
        hovertemplate="%{x}: modularity %{y:.3f}<extra></extra>"), secondary_y=True)
    _layout(fig, title, "Year", None)
    fig.update_yaxes(title_text="Density of the network", secondary_y=False)
    fig.update_yaxes(title_text="Modularity of the Leiden partition", secondary_y=True)
    _year_axis(fig)
    _mark_partial(fig)
    return fig


# --------------------------------------------------------------------------
# RQ2
# --------------------------------------------------------------------------


def format_share_lines(df: pd.DataFrame) -> go.Figure:
    """Share of each year's releases by format.

    Parameters
    ----------
    df : pandas.DataFrame
        ``rq2_format_shares``: ``year``, ``format_name``, ``share``.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    return _lines(
        df, "year", "share", "format_name",
        "Share of releases by format, by year", "Share of the year's releases",
    )


def vinyl_reissue_bars(df: pd.DataFrame) -> go.Figure:
    """Stacked bars of new and reissued vinyl releases, one set per definition.

    A dropdown switches between the reissue definitions in ``flag``.

    Parameters
    ----------
    df : pandas.DataFrame
        ``rq2_vinyl_reissues``: ``year``, ``n_reissues``, ``n_new``, ``flag``.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    title = "Vinyl releases per year: new and reissue"
    if df.empty:
        return _empty(title, "No rows in this table.")
    flags = [f for f in FLAG_LABELS if f in set(df["flag"])]
    flags += sorted(set(df["flag"]) - set(flags))
    fig = go.Figure()
    for i, flag in enumerate(flags):
        part = df[df["flag"] == flag].sort_values("year")
        for column, name, colour in (("n_new", "New releases", BLUE),
                                     ("n_reissues", "Reissues", ORANGE)):
            fig.add_trace(go.Bar(
                x=part["year"], y=part[column], name=name, marker={"color": colour},
                visible=i == 0,
                hovertemplate=f"{name}<br>%{{x}}: %{{y:,.0f}}<extra></extra>"))
    buttons = []
    for i, flag in enumerate(flags):
        visible = [j // 2 == i for j in range(2 * len(flags))]
        buttons.append({
            "label": label_text(FLAG_LABELS.get(flag, flag)),
            "method": "update",
            "args": [{"visible": visible}],
        })
    _layout(fig, title, "Year", "Vinyl releases", height=480)
    fig.update_layout(
        barmode="stack",
        margin={"l": 70, "r": 30, "t": 110, "b": 60},
        updatemenus=[{
            "type": "dropdown", "buttons": buttons, "x": 0.0, "xanchor": "left",
            "y": 1.12, "yanchor": "top", "showactive": True,
        }],
    )
    fig.update_yaxes(tickformat=",d")
    _year_axis(fig)
    _mark_partial(fig)
    return fig


def growth_explained_bars(df: pd.DataFrame) -> go.Figure:
    """Share of the growth in vinyl releases explained by reissues.

    Parameters
    ----------
    df : pandas.DataFrame
        ``rq2_growth_explained``: ``flag``, ``growth_explained``,
        ``start_year``, ``end_year`` (and optionally the four count columns).

    Returns
    -------
    plotly.graph_objects.Figure
    """
    if df.empty:
        return _empty("Share of vinyl growth explained by reissues", "No rows in this table.")
    start, end = int(df["start_year"].iloc[0]), int(df["end_year"].iloc[0])
    title = f"Share of the growth in vinyl releases explained by reissues, {start} to {end}"
    names = [label_text(FLAG_LABELS.get(f, f)) for f in df["flag"]]
    extra = {}
    if {"releases_start", "releases_end", "reissues_start", "reissues_end"} <= set(df.columns):
        extra = {
            "customdata": df[["releases_start", "releases_end",
                              "reissues_start", "reissues_end"]].to_numpy(),
            "hovertemplate": (
                "%{x}<br>Explained: %{y:.1%}"
                f"<br>All vinyl: %{{customdata[0]:,.0f}} to %{{customdata[1]:,.0f}}"
                f"<br>Reissues: %{{customdata[2]:,.0f}} to %{{customdata[3]:,.0f}}<extra></extra>"
            ),
        }
    fig = go.Figure(go.Bar(
        x=names, y=df["growth_explained"], marker={"color": BLUE},
        text=[f"{v:.1%}" for v in df["growth_explained"]], textposition="outside",
        cliponaxis=False, **extra))
    _layout(fig, title, "Reissue definition", "Share of the growth in vinyl releases", height=400)
    top = float(df["growth_explained"].max())
    fig.update_yaxes(tickformat=".0%", range=[0, max(top * 1.25, 0.05)])
    return fig


def reissue_share_by_genre(df: pd.DataFrame) -> go.Figure:
    """Reissue share of vinyl releases by genre, with a genre dropdown.

    Parameters
    ----------
    df : pandas.DataFrame
        ``rq2_reissue_by_genre``: ``year``, ``genre``, ``reissue_share``.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    title = "Reissue share of vinyl releases, by genre"
    fig = _lines(df, "year", "reissue_share", "genre", title, "Reissues as a share of vinyl releases")
    if df.empty:
        return fig
    n = len(fig.data)
    buttons = [{"label": "All genres", "method": "update",
                "args": [{"visible": [True] * n}]}]
    for i, trace in enumerate(fig.data):
        buttons.append({
            "label": str(trace.name), "method": "update",
            "args": [{"visible": [j == i for j in range(n)]}],
        })
    fig.update_layout(
        height=520,
        margin={"l": 70, "r": 30, "t": 110, "b": 60},
        updatemenus=[{"type": "dropdown", "buttons": buttons, "x": 0.0, "xanchor": "left",
                      "y": 1.12, "yanchor": "top", "showactive": True}],
    )
    return fig


def reissue_age_bars(df: pd.DataFrame) -> go.Figure:
    """Age of the master at release, vinyl against CD, with a year dropdown.

    Parameters
    ----------
    df : pandas.DataFrame
        ``rq2_reissue_age``: ``year``, ``age_bin``, ``age_bin_order``,
        ``n_releases``, ``format_name`` (and optionally ``median_age``).

    Returns
    -------
    plotly.graph_objects.Figure
    """
    title = "Age of the master at release: vinyl and CD"
    if df.empty:
        return _empty(title, "No rows in this table.")
    data = df.copy()
    data["age_bin"] = data["age_bin"].astype(str)
    bins = data.drop_duplicates("age_bin").sort_values("age_bin_order")["age_bin"].tolist()
    years = sorted(int(y) for y in data["year"].unique())
    formats = sorted(data["format_name"].astype(str).unique())
    palette = _colors(formats)
    default_year = LAST_FULL_YEAR if LAST_FULL_YEAR in years else years[-1]
    fig = go.Figure()
    index: list[tuple[int, str]] = []
    for year in years:
        for fmt in formats:
            part = data[(data["year"] == year) & (data["format_name"].astype(str) == fmt)]
            part = part.set_index("age_bin").reindex(bins)
            total = part["n_releases"].sum()
            share = (part["n_releases"] / total).fillna(0.0) if total else part["n_releases"].fillna(0.0)
            median = part["median_age"].dropna().iloc[0] if (
                "median_age" in part and part["median_age"].notna().any()) else None
            median_text = f"<br>Median age: {median:g} years" if median is not None else ""
            shown = label_text(fmt)
            fig.add_trace(go.Bar(
                x=bins, y=share, name=shown, marker={"color": palette[fmt][0]},
                legendgroup=shown, showlegend=True, visible=year == default_year,
                customdata=part["n_releases"].fillna(0).to_numpy(),
                hovertemplate=(f"%{{fullData.name}}, {year}<br>Age %{{x}} years: %{{y:.1%}}"
                               f"<br>Releases: %{{customdata:,.0f}}{median_text}<extra></extra>")))
            index.append((year, fmt))
    buttons = [{
        "label": f"{year} (Jan-Sep, partial)" if year > LAST_FULL_YEAR else str(year),
        "method": "update",
        "args": [{"visible": [y == year for y, _ in index]}],
    } for year in years]
    _layout(fig, title, "Years between the master's year and the release",
            "Share of the format's releases that year", height=480)
    fig.update_layout(
        barmode="group",
        margin={"l": 70, "r": 30, "t": 110, "b": 60},
        updatemenus=[{"type": "dropdown", "buttons": buttons, "x": 0.0, "xanchor": "left",
                      "y": 1.12, "yanchor": "top", "showactive": True,
                      "active": years.index(default_year)}],
    )
    fig.update_xaxes(type="category", categoryorder="array", categoryarray=bins)
    fig.update_yaxes(tickformat=".0%")
    return fig


def pressing_coverage(df: pd.DataFrame) -> go.Figure:
    """Share of vinyl releases that name a pressing plant, by year.

    Parameters
    ----------
    df : pandas.DataFrame
        ``rq2_pressing_plants``: ``year``, ``n_format``, ``n_with_role``,
        ``coverage``.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    title = "Vinyl releases that name a pressing plant"
    if df.empty:
        return _empty(title, "No rows in this table.")
    data = df.drop_duplicates("year").sort_values("year")
    fig = go.Figure(go.Bar(
        x=data["year"], y=data["coverage"], marker={"color": BLUE},
        customdata=data[["n_with_role", "n_format"]].to_numpy(),
        hovertemplate=("%{x}: %{y:.1%}<br>%{customdata[0]:,.0f} of "
                       "%{customdata[1]:,.0f} vinyl releases<extra></extra>")))
    _layout(fig, title, "Year", "Share of vinyl releases naming a plant", height=380)
    fig.update_yaxes(tickformat=".0%", rangemode="tozero")
    _year_axis(fig)
    _mark_partial(fig)
    return fig


def pressing_plant_share(df: pd.DataFrame, top: int = 8) -> go.Figure:
    """Share of plant-naming vinyl releases handled by the largest plants.

    Parameters
    ----------
    df : pandas.DataFrame
        ``rq2_pressing_plants``: ``year``, ``company_name``,
        ``share_of_format_year`` (and optionally ``n_releases``).
    top : int
        Number of plants to draw, ranked by their releases over all years.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    title = "Largest pressing plants: share of vinyl releases that name a plant"
    if df.empty:
        return _empty(title, "No rows in this table.")
    weight = "n_releases" if "n_releases" in df.columns else "share_of_format_year"
    leaders = (df.groupby(df["company_name"].astype(str))[weight].sum()
               .sort_values(ascending=False).head(top).index.tolist())
    subset = df[df["company_name"].astype(str).isin(leaders)]
    fig = _lines(subset, "year", "share_of_format_year", "company_name", title,
                 "Share of releases that name a plant", order=leaders)
    fig.update_layout(height=460)
    return fig


# --------------------------------------------------------------------------
# RQ3
# --------------------------------------------------------------------------

MAJOR_ORDER: tuple[str, ...] = ("Universal", "Sony", "Warner", "Independent")


def _majors(values: Sequence[str]) -> list[str]:
    """Order group names with the three majors first."""
    present = [str(v) for v in dict.fromkeys(values)]
    return [m for m in MAJOR_ORDER if m in present] + sorted(set(present) - set(MAJOR_ORDER))


def major_share_lines(df: pd.DataFrame) -> go.Figure:
    """Share of each year's releases on labels of each major and independents.

    Parameters
    ----------
    df : pandas.DataFrame
        ``rq3_major_share``: ``year``, ``major``, ``share``.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    order = _majors(df["major"]) if len(df) else []
    fig = _lines(df, "year", "share", "major", "Share of releases by label group, by year",
                 "Share of the year's releases", order=order)
    fig.update_yaxes(rangemode="tozero")
    return fig


def group_hhi_lines(df: pd.DataFrame) -> go.Figure:
    """Artist concentration (HHI) inside each label group, by year.

    Parameters
    ----------
    df : pandas.DataFrame
        ``rq3_group_concentration``: ``major``, ``hhi``, ``scope``,
        ``year`` (and optionally ``top_artist_name``, ``top_artist_share``).
        Only the rows with ``scope == "year"`` are drawn.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    title = "Artist concentration (HHI) inside each label group, by year"
    data = df[df["scope"].astype(str) == "year"].dropna(subset=["year"]).copy()
    if data.empty:
        return _empty(title, "No per-year rows in this table.")
    data["year"] = data["year"].astype(int)
    order = _majors(data["major"])
    palette = _colors(order)
    has_top = {"top_artist_name", "top_artist_share"} <= set(data.columns)
    fig = go.Figure()
    for key in order:
        part = data[data["major"].astype(str) == key].sort_values("year")
        shown = label_text(key)
        extra = {}
        template = "%{fullData.name}<br>%{x}: HHI %{y:.2e}<extra></extra>"
        if has_top:
            extra["customdata"] = np.column_stack([
                [label_text(v) for v in part["top_artist_name"]], part["top_artist_share"]])
            template = ("%{fullData.name}<br>%{x}: HHI %{y:.2e}"
                        "<br>Top artist: %{customdata[0]} (%{customdata[1]:.2%} of pairs)"
                        "<extra></extra>")
        fig.add_trace(go.Scatter(
            x=part["year"], y=part["hhi"], name=shown, mode="lines+markers",
            line={"color": palette[key][0], "dash": palette[key][1], "width": 2},
            marker={"size": 5}, hovertemplate=template, **extra))
    _layout(fig, title, "Year", "Herfindahl-Hirschman index of artists (log scale)")
    fig.update_yaxes(type="log", exponentformat="e")
    _year_axis(fig)
    _mark_partial(fig)
    return fig


def parent_coverage_bars(df: pd.DataFrame) -> go.Figure:
    """Stacked shares of releases by how much parent information their labels have.

    Parameters
    ----------
    df : pandas.DataFrame
        ``rq3_parent_coverage`` with the four ``share_*`` columns.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    title = "Releases by parent information of their labels"
    if df.empty:
        return _empty(title, "No rows in this table.")
    data = df.sort_values("year")
    parts = (
        ("share_in_tree", "In a label tree", BLUE),
        ("share_on_root_alone", "On a label with no parent and no sublabels", ORANGE),
        ("share_self_released", "Self-released", PALETTE[3]),
        ("share_unknown_label", "Unknown label", GREY),
    )
    fig = go.Figure()
    for column, name, colour in parts:
        fig.add_trace(go.Bar(
            x=data["year"], y=data[column], name=name, marker={"color": colour},
            hovertemplate=f"{name}<br>%{{x}}: %{{y:.1%}}<extra></extra>"))
    _layout(fig, title, "Year", "Share of the year's releases")
    fig.update_layout(barmode="stack")
    fig.update_yaxes(tickformat=".0%", range=[0, 1])
    _year_axis(fig)
    _mark_partial(fig)
    return fig


def writer_levels_lines(df: pd.DataFrame) -> go.Figure:
    """Releases with a writer credit, at release level and at track level.

    Parameters
    ----------
    df : pandas.DataFrame
        ``rq3_writer_levels``: ``year``, ``level``, ``n_releases``.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    order = [k for k in LEVEL_LABELS if k in set(df["level"].astype(str))]
    order += sorted(set(df["level"].astype(str)) - set(order))
    return _lines(
        df, "year", "n_releases", "level", "Releases with a writer credit, by credit level",
        "Releases with a credit", percent=False, names=LEVEL_LABELS, order=order,
    )


# --------------------------------------------------------------------------
# Snapshot comparison
# --------------------------------------------------------------------------


def completeness_by_age(df: pd.DataFrame) -> go.Figure:
    """Share of releases already catalogued in January 2020, by their age then.

    Parameters
    ----------
    df : pandas.DataFrame
        ``cmp_lag``: ``year``, ``completeness``, ``age`` (and optionally
        ``n_new``, ``n_already``).

    Returns
    -------
    plotly.graph_objects.Figure
    """
    title = "Cataloguing lag: share already in the January 2020 snapshot"
    if df.empty:
        return _empty(title, "No rows in this table.")
    data = df.sort_values("age")
    fig = go.Figure(go.Scatter(
        x=data["age"], y=data["completeness"], mode="lines+markers+text",
        text=[f"{y}" for y in data["year"]], textposition="top center",
        line={"color": BLUE, "width": 2}, marker={"size": 8},
        customdata=data["year"].to_numpy(),
        hovertemplate=("Release year %{customdata}<br>Age at the snapshot: %{x} years"
                       "<br>Already catalogued: %{y:.1%}<extra></extra>")))
    _layout(fig, title, "Years between the release year and the January 2020 snapshot",
            "Share of the release year already catalogued", height=400)
    fig.update_yaxes(tickformat=".0%", range=[0, 1])
    fig.update_xaxes(dtick=1)
    return fig


def undercount_bars(df: pd.DataFrame) -> go.Figure:
    """Releases per year as catalogued and as estimated after the lag correction.

    Parameters
    ----------
    df : pandas.DataFrame
        ``cmp_undercount``: ``year``, ``n_releases``, ``n_estimated``,
        ``partial_year`` (and optionally ``seasonal_factor``).

    Returns
    -------
    plotly.graph_objects.Figure
    """
    title = "Releases per year: catalogued and estimated"
    if df.empty:
        return _empty(title, "No rows in this table.")
    data = df.sort_values("year").reset_index(drop=True)
    partial = data["partial_year"].astype(str).str.lower().eq("true")
    pattern = ["/" if p else "" for p in partial]
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=data["year"], y=data["n_releases"], name="Catalogued (n_releases)",
        marker={"color": BLUE, "pattern": {"shape": pattern, "fgcolor": "#ffffff"}},
        hovertemplate="%{x}: %{y:,.0f} catalogued<extra></extra>"))
    fig.add_trace(go.Bar(
        x=data["year"], y=data["n_estimated"], name="Estimated (n_estimated)",
        marker={"color": ORANGE, "pattern": {"shape": pattern, "fgcolor": "#ffffff"}},
        hovertemplate="%{x}: %{y:,.0f} estimated<extra></extra>"))
    _layout(fig, title, "Year", "Releases")
    fig.update_layout(barmode="group")
    fig.update_yaxes(tickformat=",d")
    _year_axis(fig)
    for i in data.index[partial.to_numpy()]:
        note = f"{int(data.loc[i, 'year'])}: partial year (Jan-Sep)"
        if "seasonal_factor" in data.columns and pd.notna(data.loc[i, "seasonal_factor"]):
            note += f"<br>seasonal factor x{data.loc[i, 'seasonal_factor']:.2f}"
        fig.add_annotation(
            x=data.loc[i, "year"], y=max(data.loc[i, "n_releases"], data.loc[i, "n_estimated"]),
            text=note, showarrow=True, arrowhead=2, ay=-40, font={"size": 10})
    return fig


def lag_heatmap(df: pd.DataFrame, category: str, title: str, ylabel: str) -> go.Figure:
    """Heatmap of completeness by release year and a category.

    Parameters
    ----------
    df : pandas.DataFrame
        ``cmp_lag_by_format`` or ``cmp_lag_by_genre``: ``year``, the
        ``category`` column, ``n_new``, ``completeness``.
    category : str
        Column for the vertical axis (``"format_name"`` or ``"genre"``).
    title : str
        Chart title.
    ylabel : str
        Vertical axis label.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    if df.empty:
        return _empty(title, "No rows in this table.")
    order = (df.groupby(df[category].astype(str))["n_new"].sum()
             .sort_values(ascending=False).index.tolist())
    grid = (df.assign(**{category: df[category].astype(str)})
            .pivot_table(index=category, columns="year", values="completeness", aggfunc="mean")
            .reindex(order))
    years = [str(int(y)) for y in grid.columns]
    fig = go.Figure(go.Heatmap(
        z=grid.to_numpy(), x=years, y=[label_text(c) for c in grid.index],
        zmin=0, zmax=1, colorscale="Viridis", texttemplate="%{z:.0%}",
        colorbar={"title": {"text": "Already<br>catalogued"}, "tickformat": ".0%"},
        hovertemplate=f"{label_text(ylabel)}: %{{y}}<br>Release year %{{x}}"
                      "<br>Already catalogued: %{z:.1%}<extra></extra>"))
    _layout(fig, title, "Release year", ylabel, height=max(320, 140 + 36 * len(grid)))
    fig.update_xaxes(type="category")
    fig.update_yaxes(autorange="reversed", automargin=True)
    return fig


def lag_by_format(df: pd.DataFrame) -> go.Figure:
    """Cataloguing lag by format (see ``lag_heatmap``).

    Parameters
    ----------
    df : pandas.DataFrame
        ``cmp_lag_by_format``.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    return lag_heatmap(df, "format_name", "Share already catalogued in January 2020, by format",
                       "Format")


def lag_by_genre(df: pd.DataFrame) -> go.Figure:
    """Cataloguing lag by genre (see ``lag_heatmap``).

    Parameters
    ----------
    df : pandas.DataFrame
        ``cmp_lag_by_genre``.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    return lag_heatmap(df, "genre", "Share already catalogued in January 2020, by genre", "Genre")


def major_moves_table(df: pd.DataFrame) -> str:
    """Table of the labels that moved into, out of or between major trees.

    Parameters
    ----------
    df : pandas.DataFrame
        ``cmp_major_moves``.

    Returns
    -------
    str
        HTML table.
    """
    data = df.assign(move=df["move"].astype(str).str.replace("_", " ", regex=False))
    return html_table(data, [
        ("label_name", "Label", "text"), ("major_old", "Group in 2020", "text"),
        ("major_new", "Group in 2026", "text"), ("move", "Move", "text"),
        ("n_releases", "Releases", "int"),
    ], title="Labels with the most releases that changed major group, 2020 to 2026")


# --------------------------------------------------------------------------
# RQ4
# --------------------------------------------------------------------------


def graph_summary_table(df: pd.DataFrame) -> str:
    """Table of nodes, edges, communities and agreement for each graph.

    Parameters
    ----------
    df : pandas.DataFrame
        ``rq4_graph_summary``.

    Returns
    -------
    str
        HTML table.
    """
    return html_table(df, [
        ("graph", "Graph", "text"), ("tag", "Compared with", "text"),
        ("min_weight", "Min. edge weight", "int"), ("nodes", "Nodes", "int"),
        ("edges", "Edges", "int"), ("communities", "Communities", "int"),
        ("communities_ge50", "Communities of 50+", "int"),
        ("largest_community_share", "Largest community", "pct"),
        ("modularity", "Modularity", "f3"), ("nmi", "NMI", "f3"), ("ari", "ARI", "f3"),
        ("purity", "Purity", "f3"), ("share_tagged", "Nodes with a tag", "pct"),
    ], title="The three graphs and their Leiden communities")


def communities_table(df: pd.DataFrame, title: str, node_header: str) -> str:
    """Table of the largest Leiden communities of one graph.

    Parameters
    ----------
    df : pandas.DataFrame
        ``rq4_communities_collab`` or ``rq4_communities_artist_label``.
    title : str
        Table heading.
    node_header : str
        Header of the ``top_nodes`` column.

    Returns
    -------
    str
        HTML table.
    """
    return html_table(df, [
        ("community", "Community", "text"), ("size", "Size", "int"),
        ("top_nodes", node_header, "text"), ("top_genres", "Top genres", "text"),
        ("genre_purity", "Genre purity", "f3"), ("top_styles", "Top styles", "text"),
        ("style_purity", "Style purity", "f3"),
    ], title=title, scroll=True)


def resolution_sweep(df: pd.DataFrame) -> go.Figure:
    """Leiden communities and modularity at several resolutions.

    Parameters
    ----------
    df : pandas.DataFrame
        ``rq4_resolution_sweep``: ``resolution``, ``n_communities_ge50``,
        ``modularity`` (and optionally ``graph``, ``nmi_vs_reference``).

    Returns
    -------
    plotly.graph_objects.Figure
    """
    title = "Leiden resolution sweep on the collaboration graph"
    if df.empty:
        return _empty(title, "No rows in this table.")
    data = df.sort_values("resolution")
    levels = [f"{r:g}" for r in data["resolution"]]
    nmi = (data["nmi_vs_reference"].to_numpy() if "nmi_vs_reference" in data.columns
           else np.full(len(data), np.nan))
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(
        x=levels, y=data["n_communities_ge50"], name="Communities of 50+ nodes (left axis)",
        marker={"color": BLUE}, customdata=nmi,
        hovertemplate=("Resolution %{x}<br>%{y} communities of 50+ nodes"
                       "<br>NMI with resolution 1: %{customdata:.3f}<extra></extra>")),
        secondary_y=False)
    fig.add_trace(go.Scatter(
        x=levels, y=data["modularity"], name="Modularity (right axis)", mode="lines+markers",
        line={"color": ORANGE, "dash": "dash", "width": 2}, marker={"symbol": "square", "size": 8},
        hovertemplate="Resolution %{x}<br>Modularity %{y:.3f}<extra></extra>"),
        secondary_y=True)
    _layout(fig, title, "Leiden resolution", None, height=400)
    fig.update_xaxes(type="category")
    fig.update_yaxes(title_text="Communities of 50+ nodes", secondary_y=False, rangemode="tozero")
    fig.update_yaxes(title_text="Modularity", secondary_y=True)
    return fig


def umap_scatter(points: pd.DataFrame, clusters: pd.DataFrame | None = None) -> go.Figure:
    """UMAP map of sampled artists, coloured by HDBSCAN cluster, noise in grey.

    Parameters
    ----------
    points : pandas.DataFrame
        ``rq4_cluster_points``: ``x``, ``y``, ``cluster``, ``artist_name``.
        Cluster ``-1`` is noise.
    clusters : pandas.DataFrame, optional
        ``rq4_clusters``, used only for the cluster count and noise share
        in the title.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    title = "Artists on the UMAP map, coloured by HDBSCAN cluster"
    if points.empty:
        return _empty(title, "No rows in this table.")
    if clusters is not None and len(clusters):
        flag = clusters["is_noise"].astype(str).str.lower().eq("true")
        n_clusters = int((~flag).sum())
        noise = float(clusters.loc[flag, "share"].sum())
        title += f" ({n_clusters} clusters, {noise:.1%} noise)"
    data = points.copy()
    data["cluster"] = data["cluster"].astype(int)
    fig = go.Figure()
    hover = ("%{customdata[0]}<br>Cluster %{customdata[1]}<extra></extra>")
    n_colours = len(PALETTE)
    steps = []
    for i, colour in enumerate(PALETTE):
        steps += [[i / n_colours, colour], [(i + 1) / n_colours, colour]]
    for name, part in (
        ("Noise (cluster -1)", data[data["cluster"] < 0]),
        ("Clustered artists", data[data["cluster"] >= 0]),
    ):
        if part.empty:
            continue
        if name.startswith("Noise"):
            marker = {"size": 4, "color": GREY, "opacity": 0.6}
        else:
            # One colour per cluster, cycling through the palette, via a stepped colour scale.
            marker = {
                "size": 4, "opacity": 0.9, "showscale": False, "colorscale": steps,
                "cmin": 0, "cmax": n_colours,
                "color": (part["cluster"].to_numpy() % n_colours) + 0.5,
            }
        fig.add_trace(go.Scattergl(
            x=part["x"], y=part["y"], mode="markers", name=name, marker=marker,
            customdata=np.column_stack([[label_text(a) for a in part["artist_name"]],
                                        part["cluster"].to_numpy()]),
            hovertemplate=hover))
    _layout(fig, title, "UMAP dimension 1", "UMAP dimension 2", height=640)
    fig.update_layout(legend={"orientation": "h", "y": -0.14, "itemsizing": "constant"},
                      margin={"l": 70, "r": 30, "t": 70, "b": 110})
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(showgrid=False, zeroline=False)
    return fig


def clusters_table(df: pd.DataFrame) -> str:
    """Table that describes each HDBSCAN artist cluster.

    Parameters
    ----------
    df : pandas.DataFrame
        ``rq4_clusters``.

    Returns
    -------
    str
        HTML table.
    """
    flag = df["is_noise"].astype(str).str.lower().eq("true")
    data = df.assign(cluster=[
        "noise (-1)" if f else str(c) for c, f in zip(df["cluster"], flag)])
    return html_table(data, [
        ("cluster", "Cluster", "text"), ("size", "Artists", "int"), ("share", "Share", "pct"),
        ("top_styles", "Top styles (share of cluster)", "text"),
        ("mean_share_vinyl", "Vinyl", "pct"), ("mean_share_cd", "CD", "pct"),
        ("mean_share_cassette", "Cassette", "pct"), ("mean_share_file", "File", "pct"),
        ("median_career_span", "Median career (years)", "f2"),
        ("median_n_releases", "Median releases", "f2"),
        ("median_first_year", "Median first year", "int"),
        ("examples", "Example artists", "text"),
    ], title="Artist clusters", scroll=True)


def cluster_agreement_table(df: pd.DataFrame) -> str:
    """Table of how far the clusters agree with other groupings.

    Parameters
    ----------
    df : pandas.DataFrame
        ``rq4_cluster_agreement``.

    Returns
    -------
    str
        HTML table.
    """
    return html_table(df, [
        ("compared_with", "Compared with", "text"), ("n_compared", "Artists compared", "int"),
        ("nmi", "NMI", "f3"), ("ari", "ARI", "f3"), ("n_clusters", "Clusters", "int"),
        ("noise_share", "Noise share", "pct"),
    ], title="Agreement of the clusters with other groupings")
