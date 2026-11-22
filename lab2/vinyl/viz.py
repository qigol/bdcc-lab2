"""Shared matplotlib helpers so every figure in the report looks the same.

All functions take small pandas DataFrames (the result of ``.toPandas()``
on an aggregated Spark table) and return the matplotlib ``Axes`` so the
caller can adjust it further.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from . import config

if TYPE_CHECKING:  # only for type hints; not needed at run time
    import pandas as pd
    from matplotlib.axes import Axes
    from matplotlib.figure import Figure

#: Folder where ``save_figure`` writes image files.
FIGURE_DIR: Path = config.PROJECT_ROOT / "figures"


def plot_lines(
    data: "pd.DataFrame",
    x: str,
    y: str,
    series: str | None = None,
    title: str = "",
    ylabel: str = "",
    percent: bool = False,
    ax: "Axes | None" = None,
) -> "Axes":
    """Draw one line per series over time.

    Parameters
    ----------
    data : pandas.DataFrame
        Long-format data, one row per (x, series).
    x : str
        Column for the horizontal axis, usually ``"year"``.
    y : str
        Column for the vertical axis, for example ``"share"``.
    series : str, optional
        Column that splits the data into lines (``"style"``,
        ``"format_name"``, ``"major"``). One line if omitted.
    title : str
        Figure title. State the finding, not the chart type.
    ylabel : str
        Vertical axis label. Defaults to the column name.
    percent : bool
        Format the vertical axis as percentages (values are fractions).
    ax : matplotlib.axes.Axes, optional
        Axes to draw on. A new figure is created if omitted.

    Returns
    -------
    matplotlib.axes.Axes
    """
    import matplotlib.pyplot as plt
    from matplotlib.ticker import PercentFormatter

    if ax is None:
        _, ax = plt.subplots(figsize=(9, 5))
    if series is None:
        ordered = data.sort_values(x)
        ax.plot(ordered[x], ordered[y], marker="o")
    else:
        for name, group in data.sort_values(x).groupby(series):
            ax.plot(group[x], group[y], marker="o", label=str(name))
        ax.legend(title=series, frameon=False, bbox_to_anchor=(1.01, 1), loc="upper left")
    if percent:
        ax.yaxis.set_major_formatter(PercentFormatter(1.0))
    ax.set_title(title, loc="left")
    ax.set_xlabel(x)
    ax.set_ylabel(ylabel or y)
    ax.spines[["top", "right"]].set_visible(False)
    return ax


def plot_stacked_share(
    data: "pd.DataFrame",
    x: str,
    y: str,
    series: str,
    title: str = "",
    ax: "Axes | None" = None,
) -> "Axes":
    """Draw a 100% stacked bar chart of shares over time.

    Parameters
    ----------
    data : pandas.DataFrame
        Long-format data, one row per (x, series).
    x : str
        Column for the horizontal axis, usually ``"year"``.
    y : str
        Column with the size of each piece (counts or shares). Each bar
        is rescaled to add up to 100%.
    series : str
        Column that defines the stacked pieces.
    title : str
        Figure title.
    ax : matplotlib.axes.Axes, optional
        Axes to draw on. A new figure is created if omitted.

    Returns
    -------
    matplotlib.axes.Axes
    """
    import matplotlib.pyplot as plt
    from matplotlib.ticker import PercentFormatter

    if ax is None:
        _, ax = plt.subplots(figsize=(9, 5))
    wide = data.pivot_table(index=x, columns=series, values=y, aggfunc="sum").fillna(0)
    wide = wide.div(wide.sum(axis=1), axis=0)
    wide.plot(kind="bar", stacked=True, ax=ax, width=0.85)
    ax.yaxis.set_major_formatter(PercentFormatter(1.0))
    ax.legend(title=series, frameon=False, bbox_to_anchor=(1.01, 1), loc="upper left")
    ax.set_title(title, loc="left")
    ax.set_xlabel(x)
    ax.set_ylabel("share")
    ax.spines[["top", "right"]].set_visible(False)
    return ax


def save_figure(fig: "Figure", name: str, dpi: int = 200) -> Path:
    """Save a figure under ``figures/`` as a PNG.

    Parameters
    ----------
    fig : matplotlib.figure.Figure
        Figure to save (``ax.figure`` for the helpers above).
    name : str
        File name without extension, for example ``"rq1_style_shares"``.
    dpi : int
        Resolution.

    Returns
    -------
    pathlib.Path
        Path of the saved file.
    """
    FIGURE_DIR.mkdir(parents=True, exist_ok=True)
    path = FIGURE_DIR / f"{name}.png"
    fig.savefig(path, dpi=dpi, bbox_inches="tight")
    return path
