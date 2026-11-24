"""Write the small result tables that the dashboard reads.

``main.ipynb`` calls ``export_table`` once per result table and
``export_manifest`` once at the end of section 5. The tables are
aggregates (a few KB to a few MB each) and are tracked in git, so keep
raw data out of them. Nothing in here touches Spark.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

from . import config

if TYPE_CHECKING:  # only for type hints; not needed at run time
    import pandas as pd

#: Folder that holds the dashboard's CSV tables and ``manifest.json``.
DASHBOARD_DATA_DIR: Path = config.PROJECT_ROOT / "dashboard" / "data"

#: Significant digits kept in float columns.
SIGNIFICANT_DIGITS: int = 6


def _round_sig(value: float) -> float:
    """Round one float to ``SIGNIFICANT_DIGITS`` significant digits."""
    return float(f"{value:.{SIGNIFICANT_DIGITS}g}")


def _cell_to_json(value: object) -> object:
    """Turn a list, tuple or array cell into a JSON string; leave other cells alone."""
    is_array = hasattr(value, "tolist") and getattr(value, "ndim", 0) > 0
    if isinstance(value, (list, tuple)) or is_array:
        items = value.tolist() if is_array else list(value)
        return json.dumps(items, ensure_ascii=False, default=str)
    return value


def export_table(
    df: "pd.DataFrame",
    name: str,
    index: bool = False,
    directory: Path | str | None = None,
) -> Path:
    """Write a pandas DataFrame to ``<directory>/<name>.csv`` for the dashboard.

    Parameters
    ----------
    df : pandas.DataFrame
        The table to write. It is not modified.
    name : str
        File name without extension, for example ``"rq1_entropy"``. Must
        not contain a path separator.
    index : bool
        Write the index as a first column.
    directory : pathlib.Path or str, optional
        Target folder. Defaults to ``DASHBOARD_DATA_DIR`` (read when the
        function is called, so it can be monkeypatched). Created, with
        its parents, if it does not exist.

    Returns
    -------
    pathlib.Path
        Path of the CSV file.

    Raises
    ------
    ValueError
        If ``name`` is empty or contains a path separator.

    Notes
    -----
    The file is UTF-8, with a header row. Float columns are rounded to 6
    significant digits and written without exponent notation for
    ordinary values; integer, string and boolean columns are written as
    they are. A cell that holds a list, tuple or array becomes a JSON
    string. Missing values are written as empty fields.
    """
    if not name or "/" in name or "\\" in name:
        raise ValueError(f"name must be a plain file name, got {name!r}")
    folder = Path(directory) if directory is not None else DASHBOARD_DATA_DIR
    folder.mkdir(parents=True, exist_ok=True)

    import pandas as pd

    out = df.copy()
    for column in out.columns:
        series = out[column]
        if series.dtype.kind == "f":
            out[column] = series.map(lambda v: v if pd.isna(v) else _round_sig(v))
        elif series.dtype == object:
            out[column] = series.map(_cell_to_json)

    path = folder / f"{name}.csv"
    out.to_csv(path, index=index, encoding="utf-8")
    return path


def export_manifest(entries: dict[str, str], directory: Path | str | None = None) -> Path:
    """Write ``manifest.json``, which lists every exported table.

    Parameters
    ----------
    entries : dict of str to str
        Table name (the ``name`` given to ``export_table``) -> one-line
        description.
    directory : pathlib.Path or str, optional
        Target folder. Defaults to ``DASHBOARD_DATA_DIR``. Created if it
        does not exist.

    Returns
    -------
    pathlib.Path
        Path of ``manifest.json``.

    Raises
    ------
    ValueError
        If a description is not a single line of text.

    Notes
    -----
    Names are written in sorted order so the file does not change from
    run to run.
    """
    for key, text in entries.items():
        if not isinstance(text, str) or "\n" in text:
            raise ValueError(f"description of {key!r} must be a one-line string")
    folder = Path(directory) if directory is not None else DASHBOARD_DATA_DIR
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / "manifest.json"
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(dict(sorted(entries.items())), handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    return path
