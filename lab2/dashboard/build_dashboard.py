"""Build ``vinyl_decade.html``, the offline dashboard of the Vinyl Decade study.

The page reads the small CSV tables that ``main.ipynb`` exports to
``dashboard/data/`` and writes one self-contained HTML file: plotly.js is
embedded once, there is no CDN, no web font and no network request.

Usage, from ``lab2/``::

    ../.venv/bin/python dashboard/build_dashboard.py [DATA_DIR] [-o OUTPUT]

``DATA_DIR`` defaults to ``dashboard/data`` and ``OUTPUT`` to
``dashboard/vinyl_decade.html``. A missing CSV gives a "table not
available" note in its place and a warning on standard error. A present
CSV that lacks a required column stops the build with an error.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import pandas as pd
import plotly.io as pio
from plotly.offline import get_plotlyjs

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from vinyl import dashboard as charts  # noqa: E402

#: Default input and output locations.
DEFAULT_DATA_DIR: Path = HERE / "data"
DEFAULT_OUTPUT: Path = HERE / "vinyl_decade.html"

TITLE = "The Vinyl Decade"
SUBTITLE = "Discogs 2016-2026, snapshots of 1 Jan 2020 and 1 Oct 2026"
PARTIAL_NOTE = "The 2026 data covers Jan-Sep only: the main snapshot is dated 1 Oct 2026."
PIPELINE = (
    "Pipeline: the Discogs XML dumps are parsed once with PySpark into Parquet, aggregated "
    "into small tables, exported by main.ipynb as CSV, and those tables are what this page plots."
)

#: Plotly config shared by all charts: no logo, no link to the Plotly cloud.
PLOT_CONFIG: dict[str, object] = {
    "responsive": True,
    "displaylogo": False,
    "showTips": False,
    "modeBarButtonsToRemove": ["sendDataToCloud"],
}

# Captions are trusted HTML written here; data values never go into them.
CAP_PARTIAL = (
    "2026 covers Jan-Sep only (shaded), and 2024-2026 are undercounted because releases are "
    'catalogued with a delay. See the <a href="#snapshot" data-tab="snapshot">Snapshot '
    "comparison</a> tab."
)
CAP_INDEPENDENT = (
    '"Independent" means not linked to a major\'s tree in Discogs. A label that Discogs does '
    "not link to Universal, Sony or Warner counts as independent even if a major owns it."
)
CAP_PLANTS = (
    "Pressing-plant shares cover only the vinyl releases that name a plant (see the coverage "
    "chart), not all vinyl releases."
)


@dataclass(frozen=True)
class Panel:
    """One chart or table on the page.

    Attributes
    ----------
    key : str
        Short name used in the build report.
    tables : tuple of str
        Required CSV tables, passed to ``render`` in this order.
    render : callable
        Takes the DataFrames and returns a Plotly ``Figure`` or an HTML string.
    caption : str
        Trusted HTML shown under the panel.
    optional : tuple of str
        Extra tables passed after the required ones (``None`` if missing).
    """

    key: str
    tables: tuple[str, ...]
    render: Callable[..., object]
    caption: str = ""
    optional: tuple[str, ...] = ()


@dataclass(frozen=True)
class Section:
    """One tab of the page."""

    key: str
    label: str
    intro: str
    panels: tuple[Panel, ...] = field(default_factory=tuple)


SECTIONS: tuple[Section, ...] = (
    Section(
        "overview", "Overview",
        PIPELINE,
        (
            Panel("key_numbers", ("data_summary",), charts.summary_tiles),
        ),
    ),
    Section(
        "rq1", "RQ1 Genres and styles",
        "How the mix of genres and styles shifted, and whether the catalogue became more "
        "fragmented.",
        (
            Panel("genre_shares", ("rq1_genre_shares",), charts.genre_share_lines,
                  CAP_PARTIAL + " A release can have several genres, so the shares add up to "
                  "more than 100%."),
            Panel("style_movers", ("rq1_style_movers",), charts.style_movers_bars,
                  "Gains are blue and losses orange. Only styles with at least 500 releases "
                  "are ranked. Hover a bar for the shares behind it."),
            Panel("evenness", ("rq1_entropy_sensitivity",), charts.evenness_band,
                  CAP_PARTIAL + " The band is the range over random samples of equal size "
                  "each year, so the line does not follow the changing number of releases."),
            Panel("style_network", ("rq1_style_network",), charts.style_network_dual,
                  CAP_PARTIAL + " Density is the share of possible style pairs that occur "
                  "together on a release. Modularity measures how clearly the styles split "
                  "into groups."),
        ),
    ),
    Section(
        "rq2", "RQ2 Formats and reissues",
        "How the format mix changed, and how much of the vinyl boom is reissues.",
        (
            Panel("format_shares", ("rq2_format_shares",), charts.format_share_lines,
                  CAP_PARTIAL + " Minor formats are grouped as Other."),
            Panel("vinyl_reissues", ("rq2_vinyl_reissues",), charts.vinyl_reissue_bars,
                  CAP_PARTIAL + " The dropdown picks the reissue definition: a format tag "
                  "(Reissue, Repress or Remastered), a release year later than the year of "
                  "its master, or either."),
            Panel("growth_explained", ("rq2_growth_explained",), charts.growth_explained_bars,
                  "Growth explained is the rise in reissues divided by the rise in all vinyl "
                  "releases between the first and last full year."),
            Panel("reissue_by_genre", ("rq2_reissue_by_genre",), charts.reissue_share_by_genre,
                  CAP_PARTIAL + " Use the dropdown to show one genre, or click legend entries "
                  "to hide lines."),
            Panel("reissue_age", ("rq2_reissue_age",), charts.reissue_age_bars,
                  "Pick a year in the dropdown; 2026 covers Jan-Sep only and is labelled as "
                  "partial, and recent years are undercounted because releases are catalogued "
                  "with a delay. Age is the release year minus the year of the master."),
            Panel("pressing_coverage", ("rq2_pressing_plants",), charts.pressing_coverage,
                  CAP_PLANTS + " " + CAP_PARTIAL),
            Panel("pressing_plants", ("rq2_pressing_plants",), charts.pressing_plant_share,
                  CAP_PLANTS + " " + CAP_PARTIAL),
        ),
    ),
    Section(
        "rq3", "RQ3 Label power",
        "How concentrated the Universal, Sony and Warner label trees are, and how much of "
        "the catalogue sits outside them.",
        (
            Panel("major_share", ("rq3_major_share",), charts.major_share_lines,
                  CAP_PARTIAL + " " + CAP_INDEPENDENT),
            Panel("group_concentration", ("rq3_group_concentration",), charts.group_hhi_lines,
                  "HHI is the sum of squared artist shares inside a label group. A higher "
                  "value means fewer artists make up more of the group. " + CAP_INDEPENDENT
                  + " " + CAP_PARTIAL),
            Panel("parent_coverage", ("rq3_parent_coverage",), charts.parent_coverage_bars,
                  CAP_PARTIAL + " A release on several labels takes its best class, in this "
                  "order: in a tree, on a label alone, self-released, unknown. Releases on a "
                  "label with no parent and no sublabels may include major-owned output that "
                  "Discogs does not link to a tree."),
            Panel("writer_levels", ("rq3_writer_levels",), charts.writer_levels_lines,
                  CAP_PARTIAL + " A release with credits at both levels counts in both of "
                  "those lines and once in the line for either level."),
        ),
    ),
    Section(
        "snapshot", "Snapshot comparison",
        "What the 2020 and 2026 snapshots say about cataloguing lag, and which labels "
        "changed owner.",
        (
            Panel("completeness_by_age", ("cmp_lag",), charts.completeness_by_age,
                  "Completeness is the share of a year's releases, as catalogued in Oct 2026, "
                  "that were already in the Jan 2020 snapshot. The same curve by age is used "
                  "to estimate the undercount of 2023-2026."),
            Panel("undercount", ("cmp_undercount",), charts.undercount_bars,
                  "Hatched bars are 2026, which covers Jan-Sep only. The estimate divides the "
                  "catalogued count by the completeness at the same age; for 2026 it also "
                  "scales Jan-Sep up to a full year with a seasonal factor."),
            Panel("lag_by_format", ("cmp_lag_by_format",), charts.lag_by_format,
                  "Release years 2016-2019, the ones old enough to compare."),
            Panel("lag_by_genre", ("cmp_lag_by_genre",), charts.lag_by_genre,
                  "Release years 2016-2019, for the 8 largest genres."),
            Panel("major_moves", ("cmp_major_moves",), charts.major_moves_table,
                  CAP_INDEPENDENT),
        ),
    ),
    Section(
        "rq4", "RQ4 Communities and clusters",
        "Communities in the collaboration and artist-label graphs, and clusters of artists "
        "in the UMAP map.",
        (
            Panel("graph_summary", ("rq4_graph_summary",), charts.graph_summary_table,
                  "NMI, ARI and purity compare each graph's Leiden communities with the genre "
                  "or style tag of its nodes (column Compared with)."),
            Panel("communities_collab", ("rq4_communities_collab",),
                  lambda df: charts.communities_table(
                      df, "Largest communities of the collaboration graph", "Key artists"),
                  "Purity is the share of tagged nodes in the community that carry its most "
                  "common tag."),
            Panel("communities_artist_label", ("rq4_communities_artist_label",),
                  lambda df: charts.communities_table(
                      df, "Largest communities of the artist-label graph", "Key nodes"),
                  "Nodes are artists and labels. Purity is as above."),
            Panel("resolution_sweep", ("rq4_resolution_sweep",), charts.resolution_sweep,
                  "Hover a bar for the agreement (NMI) with the partition at resolution 1."),
            Panel("umap", ("rq4_cluster_points",), charts.umap_scatter,
                  "A sample of the artists. Grey points are noise, artists that HDBSCAN did "
                  "not assign to a cluster. Only 11 colours are colourblind-safe, so nearby "
                  "clusters can share a colour; hover to read the cluster number.",
                  optional=("rq4_clusters",)),
            Panel("cluster_agreement", ("rq4_cluster_agreement",),
                  charts.cluster_agreement_table),
            Panel("clusters", ("rq4_clusters",), charts.clusters_table,
                  "Career is the span between an artist's first and last release year; the "
                  "format columns are the mean share of an artist's releases in that format."),
        ),
    ),
)

CSS = """
:root{--ink:#1c1c1c;--muted:#4a4a4a;--line:#d6d6d6;--accent:#1f77b4;--bg:#f6f7f9}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);line-height:1.45;
font-family:system-ui,-apple-system,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif}
[hidden]{display:none!important}
header{background:#fff;border-bottom:1px solid var(--line);padding:18px 24px 10px}
header h1{margin:0;font-size:1.7rem}
header .subtitle{margin:2px 0 0;font-size:1.05rem;color:var(--muted)}
header .note{margin:4px 0 8px;font-size:.9rem;color:var(--muted)}
.tabs{display:flex;flex-wrap:wrap;gap:4px;margin-top:8px}
.tabs button{font:inherit;font-size:.92rem;border:1px solid var(--line);border-bottom:none;
background:#eef0f3;color:var(--ink);padding:8px 14px;border-radius:6px 6px 0 0;cursor:pointer}
.tabs button[aria-selected="true"]{background:var(--bg);border-color:#9aa3ad;font-weight:600;
box-shadow:inset 0 3px 0 var(--accent)}
.tabs button:focus-visible,a:focus-visible,.table-wrap:focus-visible{outline:3px solid #ff7f0e;
outline-offset:2px}
main{max-width:1240px;margin:0 auto;padding:16px 24px 40px}
h2{margin:8px 0 4px;font-size:1.35rem}
h3{margin:18px 0 6px;font-size:1.05rem}
.intro{margin:0 0 14px;color:var(--muted)}
figure.panel{margin:0 0 20px;padding:10px 12px;background:#fff;border:1px solid var(--line);
border-radius:6px}
figcaption{margin-top:4px;font-size:.82rem;color:var(--muted)}
figcaption.source{font-size:.76rem}
.panel .missing{margin:6px 0;padding:10px;background:#fff7e0;border:1px solid #e0c36a;
border-radius:4px;font-size:.9rem}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:12px}
.tile{border:1px solid var(--line);border-radius:6px;padding:12px;background:#fbfcfd}
.tile-label{font-size:.85rem;color:var(--muted)}
.tile-value{font-size:1.8rem;font-weight:600;color:var(--accent)}
.tile-note{font-size:.78rem;color:var(--muted)}
.table-wrap{overflow:auto;border:1px solid var(--line);border-radius:4px}
.table-wrap.scroll{max-height:520px}
table{border-collapse:collapse;width:100%;font-size:.82rem}
th,td{padding:5px 8px;border-bottom:1px solid #e8e8e8;text-align:left;vertical-align:top}
th{background:#eef0f3;position:sticky;top:0}
td.num,th.num{text-align:right;white-space:nowrap}
code{font-size:.95em}
footer{max-width:1240px;margin:0 auto;padding:0 24px 24px;font-size:.8rem;color:var(--muted)}
"""

JS = """
(function () {
  var tabs = Array.prototype.slice.call(document.querySelectorAll('[role="tab"]'));
  function show(key) {
    tabs.forEach(function (t) {
      var on = t.getAttribute('data-tab') === key;
      t.setAttribute('aria-selected', on ? 'true' : 'false');
      t.tabIndex = on ? 0 : -1;
    });
    Array.prototype.forEach.call(document.querySelectorAll('[role="tabpanel"]'), function (p) {
      var on = p.id === 'panel-' + key;
      p.hidden = !on;
      if (on && window.Plotly) {
        Array.prototype.forEach.call(p.querySelectorAll('.plotly-graph-div'), function (g) {
          window.Plotly.Plots.resize(g);
        });
      }
    });
    try { history.replaceState(null, '', '#' + key); } catch (e) { /* file:// may refuse */ }
  }
  tabs.forEach(function (t, i) {
    t.addEventListener('click', function () { show(t.getAttribute('data-tab')); });
    t.addEventListener('keydown', function (e) {
      var d = e.key === 'ArrowRight' ? 1 : (e.key === 'ArrowLeft' ? -1 : 0);
      if (d) {
        var next = tabs[(i + d + tabs.length) % tabs.length];
        next.focus();
        show(next.getAttribute('data-tab'));
      }
    });
  });
  Array.prototype.forEach.call(document.querySelectorAll('a[data-tab]'), function (a) {
    a.addEventListener('click', function (e) {
      e.preventDefault();
      show(a.getAttribute('data-tab'));
      window.scrollTo(0, 0);
    });
  });
  var start = window.location.hash.replace('#', '');
  if (!document.getElementById('panel-' + start)) { start = tabs[0].getAttribute('data-tab'); }
  show(start);
})();
"""


def _source_line(tables: tuple[str, ...], manifest: dict[str, str]) -> str:
    """Small 'Data:' line that names the CSV files behind a panel."""
    parts = []
    for name in tables:
        text = f"<code>{charts.esc(name)}.csv</code>"
        if manifest.get(name):
            text += f": {charts.esc(manifest[name])}"
        parts.append(text)
    return f'<figcaption class="source">Data: {"; ".join(parts)}</figcaption>'


def _render_panel(
    panel: Panel,
    tables: dict[str, pd.DataFrame | None],
    manifest: dict[str, str],
    report: dict[str, object],
) -> str:
    """Render one panel to HTML, or a "table not available" note.

    A panel whose required table is missing or empty is not built; a
    warning goes to standard error and the build report.
    """
    label = charts.esc(panel.key.replace("_", " "))
    missing = [t for t in panel.tables if tables.get(t) is None]
    empty = [t for t in panel.tables if t not in missing and tables[t].empty]
    problem = missing or empty
    if problem:
        names = ", ".join(f"{t}.csv" for t in problem)
        reason = "missing" if missing else "empty"
        charts.warn(f"panel '{panel.key}' not built: {names} is {reason}.")
        report["skipped"].append(panel.key)  # type: ignore[union-attr]
        return (
            f'<figure class="panel" aria-label="{label}"><p class="missing">'
            f"Table not available: <code>{charts.esc(names)}</code> ({reason}), so "
            f"&quot;{label}&quot; was not built.</p></figure>"
        )
    frames = [tables[t] for t in panel.tables] + [tables.get(t) for t in panel.optional]
    result = panel.render(*frames)
    report["built"].append(panel.key)  # type: ignore[union-attr]
    if isinstance(result, str):
        body = f'<div class="html-panel">{result}</div>'
    else:
        height = result.layout.height or 450
        body = pio.to_html(
            result, include_plotlyjs=False, full_html=False, config=PLOT_CONFIG,
            default_width="100%", default_height=f"{height}px",
        )
    caption = f"<figcaption>{panel.caption}</figcaption>" if panel.caption else ""
    source = _source_line(panel.tables + tuple(t for t in panel.optional if tables.get(t) is not None),
                          manifest)
    return f'<figure class="panel" aria-label="{label}">{body}{caption}{source}</figure>'


def build_html(data_dir: str | Path) -> tuple[str, dict[str, list[dict[str, object]]]]:
    """Build the whole page.

    Parameters
    ----------
    data_dir : str or pathlib.Path
        Folder with the exported CSV tables and ``manifest.json``.

    Returns
    -------
    html : str
        The complete, self-contained HTML document.
    report : dict
        ``{"sections": [{"key", "label", "built", "skipped"}, ...]}``.

    Raises
    ------
    vinyl.dashboard.DashboardError
        If a present CSV lacks a required column.
    """
    tables = charts.load_tables(data_dir)
    manifest = charts.load_manifest(data_dir)
    report: dict[str, list[dict[str, object]]] = {"sections": []}
    tab_buttons: list[str] = []
    panels_html: list[str] = []
    for i, section in enumerate(SECTIONS):
        info: dict[str, object] = {
            "key": section.key, "label": section.label, "built": [], "skipped": []}
        body = "".join(_render_panel(p, tables, manifest, info) for p in section.panels)
        report["sections"].append(info)
        selected = "true" if i == 0 else "false"
        tab_buttons.append(
            f'<button type="button" role="tab" id="tab-{section.key}" '
            f'aria-controls="panel-{section.key}" aria-selected="{selected}" '
            f'data-tab="{section.key}" tabindex="{0 if i == 0 else -1}">'
            f"{charts.esc(section.label)}</button>"
        )
        panels_html.append(
            f'<section role="tabpanel" id="panel-{section.key}" '
            f'aria-labelledby="tab-{section.key}"{"" if i == 0 else " hidden"}>'
            f"<h2>{charts.esc(section.label)}</h2>"
            f'<p class="intro">{charts.esc(section.intro)}</p>{body}</section>'
        )
    head = (
        '<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        f"<title>{charts.esc(TITLE)}: Discogs 2016-2026</title>"
        f"<style>{CSS}</style><script>{get_plotlyjs()}</script></head>"
    )
    header = (
        f"<body><header><h1>{charts.esc(TITLE)}</h1>"
        f'<p class="subtitle">{charts.esc(SUBTITLE)}</p>'
        f'<p class="note">{charts.esc(PARTIAL_NOTE)}</p>'
        f'<nav class="tabs" role="tablist" aria-label="Sections">{"".join(tab_buttons)}</nav>'
        "</header>"
    )
    footer = (
        "<footer>Built from the CSV tables exported by main.ipynb. Rebuild with "
        "<code>cd lab2 &amp;&amp; ../.venv/bin/python dashboard/build_dashboard.py</code>."
        "</footer>"
    )
    page = f"{head}{header}<main>{''.join(panels_html)}</main>{footer}<script>{JS}</script></body></html>"
    return page, report


def build(data_dir: str | Path = DEFAULT_DATA_DIR, output: str | Path = DEFAULT_OUTPUT) -> dict:
    """Build the page and write it to ``output``.

    Parameters
    ----------
    data_dir : str or pathlib.Path
        Folder with the exported CSV tables.
    output : str or pathlib.Path
        HTML file to write.

    Returns
    -------
    dict
        The build report from ``build_html`` plus ``"path"`` and ``"bytes"``.
    """
    page, report = build_html(data_dir)
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(page, encoding="utf-8")
    return {**report, "path": str(path), "bytes": path.stat().st_size}


def main(argv: list[str] | None = None) -> int:
    """Command-line entry point.

    Parameters
    ----------
    argv : list of str, optional
        Arguments; ``sys.argv[1:]`` if omitted.

    Returns
    -------
    int
        0 on success, 1 if a present CSV lacks a required column.
    """
    parser = argparse.ArgumentParser(description="Build the offline Vinyl Decade dashboard.")
    parser.add_argument("data_dir", nargs="?", default=str(DEFAULT_DATA_DIR),
                        help="folder with the exported CSV tables")
    parser.add_argument("-o", "--output", default=str(DEFAULT_OUTPUT), help="HTML file to write")
    args = parser.parse_args(argv)
    try:
        result = build(args.data_dir, args.output)
    except charts.DashboardError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(f"Wrote {result['path']} ({result['bytes'] / 1e6:.2f} MB, {result['bytes']:,} bytes)")
    for section in result["sections"]:
        built, skipped = section["built"], section["skipped"]
        line = f"  {section['label']}: {len(built)} built ({', '.join(built)})"
        if skipped:
            line += f"; {len(skipped)} skipped ({', '.join(skipped)})"
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
