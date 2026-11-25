"""Self-test for T13, the offline Plotly dashboard (no Spark, synthetic CSVs only).

Builds the page from tiny synthetic CSV tables written to a temp folder and
checks: all six sections are present, plotly.js is embedded once, nothing
points to the network, a missing or empty CSV gives a "not available" note
instead of a crash, a missing column fails with a clear message, and names
such as ``<script>`` are escaped. Exit code 0 only if every check passes.

Usage, from ``lab2/``:
``timeout 600 ../.venv/bin/python scripts/test_t13.py``
"""

from __future__ import annotations

import contextlib
import io
import json
import re
import sys
import tempfile
from html.parser import HTMLParser
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "dashboard"))

import build_dashboard  # noqa: E402
from vinyl import dashboard as charts  # noqa: E402

FAILURES: list[str] = []

EVIL = "<script>alert(1)</script>"
SECTION_KEYS = ["overview", "rq1", "rq2", "rq3", "snapshot", "rq4"]
YEARS = [2024, 2025, 2026]


def check(name: str, ok: bool, detail: str = "") -> None:
    """Record and print one check result."""
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" - {detail}" if detail and not ok else ""))
    if not ok:
        FAILURES.append(name)


def synthetic_tables(evil: str = "Artist A") -> dict[str, pd.DataFrame]:
    """Tiny tables with the column names of the real exports.

    Parameters
    ----------
    evil : str
        Name used for artists, labels and styles, to test escaping.
    """
    genres = ["Rock", "Jazz"]
    formats = ["Vinyl", "CD"]
    groups = ["Universal", "Sony", "Warner", "Independent"]
    t: dict[str, pd.DataFrame] = {}
    t["data_summary"] = pd.DataFrame({
        "metric": ["releases_in_scope", "releases_with_style", "year_filter"],
        "count": [1000, 800, None], "share": [None, 0.8, None],
        "note": ["releases in scope", "with a style", "year filter"]})
    t["rq1_genre_shares"] = pd.DataFrame([
        {"year": y, "genre": g, "n_releases": 10, "n_year": 20, "share": 0.5 - 0.1 * i}
        for y in YEARS for i, g in enumerate(genres)])
    t["rq1_style_movers"] = pd.DataFrame({
        "style": [evil, "Techno", "Dub"], "share_start": [0.01, 0.02, 0.03],
        "n_start": [10, 20, 30], "share_end": [0.03, 0.01, 0.03], "n_end": [30, 10, 30],
        "change": [0.02, -0.01, 0.0], "start_year": 2016, "end_year": 2025})
    t["rq1_entropy_sensitivity"] = pd.DataFrame({
        "year": YEARS, "evenness_full": [0.77, 0.78, 0.79], "evenness_sub_mean": [0.77, 0.78, 0.79],
        "evenness_sub_min": [0.76, 0.77, 0.78], "evenness_sub_max": [0.78, 0.79, 0.80]})
    t["rq1_style_network"] = pd.DataFrame({
        "year": YEARS, "n_nodes": 300, "n_edges": 2000, "density": [0.04, 0.05, 0.05],
        "n_components": 2, "largest_component_share": 0.99, "modularity": [0.6, 0.65, 0.66]})
    t["rq2_format_shares"] = pd.DataFrame([
        {"year": y, "format_name": f, "n_releases": 10, "n_year": 20, "share": 0.5}
        for y in YEARS for f in formats + ["Other"]])
    t["rq2_vinyl_reissues"] = pd.DataFrame([
        {"year": y, "n_releases": 10, "n_reissues": 4, "n_new": 6, "reissue_share": 0.4, "flag": f}
        for y in YEARS for f in ["is_reissue", "is_reissue_master", "is_reissue_any"]])
    t["rq2_growth_explained"] = pd.DataFrame({
        "flag": ["is_reissue", "is_reissue_any"], "releases_start": 70, "releases_end": 90,
        "reissues_start": [10, 20], "reissues_end": [15, 30], "growth_explained": [0.25, 0.5],
        "start_year": 2016, "end_year": 2025})
    t["rq2_reissue_by_genre"] = pd.DataFrame([
        {"year": y, "genre": g, "n_releases": 10, "n_reissues": 4, "n_new": 6, "reissue_share": 0.4}
        for y in YEARS for g in genres])
    t["rq2_reissue_age"] = pd.DataFrame([
        {"year": y, "age_bin": b, "age_bin_order": i, "n_releases": 5 + i, "median_age": 12,
         "format_name": f}
        for y in YEARS for f in ["Vinyl", "CD"] for i, b in enumerate(["1", "2-4", "50+"])])
    t["rq2_pressing_plants"] = pd.DataFrame([
        {"year": y, "n_format": 100, "n_with_role": 20, "coverage": 0.2, "company_id": i,
         "company_name": n, "n_releases": 10 - i, "share_of_format_year": 0.5 - 0.1 * i,
         "total_full_years": 50, "rank": i + 1}
        for y in YEARS for i, n in enumerate(["GZ Media", "MPO"])])
    t["rq3_major_share"] = pd.DataFrame([
        {"year": y, "major": g, "n_releases": 10, "n_year": 40, "share": 0.25}
        for y in YEARS for g in groups])
    rows = [{"major": g, "n_pairs": 100, "n_artists": 50, "top_artist_id": 1,
             "top_artist_name": evil, "top_artist_share": 0.01, "hhi": 1e-4,
             "scope": "overall", "year": None} for g in groups]
    rows += [{**r, "scope": "year", "year": y} for r in rows for y in YEARS]
    t["rq3_group_concentration"] = pd.DataFrame(rows)
    t["rq3_parent_coverage"] = pd.DataFrame({
        "year": YEARS, "on_root_alone": 4, "in_tree": 4, "self_released": 2, "unknown_label": 0,
        "n_releases": 10, "share_on_root_alone": 0.4, "share_in_tree": 0.4,
        "share_self_released": 0.2, "share_unknown_label": 0.0})
    t["rq3_writer_levels"] = pd.DataFrame([
        {"year": y, "level": lv, "n_releases": 10} for y in YEARS for lv in ["any", "release", "track"]])
    t["cmp_lag"] = pd.DataFrame({
        "year": [2018, 2019], "n_new": 100, "n_already": [70, 55], "n_added_later": [30, 45],
        "completeness": [0.7, 0.55], "age": [2, 1]})
    t["cmp_undercount"] = pd.DataFrame({
        "year": YEARS, "n_releases": [300, 280, 150], "age": [3, 2, 1],
        "completeness": [0.74, 0.69, 0.55], "partial_year": [False, False, True],
        "n_estimated": [405.0, 405.0, 380.0], "n_estimated_no_seasonal": [405.0, 405.0, 280.0],
        "seasonal_factor": [1.0, 1.0, 1.35]})
    t["cmp_lag_by_format"] = pd.DataFrame([
        {"year": y, "format_name": f, "n_new": 50, "n_already": 30, "n_added_later": 20,
         "completeness": 0.6, "age": 2019 - y + 1} for y in [2018, 2019] for f in formats])
    t["cmp_lag_by_genre"] = pd.DataFrame([
        {"year": y, "genre": g, "n_new": 50, "n_already": 30, "n_added_later": 20,
         "completeness": 0.6, "age": 2019 - y + 1} for y in [2018, 2019] for g in genres])
    t["cmp_major_moves"] = pd.DataFrame({
        "label_id": [1, 2], "label_name": [evil, "PIAS"], "major_old": "Independent",
        "major_new": ["Universal", "Sony"], "root_id_old": 1, "root_id_new": 2,
        "parent_id_old": 1.0, "parent_id_new": 2, "move": ["into_major", "into_major"],
        "n_releases": [2000, 1900]})
    t["rq4_graph_summary"] = pd.DataFrame({
        "graph": ["collaboration", "collaboration"], "min_weight": 2, "nodes": 100, "edges": 150,
        "communities": 20, "communities_ge50": 5, "largest_community_share": 0.04,
        "modularity": 0.9, "tag": ["genre", "style"], "n_artist_nodes": 100, "n_nodes": 100,
        "nmi": 0.3, "ari": 0.03, "purity": 0.87, "share_tagged": 1.0})
    comm = pd.DataFrame({
        "community": [1, 2], "size": [900, 800], "top_nodes": [f"{evil}, Bach", "Mozart"],
        "n_genre_tagged": 100, "top_genres": ["Rock (0.9)", "Jazz (0.8)"],
        "genre_purity": [0.9, 0.8], "n_style_tagged": 100, "top_styles": [evil, "Bop (0.5)"],
        "style_purity": [0.5, 0.4]})
    t["rq4_communities_collab"] = comm
    t["rq4_communities_artist_label"] = comm.copy()
    t["rq4_resolution_sweep"] = pd.DataFrame({
        "resolution": [0.5, 1.0, 2.0], "n_communities": 100, "n_communities_ge50": [50, 80, 120],
        "largest_share": 0.05, "modularity": [0.92, 0.93, 0.92],
        "nmi_vs_reference": [0.96, 1.0, 0.96], "graph": "collaboration"})
    t["rq4_cluster_points"] = pd.DataFrame({
        "x": [0.0, 1.0, 2.0, 3.0], "y": [0.0, 1.0, 0.5, 2.0], "cluster": [-1, 0, 1, 1],
        "artist_name": ["Noisy", evil, "B", "C"]})
    t["rq4_clusters"] = pd.DataFrame({
        "cluster": [-1, 0, 1], "size": [10, 20, 30], "share": [0.17, 0.33, 0.5],
        "is_noise": [True, False, False], "top_styles": ["Techno (0.2)", evil, "Dub (0.4)"],
        "mean_share_vinyl": 0.2, "mean_share_cd": 0.1, "mean_share_cassette": 0.05,
        "mean_share_file": 0.6, "median_career_span": 6.0, "median_n_releases": 9.0,
        "median_first_year": 2017.0, "examples": [evil, "A; B", "C"]})
    t["rq4_cluster_agreement"] = pd.DataFrame({
        "compared_with": ["Leiden communities (collaboration graph)", "dominant genre"],
        "n_common": 100, "n_compared": 90, "nmi": [0.56, 0.4], "ari": [0.3, 0.08],
        "n_clusters": 2, "noise_share": 0.17})
    return t


def write_tables(folder: Path, tables: dict[str, pd.DataFrame], skip: tuple[str, ...] = ()) -> None:
    """Write the synthetic tables and a manifest into ``folder``."""
    folder.mkdir(parents=True, exist_ok=True)
    for name, frame in tables.items():
        if name not in skip:
            frame.to_csv(folder / f"{name}.csv", index=False)
    manifest = {name: f"Synthetic {name}." for name in tables}
    (folder / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


class _Tags(HTMLParser):
    """Collect every network-looking src/href and every script tag."""

    def __init__(self) -> None:
        super().__init__()
        self.network: list[tuple[str, str, str]] = []
        self.scripts = 0
        self.links = 0
        self.title = ""
        self._in_title = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.scripts += tag == "script"
        self.links += tag == "link"
        self._in_title = tag == "title"
        for key, value in attrs:
            if key in ("src", "href", "action", "data", "poster", "srcset") and value and re.match(
                r"\s*(https?:)?//", value, flags=re.I
            ):
                self.network.append((tag, key, value))

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self.title += data

    def handle_endtag(self, tag: str) -> None:
        self._in_title = False


def parse(page: str) -> _Tags:
    """Parse a page and return the collected tag facts."""
    parser = _Tags()
    parser.feed(page)
    return parser


def build_quiet(folder: Path) -> tuple[str, dict, str]:
    """Build a page, returning the HTML, the report and what went to stderr."""
    err = io.StringIO()
    with contextlib.redirect_stderr(err):
        page, report = build_dashboard.build_html(folder)
    return page, report, err.getvalue()


def test_full_build(tmp: Path) -> str:
    """All sections, one plotly.js, no network references."""
    print("full build from synthetic CSVs")
    folder = tmp / "full"
    write_tables(folder, synthetic_tables())
    page, report, err = build_quiet(folder)
    keys = [s["key"] for s in report["sections"]]
    check("six sections in the report", keys == SECTION_KEYS, str(keys))
    check("each section has a tab and a panel",
          all(f'id="tab-{k}"' in page and f'id="panel-{k}"' in page for k in SECTION_KEYS))
    check("every section built at least one panel", all(s["built"] for s in report["sections"]))
    check("nothing skipped with complete data", not any(s["skipped"] for s in report["sections"]),
          str([s["skipped"] for s in report["sections"]]))
    check("no warnings with complete data", err == "", err)
    check("plotly.js embedded exactly once", page.count("plotly.js v") == 1, str(page.count("plotly.js v")))
    n_figs = page.count('class="plotly-graph-div"')
    n_calls = len(re.findall(r'Plotly\.newPlot\(\s*"[0-9a-f-]{36}"', page))
    check("every figure draws with Plotly.newPlot", n_figs >= 20 and n_calls == n_figs,
          f"{n_figs} figures, {n_calls} newPlot calls")
    facts = parse(page)
    check("no src/href that points to the network", not facts.network, str(facts.network))
    check("no <link> tags (no external CSS or fonts)", facts.links == 0)
    check("no @import or url(http in the CSS", "@import" not in build_dashboard.CSS
          and "url(http" not in build_dashboard.CSS)
    check("page has a <title>", "The Vinyl Decade" in facts.title, facts.title)
    check("header title, subtitle and 2026 note",
          "<h1>The Vinyl Decade</h1>" in page
          and "Discogs 2016-2026, snapshots of 1 Jan 2020 and 1 Oct 2026" in page
          and "Jan-Sep" in page)
    check("honesty captions present",
          "not linked to a major's tree in Discogs" in page
          and "only the vinyl releases that name a plant" in page
          and 'data-tab="snapshot"' in page)
    check("tab switching script present", "role=\"tab\"" in page and "Plots.resize" in page)
    check("charts have axis titles", '"xaxis":{' in page and '"title":{"text":"Year"}' in page)
    check("colorblind-safe palette in use", charts.PALETTE == list(charts.qualitative.Safe[:-1])
          and "rgb(136, 204, 238)" in page)
    check("size under 15 MB", len(page.encode("utf-8")) < 15_000_000)
    return page


def test_umap_trace() -> None:
    """The scatter uses WebGL, grey noise, and the artist name in the hover."""
    print("UMAP scatter")
    t = synthetic_tables()
    fig = charts.umap_scatter(t["rq4_cluster_points"], t["rq4_clusters"])
    check("traces are scattergl", all(tr.type == "scattergl" for tr in fig.data))
    noise = [tr for tr in fig.data if tr.name.startswith("Noise")]
    check("noise trace is grey", len(noise) == 1 and noise[0].marker.color == charts.GREY)
    check("hover shows artist name and cluster",
          all("customdata[0]" in tr.hovertemplate and "Cluster" in tr.hovertemplate for tr in fig.data))
    check("title has cluster count and noise share", "2 clusters" in fig.layout.title.text
          and "17.0% noise" in fig.layout.title.text, fig.layout.title.text)


def test_missing_csv(tmp: Path) -> None:
    """A missing CSV, an empty CSV and a missing manifest do not crash."""
    print("missing and empty CSVs")
    tables = synthetic_tables()
    folder = tmp / "missing"
    write_tables(folder, tables, skip=("rq1_genre_shares", "cmp_lag"))
    (folder / "manifest.json").unlink()
    page, report, err = build_quiet(folder)
    check("build succeeds without the CSVs", "panel-rq1" in page)
    check("'not available' note shown", page.count("Table not available") == 2
          and "rq1_genre_shares.csv" in page, str(page.count("Table not available")))
    check("warning printed for each missing CSV",
          "rq1_genre_shares.csv not found" in err and "cmp_lag.csv not found" in err, err)
    rq1 = next(s for s in report["sections"] if s["key"] == "rq1")
    check("rest of the section still built", "genre_shares" in rq1["skipped"]
          and "style_movers" in rq1["built"])
    check("one plotly.js even with fewer charts", page.count("plotly.js v") == 1)

    folder = tmp / "empty"
    write_tables(folder, tables)
    (folder / "rq3_writer_levels.csv").write_text("year,level,n_releases\n", encoding="utf-8")
    page, report, err = build_quiet(folder)
    check("header-only CSV gives a note", "Table not available" in page and "empty" in page)

    folder = tmp / "nothing"
    folder.mkdir()
    page, report, err = build_quiet(folder)
    check("build succeeds with no CSVs at all", all(not s["built"] for s in report["sections"])
          and page.count("Table not available") == sum(len(s.panels) for s in build_dashboard.SECTIONS))


def test_missing_column(tmp: Path) -> None:
    """A present CSV without a required column stops the build with a clear message."""
    print("missing column")
    folder = tmp / "badcol"
    tables = synthetic_tables()
    tables["rq3_major_share"] = tables["rq3_major_share"].drop(columns=["share"])
    write_tables(folder, tables)
    try:
        charts.load_tables(folder)
    except charts.DashboardError as exc:
        check("DashboardError names file and column", "rq3_major_share.csv" in str(exc)
              and "'share'" in str(exc), str(exc))
    else:
        check("DashboardError raised", False)
    err = io.StringIO()
    with contextlib.redirect_stderr(err):
        code = build_dashboard.main([str(folder), "-o", str(tmp / "bad.html")])
    check("command line exits with 1 and prints ERROR", code == 1 and "ERROR:" in err.getvalue())
    check("no HTML written on error", not (tmp / "bad.html").exists())


def test_escaping(tmp: Path, clean_page: str) -> None:
    """Names such as <script> are escaped in tables, labels and hover text."""
    print("HTML escaping")
    folder = tmp / "evil"
    write_tables(folder, synthetic_tables(evil=EVIL))
    page, _, _ = build_quiet(folder)
    check("raw <script>alert tag never appears", "<script>alert" not in page)
    check("escaped name appears in the tables", "&lt;script&gt;alert(1)&lt;/script&gt;" in page)
    check("script tag count unchanged by the evil name",
          parse(page).scripts == parse(clean_page).scripts, f"{parse(page).scripts} vs {parse(clean_page).scripts}")
    check("no network reference after escaping", not parse(page).network)
    check("esc() handles quotes and NaN", charts.esc('a"b<') == "a&quot;b&lt;" and charts.esc(float("nan")) == "")
    table = charts.html_table(pd.DataFrame({"n": [EVIL]}), [("n", EVIL, "text")], title=EVIL)
    check("html_table escapes header, title and cells", "<script>" not in table and table.count("&lt;script&gt;") >= 3)


def main() -> int:
    """Run all checks."""
    with tempfile.TemporaryDirectory() as name:
        tmp = Path(name)
        clean = test_full_build(tmp)
        test_umap_trace()
        test_missing_csv(tmp)
        test_missing_column(tmp)
        test_escaping(tmp, clean)
    print()
    if FAILURES:
        print(f"FAILED: {len(FAILURES)} check(s): {FAILURES}")
        return 1
    print("ALL PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
