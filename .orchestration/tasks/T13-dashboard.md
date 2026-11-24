# T13 - self-contained Plotly dashboard

## Goal
Build `lab2/dashboard/vinyl_decade.html`: one self-contained HTML file that opens offline in a browser and shows the report's main results as interactive Plotly charts. Its data is the small CSV tables `main.ipynb` exports to `lab2/dashboard/data/` (`manifest.json` lists them, each with a description).

## Context
- The orchestrator has run `main.ipynb` on full data, so `lab2/dashboard/data/*.csv` and `manifest.json` exist. **Read the CSVs and the manifest first. Column names come from there, not from this spec.**
- Plotly 5.x is in the venv (`.venv/bin/python -c "import plotly; print(plotly.__version__)"`).
- The 2026 snapshot is dated 1 Oct 2026, so 2026 is a partial year. `config.LAST_FULL_YEAR = 2025`.

## Files in scope
- New `lab2/dashboard/build_dashboard.py`: a script that reads the CSVs and writes the HTML.
- New `lab2/vinyl/dashboard.py`, optional: chart builders, if that keeps the script short. Each builder takes a pandas DataFrame and returns a `plotly.graph_objects.Figure`.
- The generated `lab2/dashboard/vinyl_decade.html`, tracked.
- New `lab2/scripts/test_t13.py`.
- `lab2/main.ipynb`: only the "## 9. Dashboard" markdown cell. Give the relative link `dashboard/vinyl_decade.html`, list the views, and give the command to rebuild.

## Requirements
1. **Self-contained.**
   - Use `plotly.io.to_html(fig, include_plotlyjs=...)`. Embed plotly.js **once**: include it in the first figure with `include_plotlyjs="inline"` or `True`, and use `False` for the rest, or use one combined page template.
   - No CDN, no external fonts, no network requests. The file must work offline.
   - Target under 15 MB.
2. **Layout.**
   - A header with the title "The Vinyl Decade", the subtitle "Discogs 2016-2026, snapshots of 1 Jan 2020 and 1 Oct 2026", and a one-line note that 2026 covers Jan-Sep.
   - Tabs or sections, one per research question, built with plain HTML, CSS and a few lines of vanilla JS for tab switching:
     1. **Overview**: key numbers as tiles from `data_summary`, plus the pipeline in one sentence.
     2. **RQ1 Genres and styles**:
        - genre share lines;
        - style movers as a diverging bar chart of the top 15 gains and losses;
        - evenness by year, with the subsample band from `rq1_entropy_sensitivity`;
        - style-network density and modularity by year.
     3. **RQ2 Formats and reissues**:
        - format share lines;
        - vinyl new vs reissue stacked bars;
        - a growth-explained table or bars for the three flags;
        - reissue share by genre, with a genre dropdown or legend toggle;
        - the reissue age distribution;
        - pressing-plant coverage.
     4. **RQ3 Label power**:
        - major share lines;
        - group concentration (HHI by major by year);
        - parent coverage as stacked shares, including `self_released`;
        - writer credit levels.
     5. **Snapshot comparison**:
        - completeness by age;
        - undercount bars (`n_releases` vs `n_estimated`, with 2026 marked partial);
        - lag by format and by genre heatmaps;
        - a table of the top major moves.
     6. **RQ4 Communities and clusters**:
        - a graph summary table;
        - the top communities tables;
        - the resolution sweep;
        - a UMAP scatter from `rq4_cluster_points`, colored by cluster with noise in grey, `scattergl`, and hover showing `artist_name` and cluster;
        - the cluster description table.
   - Tables are rendered with `plotly.graph_objects.Table`, or as plain HTML tables built in Python with HTML escaping, never raw string interpolation of data.
3. **Honesty notes, shown as small captions next to the charts:**
   - 2026 is partial, and 2024-2026 are undercounted by cataloguing lag. Point to the snapshot tab.
   - "Independent" means not linked to a major's tree in Discogs.
   - Pressing-plant shares cover only releases that name a plant.
4. **Robustness.** If a CSV is missing, show the section with a "table not available" note instead of crashing, and print a warning. Column names come from the CSVs. Fail with a clear message only if a required column is missing from a present CSV.
5. **Accessibility basics.** Every chart has a title and axis labels, the color palette is colorblind-safe (Plotly's `Safe` or `D3` qualitative), and there is a `<title>`.
6. **Run command:** `cd lab2 && ../.venv/bin/python dashboard/build_dashboard.py` writes the HTML and prints its size.
7. Numpy docstrings and no TODOs.

## Acceptance checks
1. `cd lab2 && timeout 600 ../.venv/bin/python scripts/test_t13.py` exits 0. It covers:
   - building from **tiny synthetic CSVs** written to a temp dir, by passing a data dir argument: all six sections are present, plotly.js appears once (count `plotly.js v` or the bundle marker), and there is no `http://` or `https://` script or link `src`/`href` except plain text links in captions;
   - a missing-CSV case that still builds, with a "not available" note;
   - HTML escaping of a name like `<script>`.
2. `cd lab2 && timeout 600 ../.venv/bin/python dashboard/build_dashboard.py` on the real CSVs succeeds and the file is under 15 MB. Paste the size and the list of sections and charts built.
3. `git diff --stat` touches only files in scope.

## Out of scope
Changing analyses or exports, other notebook cells, and hosting.
