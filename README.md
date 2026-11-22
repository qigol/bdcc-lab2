# The Vinyl Decade (BDCC Lab 2)

A PySpark study of the Discogs data dumps, for the Big Data and Cloud Computing course. It asks how genres, formats and label power changed in releases dated 2016-2026. The main snapshot is 1 October 2026 (`20261001`). The 1 January 2020 snapshot (`20200101`) is used only for the before/after comparison.

Four research questions:

- **RQ1** Genre and style shifts and fragmentation (Shannon entropy, style co-occurrence).
- **RQ2** Format mix, and how much of the vinyl boom is reissues.
- **RQ3** Concentration inside the Universal, Sony and Warner label trees, and ownership changes between snapshots.
- **RQ4** Communities of artists, producers and labels (Leiden, node2vec, UMAP, HDBSCAN).

## Repository layout

| Path | Contents |
|---|---|
| `lab2/main.ipynb` | The report. Calls functions from `lab2/vinyl/` only. |
| `lab2/explorers/01-07_*.ipynb` | Working notebooks, one per stage (ingest, genres, formats, labels, snapshots, networks, clustering). |
| `lab2/vinyl/` | All logic. `config` (constants, paths), `ingest`, `tables`, `trends` (RQ1), `formats` (RQ2), `labels` and `snapshots` (RQ3), `networks` and `clustering` (RQ4), `viz`. |
| `lab2/schemas/` | Saved Spark schemas for the four raw files. |
| `scripts/setup_env.sh` | One-command environment setup (see below). |
| `scripts/kiro_run.sh` | Runs one Kiro agent on one orchestration task. |
| `.orchestration/` | Task specs, reviews and run logs for the Claude + Kiro workflow. |
| `.kiro/` | Agent configs and project rules (`.kiro/steering/project.md`). |
| `discogs_loader.py` | Root-level helper that loads the dumps as Spark DataFrames via Parquet. |
| `requirements.txt` | Python dependencies. |

## Setup

```bash
scripts/setup_env.sh --dry-run   # print what would happen, change nothing
scripts/setup_env.sh             # do it; safe to re-run
scripts/setup_env.sh --check     # verify imports; exits non-zero on failure
```

The script:

1. Creates `.venv/` with `/opt/conda/bin/python3 -m venv --system-site-packages`, so the conda packages (pandas, numpy, scikit-learn, jupyterlab and others) are reused.
2. Installs `requirements.txt` without the `pyspark` line, and without `--upgrade`, so conda packages stay as they are. It adds `python-igraph`, `leidenalg`, `node2vec` and `umap-learn`.
3. Registers a Jupyter kernel named `vinyl` (display name "Python (vinyl)"). Its `kernel.json` sets `DISCOGS_DATA_DIR`, `SPARK_HOME` and `PYTHONPATH`, so notebooks find Spark and the data.
4. Creates `/tmp/$USER/discogs/{raw,parquet}` and warns if `raw/` has fewer than 8 non-empty `.xml.gz` files.

Then, in each shell:

```bash
export DISCOGS_DATA_DIR=/tmp/$USER/discogs
source .venv/bin/activate
```

**PySpark is not pip-installed.** On this server Spark 4.1.1 comes from `SPARK_HOME=/usr/local/spark` via `PYTHONPATH`. Installing the pip package would shadow the cluster's Spark. `--check` fails if `pyspark` is imported from inside `.venv`.

## Data location

Data lives under `$DISCOGS_DATA_DIR` (read by `lab2/vinyl/config.py`):

```
$DISCOGS_DATA_DIR/
    raw/discogs_<date>_{releases,masters,labels,artists}.xml.gz   (symlinks to the shared dumps)
    parquet/<snapshot>/<table>/                                   (written by tables.build_all)
```

The default is `/tmp/$USER/discogs`. The home filesystem `/home2` is nearly full, so data stays out of the repo. `/tmp` is not backed up and may be cleared, so Parquet can be rebuilt by re-running explorer 01. The shared dumps in `/mnt/data/public/discogs` are read-only; never write there.

## Pipeline order

1. `lab2/explorers/01_ingest_and_parse.ipynb`: reads the raw XML once and writes normalized Parquet for each snapshot. Run this first. The releases files take the longest.
2. `lab2/explorers/02` to `07`: genres and styles, formats and reissues, labels and majors, snapshot comparison, networks and communities, artist clustering. They read Parquet only.
3. `lab2/main.ipynb`: the report, which re-runs the final analyses through the same `vinyl/` functions.

Select the "Python (vinyl)" kernel for all notebooks.

## Claude + Kiro workflow

Work is split into task specs. A Claude Code orchestrator writes each spec and runs full-data Spark jobs. Kiro worker and reviewer agents implement and check each task. See [`.orchestration/README.md`](.orchestration/README.md).
