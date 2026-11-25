# The Vinyl Decade (BDCC Lab 2)

A PySpark study of the Discogs data dumps for the Big Data and Cloud Computing course. It looks at releases dated 2016-2026 and asks four questions:

- **RQ1** How did genre and style composition shift, and has music become more fragmented? (Shannon entropy, style co-occurrence)
- **RQ2** How did the format mix change, and how much of the vinyl boom is reissues?
- **RQ3** How concentrated is release activity inside the Universal, Sony and Warner label trees, and how did ownership change between snapshots?
- **RQ4** What communities of artists, producers and labels emerge, and do they match genre tags? (Leiden, node2vec, UMAP, HDBSCAN)

The main snapshot is the dump of 1 October 2026 (`20261001`). The dump of 1 January 2020 (`20200101`) is used only for the before/after comparison, mainly to measure how late releases are catalogued.

## Results

| Where | What |
|---|---|
| [`lab2/main.ipynb`](lab2/main.ipynb) | The report: method, results for RQ1-RQ4, limitations (section 7), references and a function reference. |
| [`lab2/dashboard/vinyl_decade.html`](lab2/dashboard/vinyl_decade.html) | Interactive dashboard, one self-contained file. Open it in a browser; it needs no network. |
| [`lab2/explorers/executed/`](lab2/explorers/executed/) | The seven working notebooks with their outputs from the full-data runs. See its [README](lab2/explorers/executed/README.md) for run dates and notes. |

Headline numbers, copied from the executive summary of `main.ipynb` (3,829,109 releases dated 2016-2026 in the 2026 snapshot):

- Vinyl rose from 19.1% to 33.0% of releases between 2016 and 2025, while CD fell from 27.8% to 15.3%.
- Reissues explain 45.1% of the growth in vinyl releases, so more than half of the boom is new music.
- Labels in the Universal, Sony and Warner trees carry at most 8.7% of catalogued releases in 2016 and 6.8% in 2025.
- Style evenness rose from 0.772 in 2016 to 0.785 in 2025. Genre shares hardly moved.
- Only 54.7% of the releases dated 2019 were in Discogs by January 2020. Applying such rates, 2025 is undercounted by about 31%.

## Setup and reproduce

Run everything on the course server, from the repository root.

### Environment

```bash
scripts/setup_env.sh --dry-run   # print what would happen, change nothing
scripts/setup_env.sh             # do it; safe to re-run
scripts/setup_env.sh --check     # verify imports; exits non-zero on failure
```

The script creates `.venv/` (with `--system-site-packages`, so the conda packages are reused), installs `requirements.txt` without the `pyspark` line, registers the Jupyter kernel `vinyl` ("Python (vinyl)"), creates `lab2/data/{raw,parquet}` and links the shared dumps from `/mnt/data/public/discogs/<year>/` into `raw/`. The kernel's `kernel.json` sets `DISCOGS_DATA_DIR`, `SPARK_HOME` and `PYTHONPATH`. In a plain shell:

```bash
export DISCOGS_DATA_DIR=$PWD/lab2/data
source .venv/bin/activate
```

PySpark is not pip-installed. Spark 4.1.1 comes from `SPARK_HOME=/usr/local/spark` via `PYTHONPATH`, and a pip copy would shadow it. `--check` fails if `pyspark` is imported from inside `.venv`.

The pod has 4 CPUs and 16 GiB of memory. Keep the Spark driver heap at 10 GB or less. Do not put the data in `/tmp`, which is wiped when the pod restarts. The shared dumps are read-only; never write there.

### 1. Ingest (about 3 h 17 min)

The raw XML is read once and written as normalized Parquet under `lab2/data/parquet/<snapshot>/<table>/`. [`lab2/scripts/run_full_ingest.sh`](lab2/scripts/run_full_ingest.sh) pre-splits the gzip files, then runs `explorers/01_ingest_and_parse.ipynb` on the `vinyl` kernel. It stages about 35 GB of temporary chunks in `/tmp/$USER/discogs_stage` and deletes them as it goes.

```bash
mkdir -p .orchestration/runs/ingest
nohup setsid lab2/scripts/run_full_ingest.sh > .orchestration/runs/ingest/run.log 2>&1 < /dev/null &
```

The executed copy of the notebook lands in `$T03_OUT` (default `.orchestration/runs/T03`). Run it first; everything else reads Parquet only.

### 2. Explorers

`02` to `07` are independent of each other except that `07` reuses the cache described below. Each command writes the executed copy to a scratch folder and leaves the tracked notebook alone:

```bash
export PYSPARK_SUBMIT_ARGS="--driver-memory 4g pyspark-shell"
mkdir -p /tmp/$USER/nbrun
cd lab2/explorers
timeout 6h ../../.venv/bin/python -m jupyter nbconvert --to notebook --execute \
  --ExecutePreprocessor.timeout=3600 --ExecutePreprocessor.kernel_name=vinyl \
  --output-dir /tmp/$USER/nbrun --output 02_genres_and_styles.executed.ipynb \
  02_genres_and_styles.ipynb
```

Change the two notebook names for the other explorers. The 3600 s limit is per cell; with the derived cache in place no cell comes close (see below). Explorer 07 took 42 min with the caches in place. Copy a result into `lab2/explorers/executed/` only if you want to replace the committed one.

### 3. The report

```bash
export PYSPARK_SUBMIT_ARGS="--driver-memory 4g pyspark-shell"
cd lab2
timeout 5h ../.venv/bin/python -m jupyter nbconvert --to notebook --execute \
  --ExecutePreprocessor.timeout=3600 --ExecutePreprocessor.kernel_name=vinyl \
  --output-dir /tmp/$USER/nbrun --output main.executed.ipynb main.ipynb
```

This took 2 h 02 min with the caches in place. It also rewrites the CSV tables in `lab2/dashboard/data/`.

### Derived cache

node2vec embeddings and the UMAP runs are the slow steps of RQ4. They are saved in `lab2/data/derived/2026/` (`artist_n2v.parquet`, `features.npy`, `artist_ids.npy`, `embedding_2d.npy`, `embedding_10d.npy`) and reused when they match the current artist set. Building the UMAP part from scratch took about 2 h 48 min. UMAP and node2vec are not seeded, so a rebuild gives slightly different coordinates and cluster numbers than the report shows (see section 7 of the report).

To force a recompute, move the folder aside (`mv lab2/data/derived/2026 lab2/data/derived/2026.bak`), or set `REUSE_CACHE = False` in the cache cells of `main.ipynb` (`REUSE_EMBEDDINGS = False` in explorer 07). Keep the old folder if you want to reproduce the committed numbers.

### Dashboard

After `main.ipynb` has exported the tables, rebuild the page from `lab2/`:

```bash
../.venv/bin/python dashboard/build_dashboard.py
```

### Tests

```bash
cd lab2 && bash scripts/run_all_tests.sh
```

This runs every `lab2/scripts/test_*.py` one at a time with a 900 s limit and prints a PASS or FAIL line for each. The header of the script lists which tests need Spark and how long each takes. Tests `test_t04` to `test_t08` read the real Parquet tables.

## Layout

| Path | Contents |
|---|---|
| `lab2/main.ipynb` | The report. Calls functions from `lab2/vinyl/` only. |
| `lab2/explorers/01-07_*.ipynb` | Working notebooks, one per stage (ingest, genres, formats, labels, snapshots, networks, clustering), without outputs. |
| `lab2/explorers/executed/` | The same notebooks with outputs from the full-data runs, plus a README with run dates. |
| `lab2/vinyl/` | All logic. `config` (constants, paths), `ingest`, `tables`, `trends` (RQ1), `formats` (RQ2), `labels` and `snapshots` (RQ3), `networks` and `clustering` (RQ4), `export` (dashboard tables), `viz`, `dashboard`. |
| `lab2/dashboard/` | `vinyl_decade.html` (the dashboard), `build_dashboard.py` (builds it) and `data/` (CSV tables and `manifest.json`). |
| `lab2/schemas/` | Saved Spark schemas for the four raw files. |
| `lab2/scripts/` | Full-ingest runner, ingest checks, the `test_*.py` self-tests and `run_all_tests.sh`. |
| `lab2/data/` | Gitignored. `raw/` (links to the dumps), `parquet/` and `derived/`. Set `DISCOGS_DATA_DIR` to use another place. |
| `lab2/figures/` | Gitignored. Written by the notebooks; the figures are also embedded in the notebook outputs. |
| `scripts/setup_env.sh` | One-command environment setup. |
| `scripts/kiro_run.sh` | Runs one Kiro agent on one orchestration task. |
| `.orchestration/` | Task specs, reviews and run logs for the Claude + Kiro workflow. |
| `.kiro/` | Agent configs and project rules (`.kiro/steering/project.md`). |
| `requirements.txt` | Python dependencies. |

## How it was built

Each piece of work was written as a task spec with its goal, the files in scope and acceptance checks. A Claude Code session acted as orchestrator: it wrote the specs, ran the full-data Spark jobs, reviewed the diffs and handled git. Kiro agents did the implementation (worker) and a second pass on the result (reviewer). A worker could not commit, and the reviewer could only write review files. When a review asked for changes, the worker ran again, and after two rounds the orchestrator fixed the rest. The specs and reviews are kept in `.orchestration/`; see [`.orchestration/README.md`](.orchestration/README.md).

## Known limitations

Section 7 of the report ([`lab2/main.ipynb`](lab2/main.ipynb)) has the full list and the numbers. The main ones:

- **Cataloguing lag.** Discogs fills in recent years over time, and unevenly by format and genre, so 2023-2026 are undercounted and recent shares are biased. The 2026 snapshot also stops at September.
- **Contributor bias and label trees.** Discogs holds what its users catalogue, which leans towards physical media. "Independent" only means "not linked to a major's tree in Discogs", so the majors' shares are lower bounds.
- **Unseeded embeddings.** UMAP and node2vec are not bit-reproducible. The reported clusters come from the saved embeddings in `lab2/data/derived/2026/`, and recomputing them gives slightly different clusters.
