# T01 - environment setup script and README

## Goal
Write a re-runnable setup script plus a README so anyone on this server, including future sessions, can get a working environment in one command. The orchestrator runs the script. You only write it, and you test it in dry-run mode.

## Facts about this server (already verified)
- Python is `/opt/conda/bin/python3` (3.12). Java 17 is installed.
- PySpark 4.1.1 comes from `SPARK_HOME=/usr/local/spark` via `PYTHONPATH`, not pip. **Never pip-install pyspark** (it would shadow the cluster's Spark).
- Conda already has pyarrow, pandas, numpy 1.26, matplotlib, scikit-learn 1.8, jupyterlab, plotly.
- Missing: `python-igraph`, `leidenalg`, `node2vec`, `umap-learn`.
- Raw dumps are symlinked into `/tmp/$USER/discogs/raw/` (done by the orchestrator). Parquet goes to `/tmp/$USER/discogs/parquet/`. `/home2` is 98% full, so data must stay out of the repo.
- `lab2/vinyl/config.py` reads `DISCOGS_DATA_DIR`.

## Files in scope
- `scripts/setup_env.sh` (new)
- `README.md` (new, repo root)
- `requirements.txt`: only add a comment line saying pyspark comes from `SPARK_HOME` on this server. Keep the entry.

## `scripts/setup_env.sh` requirements
- `set -euo pipefail`. Idempotent: safe to re-run, and it skips steps already done.
- Creates `.venv/` at the repo root with `/opt/conda/bin/python3 -m venv --system-site-packages`.
- Installs every requirement except pyspark: `pip install` the requirements file with the `pyspark` line filtered out. Use the default upgrade strategy so conda packages are not upgraded needlessly.
- Registers a Jupyter kernel named `vinyl` (display name "Python (vinyl)") with `ipykernel install --user`. Then patch its `kernel.json` `env` to set `DISCOGS_DATA_DIR=/tmp/<user>/discogs`, `SPARK_HOME`, and `PYTHONPATH` to the current values, so notebooks see Spark and the data dir.
- Creates `/tmp/$USER/discogs/{raw,parquet}` if missing. Warns, without failing, if `raw/` has fewer than 8 non-empty `.xml.gz` files.
- `--dry-run` flag: prints every action without executing anything that installs or writes.
- `--check` flag: verifies the finished environment by importing `pyspark igraph leidenalg node2vec umap sklearn plotly vinyl.config` with the venv Python, run from `lab2/`, and printing `pyspark.__version__` and `config.DATA_DIR`. It exits non-zero on failure.
- Ends by printing the two lines to export (`DISCOGS_DATA_DIR`, `source .venv/bin/activate`).

## README.md requirements
Short (aim for under 120 lines). Include: one-paragraph project summary (from `lab2/main.ipynb` intro and `.kiro/steering/project.md`), repo layout table, setup (the script, the kernel, data location and why `/tmp`), the pipeline order (explorer 01 ingest, then explorers 02-07, then `main.ipynb`), and a pointer to `.orchestration/README.md` for the Claude + Kiro workflow. Don't invent results or numbers.

## Acceptance checks
1. `bash -n scripts/setup_env.sh`
2. `scripts/setup_env.sh --dry-run` exits 0 and installs nothing (`.venv` must not exist afterwards if it did not before).
3. `grep -c pyspark scripts/setup_env.sh` shows pyspark is only filtered out or imported for checks, never installed.
4. README renders sensibly (`head -60 README.md`).

## Out of scope
Actually running the install, any change under `lab2/`, `.kiro/` or `.orchestration/`.
