# Project: The Vinyl Decade (BDCC Lab 2)

PySpark study of the Discogs data dumps. It asks how genres, formats and label power changed in releases dated 2016-2026.
The main snapshot is 20261001. The 20200101 snapshot is used only for the before/after comparison.

## Research questions
- RQ1: genre and style shifts, fragmentation (Shannon entropy, style co-occurrence). Module `lab2/vinyl/trends.py`.
- RQ2: format mix and the share of reissues in the vinyl boom. Module `lab2/vinyl/formats.py`.
- RQ3: concentration inside the Universal, Sony and Warner label trees, and ownership changes between snapshots. Modules `lab2/vinyl/labels.py` and `lab2/vinyl/snapshots.py`.
- RQ4: communities of artists, producers and labels (Leiden, node2vec, UMAP, HDBSCAN). Modules `lab2/vinyl/networks.py` and `lab2/vinyl/clustering.py`.

## Rules
- All logic lives in `lab2/vinyl/`. Notebooks (`lab2/main.ipynb`, `lab2/explorers/*.ipynb`) only call those functions.
- Match the existing style: `from __future__ import annotations`, type hints, numpy-style docstrings (Parameters / Returns / Raises).
- Constants go in `lab2/vinyl/config.py`. Do not hard-code paths. Data lives under `$DISCOGS_DATA_DIR`.
- `$DISCOGS_DATA_DIR` is `lab2/data` (gitignored). Full Parquet is in `lab2/data/parquet/{20200101,20261001}`. Set `DISCOGS_DATA_DIR=$PWD/data` when running from `lab2/`.
- The pod has a **16 GiB memory limit** and 4 CPUs. Going over it restarts the pod and wipes `/tmp`. Run Spark with `PYSPARK_SUBMIT_ARGS="--driver-memory 4g pyspark-shell"` at most.
- Never write to or delete anything under `/mnt/data/public/discogs`. It is shared read-only raw data.
- Do not run Spark on the full dumps. Test on small samples only (`.limit()`, a few partitions, `timeout 600`). The orchestrator runs full-data jobs.
- Do not commit, push, switch branches or reset. The orchestrator handles git.
- Stay inside the files the task spec names. If something outside scope needs changing, say so in your summary instead of doing it.
- Notebooks are JSON. Edit only the cells the task names, and keep the outputs and metadata of other cells.

## Python and search
- Run all Python with the repo venv: `<repo root>/.venv/bin/python`. From `lab2/` that is `../.venv/bin/python`. It has igraph, leidenalg, gensim, umap, hdbscan and sklearn.
  - Spark needs `SPARK_HOME=/usr/local/spark` and `PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.9.9-src.zip:$SPARK_HOME/python:$PWD`.
  - `/opt/conda/bin/python3` is the system Python and lacks igraph and others. Do not use it.
  - Notebooks run on the `vinyl` kernel.
- **Never run `find /` or any search outside the repo.** The filesystem is NFS and such searches hang for many minutes. Search only inside the repo, or ask the venv directly, for example `.venv/bin/python -c "import igraph; print(igraph.__file__)"`.
