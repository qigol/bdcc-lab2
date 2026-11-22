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
- Never write to or delete anything under `/mnt/data/public/discogs`. It is shared read-only raw data.
- Do not run Spark on the full dumps. Test on small samples only (`.limit()`, a few partitions, `timeout 600`). The orchestrator runs full-data jobs.
- Do not commit, push, switch branches or reset. The orchestrator handles git.
- Stay inside the files the task spec names. If something outside scope needs changing, say so in your summary instead of doing it.
- Notebooks are JSON. Edit only the cells the task names, and keep the outputs and metadata of other cells.
