# T00 - requirements.txt

## Goal
Add `requirements.txt` at the repo root listing every third-party package imported anywhere under `lab2/vinyl/`, plus the extras the plan needs.

## Context
- `lab2/vinyl/*.py` imports, some lazily inside functions: pyspark, pandas, numpy, matplotlib, igraph, sklearn, umap. Check this yourself with grep. Don't trust this list blindly.
- Already installed system-wide: Python 3.12, PySpark 4.1.1, Java 17.
- Planned additions: `leidenalg` (Leiden communities on igraph), `node2vec` (embeddings, task T10), `plotly` (dashboard), `jupyterlab`, `pyarrow` (Spark Arrow conversion).

## Files in scope
- `requirements.txt` (new). Nothing else.

## Requirements
- Use PyPI distribution names, not import names (`python-igraph`, `scikit-learn`, `umap-learn`).
- Pin with lower bounds only (`>=`), not exact pins. `pyspark>=4.0` (code relies on built-in XML).
- `scikit-learn>=1.3`, because `clustering.py` uses `sklearn.cluster.HDBSCAN`.
- Group with short comments: core, analysis, graphs, clustering, notebooks/dashboard.

## Acceptance checks
1. `python3 -I -c "import re,sys; names=[re.split(r'[<>=!~ ]',l)[0].lower() for l in open('requirements.txt') if l.strip() and not l.startswith('#')]; need={'pyspark','pandas','numpy','matplotlib','python-igraph','scikit-learn','umap-learn','leidenalg','node2vec','plotly','pyarrow','jupyterlab'}; missing=need-set(names); print('missing',missing); sys.exit(bool(missing))"`. It must print `missing set()`.
2. `grep -rhoE '^\s*(import|from) [a-z_]+' lab2/vinyl/ | awk '{print $2}' | sort -u` shows that every third-party module has a matching entry.

## Out of scope
Installing packages, creating a venv, README.
