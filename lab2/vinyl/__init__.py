"""Helper package for *The Vinyl Decade* (BDCC Lab 2).

The report lives in ``main.ipynb``; exploration lives in ``explorers/``.
Both import their logic from the modules below so that every number in
the report can be traced to one documented function.

Modules
-------
config      Paths, snapshot names, year window, and domain constants.
ingest      Spark session, schema loading, raw XML reading, Parquet I/O.
tables      Raw nested XML rows -> flat, normalized Parquet tables.
trends      RQ1: style shares, Shannon entropy, style co-occurrence.
formats     RQ2: format mix and reissue share.
labels      RQ3: label hierarchies, major share, artist concentration.
snapshots   Snapshot comparison: ownership changes and cataloguing lag.
networks    RQ4: edge lists, centrality, Leiden communities.
clustering  RQ4: artist features, node2vec, UMAP, HDBSCAN.
viz         Shared matplotlib helpers so every figure looks the same.
"""

__all__ = [
    "config",
    "ingest",
    "tables",
    "trends",
    "formats",
    "labels",
    "snapshots",
    "networks",
    "clustering",
    "viz",
]
