"""Self-test for T12a (no pytest, no Spark session, tiny synthetic data only).

Covers ``vinyl.export`` and static checks of ``main.ipynb``: valid JSON,
every code cell parses, no ``TODO`` in code, every dashboard table is
exported exactly once and listed in the manifest, and every call to a
``vinyl`` function names parameters that exist. It does not run the
notebook. Exit code 0 only if every check passes.

Usage, from ``lab2/``:
``timeout 600 ../.venv/bin/python scripts/test_t12a.py``
"""

from __future__ import annotations

import ast
import inspect
import json
import re
import sys
import tempfile
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from vinyl import clustering, config, export, formats, ingest, labels, networks, snapshots, tables, trends, viz  # noqa: E402

FAILURES: list[str] = []

#: Every table main.ipynb must export, once each (spec of T12a).
EXPECTED_TABLES = [
    "data_summary",
    "rq1_genre_shares", "rq1_style_movers", "rq1_entropy", "rq1_entropy_sensitivity",
    "rq1_style_network", "rq1_movers_no_prolific",
    "rq2_format_shares", "rq2_vinyl_reissues", "rq2_growth_explained", "rq2_reissue_by_genre",
    "rq2_reissue_age", "rq2_pressing_plants",
    "rq3_major_share", "rq3_group_concentration", "rq3_parent_coverage", "rq3_writer_levels",
    "cmp_major_moves", "cmp_lag", "cmp_undercount", "cmp_lag_by_format", "cmp_lag_by_genre",
    "rq4_graph_summary", "rq4_communities_collab", "rq4_communities_artist_label",
    "rq4_resolution_sweep", "rq4_top_pagerank",
    "rq4_clusters", "rq4_cluster_agreement", "rq4_cluster_points",
]

MODULES = {
    "config": config, "ingest": ingest, "tables": tables, "trends": trends, "formats": formats,
    "labels": labels, "snapshots": snapshots, "networks": networks, "clustering": clustering,
    "viz": viz, "export": export,
}


def check(name: str, ok: bool, detail: str = "") -> None:
    """Record and print one check result."""
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" - {detail}" if detail and not ok else ""))
    if not ok:
        FAILURES.append(name)


def test_export() -> None:
    """``export_table`` and ``export_manifest`` on a temporary folder."""
    print("vinyl.export")
    check("DASHBOARD_DATA_DIR is <project>/dashboard/data",
          export.DASHBOARD_DATA_DIR == config.PROJECT_ROOT / "dashboard" / "data", str(export.DASHBOARD_DATA_DIR))
    with tempfile.TemporaryDirectory() as tmp:
        target = Path(tmp) / "a" / "b" / "data"
        df = pd.DataFrame(
            {
                "name": ["Zażółć", "Björk", None],
                "n": [1234567, 2, 3],
                "x": [1234.56789, 0.000123456789, float("nan")],
                "big": [1234567.891, 3.0, 0.5],
                "flag": [True, False, True],
                "items": [[1, "a"], ("b", 2), []],
            }
        )
        before = df.copy(deep=True)
        path = export.export_table(df, "demo", directory=target)
        check("returns the csv path and creates the folder", path == target / "demo.csv" and path.exists(), str(path))
        check("input frame is not modified", df.equals(before) or str(df.to_dict()) == str(before.to_dict()))
        text = path.read_text(encoding="utf-8")
        back = pd.read_csv(path, encoding="utf-8")
        check("utf-8 text survives", list(back["name"][:2]) == ["Zażółć", "Björk"], str(back["name"].tolist()))
        check("floats rounded to 6 significant digits", back["x"][0] == 1234.57 and back["x"][1] == 0.000123457,
              str(back["x"].tolist()))
        check("large float rounded to 6 significant digits", back["big"][0] == 1234570.0, str(back["big"].tolist()))
        check("integers are written unchanged", back["n"][0] == 1234567, str(back["n"].tolist()))
        check("missing values are empty fields", text.splitlines()[3].split(",")[2] == "", text.splitlines()[3])
        check("booleans stay booleans", list(back["flag"]) == [True, False, True])
        check("list cells become JSON", json.loads(back["items"][0]) == [1, "a"] and back["items"][2] == "[]",
              str(back["items"].tolist()))
        check("no index column by default", back.columns.tolist() == list(df.columns), str(back.columns.tolist()))
        with_index = pd.read_csv(export.export_table(df, "indexed", index=True, directory=target))
        check("index=True adds the index", with_index.shape[1] == df.shape[1] + 1)
        for bad in ("", "../x", "a/b"):
            try:
                export.export_table(df, bad, directory=target)
                check(f"bad name {bad!r} is rejected", False)
            except ValueError:
                check(f"bad name {bad!r} is rejected", True)

        # The default folder can be monkeypatched.
        saved = export.DASHBOARD_DATA_DIR
        export.DASHBOARD_DATA_DIR = Path(tmp) / "patched"
        try:
            default_path = export.export_table(pd.DataFrame({"a": [1]}), "t")
            manifest_path = export.export_manifest({"t": "A table."})
        finally:
            export.DASHBOARD_DATA_DIR = saved
        check("default folder is read at call time", default_path == Path(tmp) / "patched" / "t.csv" and default_path.exists())

        manifest = export.export_manifest({"b": "Second.", "a": "First."}, directory=target)
        loaded = json.loads(manifest.read_text(encoding="utf-8"))
        check("manifest is valid JSON named manifest.json", manifest.name == "manifest.json" and loaded == {"a": "First.", "b": "Second."},
              str(loaded))
        check("manifest keys are sorted", list(loaded) == ["a", "b"])
        check("manifest in the patched default folder", manifest_path == Path(tmp) / "patched" / "manifest.json" and manifest_path.exists())
        try:
            export.export_manifest({"a": "two\nlines"}, directory=target)
            check("multi-line description is rejected", False)
        except ValueError:
            check("multi-line description is rejected", True)


def code_cells(nb: dict) -> list[tuple[int, str]]:
    """Return (index, source with ``%`` magic lines removed) for each code cell."""
    out = []
    for i, cell in enumerate(nb["cells"]):
        if cell["cell_type"] == "code":
            source = "".join(cell["source"])
            out.append((i, "\n".join("" if line.lstrip().startswith("%") else line for line in source.split("\n"))))
    return out


def test_config() -> None:
    """The two new constants."""
    print("config")
    # T11 raised it from 50 to 100 after the min_cluster_size sweep in explorer 07.
    check("HDBSCAN_MIN_CLUSTER_SIZE is 100", config.HDBSCAN_MIN_CLUSTER_SIZE == 100 and isinstance(config.HDBSCAN_MIN_CLUSTER_SIZE, int))
    check("UMAP_N_NEIGHBORS is 30", config.UMAP_N_NEIGHBORS == 30 and isinstance(config.UMAP_N_NEIGHBORS, int))


def test_notebook() -> None:
    """Static checks of main.ipynb."""
    print("main.ipynb")
    path = ROOT / "main.ipynb"
    nb = json.loads(path.read_text(encoding="utf-8"))
    check("valid JSON with cells", isinstance(nb.get("cells"), list) and len(nb["cells"]) > 0)
    ids = [c["id"] for c in nb["cells"]]
    check("cell ids are unique", len(ids) == len(set(ids)))
    # The full-data run's outputs were copied in at the end of the project: outputs are
    # expected now, but none may be an error.
    check("no code cell output is an error",
          not any(o.get("output_type") == "error"
                  for c in nb["cells"] if c["cell_type"] == "code" for o in c.get("outputs", [])))

    cells = code_cells(nb)
    trees: dict[int, ast.AST] = {}
    bad = []
    for i, source in cells:
        try:
            trees[i] = ast.parse(source)
        except SyntaxError as err:
            bad.append((i, str(err)))
    check("every code cell passes ast.parse", not bad, str(bad))

    with_todo = [i for i, source in cells if "TODO" in "".join(nb["cells"][i]["source"])]
    check("no TODO in code cells", not with_todo, str(with_todo))
    check("betweenness stays off", "betweenness=True" not in "\n".join(s for _, s in cells))

    # export_table calls: the table name is the second positional argument.
    counts: dict[str, int] = {}
    for tree in trees.values():
        for node in ast.walk(tree):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "export_table"
                    and isinstance(node.func.value, ast.Name) and node.func.value.id == "export"):
                arg = node.args[1] if len(node.args) > 1 else None
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    counts[arg.value] = counts.get(arg.value, 0) + 1
                else:
                    counts["<non-literal>"] = counts.get("<non-literal>", 0) + 1
    missing = [n for n in EXPECTED_TABLES if counts.get(n, 0) != 1]
    check("every expected table is exported exactly once", not missing, f"{missing}: {[counts.get(n, 0) for n in missing]}")
    extra = sorted(set(counts) - set(EXPECTED_TABLES))
    check("no other table is exported", not extra, str(extra))

    # The manifest literal lists exactly the exported tables.
    manifest = None
    for tree in trees.values():
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "manifest" for t in node.targets):
                manifest = ast.literal_eval(node.value)
    check("manifest dict found", isinstance(manifest, dict))
    if isinstance(manifest, dict):
        check("manifest lists every exported table", set(manifest) == set(EXPECTED_TABLES),
              str(sorted(set(manifest) ^ set(EXPECTED_TABLES))))
        check("manifest descriptions are one line", all(isinstance(v, str) and v and "\n" not in v for v in manifest.values()))
    manifest_calls = sum(
        1 for tree in trees.values() for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "export_manifest"
    )
    check("export_manifest is called once", manifest_calls == 1, str(manifest_calls))

    # T12b filled the findings: no "Finding (TODO)" is left and the 11 findings read "> **Finding.**".
    todo_findings = [c for c in nb["cells"] if c["cell_type"] == "markdown" and "**Finding (TODO)" in "".join(c["source"])]
    check("no Finding (TODO) cell is left", len(todo_findings) == 0, str(len(todo_findings)))
    findings = [c for c in nb["cells"] if c["cell_type"] == "markdown" and "**Finding.**" in "".join(c["source"])]
    check("11 filled Finding cells", len(findings) == 11, str(len(findings)))

    # Markdown edits.
    cell0 = "".join(nb["cells"][0]["source"])
    # T12b replaced the <RUN DATE> placeholder with the date of the full run.
    check("cell 0 has the final status line", "Status: code final; results run on full data on 25 November 2026" in cell0
          and "<RUN DATE>" not in cell0 and "skeleton" not in cell0, cell0[-200:])
    section_43 = next("".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "markdown" and "### 4.3 Methods in brief" in "".join(c["source"]))
    check("4.3 has no TODO", "TODO" not in section_43)
    for needle in ("is_reissue_any", "GRAPH_MIN_WEIGHT", "Resolution 1.0, seed 42", "64 dimensions, 10 walks × 30 steps per node, p = q = 1",
                   "UMAP_N_NEIGHBORS", "HDBSCAN_MIN_CLUSTER_SIZE", "Not On Label", "212", "38404", "353657", "2345", "0.739"):
        check(f"4.3 mentions {needle!r}", needle in section_43)
    appendix = next("".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "markdown" and "## Appendix A" in "".join(c["source"]))
    check("Appendix A lists export.py, executed/ and dashboard/",
          all(s in appendix for s in ("export.py", "executed/", "dashboard/")))
    # T12b wrote sections 1 and 6-8 and T13 wrote section 9: no markdown cell has a TODO left.
    summary = next("".join(c["source"]) for c in nb["cells"] if "## 1. Executive summary" in "".join(c["source"]))
    todo_cells = [i for i, c in enumerate(nb["cells"]) if c["cell_type"] == "markdown" and "TODO" in "".join(c["source"])]
    check("sections 1 and 6-10 are written (no TODO left)",
          "TODO" not in summary and not todo_cells
          and any("## 8. Conclusion" in "".join(c["source"]) for c in nb["cells"]), str(todo_cells))

    # The self-released ids stay a DataFrame.
    all_code = "\n".join(s for _, s in cells)
    check("artist-label graph excludes the self-released DataFrame", "exclude_label_ids=self_released" in all_code)
    check("self-released ids are never turned into a list",
          not re.search(r"(list\(\s*self_released|\bself_released\.(collect|toPandas)\(|isin\(\s*self_released)", all_code))
    check("label roots are cached once", all_code.count("labels.resolve_label_roots(") == 1)

    # Every vinyl.<module>.<function> call names a real function and real parameters.
    problems = []
    for i, tree in trees.items():
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name)):
                continue
            module = MODULES.get(node.func.value.id)
            if module is None:
                continue
            func = getattr(module, node.func.attr, None)
            if func is None:
                problems.append(f"cell {i}: {node.func.value.id}.{node.func.attr} does not exist")
                continue
            if not callable(func) or any(isinstance(a, ast.Starred) for a in node.args) or any(k.arg is None for k in node.keywords):
                continue
            try:
                inspect.signature(func).bind_partial(*[None] * len(node.args), **{k.arg: None for k in node.keywords})
            except TypeError as err:
                problems.append(f"cell {i}: {node.func.value.id}.{node.func.attr}: {err}")
    check("calls into vinyl match the function signatures", not problems, "; ".join(problems))

    # Attribute reads such as config.UMAP_N_NEIGHBORS.
    missing_attrs = []
    for i, tree in trees.items():
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id in ("config", "export", "networks"):
                if not hasattr(MODULES[node.value.id], node.attr):
                    missing_attrs.append(f"cell {i}: {node.value.id}.{node.attr}")
    check("config / export / networks attributes exist", not missing_attrs, "; ".join(missing_attrs))


def main() -> int:
    test_export()
    test_config()
    test_notebook()
    print()
    if FAILURES:
        print(f"FAILED: {len(FAILURES)} check(s): {FAILURES}")
        return 1
    print("ALL PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
