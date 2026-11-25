"""Self-test for T12b (static checks only: no Spark, no notebook execution).

Checks the report prose in ``main.ipynb`` and the Decisions checklists of
the explorer notebooks against ``git HEAD``:

* ``main.ipynb`` is valid JSON, has the same cells in the same order as
  HEAD, and its code cells are byte-identical to HEAD (so outputs stay
  cleared);
* no ``TODO`` or ``<RUN DATE>`` is left in a markdown cell, except in the
  section 9 (Dashboard) cell;
* no banned phrase from the T12b style list appears in a markdown cell of
  ``main.ipynb`` or in a Decisions cell (case-insensitive), and there are
  no exclamation marks or em dashes in the prose;
* the explorers and their executed copies have unchanged code cells
  (source and outputs) and unchanged markdown apart from the Decisions
  cell, and each source explorer has the same Decisions cell as its
  executed copy;
* every Decisions item is ticked (``- [x]``) or says "not decided".

Usage, from ``lab2/``:
``timeout 300 ../.venv/bin/python scripts/test_t12b.py``
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

LAB2 = Path(__file__).resolve().parents[1]
REPO = LAB2.parent

FAILURES: list[str] = []

#: Phrases the T12b style list bans (matched case-insensitively).
BANNED = [
    "delve", "it is worth noting", "it is important to note", "in today's", "landscape",
    "tapestry", "plays a crucial role", "a testament to",
]
#: "In conclusion," as the opener of a paragraph, list item or blockquote line.
IN_CONCLUSION = re.compile(r"(?im)^\s*(?:[>*-]\s*)*in conclusion,")
DECISIONS_PREFIX = "## Decisions to carry"
DASHBOARD_PREFIX = "## 9. Dashboard"


def check(name: str, ok: bool, detail: str = "") -> None:
    """Record and print one check result."""
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" - {detail}" if detail and not ok else ""))
    if not ok:
        FAILURES.append(name)


def text(cell: dict) -> str:
    """Return the source of a cell as one string."""
    return "".join(cell["source"])


def load_head(rel: str) -> dict:
    """Load a notebook as it is in git HEAD (``rel`` relative to the repo root)."""
    out = subprocess.run(["git", "-C", str(REPO), "show", f"HEAD:{rel}"], capture_output=True, check=True)
    return json.loads(out.stdout.decode("utf-8"))


def load_work(path: Path) -> dict | None:
    """Load a working-tree notebook, or return None if it is not valid JSON."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        check(f"{path.relative_to(REPO)} is valid JSON", False, str(exc))
        return None


def same_code_cells(new: dict, old: dict) -> tuple[bool, str]:
    """Compare the code cells of two notebooks exactly (source, outputs, metadata)."""
    a = [c for c in new["cells"] if c["cell_type"] == "code"]
    b = [c for c in old["cells"] if c["cell_type"] == "code"]
    if len(a) != len(b):
        return False, f"{len(a)} code cells, HEAD has {len(b)}"
    bad = [i for i, (x, y) in enumerate(zip(a, b)) if json.dumps(x, sort_keys=True) != json.dumps(y, sort_keys=True)]
    return not bad, f"code cells differ at code-cell positions {bad}"


def same_layout(new: dict, old: dict) -> tuple[bool, str]:
    """Same number and order of cell types, and same cell ids."""
    a = [(c["cell_type"], c.get("id")) for c in new["cells"]]
    b = [(c["cell_type"], c.get("id")) for c in old["cells"]]
    return a == b, f"{len(a)} cells vs {len(b)} in HEAD, or types/ids differ"


def style_problems(md: str) -> list[str]:
    """Return the style rules a markdown string breaks."""
    low = md.lower()
    found = [p for p in BANNED if p in low]
    if IN_CONCLUSION.search(md):
        found.append('"In conclusion," opener')
    if re.search(r"!(?!\[)", md):
        found.append("exclamation mark")
    if "\u2014" in md:
        found.append("em dash")
    return found


def test_main() -> None:
    """Checks on lab2/main.ipynb."""
    print("main.ipynb")
    rel = "lab2/main.ipynb"
    new = load_work(REPO / rel)
    if new is None:
        return
    check("main.ipynb is valid JSON", True)
    old = load_head(rel)
    ok, detail = same_layout(new, old)
    check("same cells in the same order as HEAD", ok, detail)
    ok, detail = same_code_cells(new, old)
    check("code cells byte-identical to HEAD (outputs still cleared)", ok, detail)
    check("every code cell has no outputs",
          all(not c.get("outputs") for c in new["cells"] if c["cell_type"] == "code"))

    left, style = [], []
    for i, cell in enumerate(new["cells"]):
        if cell["cell_type"] != "markdown":
            continue
        md = text(cell)
        if md.startswith(DASHBOARD_PREFIX):
            continue   # section 9 belongs to T13
        if "TODO" in md or "<RUN DATE>" in md:
            left.append(i)
        if problems := style_problems(md):
            style.append(f"cell {i}: {problems}")
    check("no TODO or <RUN DATE> outside section 9", not left, f"cells {left}")
    check("no banned phrases, exclamation marks or em dashes in markdown", not style, "; ".join(style))

    md_cells = [text(c) for c in new["cells"] if c["cell_type"] == "markdown"]
    # The spec says 12 findings; HEAD has 11 "> **Finding (TODO).**" cells, so compare with HEAD.
    findings = [m for m in md_cells if m.lstrip().startswith("> **Finding")]
    n_todo = sum(text(c).lstrip().startswith("> **Finding (TODO).**") for c in old["cells"] if c["cell_type"] == "markdown")
    check("every '> **Finding (TODO).**' of HEAD is now '> **Finding.**'",
          len(findings) == n_todo and all(m.lstrip().startswith("> **Finding.**") for m in findings),
          f"{len(findings)} findings, HEAD has {n_todo} TODO findings")
    dash = [m for m in md_cells if m.startswith(DASHBOARD_PREFIX)]
    dash_old = [text(c) for c in old["cells"] if c["cell_type"] == "markdown" and text(c).startswith(DASHBOARD_PREFIX)]
    check("section 9 cell unchanged", dash == dash_old)
    check("cell 0 has the run date", "25 November 2026" in md_cells[0])


def decision_items(md: str) -> list[str]:
    """Return the checklist lines of a Decisions cell."""
    return [line for line in md.splitlines() if re.match(r"\s*- \[[ xX]\]", line)]


def test_explorers() -> None:
    """Checks on lab2/explorers/0*.ipynb and lab2/explorers/executed/0*.ipynb."""
    print("explorers")
    sources = sorted((LAB2 / "explorers").glob("0*.ipynb"))
    check("seven source explorers", len(sources) == 7, str(len(sources)))
    for src in sources:
        prefix = src.name[:2]
        executed = sorted((LAB2 / "explorers" / "executed").glob(f"{prefix}_*.ipynb"))
        check(f"{prefix}: one executed copy", len(executed) == 1, str(executed))
        decisions = {}
        for path in [src, *executed]:
            rel = str(path.relative_to(REPO))
            new = load_work(path)
            if new is None:
                continue
            old = load_head(rel)
            ok, detail = same_layout(new, old)
            check(f"{rel}: same cells as HEAD", ok, detail)
            ok, detail = same_code_cells(new, old)
            check(f"{rel}: code cells and outputs unchanged", ok, detail)
            changed = [
                i for i, (a, b) in enumerate(zip(new["cells"], old["cells"]))
                if a["cell_type"] == "markdown" and text(a) != text(b) and not text(b).startswith(DECISIONS_PREFIX)
            ]
            check(f"{rel}: only the Decisions markdown changed", not changed, f"cells {changed}")
            cells = [text(c) for c in new["cells"] if c["cell_type"] == "markdown" and text(c).startswith(DECISIONS_PREFIX)]
            check(f"{rel}: one Decisions cell", len(cells) == 1, str(len(cells)))
            if len(cells) != 1:
                continue
            md = decisions[path] = cells[0]
            items = decision_items(md)
            open_items = [line for line in items if not line.lstrip().startswith("- [x]") and "not decided" not in line]
            check(f"{rel}: every item ticked or 'not decided'", bool(items) and not open_items, "; ".join(open_items))
            check(f"{rel}: no TODO in the Decisions cell", "TODO" not in md)
            problems = style_problems(md)
            check(f"{rel}: Decisions cell style", not problems, str(problems))
        if len(decisions) == 2:
            a, b = decisions.values()
            check(f"{prefix}: source and executed Decisions cells match", a == b)


def main() -> int:
    """Run every check and return the exit code."""
    test_main()
    test_explorers()
    print(f"\n{'FAILED: ' + ', '.join(FAILURES) if FAILURES else 'all checks passed'}")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
