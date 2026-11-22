"""Self-test for ``vinyl.ingest.iter_records`` (no pytest needed).

Each case feeds a small XML text, split into lines as ``gzip -dc`` would
give them, and checks the exact records yielded. Exit code 0 only if every
case passes.

Usage, from ``lab2/``: ``python scripts/test_iter_records.py``
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from vinyl.ingest import iter_records  # noqa: E402

R1 = b'<release id="1" status="Accepted"><title>A</title><released>2017</released></release>'
R2 = b'<release id="2" status="Accepted"><title>B</title><released>1999</released></release>'
R3 = b"<release><title>C</title></release>"  # bare start tag, no attributes
MULTI = b'<release id="4"><notes>line one\nline two\n  &lt;b&gt;indented\n</notes></release>'

#: name -> (input text, expected records without their trailing newline)
CASES: dict[str, tuple[bytes, list[bytes]]] = {
    "one-line records": (
        b"<releases>\n" + R1 + b"\n" + R2 + b"\n" + R3 + b"\n</releases>\n",
        [R1, R2, R3],
    ),
    "multi-line records": (
        b"<releases>\n" + MULTI + b"\n" + R1 + b"\n" + MULTI + b"\n</releases>\n",
        [MULTI, R1, MULTI],
    ),
    "root and first record on one line": (
        b"<releases>" + R1 + b"\n" + R2 + b"\n</releases>\n",
        [R1, R2],
    ),
    "xml declaration, root and first record on one line": (
        b'<?xml version="1.0" encoding="UTF-8"?><releases>' + R1 + b"\n" + R2 + b"\n</releases>\n",
        [R1, R2],
    ),
    "xml declaration and root on one line, multi-line first record": (
        b'<?xml version="1.0"?>\n<releases>' + MULTI + b"\n" + R2 + b"\n</releases>\n",
        [MULTI, R2],
    ),
    "closing root shares the last line": (
        b"<releases>\n" + R1 + b"\n" + R2 + b"</releases>\n",
        [R1, R2],
    ),
    "closing root shares the last line of a multi-line record": (
        b"<releases>\n" + R1 + b"\n" + MULTI + b"</releases>",
        [R1, MULTI],
    ),
    "everything on one line, one record": (
        b'<?xml version="1.0"?><releases>' + R1 + b"</releases>",
        [R1],
    ),
    "no trailing newline at end of file": (
        b"<releases>\n" + R1 + b"\n" + R2 + b"\n</releases>",
        [R1, R2],
    ),
    "empty root": (b"<releases>\n</releases>\n", []),
}

#: Entities other than releases, one case each, to check the row tag lookup.
ENTITY_CASES: dict[str, tuple[bytes, list[bytes]]] = {
    "labels": (
        b'<labels><label><id>1</id><name>X</name></label>\n<label><id>2</id></label>\n</labels>\n',
        [b"<label><id>1</id><name>X</name></label>", b"<label><id>2</id></label>"],
    ),
    "artists": (
        b"<artists>\n<artist><id>1</id><profile>a\nb</profile></artist>\n</artists>\n",
        [b"<artist><id>1</id><profile>a\nb</profile></artist>"],
    ),
}


def run_case(name: str, entity: str, text: bytes, expected: list[bytes]) -> bool:
    """Run one case and print PASS or FAIL with a diff of the records.

    Parameters
    ----------
    name : str
        Case label for the output.
    entity : str
        Entity passed to ``iter_records``.
    text : bytes
        Plain XML input.
    expected : list of bytes
        Records expected, without their trailing newline.

    Returns
    -------
    bool
        True if the records match exactly and each ends in one newline.
    """
    lines = text.splitlines(keepends=True)
    got = list(iter_records(lines, entity))
    want = [record + b"\n" for record in expected]
    ok = got == want
    print(f"{'PASS' if ok else 'FAIL'}  [{entity}] {name}")
    if not ok:
        print(f"      expected {want!r}\n      got      {got!r}")
    return ok


def main() -> int:
    """Run all cases.

    Returns
    -------
    int
        0 if every case passes, else 1.
    """
    results = [run_case(name, "releases", *case) for name, case in CASES.items()]
    results += [run_case("basic", entity, *case) for entity, case in ENTITY_CASES.items()]
    print(f"{sum(results)}/{len(results)} cases passed")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
