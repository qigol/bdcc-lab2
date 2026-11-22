VERDICT: CHANGES

1. `lab2/scripts/sample_ingest_check.py:799` — The default run caps each skip at 420 seconds, so it does not take the required later sample at about 80% of either releases file. The supplied exit-0 summary confirms that sampling stopped by time at only 11% for 2020 and 7% for 2026, finding 0 and 1 in-window releases rather than the requested minimum of 200. This leaves the late-file/year-window check incomplete and bases the size estimates on early prefixes rather than the requested later probe. Make the default acceptance path reach approximately 80% (or obtain an equivalent representative late sample within the timeout), and treat an early time stop as incomplete rather than allowing the run to report success. A time cap can remain as an explicit opt-in diagnostic option.

2. `lab2/scripts/sample_ingest_check.py:679` — When the late samples do not contain enough 2016–2026 releases, the script stages releases with the unrelated `(1000, 2100)` window and lets `build_all` reuse that staged data. Consequently, the reported release-table counts of 38,783 and 38,743 are not study-window counts, and the acceptance run never validates `build_all` with its normal inclusive 2016–2026 staging behavior. Run the acceptance build with the normal window and a fresh/default stage. If a widened build is useful as a supplemental schema exercise, run and label it separately; do not substitute it for the required build or report its row count in the 2016–2026 section.

3. `lab2/scripts/sample_ingest_check.py:434` — The recursive schema comparison strips every `ArrayType` before recording a path. A saved scalar and an inferred array therefore compare as the same shape when their element types match, even though a saved scalar can lose repeated XML values. Preserve array wrappers/cardinality in the flattened representation and report array-versus-scalar differences as shape drift.

## Nits

- `lab2/scripts/sample_ingest_check.py:652` — At 1,023 lines and 39.8 KB, the one-off check is harder to maintain than needed. Fixing finding 2 removes the `WIDE_WINDOW`/`widen` branch. The table-by-table rebuild fallback can also be replaced with one `build_all` attempt plus the original exception, and the thread-local output-buffer machinery can be replaced with returned result data. Those changes would keep the four required sections while reducing state and duplicate execution.

## Checks

- Used the orchestrator-provided `/tmp/jsandiego/discogs_sample/summary.txt`; the full run exited 0 and reported 11/11 tables for each snapshot.
- `python -m py_compile lab2/scripts/sample_ingest_check.py` passed.
- `git status --short --untracked-files=all` showed only the new script before this review was written; `lab2/vinyl/` and `lab2/schemas/` were unchanged.

## Orchestrator resolution (Claude)
- Finding 1 (late sample stopped at 7-11%) and finding 2 (widened 1000-2100 window): accepted as valid and moved to T02b instead of another worker round. T02b builds a tail sample (`tail -n 30000` of each releases file) that is guaranteed to contain in-window releases, and its equivalence test uses the normal 2016-2026 window. The widened-window counts in this run are labelled as such in the summary and are not used anywhere.
- Finding 3 (array wrapper stripped): fixed by the orchestrator in `flatten` / `check_drift` (array depth now compared as shape drift). Re-check on all 8 samples: 0 array-depth differences.
- Nit (size): accepted as a one-off diagnostic. Not refactored.
