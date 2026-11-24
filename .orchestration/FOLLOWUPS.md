# Follow-ups found while running the explorers

1. **"Not On Label" (label 1818) has a parent in 2026.** Its parent is Stolen Riff Records (205754), whose own parent is 212. This looks like a data-entry error.
   - 1818 holds 4-6% of each year's releases (24,562 in 2016, 11,613 in 2025).
   - Major tagging is unaffected, since the root is not a major.
   - `labels.parent_coverage` (T07) counts these releases as `in_tree`. Add a self-released class, covering 1818 and "Not On Label (... Self-released)" labels, or exclude them, before quoting coverage in the report.
   - It is also the top "changed parent" row in explorer 05.
2. **Top "independent" artist.** It is "THIS IS WHAT I HEAR WHEN YOU TALK" (6351861, about 3.4 K releases), which looks like a bulk uploader. Check it before naming top artists in the report, and consider adding it to a placeholder or exclusion list.
3. **Decisions checklists** in explorers 02-05 are still open. Fill them from the executed outputs in `.orchestration/runs/T05..T08/` when drafting the report (T12).
4. **Executed explorer outputs**: done. They are committed in `lab2/explorers/executed/`.

## Status (2026-11-24)
- **Item 1 resolved in T08b.** Self-released labels (1818 plus the "Not On Label …" pattern) now have their own `self_released` class in `parent_coverage`. Releases by class in 2016-2026: self-released 12-18%, in a tree 41-46%, standing alone 38-45%.
- **Item 2 resolved in T08b.** 6351861 is a real harsh-noise-wall project, not a placeholder. Dropping prolific artist-years (>= 300 releases) leaves the style-change ranking intact (Spearman 0.995). Only Progressive House loses about 25% of its gain. Mention this as a robustness check in the report.
- **Item 3:** fill the checklists from `lab2/explorers/executed/`.
