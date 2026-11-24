# T12b - main.ipynb prose: findings, summary, discussion, limitations, conclusion

## Goal
Write every remaining prose cell of `lab2/main.ipynb`, the notebook that will be submitted, from the full-data outputs. Also fill the Decisions checklists in the explorer notebooks. **Every number in the prose must come from an executed output. Do not invent or round in a way that changes meaning.**

## Inputs (read these first)
- `.orchestration/runs/T12/main.orch.ipynb`: main.ipynb executed top to bottom on full data by the orchestrator. This is the **source of truth for numbers**.
- `lab2/dashboard/data/*.csv` and `manifest.json`: the same key tables as CSV.
- `lab2/explorers/executed/0*.ipynb`: the executed explorers, for detail and for the Decisions checklists.
- `lab2/main.ipynb`: the file you edit. Its outputs are cleared; leave them cleared.

## Style (the user's request; the reviewer checks it)
The prose should **sound natural and human, but still a bit academic**, like a careful student report, not a generated summary.
- Write in plain, varied sentences, in paragraphs. "We" is fine. Use bullets only where a list really helps, such as the executive summary's key results.
- Give a number, then say what it means. Hedge where the data is uncertain (cataloguing lag, partial 2026, Discogs is user-contributed), but don't hedge every sentence.
- **Avoid:**
  - stock phrasing: "delve", "it is worth noting", "it is important to note", "in today's", "landscape", "tapestry", "plays a crucial role", "a testament to";
  - "In conclusion," as an opener, rhetorical questions as section openers, and chains of em dashes;
  - bold everywhere and exclamation marks.
- Use British or American spelling consistently, matching the existing cells, which are British-leaning ("catalogued", "neighbour").
- Make each finding 2-5 sentences. Name the figure or table it rests on when that helps the reader.

## Cells to write (indices as of commit 6a4c96f plus the UMAP patch; find them by their text)
1. **Cell 0:** replace `<RUN DATE>` with the date in the executed notebook's run metadata or log, or with the orchestrator's note in `KIRO_EXTRA`.
2. **Every `> **Finding (TODO).** ...` blockquote** in sections 3 and 5 (12 cells):
   - replace the `(TODO)` text with the finding and keep the `> **Finding.**` format;
   - answer exactly what each TODO line asks.
3. **Section 1, Executive summary** (write it last):
   - one short paragraph of context;
   - 4-6 bullets, one main result per research question plus the snapshot result, each with its key number;
   - one sentence on the biggest caveat.
4. **Section 5.6 intro:** it's stale. It still says node2vec and the features are stubs. Bring it in line with what runs now: profile + TF-IDF + node2vec, then `combine_features`, UMAP and HDBSCAN.
5. **Section 6, Discussion:**
   - 3-5 paragraphs tying the RQs together: diversity vs concentration, the vinyl boom as a catalogue effect, majors versus the long tail, and what the community structure says about how music is organised;
   - say where the findings agree or disagree with common claims, without external citations beyond those in References.
6. **Section 7, Limitations:** keep what is there, sharpen it, and add from the outputs:
   - cataloguing lag, quantified from 5.4;
   - partial 2026;
   - parent-coverage gaps (the share on a root alone);
   - pressing-plant coverage;
   - prolific-artist sensitivity;
   - the `GRAPH_MIN_WEIGHT` thresholds;
   - unseeded UMAP, with results fixed by saved files;
   - self-released labels.
7. **Section 8, Conclusion:** one or two paragraphs that answer each RQ in a sentence and suggest one or two follow-ups.
8. **Section 10, References:** keep the existing entries and complete them. Cite Discogs data dumps, PySpark, Leiden (Traag et al. 2019), node2vec (Grover & Leskovec 2016), UMAP (McInnes et al. 2018), HDBSCAN (Campello et al. 2013; McInnes et al. 2017) and Shannon entropy / Pielou evenness, using one consistent author-year style. **Only cite what the notebook actually uses.**
9. **Do not touch section 9, Dashboard.** T13 writes it.

## Explorer Decisions checklists
- Each explorer `lab2/explorers/0*.ipynb` (source, not executed) has a "Decisions" markdown checklist. Tick each item (`- [x]`) and add the decision and its evidence in a short clause, using the numbers from the executed copy, for example `- [x] min_weight for the collaboration graph: 2 (keeps 152K edges; modularity 0.93)`.
- Make the same markdown edit in the matching `lab2/explorers/executed/` copy. Markdown cells only; never touch code or outputs.
- Where an item was not decided, write `- [ ] ... - not decided: <reason>`.

## Files in scope
`lab2/main.ipynb` (markdown cells listed above only), `lab2/explorers/0*.ipynb` and `lab2/explorers/executed/0*.ipynb` (Decisions markdown cells only), and new `lab2/scripts/test_t12b.py`.

## Acceptance checks
1. `cd lab2 && timeout 300 ../.venv/bin/python scripts/test_t12b.py` exits 0. It checks:
   - main.ipynb is valid JSON and its code cells are byte-identical to `git show HEAD:lab2/main.ipynb`;
   - no `TODO` or `<RUN DATE>` remains in markdown, except in the section 9 cell;
   - no banned phrase from the Style list (case-insensitive);
   - explorers' code cells are unchanged versus HEAD, and the executed copies' outputs are unchanged;
   - every Decisions item is `[x]` or says "not decided".
2. **Number audit:** list in your final summary 15 numbers from the prose and the output cell each comes from (cell index + the printed value). The reviewer spot-checks them.
3. Do not run any notebook or Spark.
4. `git diff --stat` touches only files in scope.

## Out of scope
Code cells, analysis functions, the dashboard, and section 9.
