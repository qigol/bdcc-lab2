# Orchestration: Claude Code + Kiro

Claude Code is the orchestrator. Kiro CLI agents do the work and the review.

| Role | Agent config | Default model | Can do |
|---|---|---|---|
| Orchestrator | Claude Code session | - | writes specs, runs full-data Spark jobs, final review, commits, merges |
| Worker | `.kiro/agents/vinyl-worker.json` | `claude-sonnet-5.5` | read the repo, write inside the repo (not `.git`, `.kiro`, task specs or reviews), run shell commands |
| Reviewer | `.kiro/agents/vinyl-reviewer.json` | `gpt-5.6-sol` | read the repo, run read-only and test commands, write only `.orchestration/reviews/` |

Both agents load `.kiro/steering/project.md`, which holds the project rules.

## Layout
- `tasks/Txx-<slug>.md`: one spec per task, with goal, files in scope, acceptance checks, and what is out of scope. Committed.
- `reviews/Txx-<slug>.md`: the reviewer's verdict. The first line is `VERDICT: PASS` or `VERDICT: CHANGES`. Committed.
- `runs/`: raw Kiro logs (`.jsonl` event stream, `.txt` final answer, `.stderr`). Gitignored.

## Loop for one task
1. Write `tasks/Txx-<slug>.md`.
2. `git switch -c task/Txx-<slug>` from main.
3. `scripts/kiro_run.sh worker Txx-<slug> [model]`. Use `claude-opus-5.5` for hard tasks.
4. The orchestrator re-runs the acceptance checks.
5. `scripts/kiro_run.sh reviewer Txx-<slug>`.
6. On `VERDICT: CHANGES`, run the worker again. The script detects the review and asks the worker to address it. Then run the reviewer again. After 2 rounds, the orchestrator fixes it or asks the user.
7. The orchestrator reads `git diff main`, commits on the branch, and merges into main with `--no-ff`.

Full-data Spark jobs (ingest, full explorer and notebook runs) are run by the orchestrator, not by Kiro.

## Sandbox notes (verified with a permission probe, 2026-11-23)
- `deniedCommands` regexes are enforced. For example, `rm -rf` was rejected with "arguments supplied are forbidden".
- `fs_write` `deniedPaths` is enforced. A write to `.kiro/**` was rejected.
- When one call in a parallel batch is rejected, Kiro cancels its sibling calls, and these show up as "cancelled by the user". Rejected calls can therefore make allowed calls look denied.
- The models also refuse `git commit` and `git switch` because of the steering rules, so that guard has two layers.
- Kiro runs headless (`--no-interactive`). Any tool call that would need approval is cancelled, not prompted.
