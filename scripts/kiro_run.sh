#!/usr/bin/env bash
# Run one Kiro agent headlessly on one orchestration task.
#
#   scripts/kiro_run.sh worker   T03-config-todos            # default model
#   scripts/kiro_run.sh worker   T10-clustering claude-opus-5.5
#   scripts/kiro_run.sh reviewer T03-config-todos
#
# Logs go to .orchestration/runs/<task>-<role>-<n>.jsonl (stream-json events)
# and a plain-text copy of the final answer to the matching .txt file.
# Exit code: Kiro's exit code (or 124 on timeout, which also kills the whole
# process group). KIRO_TIMEOUT sets the limit in seconds (default 3600);
# KIRO_EXTRA appends an orchestrator note to the prompt.
set -euo pipefail

role=${1:?role: worker|reviewer}
task=${2:?task id, e.g. T00-requirements}
case "$role" in
  worker)   model=${3:-claude-sonnet-5.5} ;;
  reviewer) model=${3:-gpt-5.6-sol} ;;
  *) echo "role must be worker or reviewer" >&2; exit 2 ;;
esac

root=$(git rev-parse --show-toplevel)
cd "$root"
spec=".orchestration/tasks/$task.md"
[[ -f $spec ]] || { echo "missing $spec" >&2; exit 2; }

runs=.orchestration/runs
mkdir -p "$runs" .orchestration/reviews
n=1; while [[ -e $runs/$task-$role-$n.jsonl ]]; do n=$((n+1)); done
log=$runs/$task-$role-$n.jsonl

if [[ $role == worker ]]; then
  review=.orchestration/reviews/$task.md
  prompt="Task spec: $spec. Implement it."
  if [[ -f $review ]] && head -1 "$review" | grep -q 'VERDICT: CHANGES'; then
    prompt+=" This is a revision round: the reviewer's findings are in $review. Address every numbered finding."
  fi
else
  prompt="Review task $task. Spec: $spec. Write the verdict to .orchestration/reviews/$task.md."
fi

echo "[kiro_run] $role $task model=$model branch=$(git branch --show-current) log=$log" >&2
[[ -n ${KIRO_EXTRA:-} ]] && prompt+=" Orchestrator note: $KIRO_EXTRA"

# Run Kiro in its own process group so a timeout also kills every command
# it started (plain `timeout` would leave Spark jobs etc. running).
set +e
setsid kiro-cli chat \
  --no-interactive \
  --agent "vinyl-$role" \
  --model "$model" \
  --trust-workspace \
  --output-format stream-json \
  "$prompt" </dev/null >"$log" 2>"${log%.jsonl}.stderr" &
kpid=$!
(
  sleep "${KIRO_TIMEOUT:-3600}"
  if kill -0 "$kpid" 2>/dev/null; then
    echo "[kiro_run] timeout after ${KIRO_TIMEOUT:-3600}s, killing process group $kpid" >&2
    kill -TERM -- "-$kpid" 2>/dev/null; sleep 20; kill -KILL -- "-$kpid" 2>/dev/null
    touch "${log%.jsonl}.timeout"
  fi
) &
wpid=$!
wait "$kpid"; rc=$?
kill "$wpid" 2>/dev/null; wait "$wpid" 2>/dev/null
[[ -e ${log%.jsonl}.timeout ]] && rc=124
set -e

# Pull the assistant's text out of the event stream for quick reading.
python3 -I - "$log" >"${log%.jsonl}.txt" <<'PY' || true
import json, sys
out = []
for line in open(sys.argv[1], errors="replace"):
    try:
        ev = json.loads(line)
    except ValueError:
        continue
    def walk(o):
        if isinstance(o, dict):
            if o.get("type") in ("text", "agent_message_chunk") and isinstance(o.get("text"), str):
                out.append(o["text"])
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(ev)
print("".join(out))
PY

echo "[kiro_run] exit=$rc" >&2
exit $rc
