#!/usr/bin/env bash
# T03: full ingest of both snapshots, detached from the terminal.
#
#   mkdir -p .orchestration/runs/T03 && nohup setsid lab2/scripts/run_full_ingest.sh \
#     > .orchestration/runs/T03/run.log 2>&1 < /dev/null &
#
# 1. Pre-split the raw gz files with ingest.split_raw_xml, the two releases
#    files concurrently (the split is gzip-bound, about 1 core each). The
#    parameters match what tables.build_all(fast=True) uses, so build_all
#    finds the _DONE marker and reuses the chunks.
# 2. Execute explorers/01_ingest_and_parse.ipynb headless on the "vinyl"
#    kernel with a 10 GB driver heap (the pod limit is 16 GiB). The executed copy (with outputs) goes
#    to $OUT, the tracked notebook is not modified.
set -euo pipefail

here=$(cd "$(dirname "$0")/.." && pwd)          # lab2/
repo=$(cd "$here/.." && pwd)
py=$repo/.venv/bin/python
OUT=${T03_OUT:-$repo/.orchestration/runs/T03}
mkdir -p "$OUT"
export DISCOGS_DATA_DIR=${DISCOGS_DATA_DIR:-$here/data}
export SPARK_HOME=${SPARK_HOME:-/usr/local/spark}
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.9.9-src.zip:$SPARK_HOME/python:$here

stamp() { date '+%F %T'; }

split() {  # split <snapshot> <entity...>
  local snap=$1; shift
  for entity in "$@"; do
    echo "[$(stamp)] split $snap $entity start"
    (cd "$here" && "$py" - "$snap" "$entity" <<'PY')
import json, sys, time
from vinyl import config, ingest
snap, entity = sys.argv[1], sys.argv[2]
window = (config.YEAR_MIN, config.YEAR_MAX) if entity == "releases" else None
t = time.time()
d = ingest.split_raw_xml(snap, entity, chunk_bytes=config.STAGE_CHUNK_BYTES, year_window=window)
print(snap, entity, d, f"{time.time() - t:.0f}s", json.dumps(ingest.read_stage_stats(d)))
PY
    echo "[$(stamp)] split $snap $entity done"
  done
}

echo "[$(stamp)] T03 start, out=$OUT"
split 2026 releases > "$OUT/split-2026-releases.log" 2>&1 & p1=$!
split 2020 releases > "$OUT/split-2020-releases.log" 2>&1 & p2=$!
split 2026 labels artists masters > "$OUT/split-2026-entities.log" 2>&1 \
  && split 2020 labels >> "$OUT/split-2026-entities.log" 2>&1 & p3=$!
rc=0
for p in $p1 $p2 $p3; do wait "$p" || rc=1; done
cat "$OUT"/split-*.log
[[ $rc == 0 ]] || { echo "[$(stamp)] a split failed"; exit 1; }
echo "[$(stamp)] splits done"

export PYSPARK_SUBMIT_ARGS="--driver-memory 10g pyspark-shell"
cd "$here/explorers"
"$py" -m jupyter nbconvert --to notebook --execute \
  --ExecutePreprocessor.timeout=-1 --ExecutePreprocessor.kernel_name=vinyl \
  --output-dir "$OUT" --output 01_ingest_and_parse.executed.ipynb \
  01_ingest_and_parse.ipynb
echo "[$(stamp)] notebook done"
du -sh "$DISCOGS_DATA_DIR"/parquet/*/* 2>/dev/null || true
ls "${DISCOGS_STAGE_DIR:-/tmp/$USER/discogs_stage}" 2>/dev/null || echo "stage dir empty or gone"
echo "[$(stamp)] T03 end"
