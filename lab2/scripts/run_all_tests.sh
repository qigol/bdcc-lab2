#!/usr/bin/env bash
# Run every lab2/scripts/test_*.py one after another and print PASS/FAIL per script.
#
#   cd lab2 && bash scripts/run_all_tests.sh                # all tests
#   cd lab2 && bash scripts/run_all_tests.sh t09 t12a       # only test_t09.py and test_t12a.py
#
# Each test gets `timeout 900` (2400 for test_t07/test_t08; TEST_TIMEOUT overrides all) and its own log in $TEST_LOG_DIR (default /tmp/$USER/lab2_tests).
# Exit status is 0 only if every test passed. Tests run strictly sequentially: the pod has 4 CPUs
# and 16 GiB, and two Spark sessions at once can push it over the limit.
#
# Which tests need what (seconds measured on the 4-CPU pod, 2026-11-25, repo venv):
#   test_iter_records   no Spark, no data         9 s
#   test_t04            Spark; real Parquet       283 s
#   test_t05            Spark; real Parquet       390 s
#   test_t06            Spark; real Parquet       572 s
#   test_t07            Spark; real Parquet       939 s (limit 2400 s)
#   test_t08            Spark; real Parquet       958 s (limit 2400 s)
#   test_t08b           Spark; tiny hand-built   281 s
#   test_t09            Spark; tiny hand-built   199 s
#   test_t10            no Spark                 239 s
#   test_t11            no Spark                  30 s
#   test_t12a           no Spark                  16 s
#   test_t12b           no Spark; needs git      2 s  (compares with commit 73d9345, see T12B_BASE)
#   test_t13            no Spark                  45 s
# "Real Parquet" means test_t04 to test_t08 read the full tables under $DISCOGS_DATA_DIR/parquet
# (test_t04 only 20,000 staged releases; the others filter to a few years after reading).
# Without that data they fail or skip, so run them where the Parquet exists.
set -uo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)   # lab2/
repo=$(cd "$here/.." && pwd)
py=$repo/.venv/bin/python
log_dir=${TEST_LOG_DIR:-/tmp/${USER:-$(id -un)}/lab2_tests}
limit=${TEST_TIMEOUT:-900}

export DISCOGS_DATA_DIR=${DISCOGS_DATA_DIR:-$here/data}
export SPARK_HOME=${SPARK_HOME:-/usr/local/spark}
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.9.9-src.zip:$SPARK_HOME/python:$here

[[ -x $py ]] || { echo "missing $py; run scripts/setup_env.sh first" >&2; exit 2; }
mkdir -p "$log_dir"
cd "$here"

if [[ $# -gt 0 ]]; then
  tests=(); for name in "$@"; do tests+=("scripts/test_${name#test_}.py"); done
else
  tests=(scripts/test_*.py)
fi

passed=0; failed=0; failed_names=()
for t in "${tests[@]}"; do
  name=$(basename "$t" .py)
  log=$log_dir/$name.log
  this_limit=$limit
  # The two real-data smoke tests read full tables and need longer (measured 2026-11-25).
  [[ -z ${TEST_TIMEOUT:-} && ( $name == test_t07 || $name == test_t08 ) ]] && this_limit=2400
  start=$(date +%s)
  timeout "$this_limit" "$py" "$t" > "$log" 2>&1
  rc=$?
  secs=$(( $(date +%s) - start ))
  if [[ $rc -eq 0 ]]; then
    printf 'PASS  %-22s %5ds\n' "$name" "$secs"; passed=$((passed + 1))
  else
    why="exit $rc"; [[ $rc -eq 124 ]] && why="timeout after ${this_limit}s"
    printf 'FAIL  %-22s %5ds  (%s, log: %s)\n' "$name" "$secs" "$why" "$log"
    failed=$((failed + 1)); failed_names+=("$name")
  fi
done

echo "----"
echo "Summary: $passed passed, $failed failed, ${#tests[@]} run"
[[ $failed -eq 0 ]] || { echo "Failed: ${failed_names[*]}"; exit 1; }
