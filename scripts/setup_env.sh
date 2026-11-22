#!/usr/bin/env bash
# One-command, re-runnable environment setup for the Vinyl Decade project.
#
#   scripts/setup_env.sh             # create .venv, install packages, register kernel, make data dirs, link raw dumps
#   scripts/setup_env.sh --dry-run   # print every action, change nothing
#   scripts/setup_env.sh --check     # verify the finished environment (non-zero exit on failure)
#
# Spark is NOT installed here. On this server PySpark comes from SPARK_HOME
# (/usr/local/spark) via PYTHONPATH, and pip-installing it would shadow the
# cluster's Spark. The requirements file is therefore filtered before install.
set -euo pipefail

BASE_PYTHON=/opt/conda/bin/python3
KERNEL_NAME=vinyl
KERNEL_DISPLAY="Python (vinyl)"
MIN_RAW_FILES=8

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
VENV="$ROOT/.venv"
VENV_PY="$VENV/bin/python"
REQS="$ROOT/requirements.txt"
USER_NAME=${USER:-$(id -un)}
# DISCOGS_SETUP_DATA_DIR overrides the data dir (used to test the link step).
DATA_DIR="${DISCOGS_SETUP_DATA_DIR:-/tmp/$USER_NAME/discogs}"
SOURCE_ROOT=/mnt/data/public/discogs   # shared, read-only; never written

DRY_RUN=0
CHECK=0
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=1 ;;
    --check)   CHECK=1 ;;
    -h|--help) sed -n '2,10p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "unknown argument: $arg (use --dry-run, --check)" >&2; exit 2 ;;
  esac
done

log()  { echo "[setup] $*"; }
warn() { echo "[setup] WARNING: $*" >&2; }
die()  { echo "[setup] ERROR: $*" >&2; exit 1; }
plan() { echo "[dry-run] $*"; }

# --------------------------------------------------------------------------
# --check: verify the finished environment
# --------------------------------------------------------------------------
if (( CHECK )); then
  if (( DRY_RUN )); then
    plan "cd $ROOT/lab2 && DISCOGS_DATA_DIR=\${DISCOGS_DATA_DIR:-$DATA_DIR} $VENV_PY -c '<import check>'"
    exit 0
  fi
  [[ -x $VENV_PY ]] || die "$VENV_PY not found. Run scripts/setup_env.sh first."
  export DISCOGS_DATA_DIR=${DISCOGS_DATA_DIR:-$DATA_DIR}
  cd "$ROOT/lab2"
  "$VENV_PY" - <<'PY'
import pyspark, igraph, leidenalg, node2vec, umap, sklearn, plotly  # noqa: F401
import vinyl.config as config

print("pyspark", pyspark.__version__, "from", pyspark.__file__)
print("DATA_DIR", config.DATA_DIR)

# pyspark must come from SPARK_HOME, never from the venv.
if "/.venv/" in pyspark.__file__:
    raise SystemExit("pyspark was installed into .venv; uninstall it so SPARK_HOME is used")
PY
  log "check passed"
  exit 0
fi

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
# run <cmd...>: print the command in dry-run mode, execute it otherwise.
run() {
  if (( DRY_RUN )); then plan "$*"; else "$@"; fi
}

# --------------------------------------------------------------------------
# 1. Virtual environment
# --------------------------------------------------------------------------
[[ -x $BASE_PYTHON ]] || die "$BASE_PYTHON not found"
if [[ -x $VENV_PY ]]; then
  log "venv exists, skipping: $VENV"
else
  log "creating venv: $VENV"
  run "$BASE_PYTHON" -m venv --system-site-packages "$VENV"
fi

# --------------------------------------------------------------------------
# 2. Packages (everything in requirements.txt except pyspark)
# --------------------------------------------------------------------------
[[ -f $REQS ]] || die "$REQS not found"
PYSPARK_LINE='^[[:space:]]*pyspark([[:space:]]|[][<>=!~;]|$)'
if (( DRY_RUN )); then
  log "packages to install (pyspark filtered out):"
  { grep -viE "$PYSPARK_LINE" "$REQS" || true; } | grep -vE '^[[:space:]]*(#|$)' | sed 's/^/    /'
  plan "$VENV_PY -m pip install -r <requirements without pyspark>"
else
  filtered=$(mktemp)
  trap 'rm -f "$filtered"' EXIT
  { grep -viE "$PYSPARK_LINE" "$REQS" || true; } > "$filtered"
  log "installing requirements (pyspark excluded; pip skips satisfied ones)"
  # No --upgrade: the default only-if-needed strategy keeps conda packages as they are.
  "$VENV_PY" -m pip install -r "$filtered"
fi

# --------------------------------------------------------------------------
# 3. Jupyter kernel
# --------------------------------------------------------------------------
# The kernel gets DISCOGS_DATA_DIR, SPARK_HOME and PYTHONPATH so notebooks see
# Spark and the data. The Python helper reads values from the environment
# (no shell interpolation into code).
kernel_helper() {  # $1 = "check" | "patch"
  KERNEL_NAME=$KERNEL_NAME VENV_PY=$VENV_PY DATA_DIR=$DATA_DIR \
  "$VENV_PY" - "$1" <<'PY'
import json, os, sys
from pathlib import Path
from jupyter_core.paths import jupyter_data_dir

mode = sys.argv[1]
path = Path(jupyter_data_dir()) / "kernels" / os.environ["KERNEL_NAME"] / "kernel.json"
want_env = {
    "DISCOGS_DATA_DIR": os.environ["DATA_DIR"],
    "SPARK_HOME": os.environ["SPARK_HOME"],
    "PYTHONPATH": os.environ.get("PYTHONPATH", ""),
}

if mode == "check":  # exit 0 when the kernel is already correct
    try:
        spec = json.loads(path.read_text())
    except (OSError, ValueError):
        sys.exit(1)
    ok = spec["argv"][0] == os.environ["VENV_PY"] and all(
        spec.get("env", {}).get(k) == v for k, v in want_env.items()
    )
    sys.exit(0 if ok else 1)

spec = json.loads(path.read_text())
spec.setdefault("env", {}).update(want_env)
path.write_text(json.dumps(spec, indent=1) + "\n")
print(f"patched {path}")
PY
}

if (( DRY_RUN )); then
  plan "$VENV_PY -m ipykernel install --user --name $KERNEL_NAME --display-name \"$KERNEL_DISPLAY\""
  plan "patch kernel.json env: DISCOGS_DATA_DIR=$DATA_DIR SPARK_HOME=${SPARK_HOME:-<unset>} PYTHONPATH=${PYTHONPATH:-<unset>}"
else
  [[ -n ${SPARK_HOME:-} ]] || die "SPARK_HOME is not set; the kernel needs it to find Spark"
  if kernel_helper check; then
    log "kernel '$KERNEL_NAME' already registered and up to date, skipping"
  else
    log "registering kernel '$KERNEL_NAME'"
    "$VENV_PY" -m ipykernel install --user --name "$KERNEL_NAME" --display-name "$KERNEL_DISPLAY"
    kernel_helper patch
  fi
fi

# --------------------------------------------------------------------------
# 4. Data directories (kept in /tmp because /home2 is nearly full)
# --------------------------------------------------------------------------
for sub in raw parquet; do
  if [[ -d $DATA_DIR/$sub ]]; then
    log "data dir exists, skipping: $DATA_DIR/$sub"
  else
    run mkdir -p "$DATA_DIR/$sub"
  fi
done

# --------------------------------------------------------------------------
# 5. Raw-dump symlinks ($DATA_DIR/raw/<name> -> shared read-only dump)
# --------------------------------------------------------------------------
# Snapshot dates and entities come from lab2/vinyl/config.py, and the link
# names from config.raw_path, so they cannot drift from what the code reads.
# The source folder is the snapshot key (2020, 2026). Nothing under
# $SOURCE_ROOT is ever written; it is only tested and read through the links.
link_raw_dumps() {
  local lines year name src dst current
  lines=$(cd "$ROOT/lab2" && "$BASE_PYTHON" - <<'PY'
import os
from vinyl import config

for snapshot in config.SNAPSHOTS:
    for entity in config.ROW_TAGS:
        print(snapshot, os.path.basename(config.raw_path(snapshot, entity)))
PY
  ) || die "could not read snapshots from lab2/vinyl/config.py"

  while read -r year name; do
    [[ -n $name ]] || continue
    src="$SOURCE_ROOT/$year/$name"
    dst="$DATA_DIR/raw/$name"

    if [[ ! -s $src ]]; then   # -s: exists and is not 0 bytes
      warn "source missing or empty, not linking: $src"
      continue
    fi
    if [[ -L $dst && $(readlink "$dst") == "$src" ]]; then
      log "already linked: $name"
      continue
    fi
    if [[ -e $dst && ! -L $dst ]]; then
      warn "$dst exists and is not a symlink, leaving it alone"
      continue
    fi
    run ln -sfn "$src" "$dst"
  done <<< "$lines"
}

link_raw_dumps

n=0
shopt -s nullglob
for f in "$DATA_DIR"/raw/*.xml.gz; do
  [[ -s $f ]] && n=$((n + 1))   # -s follows symlinks, so broken links do not count
done
shopt -u nullglob
if (( n < MIN_RAW_FILES )); then
  warn "found $n non-empty .xml.gz file(s) in $DATA_DIR/raw, expected $MIN_RAW_FILES (4 dumps x 2 snapshots)"
else
  log "raw dumps present: $n files"
fi

# --------------------------------------------------------------------------
# Done
# --------------------------------------------------------------------------
echo
log "done. Add these to your shell:"
echo "export DISCOGS_DATA_DIR=$DATA_DIR"
echo "source $VENV/bin/activate"
log "then verify with: scripts/setup_env.sh --check"
