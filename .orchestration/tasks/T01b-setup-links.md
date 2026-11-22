# T01b - setup recreates raw-data links, README fix

## Goal
Make `scripts/setup_env.sh` fully re-runnable after `/tmp` is wiped by creating the raw-dump symlinks itself, and fix a misleading README line.

## Facts
- Shared read-only dumps: `/mnt/data/public/discogs/2020/discogs_20200101_<entity>.xml.gz` and `/mnt/data/public/discogs/2026/discogs_20261001_<entity>.xml.gz`, entities `artists labels masters releases`.
- Targets: `$DATA_DIR/raw/<same file name>` (`DATA_DIR=/tmp/$USER/discogs`), names exactly as `lab2/vinyl/config.py::raw_path` builds them. Currently they are created by hand with `ln -sfn`.
- Snapshot dates must come from `lab2/vinyl/config.py` `SNAPSHOTS` (do not hard-code `20200101`/`20261001` in the script). Read them with the venv Python or `/opt/conda/bin/python3` importing `vinyl.config` from `lab2/`. The source year folder is the snapshot key (`2020`, `2026`).
- Some files in `/mnt/data/public/discogs/2026` are 0 bytes (failed downloads), e.g. `discogs_20260701_releases.xml.gz`.

## Files in scope
- `scripts/setup_env.sh`
- `README.md`

## Requirements
1. New step in `setup_env.sh` after creating the data dirs: for every snapshot and entity, link `$DATA_DIR/raw/<name>` to the source file.
   - Skip if the link already points at that source (idempotent). Replace a wrong or broken link.
   - If the source is missing or 0 bytes, warn, don't link, and keep going.
   - Never create, modify or delete anything under `/mnt/data/public/discogs`.
   - Respect `--dry-run`: print the `ln` actions only.
   - The existing "N non-empty files" count check stays and runs after linking.
2. README: in the layout table, describe `discogs_loader.py` as "Older standalone loader, superseded by `lab2/vinyl/`. Uses a different snapshot date (20261101). Do not use; to be removed." Mention in the Setup steps list that the script also links the raw dumps.

## Acceptance checks
1. `bash -n scripts/setup_env.sh`
2. `scripts/setup_env.sh --dry-run` exits 0. It shows the 8 link checks: "already linked" for each, since the links exist now.
3. Simulate a wiped `/tmp` without touching the real links. Run the link step against a temporary data dir: if you add an env override such as `DISCOGS_SETUP_DATA_DIR`, use it with `mktemp -d`, then confirm that 8 symlinks are created and resolve (`ls -laL`). Leave the temp dir. You can't `rm -r`, so report its path.
4. `scripts/setup_env.sh --check` still passes.

## Out of scope
Anything under `lab2/`, and other README sections.
