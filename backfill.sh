#!/usr/bin/env sh
# Lay du lieu lich su hoac doan con thieu. Dung cho macOS, Linux, Git Bash.
#   sh backfill.sh 2015-01-01
#   sh backfill.sh 2026-10-29 2026-11-05
#   sh backfill.sh 2015-01-01 --sources era5 air_quality
set -e
cd "$(dirname "$0")"
exec docker compose exec -T airflow-scheduler python -u -m pipeline.backfill_cli "$@"
