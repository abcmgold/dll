@echo off
rem Lay du lieu lich su hoac doan con thieu. Dung cho Windows (cmd, PowerShell).
rem   backfill.cmd 2015-01-01
rem   backfill.cmd 2026-10-29 2026-11-05
rem   backfill.cmd 2015-01-01 --sources era5 air_quality
cd /d "%~dp0"
docker compose exec -T airflow-scheduler python -u -m pipeline.backfill_cli %*
exit /b %errorlevel%
