"""B1/B2 – Lấy dữ liệu từ 3 API của Open-Meteo và ghi thô vào Iceberg (Bronze)."""
from __future__ import annotations

import importlib
from datetime import date

SOURCES = ("era5", "historical_forecast", "air_quality")


def run_ingest(source: str, start: date, end: date, run_id: str, backfill: bool = False) -> int:
    if source not in SOURCES:
        raise ValueError(f"Unknown source {source!r}")
    module = importlib.import_module(f"pipeline.ingest.{source}")
    return module.ingest(start, end, run_id, backfill)
