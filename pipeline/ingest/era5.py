"""Nguồn 1 – Open-Meteo Archive API, ERA5 (tái phân tích ECMWF), model era5_seamless.

era5_seamless = ERA5-Land (0.1°) cho nhiệt độ/độ ẩm, bổ sung bằng ERA5 (0.25°).
Không dùng model era5_land thuần vì Open-Meteo trả precipitation = null cho model này.
Cập nhật 1 lần/ngày, dữ liệu trễ 5–6 ngày (các giờ chưa có trả null → bỏ ở Silver). Lịch sử từ 1950.
"""
from __future__ import annotations

from datetime import date

from pipeline.ingest.common import expected_fields, ingest_open_meteo

SOURCE = "era5"
URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY_VARS = ["temperature_2m", "relative_humidity_2m", "precipitation"]
MODEL = "era5_seamless"
FIRST_AVAILABLE = date(1950, 1, 1)


def ingest(start: date, end: date, run_id: str, backfill: bool = False) -> int:
    start = max(start, FIRST_AVAILABLE)
    if start > end:
        return 0
    params = {"hourly": ",".join(HOURLY_VARS), "models": MODEL}
    return ingest_open_meteo(SOURCE, URL, params, expected_fields(HOURLY_VARS), start, end, run_id, backfill)
