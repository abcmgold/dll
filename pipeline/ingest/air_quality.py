"""Nguồn 3 – Open-Meteo Air Quality API (CAMS global, lưới ~0.4°): PM2.5, PM10, US AQI theo giờ.

CAMS chạy 12 giờ/lần, bước gốc 3 giờ (Open-Meteo trả theo giờ). Dữ liệu có từ 2022-08-04.
us_aqi tính từ trung bình trượt 24 giờ nên đổi chậm.
"""
from __future__ import annotations

from datetime import date

from pipeline.ingest.common import expected_fields, ingest_open_meteo

SOURCE = "air_quality"
URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
HOURLY_VARS = ["pm2_5", "pm10", "us_aqi"]
FIRST_AVAILABLE = date(2022, 8, 4)


def ingest(start: date, end: date, run_id: str, backfill: bool = False) -> int:
    start = max(start, FIRST_AVAILABLE)
    if start > end:
        return 0
    params = {"hourly": ",".join(HOURLY_VARS), "domains": "cams_global"}
    return ingest_open_meteo(SOURCE, URL, params, expected_fields(HOURLY_VARS), start, end, run_id, backfill)
