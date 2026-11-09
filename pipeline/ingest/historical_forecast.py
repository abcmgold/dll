"""Nguồn 2 – Open-Meteo Historical Forecast API: nối các giờ đầu của từng lượt chạy mô hình dự báo.

Gọi 2 model trong 1 request (ECMWF IFS 0.25° từ 03/02/2024, GFS từ 23/03/2021); API thêm hậu tố
tên model vào tên trường, vd. temperature_2m_ecmwf_ifs025. Mô hình chạy 6 giờ/lần, công bố sau ~5–7 giờ;
API trả cả giá trị dự báo cho giờ tương lai → Silver bỏ các giờ > thời điểm lấy.
"""
from __future__ import annotations

from datetime import date

from pipeline.ingest.common import expected_fields, ingest_open_meteo

SOURCE = "historical_forecast"
URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"
HOURLY_VARS = ["temperature_2m", "relative_humidity_2m", "precipitation"]
MODELS = ["ecmwf_ifs025", "gfs_seamless"]
FIRST_AVAILABLE = date(2021, 3, 23)  # GFS; ECMWF IFS 0.25° từ 2024-02-03 (trước đó trả null)


def ingest(start: date, end: date, run_id: str, backfill: bool = False) -> int:
    start = max(start, FIRST_AVAILABLE)
    if start > end:
        return 0
    params = {"hourly": ",".join(HOURLY_VARS), "models": ",".join(MODELS)}
    fields = expected_fields(HOURLY_VARS, tuple(MODELS))
    return ingest_open_meteo(SOURCE, URL, params, fields, start, end, run_id, backfill)
