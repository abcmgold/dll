"""Bảo trì bảng Iceberg: gộp file nhỏ (compaction) và dọn snapshot cũ cho cả Bronze, Silver, Gold."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from pipeline.spark.common import get_spark

log = logging.getLogger("maintenance")

TABLES = [
    "bronze.era5_raw",
    "bronze.historical_forecast_raw",
    "bronze.air_quality_raw",
    "silver.weather_hourly",
    "silver.air_quality_hourly",
    "silver.dim_province",
    "silver.dq_run_log",
    "gold.weather_daily",
    "gold.weather_best_hourly",
    "gold.weather_best_daily",
    "gold.air_quality_daily",
    "gold.weather_anomaly_daily",
    "gold.weather_events",
    "gold.weather_yearly",
    "gold.weather_trend",
]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    spark = get_spark("weather-iceberg-maintenance")
    # CALL chỉ nhận literal → tính mốc thời gian trước
    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    for table in TABLES:
        if not spark.catalog.tableExists(f"iceberg.{table}"):
            continue
        rewritten = spark.sql(f"CALL iceberg.system.rewrite_data_files(table => '{table}')").first()
        log.info("%s rewrite_data_files: %s", table, rewritten.asDict())
        expired = spark.sql(
            f"CALL iceberg.system.expire_snapshots(table => '{table}', "
            f"older_than => TIMESTAMP '{cutoff}', retain_last => 5)"
        ).first()
        log.info("%s expire_snapshots: %s", table, expired.asDict())
    spark.stop()


if __name__ == "__main__":
    main()
