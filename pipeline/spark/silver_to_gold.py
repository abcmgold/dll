"""B4 (phần 2) – Tổng hợp Silver → Gold phục vụ Trino/Superset, tính TĂNG DẦN.

  gold.weather_daily        – tỉnh × ngày × model: nhiệt độ TB/min/max, độ ẩm TB, tổng mưa
  gold.weather_best_hourly  – tỉnh × giờ: 1 giá trị chuẩn, ưu tiên ERA5 → ECMWF IFS → GFS (cột chosen_model)
  gold.weather_best_daily   – tỉnh × ngày: tổng hợp từ weather_best_hourly
  gold.air_quality_daily    – tỉnh × ngày: PM2.5/PM10 TB, AQI cao nhất, mức AQI

Chỉ tính lại các (tỉnh, ngày) mà lượt chạy vừa ghi vào Silver (cột run_id), rồi MERGE vào Gold.

    spark-submit silver_to_gold.py --run-id <airflow run_id>
    spark-submit silver_to_gold.py --all          # tính lại toàn bộ
"""
from __future__ import annotations

import argparse
import logging

from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pipeline.spark.common import get_spark

log = logging.getLogger("silver_to_gold")

WEATHER_SILVER = "iceberg.silver.weather_hourly"
AQ_SILVER = "iceberg.silver.air_quality_hourly"
DIM_TABLE = "iceberg.silver.dim_province"

# Thứ tự ưu tiên chọn giá trị chuẩn (số nhỏ = ưu tiên cao)
MODEL_PRIORITY = {"era5_seamless": 1, "ecmwf_ifs025": 2, "gfs_seamless": 3}
# Thang US AQI của EPA
AQI_LEVELS = [(50, "Tốt"), (100, "Trung bình"), (150, "Kém (nhóm nhạy cảm)"), (200, "Xấu"),
              (300, "Rất xấu")]


def priority_col() -> Column:
    expr = F.lit(99)
    for model, rank in MODEL_PRIORITY.items():
        expr = F.when(F.col("model_name") == model, rank).otherwise(expr)
    return expr


def aqi_category(col: str) -> Column:
    expr = F.when(F.col(col).isNull(), None)
    for upper, label in AQI_LEVELS:
        expr = expr.when(F.col(col) <= upper, label)
    return expr.otherwise("Nguy hại")


# ----------------------------------------------------------------------------- phạm vi cần tính lại
def changed_rows(spark: SparkSession, table: str, run_id: str | None) -> DataFrame:
    """Các dòng Silver hợp lệ thuộc những (tỉnh, ngày) mà lượt chạy run_id vừa ghi (None = toàn bộ)."""
    silver = spark.table(table)
    valid = silver.where("is_valid")
    if run_id is None:
        return valid
    touched = silver.where(F.col("run_id") == run_id).select("province_id", "date_partition").distinct()
    return valid.join(F.broadcast(touched), ["province_id", "date_partition"])


# ----------------------------------------------------------------------------- builders
def build_weather_daily(rows: DataFrame) -> DataFrame:
    return (
        rows.groupBy("province_id", F.col("date_partition").alias("date"), "source_type", "model_name")
        .agg(
            F.round(F.avg("temperature_2m"), 2).alias("temp_avg"),
            F.round(F.min("temperature_2m"), 2).alias("temp_min"),
            F.round(F.max("temperature_2m"), 2).alias("temp_max"),
            F.round(F.avg("relative_humidity_2m"), 2).alias("rh_avg"),
            F.round(F.sum("precipitation"), 2).alias("precip_sum"),
            F.count("temperature_2m").alias("n_hours"),
        )
    )


def build_best_hourly(rows: DataFrame) -> DataFrame:
    best_first = Window.partitionBy("province_id", "timestamp").orderBy(priority_col())
    return (
        rows.where(F.col("temperature_2m").isNotNull())
        .withColumn("_rn", F.row_number().over(best_first)).where("_rn = 1")
        .select("province_id", "timestamp", "timestamp_local", F.col("date_partition").alias("date"),
                "temperature_2m", "relative_humidity_2m", "precipitation",
                F.col("model_name").alias("chosen_model"))
    )


def build_best_daily(best_hourly: DataFrame) -> DataFrame:
    return (
        best_hourly.groupBy("province_id", "date")
        .agg(
            F.round(F.avg("temperature_2m"), 2).alias("temp_avg"),
            F.round(F.min("temperature_2m"), 2).alias("temp_min"),
            F.round(F.max("temperature_2m"), 2).alias("temp_max"),
            F.round(F.avg("relative_humidity_2m"), 2).alias("rh_avg"),
            F.round(F.sum("precipitation"), 2).alias("precip_sum"),
            F.count("*").alias("n_hours"),
            F.mode("chosen_model").alias("chosen_model"),  # model chiếm đa số giờ trong ngày
        )
    )


def build_air_quality_daily(rows: DataFrame) -> DataFrame:
    # us_aqi đã là trung bình trượt 24 giờ → theo ngày lấy giá trị lớn nhất, không lấy trung bình
    return (
        rows.groupBy("province_id", F.col("date_partition").alias("date"))
        .agg(
            F.round(F.avg("pm2_5"), 2).alias("pm2_5_avg"),
            F.round(F.max("pm2_5"), 2).alias("pm2_5_max"),
            F.round(F.avg("pm10"), 2).alias("pm10_avg"),
            F.max("us_aqi").alias("aqi_max"),
            F.count("pm2_5").alias("n_hours"),
        )
        .withColumn("aqi_category", aqi_category("aqi_max"))
    )


# ----------------------------------------------------------------------------- ghi
def merge_gold(spark: SparkSession, df: DataFrame, name: str, keys: list[str], partition: Column) -> int:
    table = f"iceberg.gold.{name}"
    df = df.withColumn("updated_at", F.current_timestamp()).persist(StorageLevel.MEMORY_AND_DISK)
    n = df.count()
    if not spark.catalog.tableExists(table):
        (df.limit(0).writeTo(table).using("iceberg").partitionedBy(partition)
         .tableProperty("format-version", "2").tableProperty("write.parquet.compression-codec", "zstd").create())
        log.info("Created %s", table)
    if n:
        view = f"gold_batch_{name}"
        df.createOrReplaceTempView(view)
        on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
        spark.sql(f"""
            MERGE INTO {table} t USING {view} s ON {on}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    df.unpersist()
    log.info("Merged %d rows into %s", n, table)
    return n


def main() -> None:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--run-id")
    group.add_argument("--all", action="store_true")
    args = parser.parse_args()
    run_id = None if args.all else args.run_id

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    spark = get_spark("weather-silver-to-gold")
    names = F.broadcast(spark.table(DIM_TABLE).select("province_id", "province_name"))

    weather = changed_rows(spark, WEATHER_SILVER, run_id).persist(StorageLevel.MEMORY_AND_DISK)
    merge_gold(spark, build_weather_daily(weather).join(names, "province_id"), "weather_daily",
               ["province_id", "date", "model_name"], F.years("date"))
    best_hourly = build_best_hourly(weather).join(names, "province_id").persist(StorageLevel.MEMORY_AND_DISK)
    merge_gold(spark, best_hourly, "weather_best_hourly", ["province_id", "timestamp"], F.months("timestamp"))
    merge_gold(spark, build_best_daily(best_hourly).join(names, "province_id"), "weather_best_daily",
               ["province_id", "date"], F.years("date"))
    best_hourly.unpersist()
    weather.unpersist()

    aq = changed_rows(spark, AQ_SILVER, run_id)
    merge_gold(spark, build_air_quality_daily(aq).join(names, "province_id"), "air_quality_daily",
               ["province_id", "date"], F.years("date"))
    spark.stop()


if __name__ == "__main__":
    main()
