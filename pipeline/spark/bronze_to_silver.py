"""B3/B4 (phần 1) – Đọc Bronze của 3 API, map về CDM theo giờ (UTC), làm sạch, loại trùng rồi MERGE vào Silver.

  silver.weather_hourly      – khoá (province_id, timestamp, model_name); model era5_seamless | ecmwf_ifs025 | gfs_seamless
  silver.air_quality_hourly  – khoá (province_id, timestamp); model cams_global
  silver.dim_province        – 15 tỉnh (từ pipeline/config/locations.json)
  silver.dq_run_log          – mỗi lượt, mỗi model: số dòng đọc / loại / ghi / không hợp lệ và độ đầy đủ theo giờ

    spark-submit bronze_to_silver.py --run-id <airflow run_id>   # chỉ xử lý dữ liệu của lượt chạy đó
    spark-submit bronze_to_silver.py --all                       # xử lý lại toàn bộ Bronze
"""
from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType

from pipeline.config import provinces
from pipeline.spark.common import get_spark

log = logging.getLogger("bronze_to_silver")

WEATHER_TABLE = "iceberg.silver.weather_hourly"
AQ_TABLE = "iceberg.silver.air_quality_hourly"
DIM_TABLE = "iceberg.silver.dim_province"
DQ_TABLE = "iceberg.silver.dq_run_log"

WEATHER_METRICS = ["temperature_2m", "relative_humidity_2m", "precipitation"]
AQ_METRICS = ["pm2_5", "pm10", "us_aqi"]
MAX_TEMPERATURE_JUMP_C = 8.0  # theo thống kê: ERA5 và ECMWF không có lần nào vượt mức này trong 1 giờ

WEATHER_DDL = f"""
CREATE TABLE IF NOT EXISTS {WEATHER_TABLE} (
    province_id          STRING    COMMENT 'mã tỉnh mới (sau sáp nhập 01/07/2025)',
    `timestamp`          TIMESTAMP COMMENT 'đầu giờ, UTC',
    timestamp_local      TIMESTAMP_NTZ COMMENT 'đầu giờ, giờ Việt Nam (UTC+7)',
    date_partition       DATE      COMMENT 'ngày theo giờ Việt Nam',
    source_type          STRING    COMMENT 'reanalysis | historical_forecast',
    model_name           STRING    COMMENT 'era5_seamless | ecmwf_ifs025 | gfs_seamless',
    temperature_2m       DOUBLE    COMMENT '°C',
    relative_humidity_2m DOUBLE    COMMENT '%',
    precipitation        DOUBLE    COMMENT 'mm, tích luỹ 1 giờ',
    is_valid             BOOLEAN   COMMENT 'false nếu vi phạm một luật kiểm tra',
    invalid_reason       STRING    COMMENT 'các luật bị vi phạm, cách nhau bằng dấu phẩy',
    fetched_at           TIMESTAMP COMMENT 'thời điểm lấy từ API – bản mới hơn thắng khi trùng khoá',
    run_id               STRING    COMMENT 'lượt chạy Airflow đã ghi dòng này'
) USING iceberg
PARTITIONED BY (source_type, months(`timestamp`))
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')
"""

AQ_DDL = f"""
CREATE TABLE IF NOT EXISTS {AQ_TABLE} (
    province_id     STRING,
    `timestamp`     TIMESTAMP COMMENT 'đầu giờ, UTC',
    timestamp_local TIMESTAMP_NTZ,
    date_partition  DATE,
    source_type     STRING    COMMENT 'air_quality',
    model_name      STRING    COMMENT 'cams_global',
    pm2_5           DOUBLE    COMMENT 'µg/m³',
    pm10            DOUBLE    COMMENT 'µg/m³',
    us_aqi          INT       COMMENT 'US AQI (trung bình trượt 24 giờ)',
    is_valid        BOOLEAN,
    invalid_reason  STRING    COMMENT 'các luật bị vi phạm, cách nhau bằng dấu phẩy',
    fetched_at      TIMESTAMP,
    run_id          STRING
) USING iceberg
PARTITIONED BY (months(`timestamp`))
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')
"""

DQ_SCHEMA = ("run_id string, target_table string, model_name string, rows_parsed long, rows_no_data long, "
             "rows_future long, rows_duplicate long, rows_written long, rows_invalid long, invalid_detail string, "
             "window_start string, window_end string, rows_expected long, rows_actual long, completeness_pct double")
DQ_DDL = f"""
CREATE TABLE IF NOT EXISTS {DQ_TABLE} ({DQ_SCHEMA}, processed_at timestamp)
USING iceberg TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')
"""


# ----------------------------------------------------------------------------- mapping (sheet Mapping)
@dataclass(frozen=True)
class Mapping:
    target: str        # bảng Silver
    bronze: str        # bảng Bronze
    source_type: str
    model_name: str
    suffix: str        # hậu tố tên trường trong payload (Historical Forecast thêm tên model)
    metrics: list[str]


MAPPINGS = [
    Mapping(WEATHER_TABLE, "iceberg.bronze.era5_raw", "reanalysis", "era5_seamless", "", WEATHER_METRICS),
    Mapping(WEATHER_TABLE, "iceberg.bronze.historical_forecast_raw", "historical_forecast", "ecmwf_ifs025",
            "_ecmwf_ifs025", WEATHER_METRICS),
    Mapping(WEATHER_TABLE, "iceberg.bronze.historical_forecast_raw", "historical_forecast", "gfs_seamless",
            "_gfs_seamless", WEATHER_METRICS),
    Mapping(AQ_TABLE, "iceberg.bronze.air_quality_raw", "air_quality", "cams_global", "", AQ_METRICS),
]


# ----------------------------------------------------------------------------- luật kiểm tra (null = không xét)
def in_range(col: str, lo: float, hi: float) -> Column:
    return F.col(col).isNull() | F.col(col).between(lo, hi)


def pm25_not_above_pm10() -> Column:
    return F.col("pm2_5").isNull() | F.col("pm10").isNull() | (F.col("pm2_5") <= F.col("pm10"))


def no_temperature_jump() -> Column:
    change = F.abs(F.col("temperature_2m") - F.col("_prev_temperature"))
    return F.col("temperature_2m").isNull() | F.col("_prev_temperature").isNull() | (change <= MAX_TEMPERATURE_JUMP_C)


RULES = {
    WEATHER_TABLE: lambda: [
        ("temperature_out_of_range", in_range("temperature_2m", -20, 50)),
        ("humidity_out_of_range", in_range("relative_humidity_2m", 0, 100)),
        ("precipitation_out_of_range", in_range("precipitation", 0, 300)),
        ("temperature_jump", no_temperature_jump()),
    ],
    AQ_TABLE: lambda: [
        ("pm2_5_out_of_range", in_range("pm2_5", 0, 1000)),
        ("pm10_out_of_range", in_range("pm10", 0, 2000)),
        ("us_aqi_out_of_range", in_range("us_aqi", 0, 1000)),
        ("pm2_5_above_pm10", pm25_not_above_pm10()),
    ],
}


def invalid_reason(target: str) -> Column:
    joined = F.concat_ws(",", *[F.when(~passed, name) for name, passed in RULES[target]()])
    return F.when(joined != "", joined)


# ----------------------------------------------------------------------------- dim_province
def write_dim_province(spark: SparkSession) -> None:
    rows = [(p["province_id"], p["province_name"], p["merged_from"], p["center_name"],
             float(p["lat"]), float(p["lon"])) for p in provinces()]
    schema = ("province_id string, province_name string, merged_from string, center_name string, "
              "latitude double, longitude double")
    spark.createDataFrame(rows, schema).coalesce(1).writeTo(DIM_TABLE).using("iceberg").createOrReplace()


def ensure_tables(spark: SparkSession) -> None:
    for ddl in (WEATHER_DDL, AQ_DDL, DQ_DDL):
        spark.sql(ddl)
    for table in (WEATHER_TABLE, AQ_TABLE):  # bảng tạo từ phiên bản trước chưa có cột này
        if "invalid_reason" not in spark.table(table).columns:
            spark.sql(f"ALTER TABLE {table} ADD COLUMN invalid_reason STRING AFTER is_valid")
            log.info("Added invalid_reason to %s", table)


# ----------------------------------------------------------------------------- B3: parse
def parse_hourly(bronze: DataFrame, m: Mapping) -> DataFrame:
    """Payload Open-Meteo {"hourly": {"time": [...], "<biến><hậu tố>": [...]}} → 1 dòng / giờ, tên trường chung."""
    arr = ArrayType(DoubleType())
    schema = StructType([StructField("hourly", StructType(
        [StructField("time", ArrayType(StringType()))]
        + [StructField(var + m.suffix, arr) for var in m.metrics]
    ))])
    arrays = bronze.select("province_id", "fetched_at", "run_id", F.from_json("payload", schema).alias("j")).select(
        "province_id", "fetched_at", "run_id",
        F.col("j.hourly.time").alias("t"),
        *[F.col(f"j.hourly.{var}{m.suffix}").alias(var) for var in m.metrics],  # bỏ hậu tố model
    )
    # arrays_zip lấy tên field theo tên cột → struct r(t, temperature_2m, ...)
    rows = arrays.select("province_id", "fetched_at", "run_id", F.explode(F.arrays_zip("t", *m.metrics)).alias("r"))
    return rows.select(
        "province_id",
        F.to_timestamp("r.t", "yyyy-MM-dd'T'HH:mm").alias("timestamp"),
        F.lit(m.source_type).alias("source_type"),
        F.lit(m.model_name).alias("model_name"),
        *[F.col(f"r.{c}").alias(c) for c in m.metrics],
        "fetched_at", "run_id",
    )


# ----------------------------------------------------------------------------- B4: làm sạch + chuẩn hoá
def classify_drops(parsed: DataFrame, m: Mapping) -> DataFrame:
    """Cột _drop ghi lý do loại: no_data (null hết: ERA5 trễ, model chưa ra đời) hoặc future (giá trị dự báo)."""
    has_value = F.coalesce(*[F.col(c) for c in m.metrics]).isNotNull()
    return parsed.withColumn(
        "_drop",
        F.when(F.col("timestamp").isNull() | ~has_value, "no_data")
        .when(F.col("timestamp") > F.col("fetched_at"), "future"),
    )


def latest_rows(classified: DataFrame) -> DataFrame:
    latest_first = Window.partitionBy("province_id", "timestamp", "model_name").orderBy(F.col("fetched_at").desc())
    return (
        classified.where("_drop IS NULL").drop("_drop")
        .withColumn("_rn", F.row_number().over(latest_first))
        .where("_rn = 1").drop("_rn")
    )


def add_previous_temperature(spark: SparkSession, rows: DataFrame, m: Mapping) -> DataFrame:
    """Nhiệt độ của giờ liền trước. Giờ đầu của lô lấy giờ trước đó từ Silver để luật đột biến không bị hở ở biên lô."""
    lo = rows.agg(F.date_format(F.min("timestamp") - F.expr("INTERVAL 1 HOUR"), "yyyy-MM-dd HH:mm:ss")).first()[0]
    if lo is None:
        return rows.withColumn("_prev_temperature", F.lit(None).cast("double"))
    context = (
        spark.table(m.target)
        .where((F.col("model_name") == m.model_name) & (F.col("timestamp") == F.lit(lo).cast("timestamp")))
        .select("province_id", "timestamp", "temperature_2m", F.lit(True).alias("_context"))
    )
    by_time = Window.partitionBy("province_id").orderBy("timestamp")
    prev_is_adjacent = F.lag("timestamp").over(by_time) == F.col("timestamp") - F.expr("INTERVAL 1 HOUR")
    return (
        rows.withColumn("_context", F.lit(False)).unionByName(context, allowMissingColumns=True)
        .withColumn("_prev_temperature", F.when(prev_is_adjacent, F.lag("temperature_2m").over(by_time)))
        .where("NOT _context").drop("_context")
    )


def standardize(spark: SparkSession, rows: DataFrame, m: Mapping) -> DataFrame:
    if m.target == WEATHER_TABLE:
        rows = add_previous_temperature(spark, rows, m)
    local = F.from_utc_timestamp("timestamp", "Asia/Ho_Chi_Minh")
    metrics = [F.round(c, 2).alias(c) for c in m.metrics]
    if m.target == AQ_TABLE:
        metrics[-1] = F.round("us_aqi").cast("int").alias("us_aqi")
    return rows.withColumn("invalid_reason", invalid_reason(m.target)).select(
        "province_id", "timestamp",
        local.cast("timestamp_ntz").alias("timestamp_local"),
        F.to_date(local).alias("date_partition"),
        "source_type", "model_name",
        *metrics,
        F.col("invalid_reason").isNull().alias("is_valid"),
        "invalid_reason",
        "fetched_at", "run_id",
    )


def merge_into(spark: SparkSession, batch: DataFrame, m: Mapping) -> dict:
    """MERGE lô vào Silver, trả về thống kê của lô (n, lo, hi, invalid, detail)."""
    batch = batch.persist(StorageLevel.MEMORY_AND_DISK)
    stats = batch.agg(
        F.count("*").alias("n"),
        F.date_format(F.min("timestamp"), "yyyy-MM-dd HH:mm:ss").alias("lo"),
        F.date_format(F.max("timestamp"), "yyyy-MM-dd HH:mm:ss").alias("hi"),
        F.coalesce(F.sum(F.when(~F.col("is_valid"), 1).otherwise(0)), F.lit(0)).alias("invalid"),
    ).first().asDict()
    if not stats["n"]:
        log.warning("[%s] no rows to merge", m.model_name)
        batch.unpersist()
        return {**stats, "detail": None}
    by_rule = (batch.where("NOT is_valid").select(F.explode(F.split("invalid_reason", ",")).alias("rule"))
               .groupBy("rule").count().orderBy("rule").collect())
    detail = ", ".join(f"{r['rule']}={r['count']}" for r in by_rule) or None
    batch.createOrReplaceTempView("silver_batch")
    # Điều kiện model + khoảng thời gian giúp Iceberg chỉ đọc/ghi lại các partition liên quan.
    # Chỉ cập nhật khi bản mang vào mới hơn → chạy lại 1 lượt cũ không ghi đè dữ liệu mới.
    spark.sql(f"""
        MERGE INTO {m.target} t
        USING silver_batch s
        ON  t.model_name = '{m.model_name}' AND t.model_name = s.model_name
        AND t.province_id = s.province_id
        AND t.`timestamp` = s.`timestamp`
        AND t.`timestamp` BETWEEN TIMESTAMP '{stats['lo']}' AND TIMESTAMP '{stats['hi']}'
        WHEN MATCHED AND s.fetched_at >= t.fetched_at THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    batch.unpersist()
    log.info("[%s] merged %d rows into %s (%s → %s), invalid: %s",
             m.model_name, stats["n"], m.target, stats["lo"], stats["hi"], detail)
    return {**stats, "detail": detail}


# ----------------------------------------------------------------------------- giám sát chất lượng từng lượt
def check_completeness(spark: SparkSession, m: Mapping, lo: str, hi: str, n_provinces: int) -> tuple[int, int]:
    """Số dòng kỳ vọng (số giờ × số tỉnh) và số dòng thực có trong Silver cho khoảng thời gian của lô."""
    fmt = "%Y-%m-%d %H:%M:%S"
    hours = int((datetime.strptime(hi, fmt) - datetime.strptime(lo, fmt)).total_seconds() // 3600) + 1
    actual = (spark.table(m.target)
              .where((F.col("model_name") == m.model_name)
                     & F.col("timestamp").between(F.lit(lo).cast("timestamp"), F.lit(hi).cast("timestamp")))
              .count())
    expected = hours * n_provinces
    if actual != expected:
        log.warning("[%s] THIẾU DỮ LIỆU %s → %s: có %d dòng, kỳ vọng %d", m.model_name, lo, hi, actual, expected)
    return expected, actual


def process(spark: SparkSession, m: Mapping, bronze: DataFrame, run_label: str, n_provinces: int) -> tuple:
    classified = classify_drops(parse_hourly(bronze, m), m).persist(StorageLevel.MEMORY_AND_DISK)
    drops = {r["_drop"]: r["count"] for r in classified.groupBy("_drop").count().collect()}
    stats = merge_into(spark, standardize(spark, latest_rows(classified), m), m)
    classified.unpersist()
    expected, actual = (None, None)
    if stats["n"]:
        expected, actual = check_completeness(spark, m, stats["lo"], stats["hi"], n_provinces)
    parsed, no_data, future = sum(drops.values()), drops.get("no_data", 0), drops.get("future", 0)
    completeness = round(100.0 * actual / expected, 3) if expected else None
    return (run_label, m.target.split(".")[-1], m.model_name, parsed, no_data, future,
            parsed - no_data - future - stats["n"], stats["n"], stats["invalid"], stats["detail"],
            stats["lo"], stats["hi"], expected, actual, completeness)


def main() -> None:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--run-id")
    group.add_argument("--all", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    spark = get_spark("weather-bronze-to-silver")
    ensure_tables(spark)
    write_dim_province(spark)
    province_ids = [p["province_id"] for p in provinces()]
    run_label = args.run_id or f"reprocess_all__{datetime.now(timezone.utc):%Y-%m-%dT%H:%M:%S}"

    log_rows = []
    for m in MAPPINGS:
        bronze = spark.table(m.bronze)
        if not args.all:
            bronze = bronze.where(F.col("run_id") == args.run_id)
        # Bronze cũ có thêm các điểm phụ → chỉ giữ 15 toạ độ đại diện
        bronze = bronze.where((F.col("http_status") == 200) & F.col("province_id").isin(province_ids)) \
                       .select("province_id", "fetched_at", "run_id", "payload")
        log_rows.append(process(spark, m, bronze, run_label, len(province_ids)))
    spark.createDataFrame(log_rows, DQ_SCHEMA).withColumn("processed_at", F.current_timestamp()) \
        .coalesce(1).writeTo(DQ_TABLE).append()
    for row in log_rows:
        log.info("DQ %s", row)
    log.info("Silver merge done: %d rows upserted", sum(r[7] for r in log_rows))
    spark.stop()


if __name__ == "__main__":
    main()
