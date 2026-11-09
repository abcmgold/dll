"""Phân tích thời tiết trên Gold: dị thường so với khí hậu, đợt thời tiết cực đoan, xu hướng theo năm.

  gold.weather_anomaly_daily  – tỉnh × ngày: chênh lệch so với khí hậu cùng thời điểm trong năm (z-score)
  gold.weather_events         – tỉnh × đợt: nắng nóng, rét, mưa lớn (các ngày liên tiếp gộp thành 1 đợt)
  gold.weather_yearly         – tỉnh × năm: nhiệt độ, lượng mưa, số ngày nóng/rét/mưa lớn
  gold.weather_trend          – tỉnh × chỉ số: độ dốc xu hướng theo thập kỷ, hồi quy tuyến tính trên các năm đủ dữ liệu

Tính lại toàn bộ mỗi lần (vài chục nghìn dòng) từ gold.weather_best_daily, chỉ dùng ngày đủ 24 giờ.
"""
from __future__ import annotations

import logging

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pipeline.spark.common import get_spark

log = logging.getLogger("weather_analytics")

SOURCE = "iceberg.gold.weather_best_daily"
CLIMATE_HALF_WINDOW_DAYS = 7     # khí hậu của 1 ngày = các ngày trong khoảng ±7 ngày, mọi năm đủ dữ liệu
ANOMALY_Z = 2.0                  # |z| từ mức này trở lên được coi là dị thường
MIN_DAYS_FULL_YEAR = 360

# Ngưỡng theo ngày: (loại đợt, cột, điều kiện). Các loại nặng hơn nằm trong loại nhẹ hơn, được thống kê riêng.
EVENT_RULES = [
    ("Nắng nóng (Tmax ≥ 35 °C)", "temp_max", lambda c: c >= 35),
    ("Nắng nóng gay gắt (Tmax ≥ 37 °C)", "temp_max", lambda c: c >= 37),
    ("Rét đậm (Ttb ≤ 15 °C)", "temp_avg", lambda c: c <= 15),
    ("Rét hại (Ttb ≤ 13 °C)", "temp_avg", lambda c: c <= 13),
    ("Mưa to (≥ 50 mm/ngày)", "precip_sum", lambda c: c >= 50),
    ("Mưa rất to (≥ 100 mm/ngày)", "precip_sum", lambda c: c >= 100),
]
TREND_METRICS = {
    "temp_avg": "Nhiệt độ TB năm (°C)",
    "temp_max_year": "Nhiệt độ cao nhất năm (°C)",
    "precip_total": "Tổng lượng mưa năm (mm)",
    "hot_days": "Số ngày Tmax ≥ 35 °C",
    "cold_days": "Số ngày Ttb ≤ 15 °C",
    "heavy_rain_days": "Số ngày mưa ≥ 50 mm",
}


def replace_table(df: DataFrame, name: str) -> int:
    table = f"iceberg.gold.{name}"
    df = df.withColumn("updated_at", F.current_timestamp()).cache()
    n = df.count()
    df.coalesce(2).writeTo(table).using("iceberg").tableProperty("format-version", "2") \
        .tableProperty("write.parquet.compression-codec", "zstd").createOrReplace()
    df.unpersist()
    log.info("Wrote %d rows to %s", n, table)
    return n


def complete_years(days: DataFrame) -> DataFrame:
    counts = days.groupBy("province_id", F.year("date").alias("year")).count()
    return counts.where(F.col("count") >= MIN_DAYS_FULL_YEAR).drop("count")


def build_anomaly(days: DataFrame) -> DataFrame:
    # Mỗi ngày đóng góp vào khí hậu của ±7 ngày lịch quanh nó (vòng qua cuối năm)
    baseline = days.withColumn("year", F.year("date")).join(complete_years(days), ["province_id", "year"], "left_semi")
    doy = F.dayofyear("date")
    offsets = F.sequence(F.lit(-CLIMATE_HALF_WINDOW_DAYS), F.lit(CLIMATE_HALF_WINDOW_DAYS))
    climate = (
        baseline.withColumn("offset", F.explode(offsets))
        .withColumn("doy", F.pmod(doy + F.col("offset") - 1, F.lit(366)) + 1)
        .groupBy("province_id", "doy")
        .agg(F.avg("temp_avg").alias("temp_clim"), F.stddev_samp("temp_avg").alias("temp_std"),
             F.avg("precip_sum").alias("precip_clim"))
    )
    anomaly = F.col("temp_avg") - F.col("temp_clim")
    z = anomaly / F.col("temp_std")
    return (
        days.withColumn("doy", doy).join(climate, ["province_id", "doy"], "left")
        .select(
            "province_id", "province_name", "date", "temp_avg", "precip_sum", "chosen_model",
            F.round("temp_clim", 2).alias("temp_clim"),
            F.round(anomaly, 2).alias("temp_anomaly"),
            F.round(z, 2).alias("temp_z"),
            F.round(F.col("precip_sum") - F.col("precip_clim"), 2).alias("precip_anomaly"),
            F.when(z >= ANOMALY_Z, "Nóng bất thường").when(z <= -ANOMALY_Z, "Lạnh bất thường").alias("anomaly_type"),
        )
    )


def event_days(days: DataFrame) -> DataFrame:
    flagged = [
        days.where(rule(F.col(col))).select(
            "province_id", "province_name", "date", F.lit(name).alias("event_type"), F.col(col).alias("value"))
        for name, col, rule in EVENT_RULES
    ]
    out = flagged[0]
    for df in flagged[1:]:
        out = out.unionByName(df)
    return out


def build_events(days: DataFrame) -> DataFrame:
    """Gộp các ngày liên tiếp cùng loại thành 1 đợt: date − số thứ tự là hằng số trong một chuỗi liên tiếp."""
    order = Window.partitionBy("province_id", "event_type").orderBy("date")
    grouped = event_days(days).withColumn("_group", F.date_sub(F.col("date"), F.row_number().over(order)))
    return (
        grouped.groupBy("province_id", "province_name", "event_type", "_group")
        .agg(F.min("date").alias("start_date"), F.max("date").alias("end_date"),
             F.count("*").alias("duration_days"),
             F.round(F.max("value"), 2).alias("max_value"), F.round(F.min("value"), 2).alias("min_value"))
        .drop("_group")
        .withColumn("year", F.year("start_date"))
    )


def days_where(cond: Column) -> Column:
    return F.sum(F.when(cond, 1).otherwise(0))


def build_yearly(days: DataFrame) -> DataFrame:
    full = complete_years(days)
    return (
        days.withColumn("year", F.year("date"))
        .groupBy("province_id", "province_name", "year")
        .agg(
            F.round(F.avg("temp_avg"), 2).alias("temp_avg"),
            F.round(F.max("temp_max"), 2).alias("temp_max_year"),
            F.round(F.min("temp_min"), 2).alias("temp_min_year"),
            F.round(F.sum("precip_sum"), 1).alias("precip_total"),
            days_where(F.col("temp_max") >= 35).alias("hot_days"),
            days_where(F.col("temp_avg") <= 15).alias("cold_days"),
            days_where(F.col("precip_sum") >= 50).alias("heavy_rain_days"),
            F.count("*").alias("n_days"),
        )
        .join(full.withColumn("is_full_year", F.lit(True)), ["province_id", "year"], "left")
        .fillna(False, ["is_full_year"])
        .withColumn("year_date", F.make_date("year", F.lit(1), F.lit(1)))
    )


def build_trend(yearly: DataFrame) -> DataFrame:
    full = yearly.where("is_full_year")
    parts = []
    for col, label in TREND_METRICS.items():
        parts.append(
            full.groupBy("province_id", "province_name").agg(
                F.lit(col).alias("metric"), F.lit(label).alias("metric_label"),
                F.count("*").alias("n_years"), F.min("year").alias("first_year"), F.max("year").alias("last_year"),
                F.round(F.covar_samp(col, "year") / F.var_samp("year") * 10, 3).alias("slope_per_decade"),
                F.round(F.pow(F.corr(col, "year"), 2), 3).alias("r2"),
            )
        )
    out = parts[0]
    for df in parts[1:]:
        out = out.unionByName(df)
    return out


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    spark: SparkSession = get_spark("weather-analytics")
    days = spark.table(SOURCE).where("n_hours = 24").cache()
    replace_table(build_anomaly(days), "weather_anomaly_daily")
    replace_table(build_events(days), "weather_events")
    yearly = build_yearly(days).cache()
    replace_table(yearly, "weather_yearly")
    replace_table(build_trend(yearly), "weather_trend")
    spark.stop()


if __name__ == "__main__":
    main()
