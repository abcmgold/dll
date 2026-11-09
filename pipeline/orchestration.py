"""Hàm dùng chung cho các DAG Airflow."""
from __future__ import annotations

import shlex
from datetime import timedelta

SPARK_JOBS_DIR = "/opt/airflow/pipeline/spark"

DEFAULT_ARGS = {
    "owner": "weather-lakehouse",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

# Cả 3 nguồn đều là API Open-Meteo, dùng chung hạn mức theo IP → chung 1 pool 1 slot (chạy tuần tự)
OPEN_METEO_POOL = "open_meteo_api"
INGEST_SOURCES = ("era5", "historical_forecast", "air_quality")
# Mỗi lượt weather_pipeline lấy lại N ngày gần nhất của từng nguồn:
#  ERA5 trễ 5–6 ngày → 10 ngày; Forecast/CAMS: ghi đè các giờ còn là dự báo bằng lượt mô hình mới hơn
LOOKBACK_DAYS = {"era5": 10, "historical_forecast": 2, "air_quality": 2}
# Spark cluster chỉ có 1 worker 2 core → mỗi lúc chỉ 1 job Spark (tránh 2 DAG MERGE cùng bảng)
SPARK_POOL = "spark_cluster"


def spark_submit(job: str, *args: str) -> str:
    """Lệnh spark-submit (client mode): driver chạy trong container Airflow, executor chạy trên Spark worker."""
    confs = {
        "spark.driver.host": "$SPARK_DRIVER_HOST",
        "spark.driver.bindAddress": "0.0.0.0",
        "spark.driver.extraClassPath": "/opt/spark-jars/*",
        "spark.driver.memory": "1g",
        "spark.executor.memory": "1536m",
        "spark.executor.cores": "2",
        "spark.cores.max": "2",
        "spark.pyspark.python": "python3",
        "spark.ui.showConsoleProgress": "false",
    }
    conf_args = " ".join(f'--conf "{k}={v}"' for k, v in confs.items())
    job_args = " ".join(shlex.quote(a) for a in args)
    return (
        f"spark-submit --master $SPARK_MASTER_URL --deploy-mode client --name weather-{job} "
        f"{conf_args} {SPARK_JOBS_DIR}/{job}.py {job_args}"
    )
