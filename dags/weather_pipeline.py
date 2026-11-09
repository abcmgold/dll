"""A0 – Luồng định kỳ 6 giờ/lần: B1/B2 (3 API Open-Meteo → Bronze) → B3/B4 (Spark: Silver, Gold) → Superset.

Lịch canh theo nhịp cập nhật của nguồn: ECMWF IFS / GFS chạy 6 giờ/lần (công bố sau ~5–7 giờ),
CAMS 12 giờ/lần, ERA5 1 lần/ngày (~00:15 UTC, trễ 5–6 ngày). Mỗi lượt lấy lại N ngày gần nhất
(LOOKBACK_DAYS) để lấp chỗ thiếu và ghi đè giờ còn là dự báo; MERGE ở Silver nên không sinh trùng.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from pipeline.orchestration import (DEFAULT_ARGS, INGEST_SOURCES, LOOKBACK_DAYS, OPEN_METEO_POOL, SPARK_POOL,
                                    spark_submit)


def _init_lakehouse():
    from pipeline.ingest.common import ensure_lakehouse

    ensure_lakehouse()


def _ingest(source: str, data_interval_end, run_id, **_):
    from pipeline.ingest import run_ingest
    from pipeline.ingest.common import QuotaExhausted

    end = data_interval_end.date()  # tới hôm nay (UTC); giờ chưa có/tương lai bị lọc ở Silver
    try:
        return run_ingest(source, end - timedelta(days=LOOKBACK_DAYS[source]), end, run_id)
    except QuotaExhausted as exc:  # thử lại cũng vô ích cho tới 00:00 UTC
        raise AirflowFailException(str(exc)) from exc


def _publish_dashboard():
    from pipeline.serving.superset_dashboard import publish

    return publish()


with DAG(
    dag_id="weather_pipeline",
    description="3 API Open-Meteo (ERA5, Historical Forecast, CAMS) → Iceberg Bronze/Silver/Gold → Superset, 6 giờ/lần",
    schedule="30 1,7,13,19 * * *",  # UTC = 08:30 · 14:30 · 20:30 · 02:30 giờ Việt Nam
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=3),
    tags=["weather", "lakehouse"],
) as dag:
    init_lakehouse = PythonOperator(task_id="init_lakehouse", python_callable=_init_lakehouse)

    ingest_tasks = [
        PythonOperator(
            task_id=f"ingest_{source}",
            python_callable=_ingest,
            op_kwargs={"source": source},
            pool=OPEN_METEO_POOL,
            execution_timeout=timedelta(minutes=30),
        )
        for source in INGEST_SOURCES
    ]

    bronze_to_silver = BashOperator(
        task_id="spark_bronze_to_silver",
        bash_command=spark_submit("bronze_to_silver", "--run-id", "{{ run_id }}"),
        trigger_rule="all_done",  # 1 nguồn lỗi không chặn các nguồn còn lại
        pool=SPARK_POOL,
    )
    silver_to_gold = BashOperator(
        task_id="spark_silver_to_gold",
        bash_command=spark_submit("silver_to_gold", "--run-id", "{{ run_id }}"),
        pool=SPARK_POOL,
    )
    weather_analytics = BashOperator(
        task_id="spark_weather_analytics",
        bash_command=spark_submit("weather_analytics"),
        pool=SPARK_POOL,
    )
    publish_dashboard = PythonOperator(task_id="publish_superset_dashboard", python_callable=_publish_dashboard)

    init_lakehouse >> ingest_tasks >> bronze_to_silver >> silver_to_gold >> weather_analytics >> publish_dashboard
