"""Nạp dữ liệu lịch sử (chạy tay). Khoảng ngày được chia theo năm; mỗi (nguồn × năm) là 1 mapped task
→ lỗi năm nào chỉ chạy lại năm đó.

Trigger với cấu hình, ví dụ: {"start_date": "2020-01-01", "end_date": "", "sources": ["era5"]}
(end_date rỗng = hôm nay). Mỗi nguồn tự bỏ qua giai đoạn chưa có dữ liệu
(GFS từ 2021-03-23, CAMS từ 2022-08-04, ECMWF IFS từ 2024-02-03).
Nên tạm dừng DAG weather_pipeline khi đang backfill (chung pool API).
Hết hạn mức ngày của Open-Meteo → task dừng ngay (không retry); chạy lại (Clear) các task lỗi sau 00:00 UTC.
"""
from __future__ import annotations

import os
from datetime import date

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

from pipeline.orchestration import DEFAULT_ARGS, INGEST_SOURCES, OPEN_METEO_POOL, SPARK_POOL, spark_submit


def make_ingest_task(source: str):
    @task(task_id=f"ingest_{source}", pool=OPEN_METEO_POOL, max_active_tis_per_dag=1, retries=3)
    def ingest_window(window: list[str], run_id=None, params=None):
        from airflow.exceptions import AirflowFailException, AirflowSkipException

        from pipeline.ingest import run_ingest
        from pipeline.ingest.common import QuotaExhausted

        if source not in params["sources"]:
            raise AirflowSkipException(f"{source} không nằm trong params.sources")
        try:
            return run_ingest(source, date.fromisoformat(window[0]), date.fromisoformat(window[1]), run_id,
                              backfill=True)
        except QuotaExhausted as exc:
            raise AirflowFailException(str(exc)) from exc

    return ingest_window


with DAG(
    dag_id="weather_backfill",
    description="Backfill lịch sử theo năm cho 3 API Open-Meteo",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "start_date": Param(os.environ.get("BACKFILL_START_DATE", "2020-01-01"), type="string", format="date"),
        "end_date": Param("", type="string", description="YYYY-MM-DD; để trống = hôm nay (UTC)"),
        "sources": Param(list(INGEST_SOURCES), type="array",
                         items={"type": "string", "enum": list(INGEST_SOURCES)}),
    },
    tags=["weather", "lakehouse", "backfill"],
) as dag:

    @task
    def init_lakehouse():
        from pipeline.ingest.common import ensure_lakehouse

        ensure_lakehouse()

    @task
    def plan_years(params=None) -> list[list[str]]:
        from pipeline.ingest.common import year_windows

        today = pendulum.now("UTC").date()
        start = date.fromisoformat(params["start_date"])
        end = min(date.fromisoformat(params["end_date"]) if params["end_date"] else today, today)
        if start > end:
            raise ValueError("start_date phải <= end_date")
        return [[s.isoformat(), e.isoformat()] for s, e in year_windows(start, end)]  # năm gần nhất trước

    windows = plan_years()
    init_lakehouse() >> windows

    ingests = [make_ingest_task(source).expand(window=windows) for source in INGEST_SOURCES]

    bronze_to_silver = BashOperator(
        task_id="spark_bronze_to_silver",
        bash_command=spark_submit("bronze_to_silver", "--run-id", "{{ run_id }}"),
        trigger_rule="all_done",
        execution_timeout=pendulum.duration(hours=3),
        pool=SPARK_POOL,
    )
    silver_to_gold = BashOperator(
        task_id="spark_silver_to_gold",
        bash_command=spark_submit("silver_to_gold", "--run-id", "{{ run_id }}"),
        execution_timeout=pendulum.duration(hours=2),
        pool=SPARK_POOL,
    )

    @task
    def publish_superset_dashboard():
        from pipeline.serving.superset_dashboard import publish

        return publish()

    ingests >> bronze_to_silver >> silver_to_gold >> publish_superset_dashboard()
