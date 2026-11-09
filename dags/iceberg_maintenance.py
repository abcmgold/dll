"""Bảo trì Iceberg hằng tuần: compaction + expire snapshots cho Bronze, Silver, Gold."""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

from pipeline.orchestration import DEFAULT_ARGS, SPARK_POOL, spark_submit

with DAG(
    dag_id="iceberg_maintenance",
    schedule="0 20 * * 0",  # 03:00 sáng thứ Hai giờ Việt Nam
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["weather", "lakehouse", "maintenance"],
) as dag:
    BashOperator(task_id="spark_compact_and_expire", bash_command=spark_submit("maintenance"), pool=SPARK_POOL)
