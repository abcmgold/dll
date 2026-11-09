"""Chạy DAG weather_backfill từ dòng lệnh, gọi qua backfill.sh hoặc backfill.cmd.

Đợi lượt weather_pipeline đang chạy xong, tạm dừng weather_pipeline, kích hoạt backfill, theo dõi đến khi kết thúc
rồi trả weather_pipeline về trạng thái cũ. Thông báo không dấu để hiển thị đúng trên mọi terminal, kể cả cmd của Windows.
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date, datetime, timedelta, timezone

from pipeline.orchestration import INGEST_SOURCES

BACKFILL_DAG = "weather_backfill"
PIPELINE_DAG = "weather_pipeline"
POLL_SECONDS = 30
PIPELINE_WAIT = timedelta(minutes=45)


def iso_date(value: str) -> str:
    try:
        date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"'{value}' khong phai ngay YYYY-MM-DD") from None
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="backfill",
        description="Lay du lieu tu start_date den end_date (bo trong end_date = hom nay).",
    )
    parser.add_argument("start_date", type=iso_date, help="YYYY-MM-DD, vi du 2015-01-01")
    parser.add_argument("end_date", type=iso_date, nargs="?", help="YYYY-MM-DD, co the bo trong")
    parser.add_argument("--sources", nargs="+", choices=INGEST_SOURCES, default=list(INGEST_SOURCES),
                        help="Mac dinh la ca 3 nguon")
    args = parser.parse_args()
    if args.end_date and args.end_date < args.start_date:
        parser.error("end_date phai sau hoac bang start_date")
    return args


def wait_for_pipeline(was_paused: bool) -> None:
    """Máy mới phải có ít nhất một lượt pipeline thành công, vì bước dashboard cần các bảng phân tích."""
    from airflow.models import DagRun
    from airflow.utils.state import DagRunState

    deadline = datetime.now(timezone.utc) + PIPELINE_WAIT
    while True:
        runs = DagRun.find(dag_id=PIPELINE_DAG)
        active = any(r.state in (DagRunState.RUNNING, DagRunState.QUEUED) for r in runs)
        succeeded = any(r.state == DagRunState.SUCCESS for r in runs)
        if succeeded and (was_paused or not active):
            return
        if datetime.now(timezone.utc) > deadline:
            sys.exit(f"Da doi {PIPELINE_WAIT} ma {PIPELINE_DAG} chua co luot hoan tat. Kiem tra Airflow roi chay lai.")
        print(f"Dang doi {PIPELINE_DAG} chay xong...", flush=True)
        time.sleep(POLL_SECONDS)


def follow(run_id: str) -> str:
    from airflow.models import DagRun
    from airflow.utils.state import DagRunState, State

    last = ""
    while True:
        run = DagRun.find(dag_id=BACKFILL_DAG, run_id=run_id)[0]
        if run.state in (DagRunState.SUCCESS, DagRunState.FAILED):
            return run.state
        tasks = run.get_task_instances()
        done = sum(1 for t in tasks if t.state in State.finished)
        line = f"{BACKFILL_DAG}: {done}/{len(tasks)} task xong"
        if line != last:
            print(f"{datetime.now(timezone.utc):%H:%M:%S} UTC  {line}", flush=True)
            last = line
        time.sleep(POLL_SECONDS)


def main() -> int:
    args = parse_args()

    from airflow.api.common.trigger_dag import trigger_dag
    from airflow.models import DagModel, DagRun
    from airflow.utils.state import DagRunState

    if DagRun.find(dag_id=BACKFILL_DAG, state=DagRunState.RUNNING):
        sys.exit(f"{BACKFILL_DAG} dang co mot luot chay. Doi luot do xong roi chay lai.")

    pipeline = DagModel.get_dagmodel(PIPELINE_DAG)
    if pipeline is None:
        sys.exit("Airflow chua nap xong DAG. Doi khoang 1 phut roi chay lai.")
    was_paused = pipeline.is_paused
    wait_for_pipeline(was_paused)

    run_id = f"backfill_cli__{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
    conf = {"start_date": args.start_date, "end_date": args.end_date or "", "sources": args.sources}
    pipeline.set_is_paused(is_paused=True)
    try:
        trigger_dag(dag_id=BACKFILL_DAG, run_id=run_id, conf=conf)
        print(f"Da kich hoat {run_id}: {conf}", flush=True)
        state = follow(run_id)
    finally:
        pipeline.set_is_paused(is_paused=was_paused)

    if state != DagRunState.SUCCESS:
        print(f"{run_id} THAT BAI. Xem task loi tai http://localhost:8082. "
              "Neu het han muc API, Clear task loi sau 07:00 gio Viet Nam.", flush=True)
        return 1
    print(f"{run_id} THANH CONG. Du lieu da vao Silver, Gold va dashboard. "
          f"Phan phan tich se tinh lai o luot {PIPELINE_DAG} ke tiep.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
