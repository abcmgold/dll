"""Phần dùng chung cho bước B1/B2: gọi API Open-Meteo có retry + điều tốc theo hạn mức,
rồi ghi phản hồi thô vào Iceberg (Bronze) bằng PyIceberg."""
from __future__ import annotations

import json
import logging
import math
import os
import time
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import pyarrow as pa
import requests
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError
from pyiceberg.expressions import GreaterThanOrEqual
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import DateType, IntegerType, NestedField, StringType, TimestamptzType

from pipeline.config import provinces

log = logging.getLogger(__name__)

NAMESPACES = ("bronze", "silver", "gold")

BRONZE_TABLES = {
    "era5": "bronze.era5_raw",
    "historical_forecast": "bronze.historical_forecast_raw",
    "air_quality": "bronze.air_quality_raw",
}
# Số biến gọi mỗi request (dùng để tính "lượt" theo cách Open-Meteo tính hạn mức)
SOURCE_VARIABLES = {"era5": 3, "historical_forecast": 6, "air_quality": 3}

# Bronze giữ nguyên payload gốc của API + metadata để truy vết / xử lý lại
BRONZE_SCHEMA = Schema(
    NestedField(1, "source", StringType(), required=False),
    NestedField(2, "province_id", StringType(), required=False),
    NestedField(3, "window_start", DateType(), required=False),
    NestedField(4, "window_end", DateType(), required=False),
    NestedField(5, "request_url", StringType(), required=False),
    NestedField(6, "http_status", IntegerType(), required=False),
    NestedField(7, "payload", StringType(), required=False),
    NestedField(8, "fetched_at", TimestamptzType(), required=False),
    NestedField(9, "run_id", StringType(), required=False),
)
BRONZE_PARTITION = PartitionSpec(
    PartitionField(source_id=8, field_id=1000, transform=DayTransform(), name="fetched_day")
)
BRONZE_ARROW_SCHEMA = pa.schema(
    [
        pa.field("source", pa.string()),
        pa.field("province_id", pa.string()),
        pa.field("window_start", pa.date32()),
        pa.field("window_end", pa.date32()),
        pa.field("request_url", pa.string()),
        pa.field("http_status", pa.int32()),
        pa.field("payload", pa.large_string()),
        pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
        pa.field("run_id", pa.string()),
    ]
)


def get_catalog() -> Catalog:
    return load_catalog(
        "lakehouse",
        **{
            "type": "rest",
            "uri": os.environ["ICEBERG_REST_URI"],
            "warehouse": os.environ.get("ICEBERG_WAREHOUSE", "s3://warehouse/"),
            "s3.endpoint": os.environ["S3_ENDPOINT"],
            "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
            "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
            "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
            "s3.path-style-access": "true",
        },
    )


def ensure_lakehouse() -> None:
    """Tạo namespace bronze/silver/gold và các bảng Bronze nếu chưa có (idempotent)."""
    catalog = get_catalog()
    for ns in NAMESPACES:
        try:
            catalog.create_namespace(ns)
            log.info("Created namespace %s", ns)
        except NamespaceAlreadyExistsError:
            pass
    for identifier in BRONZE_TABLES.values():
        try:
            catalog.create_table(identifier, schema=BRONZE_SCHEMA, partition_spec=BRONZE_PARTITION)
            log.info("Created table %s", identifier)
        except TableAlreadyExistsError:
            table = catalog.load_table(identifier)
            # Bảng tạo từ phiên bản trước dùng cột location_id → đổi tên tại chỗ (giữ nguyên dữ liệu cũ)
            if "location_id" in table.schema().column_names:
                with table.update_schema() as update:
                    update.rename_column("location_id", "province_id")
                log.info("Renamed %s.location_id → province_id", identifier)


@dataclass
class BronzeRecord:
    source: str
    province_id: str
    window_start: date
    window_end: date
    request_url: str
    http_status: int
    payload: str
    fetched_at: datetime
    run_id: str


def write_bronze(source: str, records: list[BronzeRecord]) -> int:
    if not records:
        return 0
    columns = {f.name: [getattr(r, f.name) for r in records] for f in BRONZE_ARROW_SCHEMA}
    arrow_table = pa.Table.from_pydict(columns, schema=BRONZE_ARROW_SCHEMA)
    get_catalog().load_table(BRONZE_TABLES[source]).append(arrow_table)
    log.info("Appended %d bronze rows to %s", len(records), BRONZE_TABLES[source])
    return len(records)


class BronzeBuffer:
    """Gom bản ghi rồi ghi theo lô để tránh tạo quá nhiều snapshot/file nhỏ trong Iceberg."""

    def __init__(self, source: str, flush_bytes: int = 64 * 1024 * 1024):
        self.source = source
        self.flush_bytes = flush_bytes
        self._records: list[BronzeRecord] = []
        self._size = 0
        self.total = 0

    def add(self, record: BronzeRecord) -> None:
        self._records.append(record)
        self._size += len(record.payload)
        if self._size >= self.flush_bytes:
            self.flush()

    def flush(self) -> None:
        self.total += write_bronze(self.source, self._records)
        self._records, self._size = [], 0


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def http_get(url: str, params: dict, *, max_attempts: int = 8, timeout: int = 180) -> requests.Response:
    """GET có retry: backoff tăng dần với lỗi mạng/5xx/JSON bị cắt cụt; tôn trọng 429 (rate limit)."""
    delay = 5
    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            if resp.status_code == 200:
                resp.json()  # phản hồi lớn đôi khi bị cắt giữa chừng → coi như lỗi tạm thời
        except (requests.RequestException, ValueError) as exc:
            log.warning("Attempt %d/%d failed: %s", attempt, max_attempts, exc)
        else:
            if resp.status_code == 200:
                return resp
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 0)) or min(60 * attempt, 900)
                log.warning("429 rate limited (%s); sleeping %ds", resp.text[:200], wait)
                time.sleep(wait)
                continue
            if resp.status_code < 500:
                raise RuntimeError(f"HTTP {resp.status_code} for {resp.url}: {resp.text[:500]}")
            log.warning("HTTP %d on attempt %d/%d", resp.status_code, attempt, max_attempts)
        time.sleep(delay)
        delay = min(delay * 2, 300)
    raise RuntimeError(f"GET {url} failed after {max_attempts} attempts")


def date_chunks(start: date, end: date, max_days: int):
    """Chia [start, end] (bao gồm 2 đầu) thành các đoạn liên tiếp dài tối đa max_days ngày."""
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=max_days - 1), end)
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


def year_windows(start: date, end: date) -> list[tuple[date, date]]:
    """Chia [start, end] theo năm dương lịch, năm gần nhất trước."""
    windows, cur = [], start
    while cur <= end:
        chunk_end = min(date(cur.year, 12, 31), end)
        windows.append((cur, chunk_end))
        cur = chunk_end + timedelta(days=1)
    return windows[::-1]


# ----------------------------------------------------------------------------- hạn mức Open-Meteo
# Hạn mức miễn phí tính chung cho cả 3 API, theo IP: 600 lượt/phút, 5.000/giờ, 10.000/ngày.
# 1 lượt = 1 toạ độ × tối đa 14 ngày × tối đa 10 biến (vượt thì nhân lên).
OPENMETEO_MAX_WEIGHT_PER_HOUR = int(os.environ.get("OPENMETEO_MAX_WEIGHT_PER_HOUR", "4000"))
OPENMETEO_MAX_WEIGHT_PER_DAY = int(os.environ.get("OPENMETEO_MAX_WEIGHT_PER_DAY", "9900"))
# Backfill dừng sớm hơn chừng này lượt để luồng định kỳ (~200 lượt/ngày) vẫn còn hạn mức
BACKFILL_DAILY_RESERVE = int(os.environ.get("OPENMETEO_BACKFILL_DAILY_RESERVE", "500"))


class QuotaExhausted(RuntimeError):
    """Đã dùng gần hết hạn mức ngày của Open-Meteo – chạy tiếp sau 00:00 UTC."""


def open_meteo_weight(n_locations: int, n_days: int, n_variables: int) -> float:
    return n_locations * max(1.0, n_days / 14) * max(1.0, n_variables / 10)


def quota_used_today() -> float:
    """Số lượt đã dùng từ 00:00 UTC hôm nay, tính từ các dòng Bronze đã ghi (mỗi dòng = 1 toạ độ × 1 request)."""
    since = datetime.combine(utcnow().date(), datetime.min.time(), tzinfo=timezone.utc)
    catalog, used = get_catalog(), 0.0
    for source, identifier in BRONZE_TABLES.items():
        rows = catalog.load_table(identifier).scan(
            row_filter=GreaterThanOrEqual("fetched_at", since.isoformat()),
            selected_fields=("window_start", "window_end"),
        ).to_arrow().to_pylist()
        for r in rows:
            used += open_meteo_weight(1, (r["window_end"] - r["window_start"]).days + 1, SOURCE_VARIABLES[source])
    return used


# ----------------------------------------------------------------------------- kiểm tra cấu trúc payload
VARIABLE_UNITS = {
    "temperature_2m": "°C",
    "relative_humidity_2m": "%",
    "precipitation": "mm",
    "pm2_5": "μg/m³",
    "pm10": "μg/m³",
    "us_aqi": "USAQI",
}


class PayloadInvalid(RuntimeError):
    """Phản hồi API sai cấu trúc đã thống nhất → không ghi vào Bronze."""


def expected_fields(variables: list[str], models: tuple[str, ...] = ("",)) -> dict[str, str]:
    """{tên trường trong payload: đơn vị}. Historical Forecast thêm hậu tố tên model vào tên trường."""
    return {f"{var}_{model}" if model else var: VARIABLE_UNITS[var] for model in models for var in variables}


def _same_unit(a: object, b: str) -> bool:
    # API có thể dùng µ (U+00B5) hoặc μ (U+03BC); NFKC đưa về một dạng
    return isinstance(a, str) and unicodedata.normalize("NFKC", a) == unicodedata.normalize("NFKC", b)


def validate_payload(item: object, fields: dict[str, str], n_hours: int, label: str) -> None:
    """Hợp đồng dữ liệu: đủ `hourly.time`, đủ biến, các mảng dài đúng số giờ đã gọi, đơn vị đúng. Sai thì dừng."""
    hourly = item.get("hourly") if isinstance(item, dict) else None
    units = item.get("hourly_units") if isinstance(item, dict) else None
    if not isinstance(hourly, dict) or not isinstance(units, dict):
        raise PayloadInvalid(f"{label}: thiếu khối hourly hoặc hourly_units")
    for field in ("time", *fields):
        values = hourly.get(field)
        if not isinstance(values, list):
            raise PayloadInvalid(f"{label}: thiếu mảng hourly.{field}")
        if len(values) != n_hours:
            raise PayloadInvalid(f"{label}: hourly.{field} có {len(values)} phần tử, cần {n_hours}")
    for field, unit in fields.items():
        if not _same_unit(units.get(field), unit):
            raise PayloadInvalid(f"{label}: đơn vị của {field} là {units.get(field)!r}, cần {unit!r}")


# ----------------------------------------------------------------------------- ingest
def ingest_open_meteo(source: str, url: str, params: dict, fields: dict[str, str], start: date, end: date,
                      run_id: str, backfill: bool = False, max_days: int = 31) -> int:
    """Gửi cả 15 toạ độ trong 1 request cho mỗi đoạn ≤ max_days ngày, kiểm tra cấu trúc rồi ghi mỗi toạ độ
    thành 1 dòng Bronze. Một toạ độ sai cấu trúc thì cả đoạn không được ghi."""
    daily_limit = OPENMETEO_MAX_WEIGHT_PER_DAY - (BACKFILL_DAILY_RESERVE if backfill else 0)
    points = provinces()
    n_variables = SOURCE_VARIABLES[source]
    buffer = BronzeBuffer(source)
    used = quota_used_today()
    log.info("%s: %s → %s, Open-Meteo quota used today ≈ %.0f/%d", source, start, end, used, daily_limit)
    try:
        for chunk_start, chunk_end in date_chunks(start, end, max_days=max_days):
            weight = open_meteo_weight(len(points), (chunk_end - chunk_start).days + 1, n_variables)
            if used + weight > daily_limit:
                raise QuotaExhausted(
                    f"Hết hạn mức ngày Open-Meteo (đã dùng ≈ {used:.0f}/{daily_limit}) "
                    f"khi lấy {source} {chunk_start} → {chunk_end}. Chạy lại task sau 00:00 UTC (07:00 giờ VN).")
            t0 = time.monotonic()
            query = {
                **params,
                "latitude": ",".join(str(p["lat"]) for p in points),
                "longitude": ",".join(str(p["lon"]) for p in points),
                "start_date": chunk_start.isoformat(),
                "end_date": chunk_end.isoformat(),
                "timezone": "GMT",
            }
            resp = http_get(url, query)
            body = resp.json()
            items = body if isinstance(body, list) else [body]
            if len(items) != len(points):
                raise RuntimeError(f"Expected {len(points)} locations, got {len(items)}")
            n_hours = ((chunk_end - chunk_start).days + 1) * 24
            for point, item in zip(points, items):
                validate_payload(item, fields, n_hours, f"{source} {point['province_id']} {chunk_start}")
            fetched_at = utcnow()
            for point, item in zip(points, items):  # kết quả trả về đúng thứ tự toạ độ đã gửi
                buffer.add(BronzeRecord(
                    source=source, province_id=point["province_id"],
                    window_start=chunk_start, window_end=chunk_end,
                    request_url=resp.url, http_status=resp.status_code,
                    payload=json.dumps(item, separators=(",", ":")),
                    fetched_at=fetched_at, run_id=run_id,
                ))
            used += weight
            log.info("%s %s → %s: %d provinces", source, chunk_start, chunk_end, len(items))

            # Điều tốc để không vượt hạn mức theo giờ
            sleep_for = weight * 3600 / OPENMETEO_MAX_WEIGHT_PER_HOUR - (time.monotonic() - t0)
            if chunk_end < end and sleep_for > 0:
                log.info("Throttling %.0fs to respect Open-Meteo hourly quota", math.ceil(sleep_for))
                time.sleep(sleep_for)
    finally:
        buffer.flush()  # giữ lại phần đã lấy được kể cả khi dừng giữa chừng
    return buffer.total
