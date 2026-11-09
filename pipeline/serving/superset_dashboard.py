"""Q1–Q3 – Tự động cấu hình Superset qua REST API: kết nối Trino, tạo dataset từ các bảng Gold/Silver,
tạo chart và dashboard 2 phần (thời tiết · chất lượng không khí). Idempotent: dashboard đúng phiên bản
DASHBOARD_VERSION (ghi trong CSS) thì giữ nguyên (trừ khi force=True); dataset/kết nối đã có thì dùng lại và làm mới metadata cột.
"""
from __future__ import annotations

import json
import logging
import os
import uuid

import requests

log = logging.getLogger(__name__)

DATABASE_NAME = "Trino Lakehouse"
TRINO_URI = "trino://superset@trino:8080/iceberg"
DASHBOARD_SLUG = "weather-lakehouse"
DASHBOARD_TITLE = "Thời tiết & chất lượng không khí – 15 tỉnh miền Bắc"
# Tăng số này khi đổi bộ chart → lần publish sau tự dựng lại dashboard
DASHBOARD_VERSION = 7
# Silver chất lượng không khí chỉ có province_id → dataset ảo ghép thêm province_name cho bộ lọc tỉnh
AQ_HOURLY_SQL = """SELECT a.*, d.province_name
FROM iceberg.silver.air_quality_hourly a
JOIN iceberg.silver.dim_province d ON a.province_id = d.province_id"""
# (schema, bảng, SQL nếu là dataset ảo) dùng làm dataset
DATASETS = [("gold", "weather_best_hourly", None), ("gold", "weather_best_daily", None),
            ("gold", "weather_daily", None), ("gold", "air_quality_daily", None),
            ("silver", "air_quality_hourly_named", AQ_HOURLY_SQL),
            ("gold", "weather_anomaly_daily", None), ("gold", "weather_events", None),
            ("gold", "weather_yearly", None), ("gold", "weather_trend", None),
            ("silver", "dq_run_log", None)]
PROVINCE_FILTER_DEFAULT = "Hà Nội"
TEMPORAL_COLUMNS = {"timestamp", "timestamp_local", "date", "date_partition", "updated_at",
                    "start_date", "end_date", "year_date", "processed_at"}


class SupersetClient:
    def __init__(self, base_url: str, username: str, password: str):
        self.base = base_url.rstrip("/")
        self.s = requests.Session()
        token = self.s.post(f"{self.base}/api/v1/security/login", json={
            "username": username, "password": password, "provider": "db", "refresh": True,
        }, timeout=30)
        token.raise_for_status()
        self.s.headers["Authorization"] = f"Bearer {token.json()['access_token']}"
        csrf = self.s.get(f"{self.base}/api/v1/security/csrf_token/", timeout=30)
        csrf.raise_for_status()
        self.s.headers.update({"X-CSRFToken": csrf.json()["result"], "Referer": self.base})

    def _req(self, method: str, path: str, **kw) -> dict:
        r = self.s.request(method, f"{self.base}/api/v1/{path}", timeout=120, **kw)
        if r.status_code >= 400:
            raise RuntimeError(f"{method} {path} → {r.status_code}: {r.text[:1000]}")
        return r.json() if r.content else {}

    def get(self, path, **kw):
        return self._req("GET", path, **kw)

    def post(self, path, payload):
        return self._req("POST", path, json=payload)

    def put(self, path, payload):
        return self._req("PUT", path, json=payload)

    def delete(self, path):
        return self._req("DELETE", path)

    def list_all(self, resource: str) -> list[dict]:
        return self.get(f"{resource}/", params={"q": "(page_size:100)"}).get("result", [])


# ----------------------------------------------------------------------------- building blocks
def ensure_database(c: SupersetClient) -> int:
    for db in c.list_all("database"):
        if db["database_name"] == DATABASE_NAME:
            return db["id"]
    created = c.post("database/", {
        "database_name": DATABASE_NAME,
        "sqlalchemy_uri": TRINO_URI,
        "expose_in_sqllab": True,
        "allow_ctas": False,
        "allow_dml": False,
    })
    log.info("Created Superset database %s", created["id"])
    return created["id"]


def ensure_dataset(c: SupersetClient, db_id: int, schema: str, table: str, sql: str | None = None) -> int:
    existing = [d for d in c.list_all("dataset") if d["table_name"] == table and d.get("schema") == schema]
    if existing:
        ds_id = existing[0]["id"]
        if sql and c.get(f"dataset/{ds_id}")["result"].get("sql") != sql:
            c.put(f"dataset/{ds_id}", {"sql": sql})
        c.put(f"dataset/{ds_id}/refresh", {})
    else:
        payload = {"database": db_id, "schema": schema, "table_name": table}
        if sql:
            payload["sql"] = sql
        ds_id = c.post("dataset/", payload)["id"]
        log.info("Created dataset %s.%s (%s)", schema, table, ds_id)
    # Đánh dấu cột thời gian để Superset dùng làm trục X / bộ lọc thời gian
    columns = c.get(f"dataset/{ds_id}")["result"]["columns"]
    if any(col["column_name"] in TEMPORAL_COLUMNS and not col["is_dttm"] for col in columns):
        c.put(f"dataset/{ds_id}", {"columns": [
            {"id": col["id"], "column_name": col["column_name"],
             "is_dttm": col["is_dttm"] or col["column_name"] in TEMPORAL_COLUMNS}
            for col in columns
        ]})
    return ds_id


def metric(sql: str, label: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": sql, "label": label}


def time_range(column: str, value: str) -> dict:
    return {"clause": "WHERE", "expressionType": "SIMPLE", "subject": column,
            "operator": "TEMPORAL_RANGE", "comparator": value}


# Khoảng thời gian tự định nghĩa (các mốc có sẵn như "Last week" dừng ở 00:00 hôm nay → mất số liệu hôm nay).
# "now"/"today" của Superset tính theo UTC → cột giờ lọc theo timestamp (UTC); cột DATE (ngày giờ VN, có thể
# đi trước UTC 1 ngày) lọc tới hết ngày mai, vì với cột DATE Superset cắt mốc cuối về 00:00.
RECENT_HOURS = 'DATEADD(DATETIME("now"), -30, day) : now'  # đủ rộng để vẫn có số khi pipeline tạm dừng vài ngày


def days_back(n: int, unit: str = "day") -> str:
    return f'DATEADD(DATETIME("today"), -{n}, {unit}) : DATEADD(DATETIME("today"), 2, day)'


RECENT_DATES = days_back(30)
DATE_QUARTER = days_back(3, "month")
DATE_YEAR = days_back(1, "year")
DATE_2_YEARS = days_back(2, "year")
LATEST = '"timestamp"'  # dùng với max_by (Trino) để lấy giá trị của giờ mới nhất


def chart_specs() -> list[dict]:
    """Mỗi phần tử: tên chart, dataset (bảng), viz params, độ rộng (trên lưới 12 cột), hàng.
    by_province=True: chart xem 1 tỉnh, chịu tác động của bộ lọc "Tỉnh" trên dashboard."""
    def now_card(name, dataset, metric_sql, label, fmt, row, subheader):
        # big_number: hiển thị giá trị của giờ mới nhất + đường xu hướng gần đây, cho tỉnh đang chọn
        return {"name": name, "ds": dataset, "row": row, "width": 3, "height": 45, "by_province": True, "params": {
            "viz_type": "big_number", "x_axis": "timestamp", "time_grain_sqla": "PT1H",
            "metric": metric(metric_sql, label),
            "adhoc_filters": [time_range("timestamp", RECENT_HOURS)],
            "y_axis_format": fmt, "subheader": subheader, "show_trend_line": True,
            "start_y_axis_at_zero": False, "header_font_size": 0.4, "subheader_font_size": 0.125,
        }}

    def latest_table(name, dataset, metrics, filters, row, width, ts_format="%Y-%m-%d %H:%M", height=60,
                     groupby=("province_name",)):
        return {"name": name, "ds": dataset, "row": row, "width": width, "height": height, "params": {
            "viz_type": "table", "query_mode": "aggregate", "groupby": list(groupby),
            "metrics": [metric(sql, label) for sql, label in metrics],
            "adhoc_filters": filters, "row_limit": 100, "order_desc": False,
            "table_timestamp_format": ts_format,
        }}

    def line(name, dataset, x, grain, metric_sql, label, groupby, filters, row, width, fmt=".1f", height=60):
        return {"name": name, "ds": dataset, "row": row, "width": width, "height": height, "params": {
            "viz_type": "echarts_timeseries_line", "x_axis": x, "time_grain_sqla": grain,
            "metrics": [metric(metric_sql, label)], "groupby": groupby, "adhoc_filters": filters,
            "row_limit": 50000, "show_legend": True, "legendType": "scroll", "rich_tooltip": True,
            "y_axis_format": fmt, "zoomable": True,
        }}

    def heatmap(name, dataset, x, metric_sql, label, filters, row, scheme):
        return {"name": name, "ds": dataset, "row": row, "width": 6, "height": 60, "params": {
            "viz_type": "heatmap_v2", "x_axis": x, "time_grain_sqla": "P1M",
            "groupby": "province_name", "metric": metric(metric_sql, label), "adhoc_filters": filters,
            "linear_color_scheme": scheme, "normalize_across": "heatmap", "show_values": False,
            "legend_type": "continuous", "row_limit": 10000, "value_bounds": [None, None],
            "y_axis_format": ".1f", "x_axis_time_format": "%Y-%m",
        }}

    def monthly_bar(name, dataset, x, metric_sql, label, filters, row, width, height=60, grain="P1M",
                    groupby=("province_name",), stack="Stack"):
        return {"name": name, "ds": dataset, "row": row, "width": width, "height": height, "params": {
            "viz_type": "echarts_timeseries_bar", "x_axis": x, "time_grain_sqla": grain,
            "metrics": [metric(metric_sql, label)], "groupby": list(groupby), "adhoc_filters": filters,
            "row_limit": 10000, "show_legend": True, "legendType": "scroll", "rich_tooltip": True,
            "stack": stack,
        }}

    def trend(metric_name, label):
        return (f"MAX(CASE WHEN metric = '{metric_name}' THEN slope_per_decade END)", label)

    best_h, best_d, by_model = "weather_best_hourly", "weather_best_daily", "weather_daily"
    aq_h, aq_d = "air_quality_hourly_named", "air_quality_daily"
    return [
        # ---- Hàng thẻ "hiện tại" (tỉnh chọn ở bộ lọc): 2 thẻ thời tiết (giá trị chuẩn) + 2 thẻ CAMS
        now_card("Nhiệt độ hiện tại", best_h, "MAX(temperature_2m)", "°C", ".1f", 1,
                 "°C · tỉnh đang chọn (giá trị chuẩn)"),
        now_card("Độ ẩm hiện tại", best_h, "MAX(relative_humidity_2m)", "%", ".0f", 1,
                 "% độ ẩm · tỉnh đang chọn"),
        now_card("US AQI hiện tại", aq_h, "MAX(us_aqi)", "AQI", ".0f", 1, "US AQI · tỉnh đang chọn (CAMS)"),
        now_card("PM2.5 hiện tại", aq_h, "MAX(pm2_5)", "µg/m³", ".1f", 1, "µg/m³ PM2.5 · tỉnh đang chọn (CAMS)"),

        latest_table("Thời tiết giờ mới nhất – 15 tỉnh", best_h, [
            ("MAX(timestamp_local)", "Giờ (VN)"),
            (f"max_by(temperature_2m, {LATEST})", "Nhiệt độ (°C)"),
            (f"max_by(relative_humidity_2m, {LATEST})", "Độ ẩm (%)"),
            (f"max_by(precipitation, {LATEST})", "Mưa (mm/h)"),
            (f"max_by(chosen_model, {LATEST})", "Nguồn"),
        ], [time_range("timestamp", RECENT_HOURS)], 2, 7),
        latest_table("Chất lượng không khí mới nhất – 15 tỉnh", aq_d, [
            ("MAX(date)", "Ngày"),
            ("max_by(pm2_5_avg, date)", "PM2.5 TB (µg/m³)"),
            ("max_by(aqi_max, date)", "AQI cao nhất"),
            ("max_by(aqi_category, date)", "Mức AQI"),
        ], [time_range("date", RECENT_DATES)], 2, 5, "%Y-%m-%d"),

        # ---- Phần 1 – Thời tiết
        line("Nhiệt độ trung bình ngày theo tỉnh (°C)", best_d, "date", "P1D", "AVG(temp_avg)",
             "Nhiệt độ TB (°C)", ["province_name"], [time_range("date", DATE_YEAR)], 3, 12),
        monthly_bar("Tổng lượng mưa theo tháng (mm)", best_d, "date", "SUM(precip_sum)", "Lượng mưa (mm)",
                    [time_range("date", DATE_2_YEARS)], 4, 6),
        {**line("So sánh 3 model (tỉnh đang chọn): nhiệt độ TB ngày (°C)", by_model, "date", "P1D",
                "AVG(temp_avg)", "Nhiệt độ TB (°C)", ["model_name"], [time_range("date", DATE_QUARTER)], 4, 6),
         "by_province": True},

        # ---- Phần 2 – Chất lượng không khí
        line("PM2.5 trung bình ngày theo tỉnh (µg/m³)", aq_d, "date", "P1D", "AVG(pm2_5_avg)", "PM2.5 (µg/m³)",
             ["province_name"], [time_range("date", DATE_YEAR)], 5, 12),
        heatmap("PM2.5 TB theo tháng × tỉnh (µg/m³)", aq_d, "date", "AVG(pm2_5_avg)", "PM2.5 (µg/m³)",
                [time_range("date", DATE_2_YEARS)], 6, "oranges"),
        monthly_bar("Số ngày AQI > 100 theo tháng", aq_d, "date",
                    "SUM(CASE WHEN aqi_max > 100 THEN 1 ELSE 0 END)", "Số ngày AQI > 100",
                    [time_range("date", DATE_2_YEARS)], 6, 6),

        # ---- Phần 3 – Phân tích thời tiết
        heatmap("Chênh lệch nhiệt độ so với khí hậu, tháng × tỉnh (°C)", "weather_anomaly_daily", "date",
                "AVG(temp_anomaly)", "Chênh lệch (°C)", [time_range("date", DATE_2_YEARS)], 7, "blue_white_yellow"),
        monthly_bar("Số ngày dị thường nhiệt độ theo tháng (|z| ≥ 2)", "weather_anomaly_daily", "date",
                    "SUM(CASE WHEN anomaly_type IS NOT NULL THEN 1 ELSE 0 END)", "Số ngày",
                    [time_range("date", DATE_2_YEARS)], 7, 6),
        monthly_bar("Số ngày thời tiết cực đoan theo năm, toàn bộ 15 tỉnh", "weather_events", "start_date",
                    "SUM(duration_days)", "Số tỉnh × ngày", [], 8, 7, grain="P1Y", groupby=("event_type",), stack=None),
        latest_table("Xu hướng theo thập kỷ (trên các năm đủ dữ liệu)", "weather_trend", [
            trend("temp_avg", "Nhiệt độ TB (°C/thập kỷ)"),
            trend("hot_days", "Ngày ≥ 35 °C (ngày/thập kỷ)"),
            trend("precip_total", "Lượng mưa (mm/thập kỷ)"),
        ], [], 8, 5),
        latest_table("Giám sát chất lượng lượt xử lý gần nhất", "dq_run_log", [
            ("MAX(processed_at)", "Lúc xử lý"),
            ("max_by(rows_parsed, processed_at)", "Đọc"),
            ("max_by(rows_no_data, processed_at)", "Loại: không có dữ liệu"),
            ("max_by(rows_future, processed_at)", "Loại: giờ tương lai"),
            ("max_by(rows_duplicate, processed_at)", "Loại: trùng"),
            ("max_by(rows_written, processed_at)", "Ghi"),
            ("max_by(rows_invalid, processed_at)", "Không hợp lệ"),
            ("max_by(completeness_pct, processed_at)", "Đủ giờ (%)"),
        ], [], 10, 12, groupby=("model_name",), height=40),
    ]


def build_position(title: str, charts: list[tuple[dict, int, str]]) -> dict:
    pos = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": title}},
    }
    rows: dict[int, str] = {}
    for spec, chart_id, chart_uuid in charts:
        row_id = rows.get(spec["row"])
        if row_id is None:
            row_id = rows[spec["row"]] = f"ROW-{spec['row']}"
            pos[row_id] = {"type": "ROW", "id": row_id, "children": [], "parents": ["ROOT_ID", "GRID_ID"],
                           "meta": {"background": "BACKGROUND_TRANSPARENT"}}
            pos["GRID_ID"]["children"].append(row_id)
        node_id = f"CHART-{chart_id}"
        pos[node_id] = {"type": "CHART", "id": node_id, "children": [],
                        "parents": ["ROOT_ID", "GRID_ID", row_id],
                        "meta": {"chartId": chart_id, "width": spec["width"], "height": spec["height"],
                                 "sliceName": spec["name"], "uuid": chart_uuid}}
        pos[row_id]["children"].append(node_id)
    return pos


def province_filter(datasets: dict[str, int], charts: list[tuple[dict, int, str]]) -> dict:
    """Bộ lọc "Tỉnh" (chọn 1, mặc định Hà Nội) chỉ áp vào các chart xem 1 tỉnh; bảng/đường 15 tỉnh không bị lọc."""
    in_scope = [cid for spec, cid, _ in charts if spec.get("by_province")]
    value = [PROVINCE_FILTER_DEFAULT]
    return {
        "id": "NATIVE_FILTER-province",
        "name": "Tỉnh",
        "filterType": "filter_select",
        "type": "NATIVE_FILTER",
        "description": "Tỉnh cho các thẻ hiện tại và biểu đồ so sánh model",
        "targets": [{"datasetId": datasets["weather_best_hourly"], "column": {"name": "province_name"}}],
        "controlValues": {"multiSelect": False, "enableEmptyFilter": True, "defaultToFirstItem": False,
                          "searchAllOptions": False, "inverseSelection": False},
        "defaultDataMask": {
            "extraFormData": {"filters": [{"col": "province_name", "op": "IN", "val": value}]},
            "filterState": {"value": value, "label": PROVINCE_FILTER_DEFAULT},
            "ownState": {},
        },
        "cascadeParentIds": [],
        "scope": {"rootPath": ["ROOT_ID"],
                  "excluded": [cid for _, cid, _ in charts if cid not in in_scope]},
        "chartsInScope": in_scope,
        "tabsInScope": [],
    }


def version_marker() -> str:
    # Superset không nhận trường lạ trong json_metadata → ghi phiên bản vào comment CSS của dashboard
    return f"/* weather-lakehouse dashboard v{DASHBOARD_VERSION} */"


def is_current(c: SupersetClient, dash_id: int) -> bool:
    return version_marker() in (c.get(f"dashboard/{dash_id}").get("result", {}).get("css") or "")


def remove_stale_datasets(c: SupersetClient, db_id: int) -> None:
    """Xoá dataset của các bảng không còn dùng (bảng Gold/Silver của phiên bản trước)."""
    wanted = {(schema, table) for schema, table, _ in DATASETS}
    for d in c.list_all("dataset"):
        if d.get("database", {}).get("id") == db_id and (d.get("schema"), d["table_name"]) not in wanted:
            try:
                c.delete(f"dataset/{d['id']}")
                log.info("Deleted stale dataset %s.%s", d.get("schema"), d["table_name"])
            except RuntimeError as exc:
                log.warning("Could not delete dataset %s: %s", d["table_name"], exc)


# ----------------------------------------------------------------------------- entrypoint
def publish(force: bool = False) -> str:
    logging.basicConfig(level=logging.INFO)
    c = SupersetClient(os.environ.get("SUPERSET_URL", "http://superset:8088"),
                       os.environ["SUPERSET_ADMIN_USER"], os.environ["SUPERSET_ADMIN_PASSWORD"])
    db_id = ensure_database(c)
    url = f"{c.base}/superset/dashboard/{DASHBOARD_SLUG}/"

    existing = [d for d in c.list_all("dashboard") if d.get("slug") == DASHBOARD_SLUG]
    if existing and not force and all(is_current(c, d["id"]) for d in existing):
        for schema, table, sql in DATASETS:  # làm mới metadata cột
            ensure_dataset(c, db_id, schema, table, sql)
        log.info("Dashboard already up to date: %s", url)
        return url
    for d in existing:
        for ch in c.get(f"dashboard/{d['id']}/charts").get("result", []):
            c.delete(f"chart/{ch['id']}")
        c.delete(f"dashboard/{d['id']}")
    remove_stale_datasets(c, db_id)
    datasets = {table: ensure_dataset(c, db_id, schema, table, sql) for schema, table, sql in DATASETS}

    dash_id = c.post("dashboard/", {"dashboard_title": DASHBOARD_TITLE, "slug": DASHBOARD_SLUG,
                                    "published": True})["id"]
    created = []
    for spec in chart_specs():
        ds_id = datasets[spec["ds"]]
        params = {**spec["params"], "datasource": f"{ds_id}__table"}
        chart_id = c.post("chart/", {
            "slice_name": spec["name"], "viz_type": params["viz_type"],
            "datasource_id": ds_id, "datasource_type": "table",
            "params": json.dumps(params, ensure_ascii=False), "dashboards": [dash_id],
        })["id"]
        chart_uuid = c.get(f"chart/{chart_id}").get("result", {}).get("uuid") or str(uuid.uuid4())
        created.append((spec, chart_id, chart_uuid))
        log.info("Created chart %s: %s", chart_id, spec["name"])

    c.put(f"dashboard/{dash_id}", {
        "position_json": json.dumps(build_position(DASHBOARD_TITLE, created), ensure_ascii=False),
        "css": version_marker(),
        "json_metadata": json.dumps({"color_scheme": "supersetColors", "refresh_frequency": 0,
                                     "cross_filters_enabled": True,
                                     "native_filter_configuration": [province_filter(datasets, created)]},
                                    ensure_ascii=False),
    })
    log.info("Dashboard published: %s", url)
    return url


if __name__ == "__main__":
    import sys

    print(publish(force="--force" in sys.argv))
