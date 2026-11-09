# Weather Lakehouse – Thời tiết & chất lượng không khí 15 tỉnh miền Bắc

Luồng dữ liệu theo thiết kế trong `Weather_Data_Pipeline.pdf`:
**Lakehouse trên Apache Iceberg · điều phối bởi Apache Airflow (Python + Spark) · truy vấn bằng Trino · dashboard bằng Superset**,
lấy dữ liệu từ 3 API miễn phí của Open-Meteo. Toàn bộ chạy bằng **1 file `docker-compose.yml`**. Thiết kế chi tiết: `PLAN.md`.

```
                ┌──────────────────── A0 · Airflow (LocalExecutor) ─────────────────────┐
                │ weather_pipeline (6 giờ/lần) · weather_backfill · iceberg_maintenance │
                └──────┬────────────────────────────────────────┬──────────────────────┘
                       │ B1/B2 Python + PyIceberg               │ B3/B4 spark-submit
 Archive (ERA5) ─┐     ▼                                        ▼
 Hist. Forecast ─┼─► Bronze (JSON gốc) ──► Spark ──► Silver (CDM theo giờ) ──► Gold (theo ngày + giá trị chuẩn)
 Air Quality ────┘
        Iceberg REST Catalog (metadata trong Postgres) + MinIO s3://warehouse (Parquet)
                                                    ▲
                     Q1 Trino (iceberg connector) ◄── Q2/Q3 Superset (dashboard 2 phần)
```

## 1. Thành phần

| Service | Image | Vai trò | Truy cập (host) |
|---|---|---|---|
| postgres | postgres:16-alpine | Metadata cho Airflow, Superset, Iceberg catalog | – |
| minio | bitnamilegacy/minio:2025.4.22 | Object storage S3 (bucket `warehouse`) | Console http://localhost:9101 |
| iceberg-rest | apache/iceberg-rest-fixture 1.9.2 + JDBC Postgres | Iceberg REST Catalog dùng chung cho PyIceberg, Spark, Trino | http://localhost:8181/v1/config |
| spark-master, spark-worker | apache/spark 3.5.6 + Iceberg 1.9.2 | Xử lý Bronze → Silver → Gold | Spark UI http://localhost:8090 |
| airflow-webserver, airflow-scheduler | apache/airflow 2.10.5 + Java + PySpark + PyIceberg | Điều phối (A0), ingest (B1/B2), Spark driver | http://localhost:8082 |
| trino | trinodb/trino 476 | SQL trên bảng Iceberg (Q1) | http://localhost:8083 |
| superset | apache/superset 4.1.2 + trino driver | Dashboard (Q2/Q3) | http://localhost:8088 |

Tài khoản và cổng host cấu hình trong `.env`.

## 2. Ba nguồn dữ liệu (2 thời tiết + 1 chất lượng không khí)

| Nguồn (API) | Model | Biến | Có từ | Cập nhật |
|---|---|---|---|---|
| Open-Meteo **Archive** (ERA5) | `era5_seamless` | nhiệt độ, độ ẩm, lượng mưa | 1950 | 1 lần/ngày, trễ 5–6 ngày |
| Open-Meteo **Historical Forecast** | `ecmwf_ifs025`, `gfs_seamless` | như trên (hậu tố tên model) | 2024-02-03 / 2021-03-23 | 6 giờ/lần, công bố sau ~5–7 giờ |
| Open-Meteo **Air Quality** (CAMS) | `cams_global` | PM2.5, PM10, US AQI | 2022-08-04 | 12 giờ/lần, công bố sau ~10 giờ |

15 tỉnh mới (địa giới từ 01/07/2025), mỗi tỉnh 1 toạ độ trung tâm hành chính – `pipeline/config/locations.json`.

## 3. Chạy

Yêu cầu: Docker Desktop cấp ≥ 8 GB RAM.

```bash
docker compose up -d --build
```

1. `airflow-init` tạo DB, user admin và 2 pool (`open_meteo_api`, `spark_cluster`).
2. DAG `weather_pipeline` tự chạy ngay khi khởi động, sau đó 6 giờ/lần (08:30 · 14:30 · 20:30 · 02:30 giờ VN) → Bronze → Silver → Gold → phân tích → **tự tạo dashboard Superset**. Lần đầu chỉ có khoảng 10 ngày gần nhất.
3. Nạp lịch sử bằng một lệnh, tự đợi lượt đầu xong, tạm dừng và bật lại `weather_pipeline`:
   - macOS, Linux, Git Bash: `sh backfill.sh 2015-01-01`
   - Windows (cmd, PowerShell): `.\backfill.cmd 2015-01-01`

   Có thể thêm ngày kết thúc và nguồn: `sh backfill.sh 2026-10-29 2026-11-05 --sources era5`.
   Hoặc làm tay: đợi lượt đầu xong, **tạm dừng `weather_pipeline`**, vào Airflow → DAG `weather_backfill` → *Trigger DAG w/ config*:
   `{"start_date": "2015-01-01", "end_date": "", "sources": ["era5", "historical_forecast", "air_quality"]}`
   Xong thì bật lại `weather_pipeline`; lượt kế tiếp sẽ tính lại phần phân tích trên toàn bộ lịch sử.
   Mỗi năm × mỗi nguồn là 1 mapped task: lỗi năm nào chỉ chạy lại năm đó. Hết hạn mức ngày của Open-Meteo
   thì task dừng ngay với thông báo rõ – *Clear* các task lỗi sau 00:00 UTC (07:00 giờ VN).

Dashboard: http://localhost:8088/superset/dashboard/weather-lakehouse/

Dừng: `docker compose down` (giữ dữ liệu) · Xoá sạch: `docker compose down -v`.

## 4. Luồng xử lý (A0 → B1…B4 → Q1…Q3)

| Bước | Ở đâu | Làm gì |
|---|---|---|
| A0 | `dags/weather_pipeline.py`, `weather_backfill.py`, `iceberg_maintenance.py` | Lịch chạy, retry (3 lần, backoff 2 → 30 phút), pool giới hạn API/Spark |
| B1 | `pipeline/ingest/{era5,historical_forecast,air_quality}.py` | Gọi API cho 15 toạ độ/request, retry lỗi mạng/5xx, chờ khi 429, điều tốc theo hạn mức giờ & ngày |
| B2 | `pipeline/ingest/common.py` | PyIceberg append vào `bronze.*_raw` (JSON gốc + `run_id`, `fetched_at`) |
| B3 | `pipeline/spark/bronze_to_silver.py` | Đọc Bronze của đúng `run_id`, áp mapping (bỏ hậu tố model, 1 phản hồi Forecast → 2 model) |
| B4 | `bronze_to_silver.py`, `silver_to_gold.py` | Làm sạch, gắn cờ `is_valid`, loại trùng, `MERGE` vào Silver; tổng hợp tăng dần vào Gold |
| Q1 | Trino catalog `iceberg` | Đọc trực tiếp bảng Iceberg |
| Q2/Q3 | `pipeline/serving/superset_dashboard.py` | Tạo kết nối Trino, dataset, 12 chart, dashboard (idempotent, có số phiên bản) |

Mỗi lượt `weather_pipeline` lấy lại ERA5 10 ngày gần nhất, Forecast/CAMS 2 ngày gần nhất: lấp chỗ thiếu và
ghi đè các giờ còn là dự báo bằng lượt mô hình mới hơn. Silver `MERGE` theo khoá, chỉ cập nhật khi bản mang vào
có `fetched_at` mới hơn → chạy lại không nhân bản và không ghi đè dữ liệu mới bằng dữ liệu cũ.

## 5. Mô hình dữ liệu

| Tầng | Bảng | Hạt | Nội dung |
|---|---|---|---|
| Bronze | `era5_raw`, `historical_forecast_raw`, `air_quality_raw` | 1 lần gọi × 1 tỉnh | JSON gốc + metadata |
| Silver | `weather_hourly` | tỉnh × giờ × model | `temperature_2m, relative_humidity_2m, precipitation`, `source_type`, `model_name`, `is_valid` |
| Silver | `air_quality_hourly` | tỉnh × giờ | `pm2_5, pm10, us_aqi` |
| Silver | `dim_province` | tỉnh | tên, tỉnh cũ sáp nhập, toạ độ |
| Gold | `weather_daily` | tỉnh × ngày × model | nhiệt độ TB/min/max, độ ẩm TB, tổng mưa |
| Gold | `weather_best_hourly` | tỉnh × giờ | **giá trị chuẩn**: ưu tiên ERA5 → ECMWF IFS → GFS, cột `chosen_model` |
| Gold | `weather_best_daily` | tỉnh × ngày | tổng hợp từ giá trị chuẩn |
| Gold | `air_quality_daily` | tỉnh × ngày | PM2.5/PM10 TB, AQI cao nhất, mức AQI (thang US EPA) |

Gold chỉ tính lại các (tỉnh, ngày) mà lượt chạy vừa ghi vào Silver (theo cột `run_id`).
DAG `iceberg_maintenance` (03:00 thứ Hai giờ VN): `rewrite_data_files` + `expire_snapshots` cho mọi bảng.

## 6. Truy vấn thử bằng Trino

```bash
docker compose exec trino trino
```
```sql
SELECT model_name, count(*) rows, min(timestamp_local), max(timestamp_local)
FROM iceberg.silver.weather_hourly GROUP BY 1;

-- Giá trị chuẩn giờ mới nhất của từng tỉnh
SELECT province_name, max_by(temperature_2m, "timestamp") temp, max_by(chosen_model, "timestamp") model
FROM iceberg.gold.weather_best_hourly GROUP BY 1;

-- Kiểm tra không trùng khoá
SELECT province_id, "timestamp", model_name, count(*) FROM iceberg.silver.weather_hourly
GROUP BY 1, 2, 3 HAVING count(*) > 1;

-- Time travel của Iceberg
SELECT * FROM iceberg.silver."weather_hourly$snapshots" ORDER BY committed_at DESC;
```

## 7. Cấu trúc thư mục

```
docker-compose.yml, .env
docker/{airflow,spark,superset,iceberg-rest}/   Dockerfile + cấu hình image
trino/etc/                                      catalog iceberg + jvm.config
postgres/init.sql                               tạo DB airflow, superset, iceberg_catalog
dags/                                           3 DAG Airflow
pipeline/config/locations.json                  15 tỉnh + toạ độ
pipeline/ingest/                                B1/B2 – Python + PyIceberg
pipeline/spark/                                 B3/B4 – PySpark
pipeline/serving/superset_dashboard.py          Q2/Q3 – dựng dashboard qua API
```

## 8. Xử lý sự cố

- **Port đã bị dùng** → đổi các biến `*_PORT` trong `.env`.
- **Dựng lại dashboard**: `docker compose exec airflow-scheduler python -m pipeline.serving.superset_dashboard --force`
- **Xử lý lại toàn bộ Bronze → Silver → Gold** (không gọi lại API):
  `python -c "from pipeline.orchestration import spark_submit; print(spark_submit('bronze_to_silver','--all'))"` rồi chạy lệnh in ra; tương tự với `silver_to_gold --all`.
- Open-Meteo trả 429 → ingest tự chờ và thử lại; có thể giảm `OPENMETEO_MAX_WEIGHT_PER_HOUR` / `OPENMETEO_MAX_WEIGHT_PER_DAY`.
