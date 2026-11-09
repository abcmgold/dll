# Plan – Hệ thống xử lý dữ liệu lớn: Thời tiết & Chất lượng không khí 15 tỉnh miền Bắc

Theo flow `Weather_Data_Pipeline.pdf`: **A0 Airflow → B1 Python → B2 Iceberg Bronze → B3/B4 Spark → Silver/Gold → Q1 Trino → Q2/Q3 Superset**.
Phạm vi đã chốt: **3 nguồn** (2 thời tiết + 1 chất lượng không khí), **1 luồng định kỳ**, **1 luồng backfill**, **1 luồng bảo trì**, dashboard **2 phần**.

---

## 1. Nguồn dữ liệu

### 1.1. Ba nguồn – bốn model

| # | Nguồn (API) | Model | Loại | Biến lấy | Có từ |
|---|---|---|---|---|---|
| 1 | Open-Meteo **Archive API** – `archive-api.open-meteo.com/v1/archive` | `era5_seamless` (ERA5-Land 0.1° + ERA5 0.25°) | Thời tiết – tái phân tích | temperature_2m, relative_humidity_2m, precipitation | 1950 |
| 2 | Open-Meteo **Historical Forecast API** – `historical-forecast-api.open-meteo.com/v1/forecast` | `ecmwf_ifs025` | Thời tiết – dự báo | như trên, hậu tố `_ecmwf_ifs025` | 2024-02-03 |
| | | `gfs_seamless` | Thời tiết – dự báo | như trên, hậu tố `_gfs_seamless` | 2021-03-23 |
| 3 | Open-Meteo **Air Quality API** – `air-quality-api.open-meteo.com/v1/air-quality` | `cams_global` (0.4°) | Chất lượng không khí | pm2_5, pm10, us_aqi | 2022-08-04 |

> Nguồn = API gọi (3) · Model = mô hình sinh ra dữ liệu (4). Nguồn 2 trả cả 2 model trong 1 phản hồi.

### 1.2. Nhịp cập nhật (kiểm tra metadata Open-Meteo ngày 15/10/2026)

| Model | Cập nhật | Sẵn sàng sau | Mỗi lần thêm | Bước thời gian gốc |
|---|---|---|---|---|
| ERA5 | 1 lần/ngày (~00:15 UTC) | trễ 5–6 ngày | trọn 24 giờ của 1 ngày cũ | 1 giờ |
| ECMWF IFS 0.25° | 6 giờ/lần (00/06/12/18Z) | ~7 giờ | ~6 giờ đầu của lượt chạy | **3 giờ** (Open-Meteo nội suy ra giờ) |
| GFS | 6 giờ/lần | ~5,5 giờ | ~6 giờ | 1 giờ |
| CAMS | 12 giờ/lần (00/12Z) | ~10 giờ | ~12 giờ | 3 giờ (trả về theo giờ) |

Hệ quả thiết kế:
- Không chạy theo giờ → **6 giờ/lần** là đủ (nguồn nhanh nhất cập nhật 6 giờ/lần).
- Giờ tương lai API trả là **giá trị dự báo** → bỏ ở Silver; mỗi lần lấy lại **2 ngày gần nhất** để ghi đè bằng lượt mô hình mới hơn.
- ERA5 lấy lại **10 ngày gần nhất**; giờ chưa có trả `null` → bỏ ở Silver.
- Lượng mưa ECMWF theo giờ bị dàn đều theo khung 3 giờ → dashboard dùng **số liệu theo ngày**.

### 1.3. Địa điểm
- 15 tỉnh/thành mới (địa giới từ 01/07/2025), mỗi tỉnh **1 toạ độ trung tâm hành chính** (`is_center = true` trong `pipeline/config/locations.json`).
- 1 request gửi cả 15 toạ độ (Open-Meteo hỗ trợ nhiều toạ độ/request).

### 1.4. Hạn mức API (dùng chung cho cả 3 API, theo IP)
600 lượt/phút · 5.000/giờ · **10.000/ngày**. 1 lượt ≈ 1 toạ độ × 14 ngày × 10 biến.

| Luồng | Ước tính |
|---|---|
| Định kỳ: 4 lần/ngày × 3 nguồn × 15 điểm | ~200 lượt/ngày |
| Backfill 2020→nay cả 3 nguồn (đoạn 31 ngày, 15 điểm ≈ 33 lượt/đoạn) | ERA5 ~2.800 + Forecast ~2.200 + CAMS ~1.700 ≈ **6.700** → vừa trong 1 ngày; vẫn giữ chốt chặn (mục 5.2) |

---

## 2. Kiến trúc

```
                ┌──────────────── A0 · Airflow ─────────────────────────────┐
                │ weather_pipeline (6h/lần) · weather_backfill · iceberg_maintenance │
                └──────┬───────────────────────────────┬────────────────────┘
                       │ B1/B2 Python + PyIceberg      │ B3/B4 spark-submit
 Archive (ERA5) ─┐     ▼                               ▼
 Hist. Forecast ─┼─► BRONZE (JSON gốc) ──► Spark ──► SILVER (theo giờ, CDM) ──► GOLD (theo ngày)
 Air Quality ────┘                                                               │
        Iceberg REST Catalog (Postgres) + MinIO s3://warehouse (Parquet)         │
                                                    Q1 Trino ◄───────────────────┘
                                                       ▲
                                         Q2/Q3 Superset – Weather & Air Quality Dashboard
```

Hạ tầng giữ nguyên `docker-compose.yml` hiện có: postgres, minio, iceberg-rest, spark-master/worker, airflow-webserver/scheduler, trino, superset.

---

## 3. Mô hình dữ liệu (9 bảng: 3 Bronze · 2 Silver · 4 Gold)

### 3.1. Bronze – dữ liệu thô (giữ nguyên phản hồi API)

`bronze.era5_raw`, `bronze.historical_forecast_raw`, `bronze.air_quality_raw` – cùng schema:

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| source | string | era5 / historical_forecast / air_quality |
| province_id | string | tỉnh của toạ độ |
| window_start, window_end | date | khoảng ngày đã gọi |
| request_url | string | URL đã gọi |
| http_status | int | |
| payload | string | **JSON gốc** của 1 toạ độ |
| fetched_at | timestamptz | thời điểm lấy |
| run_id | string | lượt chạy Airflow (truy vết, xử lý lại) |

Partition `days(fetched_at)` · chỉ append.

### 3.2. Silver – CDM theo giờ

**`silver.weather_hourly`** – khoá `(province_id, timestamp, model_name)` · partition `source_type, months(timestamp)`

| Cột | Kiểu | Đơn vị | Ghi chú |
|---|---|---|---|
| province_id | string | | |
| timestamp | timestamp | UTC | đầu giờ |
| timestamp_local | timestamp_ntz | UTC+7 | |
| date_partition | date | | ngày theo giờ VN |
| source_type | string | | reanalysis / historical_forecast |
| model_name | string | | era5_seamless / ecmwf_ifs025 / gfs_seamless |
| temperature_2m | double | °C | |
| relative_humidity_2m | double | % | |
| precipitation | double | mm | tích luỹ 1 giờ |
| is_valid | boolean | | ngoài ngưỡng → false |
| fetched_at | timestamp | UTC | bản mới hơn thắng khi trùng khoá |
| run_id | string | | lượt chạy đã ghi dòng → Gold biết (tỉnh, ngày) nào cần tính lại |

**`silver.air_quality_hourly`** – khoá `(province_id, timestamp)` · partition `months(timestamp)`

Cột: province_id, timestamp, timestamp_local, date_partition, source_type (`air_quality`), model_name (`cams_global`), **pm2_5** (µg/m³), **pm10** (µg/m³), **us_aqi** (int), is_valid, fetched_at, run_id.

Thông tin tỉnh (tên, tỉnh cũ sáp nhập, toạ độ) đọc từ `locations.json`, Superset join qua view `silver.dim_province` (tạo bằng Spark từ config, 15 dòng).

### 3.3. Mapping Bronze → Silver

| Trường CDM | ERA5 | Historical Forecast | Air Quality |
|---|---|---|---|
| province_id | từ toạ độ đã gửi (config) | như ERA5 | như ERA5 |
| timestamp | `hourly.time` (gọi `timezone=GMT`) | `hourly.time` | `hourly.time` |
| source_type | `'reanalysis'` | `'historical_forecast'` | `'air_quality'` |
| model_name | `'era5_seamless'` | từ hậu tố: `ecmwf_ifs025` / `gfs_seamless` (1 phản hồi → 2 dòng) | `'cams_global'` |
| temperature_2m | `temperature_2m` | `temperature_2m_<model>` | – |
| relative_humidity_2m | `relative_humidity_2m` | `relative_humidity_2m_<model>` | – |
| precipitation | `precipitation` | `precipitation_<model>` | – |
| pm2_5 / pm10 / us_aqi | – | – | `pm2_5` / `pm10` / `us_aqi` |

Mapping khai báo bằng **cấu hình** trong code (chép từ sheet Mapping), Spark thực thi:
`from_json` theo schema → `arrays_zip` + `explode` (mảng → dòng) → bỏ hậu tố model → `unionByName`.

### 3.4. Quy tắc làm sạch (B4)

| Quy tắc | Xử lý |
|---|---|
| Mọi biến đo đều null (ERA5 chưa có, ECMWF trước 2024-02-03, CAMS trước 2022-08-04) | bỏ dòng |
| `timestamp > fetched_at` (giá trị dự báo tương lai) | bỏ dòng |
| temp ∉ [-20, 50] · rh ∉ [0, 100] · precip ∉ [0, 300] · pm < 0 · aqi ∉ [0, 1000] | giữ, `is_valid = false` |
| Trùng khoá trong cùng lô | giữ bản `fetched_at` mới nhất |
| Trùng khoá với dữ liệu đã có | `MERGE … WHEN MATCHED AND s.fetched_at >= t.fetched_at THEN UPDATE` |

### 3.5. Gold – theo ngày (phục vụ dashboard)

**`gold.weather_daily`** – tỉnh × ngày × model: `province_id, province_name, date, source_type, model_name, temp_avg, temp_min, temp_max, rh_avg, precip_sum, n_hours`

**`gold.air_quality_daily`** – tỉnh × ngày: `province_id, province_name, date, pm2_5_avg, pm2_5_max, pm10_avg, aqi_max, aqi_category, n_hours`
(aqi_category theo thang US EPA: Tốt ≤50, Trung bình ≤100, Kém cho nhóm nhạy cảm ≤150, Xấu ≤200, Rất xấu ≤300, Nguy hại)

**`gold.weather_best_hourly`** – tỉnh × giờ, **1 giá trị chuẩn** chọn theo thứ tự ưu tiên model:

| Ưu tiên | Model | Lý do |
|---|---|---|
| 1 | era5_seamless | tái phân tích, gần thực tế nhất (nhưng trễ 5–6 ngày) |
| 2 | ecmwf_ifs025 | mô hình dự báo chính xác nhất (từ 2024-02-03) |
| 3 | gfs_seamless | dự phòng khi 2 nguồn trên không có |

Quy tắc: với mỗi `(province_id, timestamp)` lấy model ưu tiên cao nhất có `is_valid = true` và `temperature_2m` khác null
(`row_number() over (partition by province_id, timestamp order by priority)` = 1).
Cột: `province_id, province_name, timestamp, timestamp_local, date, temperature_2m, relative_humidity_2m, precipitation, chosen_model`.
Giá trị của 1 giờ có thể đổi theo thời gian (hôm nay là ECMWF, ~6 ngày sau thành ERA5) → `chosen_model` cho biết nguồn đang dùng.

**`gold.weather_best_daily`** – tỉnh × ngày, tổng hợp từ `weather_best_hourly`:
`temp_avg, temp_min, temp_max, rh_avg, precip_sum, n_hours, chosen_model` (model chiếm đa số giờ trong ngày).

Chỉ chỉ số dùng `is_valid = true`. Tính **tăng dần**: mỗi lần chỉ tính lại các ngày có trong lô Silver vừa MERGE, rồi `MERGE` vào Gold (không tính lại toàn bộ lịch sử).

---

## 4. Điều phối – 3 DAG

| DAG | Lịch | Luồng |
|---|---|---|
| `weather_pipeline` | `30 1,7,13,19 * * *` UTC (08:30 · 14:30 · 20:30 · 02:30 VN) | init → ingest 3 nguồn → bronze_to_silver(run_id) → silver_to_gold → publish dashboard |
| `weather_backfill` | chạy tay | xem mục 5 |
| `iceberg_maintenance` | `0 20 * * 0` UTC (03:00 thứ Hai VN) | compaction + expire snapshots cho **cả Bronze, Silver, Gold** |

`weather_pipeline` – cửa sổ lấy dữ liệu mỗi lần:

| Nguồn | Khoảng ngày | Lý do |
|---|---|---|
| ERA5 | hôm nay − 10 → hôm nay | trễ 5–6 ngày, ngày gần có thể còn sửa |
| Historical Forecast | hôm nay − 2 → hôm nay | ghi đè giờ dự báo bằng lượt mô hình mới |
| Air Quality | hôm nay − 2 → hôm nay | như trên |

Chống lỗi chung:
- Retry 3 lần, backoff tăng dần (2 → 30 phút); HTTP 429 → chờ theo `Retry-After`.
- Pool `open_meteo_api` (1 slot): 3 nguồn gọi tuần tự, chung hạn mức.
- Pool `spark_cluster` (1 slot): không có 2 job MERGE cùng lúc.
- Bước Spark `trigger_rule = all_done`: 1 nguồn lỗi không chặn nguồn khác.
- `max_active_runs = 1`; tạm dừng `weather_pipeline` khi đang backfill (ghi trong README).

---

## 5. Backfill

### 5.1. Cách dùng
Airflow → `weather_backfill` → *Trigger DAG w/ config*:
```json
{"start_date": "2020-01-01", "end_date": "2026-10-15", "sources": ["era5", "historical_forecast", "air_quality"]}
```

### 5.2. Cơ chế
1. `plan_years` chia khoảng ngày theo **năm**, năm gần nhất trước.
2. Mỗi **(nguồn × năm)** = 1 mapped task → lỗi năm nào chạy lại năm đó.
3. Mỗi nguồn tự cắt `start` về ngày có dữ liệu (ERA5 1950, GFS 2021-03-23, CAMS 2022-08-04).
4. Trong năm: request theo đoạn 31 ngày × 15 toạ độ, tự điều tốc dưới 4.000 lượt/giờ.
5. **Chốt chặn hạn mức ngày**: đếm lượt đã dùng trong ngày (UTC) từ các dòng Bronze đã ghi; backfill dừng ở 9.400, luồng định kỳ được dùng tới 9.900 (chừa chỗ cho ~200 lượt/ngày) → task dừng ngay, báo rõ "hết quota ngày, chạy tiếp sau 00:00 UTC" (Clear task là chạy tiếp các năm còn lại).
6. Ingest xong → Spark xử lý đúng `run_id` của lượt backfill → cập nhật Gold.
7. Xử lý lại mà không gọi API: `bronze_to_silver.py --all` (đọc lại toàn bộ Bronze).

Thời gian ước tính: ~1,5–2 giờ cho toàn bộ (giới hạn theo quota giờ).

---

## 6. Dashboard (Superset qua Trino)

Bộ lọc **Tỉnh** (chọn 1, mặc định Hà Nội) áp vào 4 thẻ "hiện tại" và biểu đồ so sánh model; bảng/biểu đồ 15 tỉnh không bị lọc.

**Phần 1 – Thời tiết** (mặc định dùng giá trị chuẩn, không phải chọn model)
- Thẻ: nhiệt độ & độ ẩm giờ mới nhất của tỉnh đang chọn ("27.0 °C · 76%") – `gold.weather_best_hourly`
- Đường: nhiệt độ TB/min/max theo ngày – `gold.weather_best_daily`
- Cột: tổng lượng mưa theo tháng/tỉnh – `gold.weather_best_daily` (Superset gộp theo tháng)
- Bảng: thời tiết giờ mới nhất 15 tỉnh – `gold.weather_best_hourly`
- Đường so sánh 3 model theo ngày cho tỉnh đang chọn – `gold.weather_daily`

**Phần 2 – Chất lượng không khí**
- Thẻ: US AQI & PM2.5 mới nhất của tỉnh đang chọn – `silver.air_quality_hourly` (dataset ảo ghép tên tỉnh)
- Đường: PM2.5 trung bình theo ngày – `gold.air_quality_daily`
- Bảng/heatmap: mức AQI 15 tỉnh × ngày gần đây
- Cột: số ngày AQI > 100 theo tháng/tỉnh

Dashboard tạo tự động, idempotent bằng `pipeline/serving/superset_dashboard.py`.

---

## 7. Thay đổi so với code hiện tại

| File | Việc |
|---|---|
| `pipeline/config/locations.json` | dùng 15 điểm trung tâm (bỏ điểm phụ), đổi `province_code` → `province_id` |
| `pipeline/ingest/*.py`, `common.py` | bỏ wind/pressure; Bronze dùng `province_id`; thêm đếm quota ngày |
| `pipeline/spark/bronze_to_silver.py` | schema CDM mới (source_type, model_name, timestamp…); bỏ is_provisional; MERGE có điều kiện `fetched_at` |
| `pipeline/spark/silver_to_gold.py` | viết lại: 4 bảng Gold (theo model + giá trị chuẩn), tính tăng dần |
| `pipeline/spark/maintenance.py` | thêm bảng Gold |
| `dags/weather_daily_pipeline.py` → `weather_pipeline.py` | lịch 6 giờ/lần, cửa sổ theo nguồn |
| `dags/weather_nrt_pipeline.py` | **xoá** |
| `dags/weather_backfill.py` | tham số `start_date/end_date`, chốt chặn quota ngày |
| `pipeline/serving/superset_dashboard.py` | 2 phần như mục 6 |
| `README.md` | viết lại theo 3 nguồn Open-Meteo |
| `Cấu trúc data.xlsx` | sửa sheet Hiện trạng/CDM/Mapping theo mục 1, 3 |
| Lakehouse cũ | xoá bảng Silver/Gold cũ (`docker compose down -v` hoặc DROP) vì đổi schema |

---

## 8. Các bước triển khai

| GĐ | Nội dung | Ước lượng | Xong khi |
|---|---|---|---|
| 1 | Chốt spec: sửa Excel (Hiện trạng, CDM, Mapping, quy tắc DQ) | 0,5 ngày | Excel khớp mục 1, 3 |
| 2 | Ingest + Bronze: config 15 điểm, 3 nguồn, quota ngày | 0,5 ngày | 3 bảng Bronze có dữ liệu 14 ngày |
| 3 | Silver: mapping, làm sạch, MERGE | 1 ngày | chạy 2 lần cùng run_id → số dòng không đổi |
| 4 | Gold tăng dần (gồm bảng giá trị chuẩn) + Superset dashboard 2 phần | 1 ngày | dashboard hiện đủ 9 chart; thẻ hiện tại có `chosen_model` |
| 5 | DAG: weather_pipeline, backfill, maintenance | 0,5 ngày | 3 DAG chạy xanh |
| 6 | Backfill lịch sử + kiểm thử + README/báo cáo | 1 ngày | số liệu đủ từ 2020 (ERA5) |

### Kiểm thử chấp nhận
- **Đủ dữ liệu**: mỗi tỉnh × model có 24 dòng/ngày (trừ khoảng chưa có của từng nguồn).
- **Không trùng**: `SELECT province_id, timestamp, model_name, count(*) … HAVING count(*) > 1` → 0 dòng.
- **Idempotent**: chạy lại `weather_pipeline` cùng ngày → số dòng Silver/Gold không đổi.
- **Bản mới thắng**: chạy lại Spark của lượt cũ không ghi đè giá trị mới hơn.
- **Time travel**: truy vấn `silver."weather_hourly$snapshots"` trên Trino.
- **Đối chiếu**: so 1 ngày bất kỳ trên dashboard với gọi API trực tiếp.

---

## 9. Quyết định đã chốt
1. **Bỏ gió, áp suất** – CDM chỉ gồm nhiệt độ, độ ẩm, lượng mưa (khớp Excel).
2. **Giá trị chuẩn chọn sẵn ở Gold** (`weather_best_hourly/daily`, ưu tiên ERA5 → ECMWF → GFS); thẻ "hiện tại" và các chart chính đọc từ đây.
3. **Backfill từ 2020-01-01** (mỗi nguồn tự cắt về ngày có dữ liệu: GFS 2021-03-23, CAMS 2022-08-04, ECMWF 2024-02-03).
