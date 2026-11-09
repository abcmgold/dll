# Thống kê dữ liệu – Weather Lakehouse

*Số liệu lấy ngày 09/11/2026 bằng Trino trên các bảng Iceberg. Phạm vi: 15 tỉnh miền Bắc, mỗi tỉnh 1 toạ độ trung tâm. Thay thế cho bản ngày 29/10/2026 (`Thong_ke_du_lieu.md`).*

---

## 1. Khối lượng dữ liệu

### 1.1 Theo nguồn (tầng Silver, dữ liệu theo giờ)

| Nguồn | Loại | Từ (giờ VN) | Đến (giờ VN) | Số tỉnh | Số dòng (tỉnh × giờ) | Số biến | Số giá trị đo | Tỷ trọng |
|---|---|---|---|---|---|---|---|---|
| ERA5 (`era5_seamless`) | Tái phân tích | 2015-01-01 07:00 | 2026-11-04 06:00 | 15 | 1.557.000 | 3 | 4.671.000 | 48,3% |
| GFS (`gfs_seamless`) | Dự báo lịch sử | 2021-03-23 07:00 | 2026-11-09 22:00 | 15 | 740.760 | 3 | 2.222.280 | 23,0% |
| ECMWF IFS (`ecmwf_ifs025`) | Dự báo lịch sử | 2024-02-03 05:00 | 2026-11-09 22:00 | 15 | 363.870 | 3 | 1.091.610 | 11,3% |
| CAMS (`cams_global`) | Chất lượng không khí | 2022-08-04 07:00 | 2026-11-09 22:00 | 15 | 561.120 | 3 | 1.683.360 | 17,4% |
| **Tổng** | | | | | **3.222.750** | | **9.668.250** | 100% |

- Ba nguồn thời tiết dùng 3 biến: `temperature_2m` (°C), `relative_humidity_2m` (%), `precipitation` (mm/giờ).
- CAMS dùng 3 biến: `pm2_5` (µg/m³), `pm10` (µg/m³), `us_aqi` (trung bình trượt 24 giờ).

### 1.2 Theo năm (số dòng)

| Năm | ERA5 | GFS | ECMWF | CAMS | Tổng |
|---|---|---|---|---|---|
| 2015 | 131.295 | – | – | – | 131.295 |
| 2016 | 131.760 | – | – | – | 131.760 |
| 2017 | 131.400 | – | – | – | 131.400 |
| 2018 | 131.400 | – | – | – | 131.400 |
| 2019 | 131.400 | – | – | – | 131.400 |
| 2020 | 131.760 | – | – | – | 131.760 |
| 2021 | 131.400 | 102.135 | – | – | 233.535 |
| 2022 | 131.400 | 131.400 | – | 53.895 | 316.695 |
| 2023 | 131.400 | 131.400 | – | 131.400 | 394.200 |
| 2024 | 131.760 | 131.760 | 119.805 | 131.760 | 515.085 |
| 2025 | 131.400 | 131.400 | 131.400 | 131.400 | 525.600 |
| 2026 | 110.625 | 112.665 | 112.665 | 112.665 | 448.620 |
| **Tổng** | **1.557.000** | **740.760** | **363.870** | **561.120** | **3.222.750** |

Năm đủ có 131.400 dòng (365 ngày × 24 giờ × 15 tỉnh), năm nhuận có 131.760 dòng. Năm đầu và năm cuối của mỗi nguồn chỉ có một phần.

### 1.3 Theo tỉnh

Cả 15 tỉnh có **số dòng bằng nhau**: ERA5 103.800, GFS 49.384, ECMWF 24.258, CAMS 37.408 dòng mỗi tỉnh.

| Mã | Tỉnh | Mã | Tỉnh | Mã | Tỉnh |
|---|---|---|---|---|---|
| BN | Bắc Ninh | CB | Cao Bằng | HN | Hà Nội |
| HY | Hưng Yên | HP | Hải Phòng | LCH | Lai Châu |
| LCA | Lào Cai | LS | Lạng Sơn | NB | Ninh Bình |
| PT | Phú Thọ | QN | Quảng Ninh | SL | Sơn La |
| TN | Thái Nguyên | TQ | Tuyên Quang | DB | Điện Biên |

### 1.4 Theo tầng lakehouse

| Tầng | Bảng | Số bản ghi | Số file Parquet | Dung lượng (MiB) |
|---|---|---|---|---|
| Bronze | `era5_raw` | 3.910 | 22 | 15,6 |
| Bronze | `historical_forecast_raw` | 1.550 | 17 | 6,8 |
| Bronze | `air_quality_raw` | 1.595 | 17 | 3,9 |
| Silver | `weather_hourly` | 2.661.630 | 246 | 14,7 |
| Silver | `air_quality_hourly` | 561.120 | 52 | 4,3 |
| Silver | `dim_province` | 15 | 1 | 0,0 |
| Silver | `dq_run_log` | 12 | 3 | 0,0 |
| Gold | `weather_daily` | 110.925 | 12 | 0,9 |
| Gold | `weather_best_hourly` | 1.559.040 | 143 | 9,3 |
| Gold | `weather_best_daily` | 64.965 | 12 | 0,6 |
| Gold | `air_quality_daily` | 23.385 | 5 | 0,3 |
| Gold | `weather_anomaly_daily` | 64.935 | 2 | 0,7 |
| Gold | `weather_events` | 3.326 | 2 | 0,0 |
| Gold | `weather_yearly` | 180 | 2 | 0,0 |
| Gold | `weather_trend` | 90 | 2 | 0,0 |
| **Tổng dữ liệu Parquet** | | **5.056.678** | **538** | **57,1** |

Toàn bộ kho trên MinIO, kể cả metadata và các snapshot cũ của Iceberg, khoảng 109 MiB.

### 1.5 Gọi API (Bronze)

| Nguồn API | Số bản ghi (1 bản ghi = 1 tỉnh × 1 khoảng ngày) | HTTP khác 200 | JSON gốc (MiB) |
|---|---|---|---|
| Archive (ERA5) | 3.910 | 0 | 109,5 |
| Historical Forecast (GFS + ECMWF trong cùng 1 lượt gọi) | 1.550 | 0 | 48,4 |
| Air Quality (CAMS) | 1.595 | 0 | 29,9 |
| **Tổng** | **7.055** | **0** | **187,8** |

JSON gốc khoảng 187,8 MiB, lưu thành Parquet ở Bronze còn 26,2 MiB, tức nhỏ hơn khoảng 7,2 lần.

---

## 2. Chất lượng dữ liệu

Phần này làm theo kiểu `df.info()`, `df.isnull().sum()` và `df.describe()` của pandas, tách riêng cho từng nguồn. Thống kê tính trên toàn bộ dòng Silver, gồm cả các dòng bị gắn `is_valid = false`.

### 2.1 Cấu trúc bảng (`info()`)

**`silver.weather_hourly`**: 2.661.630 dòng × 13 cột

| # | Cột | Kiểu | Non-null | Null |
|---|---|---|---|---|
| 0 | province_id | varchar | 2.661.630 | 0 |
| 1 | timestamp | timestamp(6) with tz (UTC) | 2.661.630 | 0 |
| 2 | timestamp_local | timestamp(6) (giờ VN) | 2.661.630 | 0 |
| 3 | date_partition | date | 2.661.630 | 0 |
| 4 | source_type | varchar | 2.661.630 | 0 |
| 5 | model_name | varchar | 2.661.630 | 0 |
| 6 | temperature_2m | double | 2.661.600 | **30** |
| 7 | relative_humidity_2m | double | 2.652.330 | **9.300** |
| 8 | precipitation | double | 2.652.795 | **8.835** |
| 9 | is_valid | boolean | 2.661.630 | 0 |
| 10 | invalid_reason | varchar | 50 | 2.661.580 (null = hợp lệ) |
| 11 | fetched_at | timestamp(6) with tz | 2.661.630 | 0 |
| 12 | run_id | varchar | 2.661.630 | 0 |

**`silver.air_quality_hourly`**: 561.120 dòng × 13 cột

| # | Cột | Kiểu | Non-null | Null |
|---|---|---|---|---|
| 0–5 | province_id, timestamp, timestamp_local, date_partition, source_type, model_name | | 561.120 | 0 |
| 6 | pm2_5 | double | 561.120 | 0 |
| 7 | pm10 | double | 561.120 | 0 |
| 8 | us_aqi | integer | 560.760 | **360** |
| 9 | is_valid | boolean | 561.120 | 0 |
| 10 | invalid_reason | varchar | 55 | 561.065 (null = hợp lệ) |
| 11–12 | fetched_at, run_id | | 561.120 | 0 |

Các cột khoá và metadata có 0 giá trị null. Giá trị thiếu chỉ nằm ở các cột đo.

### 2.2 Giá trị thiếu (`isnull().sum()`)

| Nguồn | Cột | Tổng dòng | Null | % null |
|---|---|---|---|---|
| ERA5 | temperature_2m | 1.557.000 | 0 | 0,000% |
| ERA5 | relative_humidity_2m | 1.557.000 | 0 | 0,000% |
| ERA5 | precipitation | 1.557.000 | 0 | 0,000% |
| GFS | temperature_2m | 740.760 | 0 | 0,000% |
| GFS | relative_humidity_2m | 740.760 | 0 | 0,000% |
| GFS | precipitation | 740.760 | 5.190 | 0,701% |
| ECMWF | temperature_2m | 363.870 | 30 | 0,008% |
| ECMWF | relative_humidity_2m | 363.870 | 9.300 | 2,556% |
| ECMWF | precipitation | 363.870 | 3.645 | 1,002% |
| CAMS | pm2_5 | 561.120 | 0 | 0,000% |
| CAMS | pm10 | 561.120 | 0 | 0,000% |
| CAMS | us_aqi | 561.120 | 360 | 0,064% |
| **Tổng** | | **9.668.250 ô** | **18.525** | **0,192%** |

**Giá trị thiếu nằm ở tháng nào** (không có tháng nào khác bị thiếu):

| Nguồn | Tháng | temperature_2m | relative_humidity_2m | precipitation / us_aqi | Thiếu mỗi tỉnh (giờ, cột nhiều nhất) |
|---|---|---|---|---|---|
| ECMWF | 2024-02 | 30 | 9.300 | 3.525 | 620 |
| ECMWF | 2024-03 | 0 | 0 | 120 | 8 |
| GFS | 2021-03 | 0 | 0 | 15 | 1 |
| GFS | 2022-11 | 0 | 0 | 315 | 21 |
| GFS | 2022-12 | 0 | 0 | 4.860 | 324 |
| CAMS | 2022-08 | – | – | 360 | 24 |

Mỗi tỉnh có 997 dòng thời tiết thiếu ít nhất 1 ô. Cả 15 tỉnh thiếu **giống hệt nhau**, cho thấy dữ liệu bị thiếu ngay từ phía API Open-Meteo, không phải do một lượt gọi hay một tỉnh nào bị lỗi.

### 2.3 Thống kê mô tả (`describe()`)

Cột `unique` là số giá trị khác nhau. Tứ phân vị là xấp xỉ với sai số thứ hạng tối đa 0,01%.

**Nhiệt độ – `temperature_2m` (°C)**

| Nguồn | count | mean | std | min | 25% | 50% | 75% | max | skew | unique |
|---|---|---|---|---|---|---|---|---|---|---|
| ERA5 | 1.557.000 | 23,38 | 5,51 | 1,5 | 19,70 | 24,20 | 27,30 | 40,0 | -0,42 | 384 |
| GFS | 740.760 | 23,97 | 5,62 | 2,8 | 20,40 | 24,30 | 27,60 | 44,3 | -0,16 | 410 |
| ECMWF | 363.840 | 24,22 | 5,21 | 3,3 | 21,00 | 25,10 | 27,90 | 39,3 | -0,52 | 350 |

**Độ ẩm – `relative_humidity_2m` (%)**

| Nguồn | count | mean | std | min | 25% | 50% | 75% | max | skew | unique |
|---|---|---|---|---|---|---|---|---|---|---|
| ERA5 | 1.557.000 | 81,35 | 14,15 | 14,0 | 72,00 | 84,00 | 93,00 | 100,0 | -0,87 | 85 |
| GFS | 740.760 | 76,39 | 17,42 | 9,0 | 65,00 | 80,00 | 91,00 | 100,0 | -0,81 | 92 |
| ECMWF | 354.570 | 80,17 | 14,62 | 12,0 | 71,00 | 83,00 | 92,00 | 100,0 | -0,85 | 88 |

**Lượng mưa – `precipitation` (mm/giờ); % giá trị = 0 tính trên các dòng không null**

| Nguồn | count | mean | std | min | 25% | 50% | 75% | max | skew | % giá trị = 0 |
|---|---|---|---|---|---|---|---|---|---|---|
| ERA5 | 1.557.000 | 0,22 | 0,76 | 0,0 | 0,00 | 0,00 | 0,10 | 34,0 | 8,29 | 71,7% |
| GFS | 735.570 | 0,21 | 0,91 | 0,0 | 0,00 | 0,00 | 0,00 | 77,9 | 19,45 | 75,8% |
| ECMWF | 360.225 | 0,27 | 0,81 | 0,0 | 0,00 | 0,00 | 0,10 | 27,6 | 6,53 | 64,4% |

**PM2.5 – `pm2_5` (µg/m³)**

| Nguồn | count | mean | std | min | 25% | 50% | 75% | max | skew | % giá trị = 0 |
|---|---|---|---|---|---|---|---|---|---|---|
| CAMS | 561.120 | 32,14 | 25,29 | 0,0 | 15,00 | 25,70 | 41,70 | 737,6 | 2,39 | 0,0% |

**PM10 – `pm10` (µg/m³)**

| Nguồn | count | mean | std | min | 25% | 50% | 75% | max | skew | % giá trị = 0 |
|---|---|---|---|---|---|---|---|---|---|---|
| CAMS | 561.120 | 40,09 | 30,72 | 0,0 | 19,20 | 32,70 | 52,30 | 1.061,7 | 2,67 | 0,0% |

**US AQI – `us_aqi`**

| Nguồn | count | mean | std | min | 25% | 50% | 75% | max | skew | % giá trị = 0 |
|---|---|---|---|---|---|---|---|---|---|---|
| CAMS | 560.760 | 95,18 | 40,20 | 12,0 | 65,00 | 86,00 | 121,00 | 370,0 | 0,65 | 0,0% |

### 2.4 Ngoại lai theo IQR (giá trị nằm ngoài khoảng Q1 − 1,5·IQR đến Q3 + 1,5·IQR)

| Nguồn | Cột | Số ngoại lai | % giá trị không null | Nhận xét |
|---|---|---|---|---|
| ERA5 | temperature_2m | 6.601 | 0,4% | Đuôi lạnh vào mùa đông ở vùng núi |
| GFS | temperature_2m | 6.461 | 0,9% | Đuôi lạnh vào mùa đông ở vùng núi |
| ECMWF | temperature_2m | 2.760 | 0,8% | Đuôi lạnh vào mùa đông ở vùng núi |
| ERA5 | relative_humidity_2m | 13.596 | 0,9% | Những đợt hanh khô |
| GFS | relative_humidity_2m | 5.068 | 0,7% | Những đợt hanh khô |
| ECMWF | relative_humidity_2m | 3.674 | 1,0% | Những đợt hanh khô |
| ERA5 | precipitation | 248.499 | 16,0% | Mưa lệch phải rất mạnh, nên IQR không dùng được cho biến này |
| GFS | precipitation | 178.305 | 24,2% | Mưa lệch phải rất mạnh, nên IQR không dùng được cho biến này |
| ECMWF | precipitation | 67.719 | 18,8% | Mưa lệch phải rất mạnh, nên IQR không dùng được cho biến này |
| CAMS | pm2_5 | 27.373 | 4,9% | Các đợt ô nhiễm |
| CAMS | pm10 | 25.590 | 4,6% | Các đợt ô nhiễm |
| CAMS | us_aqi | 3.125 | 0,6% |  |

### 2.5 Kiểm tra tính toàn vẹn

| Kiểm tra | Kết quả | Đánh giá |
|---|---|---|
| Đủ giờ: số dòng thực tế so với (số giờ từ đầu đến cuối × 15 tỉnh) | ERA5 100,00%, GFS 100,00%, ECMWF 100,00%, CAMS 100,00% | ✅ |
| Trùng khoá (`province_id`, `timestamp`, `model_name`) | 0 | ✅ |
| Cột khoá hoặc metadata bị null | 0 | ✅ |
| Số dòng giữa các tỉnh | 15/15 tỉnh bằng nhau | ✅ |
| Bản ghi Bronze có HTTP khác 200 | 0 / 7.055 | ✅ |
| Dòng bị gắn `is_valid = false` | CAMS `pm2_5_above_pm10`: 55; GFS `temperature_jump`: 50 | ✅ Được giữ ở Silver, không đưa vào Gold |
| Nhiệt độ thay đổi hơn 8 °C trong 1 giờ | GFS 50 lần (13 tỉnh, 2021–2026). ERA5, ECMWF: 0 | ✅ Đã có luật `temperature_jump` |
| Mưa hơn 50 mm trong 1 giờ (số dòng tỉnh × giờ) | ERA5 0, GFS 12, ECMWF 0 | ⚠️ Chưa có luật, ngưỡng hiện tại là 300 mm/giờ |
| Nhiệt độ từ 44 °C trở lên (số dòng tỉnh × giờ) | ERA5 0, GFS 2, ECMWF 0 | ⚠️ Khó xảy ra thực tế ở miền Bắc, chưa có luật |
| `pm2_5` > `pm10` | 55 dòng | ✅ Đã có luật `pm2_5_above_pm10` |
| PM2.5 > 300 µg/m³ | 78 dòng, từ 2023-02-01 13:00 đến 2023-05-22 15:00 | ⚠️ Có thể là thật (mùa đốt nương, cháy rừng), chưa đối chiếu |
| Ngày ở Gold có ít hơn 24 giờ | `weather_daily`: ECMWF 30, ERA5 30, GFS 80; `weather_best_daily`: 30; `air_quality_daily`: 49 | Tỉnh × ngày. Gồm ngày đầu, ngày cuối của mỗi nguồn và các ngày có giờ bị gắn không hợp lệ hoặc thiếu nhiệt độ |

### 2.6 So sánh GFS và ECMWF với ERA5 (trên cùng tỉnh và cùng giờ)

| Chỉ số | ECMWF IFS | GFS |
|---|---|---|
| Số cặp so sánh (dòng tỉnh × giờ) | 361.830 | 738.720 |
| Nhiệt độ: chênh lệch trung bình | +0,20 °C | +0,26 °C |
| Nhiệt độ: sai số tuyệt đối trung bình (MAE) | 0,90 °C | 1,28 °C |
| Nhiệt độ: tương quan | 0,976 | 0,956 |
| Độ ẩm: chênh lệch trung bình | −0,7 % | −4,6 % |
| Độ ẩm: MAE | 4,8 % | 7,7 % |
| Lượng mưa theo giờ: tương quan | 0,473 | 0,325 |
| Lượng mưa theo ngày: chênh lệch trung bình | +0,34 mm | −0,50 mm |
| Lượng mưa theo ngày: tương quan | 0,804 | 0,689 |
| Cùng kết luận ngày mưa (≥ 1 mm) hay không mưa | 86,0% | 83,3% |

So sánh theo ngày chỉ dùng các ngày đủ 24 giờ ở cả hai model. ECMWF khớp với ERA5 tốt hơn GFS ở các chỉ số sai số và tương quan, ủng hộ thứ tự ưu tiên ERA5 → ECMWF → GFS khi chọn giá trị chuẩn. Trong `gold.weather_best_hourly`, ERA5 chiếm 99,9% số dòng. ECMWF dùng cho 2.040 dòng, tức 136 giờ của mỗi tỉnh, từ 2026-11-04 07:00 đến 2026-11-09 22:00 khi ERA5 chưa công bố.

---

## 3. Tóm tắt các thiếu sót

| # | Vấn đề | Nguồn | Quy mô | Nguyên nhân | Hướng xử lý |
|---|---|---|---|---|---|
| 1 | Thiếu độ ẩm, mưa và một ít nhiệt độ | ECMWF | 12.975 ô, chủ yếu 02/2024 | API: tháng đầu model có trong kho lưu trữ | Giữ null, không nội suy. Giá trị chuẩn ưu tiên ERA5 |
| 2 | Thiếu lượng mưa | GFS | 5.190 ô (11–12/2022 và giờ đầu chuỗi) | API | Như trên |
| 3 | Thiếu US AQI | CAMS | 360 ô (ngày đầu chuỗi) | AQI cần 24 giờ dữ liệu trước đó | Không cần xử lý |
| 4 | Nhiệt độ nhảy hơn 8 °C trong 1 giờ | GFS | 50 lần | Lỗi của model | **Đã xử lý:** luật `temperature_jump` |
| 5 | PM2.5 > PM10 | CAMS | 55 dòng | Lỗi của model | **Đã xử lý:** luật `pm2_5_above_pm10` |
| 6 | Dữ liệu ERA5 trễ | ERA5 | Đến 2026-11-04 06:00, các model khác đến 2026-11-09 22:00 (giờ VN) | ERA5 công bố trễ 5–6 ngày | Không cần xử lý: pipeline tự lấy thêm, Gold tạm dùng ECMWF |

**Kết luận:** dữ liệu **đầy đủ về thời gian và không gian** (100,00% số giờ trở lên, 15/15 tỉnh, 0 dòng trùng khoá). Tỷ lệ ô bị thiếu là **0,19%**, do phía API và dồn vào giai đoạn đầu của GFS, ECMWF và CAMS. Các giá trị bất thường về mặt vật lý ở GFS (50 lần nhảy nhiệt độ) và CAMS (55 dòng PM2.5 > PM10) đã được gắn `is_valid = false` và không đưa vào Gold.
