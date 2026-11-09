# Đặc điểm và yêu cầu xử lý dữ liệu – Nhóm 15

Nov 9, 2026 · @tuan

## 1. Đặc điểm dữ liệu thu thập

Dữ liệu là chuỗi thời gian theo giờ do các mô hình khí tượng và khí quyển sinh ra, lấy từ 3 API Open-Meteo cho 15 tỉnh miền Bắc. Đến 29/10/2026, kho có 3.205.515 dòng, tương đương 9.616.545 giá trị đo. Hệ thống không có số đo trực tiếp từ trạm.

### 1.1. Nguồn và bản chất dữ liệu

| API | Model | Loại dữ liệu | Độ phân giải không gian | Bước thời gian gốc | Nhịp cập nhật | Biến thu thập | API có từ |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Archive | `era5_seamless` | Tái phân tích | 0,1° (ERA5-Land), bổ sung 0,25° (ERA5) | 1 giờ | 1 lần/ngày, trễ 5–6 ngày | Nhiệt độ 2 m, độ ẩm 2 m, lượng mưa | 1950 |
| Historical Forecast | `ecmwf_ifs025` | Dự báo lịch sử | 0,25° | 3 giờ, API trả theo giờ | 6 giờ/lần, sau khoảng 7 giờ | Như trên | 03/02/2024 |
| Historical Forecast | `gfs_seamless` | Dự báo lịch sử | Khoảng 25 km | 1 giờ | 6 giờ/lần, sau khoảng 5,5 giờ | Như trên | 23/03/2021 |
| Air Quality | `cams_global` | Mô phỏng chất lượng không khí | 0,4° | 3 giờ, API trả theo giờ | 12 giờ/lần, sau khoảng 10 giờ | PM2.5, PM10, US AQI | 04/08/2022 |

Phạm vi không gian là 15 tỉnh, thành theo địa giới từ 01/07/2025. Mỗi tỉnh lấy 1 toạ độ trung tâm hành chính.

### 1.2. Định dạng và cấu trúc

- Dữ liệu bán cấu trúc dạng JSON. Mỗi toạ độ là một đối tượng, các biến là mảng song song với mảng `hourly.time`.
- Thời gian theo ISO 8601, không kèm múi giờ. Hệ thống gọi API với `timezone=GMT` nên giá trị là giờ UTC.
- Historical Forecast trả 2 model trong 1 phản hồi và thêm hậu tố vào tên trường, ví dụ `temperature_2m_ecmwf_ifs025`.
- Đơn vị: °C, %, mm/giờ, µg/m³. `us_aqi` tính từ trung bình trượt 24 giờ nên thay đổi chậm.
- Ba API cùng nhà cung cấp và cùng định dạng, nên phép biến đổi chủ yếu là chuẩn hoá cấu trúc.

### 1.3. Khối lượng và tốc độ phát sinh

| Model | Khoảng dữ liệu trong kho (giờ VN) | Số dòng Silver | Số giá trị đo | Tỷ trọng |
| --- | --- | --- | --- | --- |
| `era5_seamless` | 01/01/2015 – 23/10/2026 | 1.552.680 | 4.658.040 | 48,4% |
| `gfs_seamless` | 23/03/2021 – 28/10/2026 | 736.455 | 2.209.365 | 23,0% |
| `cams_global` | 04/08/2022 – 28/10/2026 | 556.815 | 1.670.445 | 17,4% |
| `ecmwf_ifs025` | 03/02/2024 – 28/10/2026 | 359.565 | 1.078.695 | 11,2% |

Mỗi model sinh 360 dòng/ngày (15 tỉnh × 24 giờ), cả 4 model là 1.440 dòng/ngày. Bronze có 6.740 bản ghi với khoảng 186 MB JSON gốc, lưu thành Parquet còn 25,8 MB. Toàn bộ kho trên MinIO khoảng 83 MB. Hạn mức miễn phí của Open-Meteo là 600 lượt/phút, 5.000 lượt/giờ và 10.000 lượt/ngày.

### 1.4. Đặc điểm ảnh hưởng đến xử lý

| Đặc điểm | Biểu hiện | Hệ quả với xử lý và sử dụng |
| --- | --- | --- |
| Dữ liệu mô hình, không phải số đo | Cả 4 model đều là giá trị trên lưới | Không có giá trị thực địa để đối chiếu, chỉ so sánh được giữa các model với ERA5 |
| Một điểm đại diện cho một tỉnh | Tỉnh sáp nhập rất rộng, ví dụ Lào Cai mới lấy điểm tại Yên Bái | Giá trị không đại diện cho toàn tỉnh, nhất là vùng núi |
| Lưới thô so với khoảng cách giữa các tỉnh | CAMS 0,4° (khoảng 45 km) | Hà Nội, Bắc Ninh, Hưng Yên có thể nhận giá trị gần giống nhau |
| Nhịp và độ trễ khác nhau | ERA5 trễ 5–6 ngày, ECMWF và GFS cập nhật 6 giờ/lần | Mỗi lượt phải lấy lại một số ngày gần nhất theo từng nguồn |
| Giá trị của cùng một giờ có thể thay đổi | Lượt mô hình mới thay giá trị của lượt cũ | Cần cập nhật theo khoá và giữ bản lấy về sau cùng |
| API trả cả giờ tương lai | Giờ sau thời điểm lấy là giá trị dự báo | Phải loại trước khi đưa vào Silver |
| Giá trị theo giờ có thể là nội suy | ECMWF và CAMS có bước gốc 3 giờ, lượng mưa ECMWF bị dàn đều theo khung 3 giờ | Lượng mưa nên so sánh theo ngày |
| Giá trị thiếu dồn theo giai đoạn | 18.525/9.616.545 ô (0,193%), chủ yếu ECMWF 02–03/2024 và GFS 11–12/2022. Cả 15 tỉnh thiếu giống hệt nhau | Thiếu từ phía API, không phải lỗi của một lượt gọi |
| Phân phối lệch mạnh | Lượng mưa: 64,4–75,8% số giờ bằng 0, độ lệch 6,53–19,45 | Phương pháp IQR không phù hợp, gắn nhầm 16,0–24,2% số giờ là ngoại lai |
| Có giá trị bất thường do mô hình | 55 dòng PM2.5 > PM10, 50 lần GFS đổi hơn 8 °C trong 1 giờ | Gắn không hợp lệ bằng luật riêng, không đưa vào Gold (xem C2, C3) |
| Có đợt giá trị cực đoan chưa đối chiếu | PM2.5 > 300 µg/m³ chỉ xuất hiện trong 02–05/2023 | Có thể là thật (mùa đốt nương, cháy rừng), chưa loại bỏ |

## 2. Yêu cầu, quy tắc xử lý và minh chứng

Hệ thống đã thực hiện đủ 20 yêu cầu xử lý. Dữ liệu đi qua hai chặng: Bronze là vùng tiếp nhận giữ bản gốc để xử lý lại khi cần, còn Silver và Gold là kho phục vụ phân tích. Dữ liệu chỉ vào Silver sau khi qua các quy tắc dưới đây.

### 2.1. Các yêu cầu đã thực hiện

Số liệu minh chứng lấy bằng Trino ngày 29/10/2026. Mã nguồn nằm trong thư mục `pipeline/`.

| Mã | Chặng | Yêu cầu | Quy tắc xử lý | Minh chứng |
| --- | --- | --- | --- | --- |
| Y1 | Trước Bronze | Phản hồi API hợp lệ | Chỉ nhận HTTP 200 và JSON đọc được. Lỗi mạng, 5xx, JSON cắt cụt thử lại tối đa 8 lần. HTTP 429 chờ theo `Retry-After`. Lỗi 4xx khác thì dừng | `http_get` trong `ingest/common.py`; 0/6.740 bản ghi có HTTP khác 200 |
| Y2 | Trước Bronze | Đủ toạ độ, gán đúng tỉnh | Số đối tượng trả về phải bằng 15, nếu khác thì dừng. Gán tỉnh theo thứ tự toạ độ đã gửi | `ingest_open_meteo`; 15/15 tỉnh có số dòng bằng nhau |
| Y3 | Trước Bronze | Không vượt hạn mức API | Đếm lượt đã dùng trong ngày. Luồng định kỳ dừng ở 9.900, backfill dừng ở 9.400. Tự điều tốc dưới 4.000 lượt/giờ | `quota_used_today`, `ingest_open_meteo` |
| Y4 | Trước Bronze | Lấy đủ dữ liệu đến trễ | Mỗi lượt lấy lại 10 ngày với ERA5, 2 ngày với Historical Forecast và Air Quality | `LOOKBACK_DAYS` trong `orchestration.py`; đủ 100% số giờ ở cả 4 model |
| Y5 | Vào Bronze | Giữ bản gốc, truy vết được | Lưu nguyên JSON cùng `request_url`, `http_status`, khoảng ngày, `fetched_at`, `run_id`. Chỉ ghi thêm | `BRONZE_SCHEMA`; 6.740 bản ghi; xử lý lại bằng `bronze_to_silver.py --all` |
| Y6 | Trước Silver | Đúng phạm vi | Chỉ lấy bản ghi HTTP 200 thuộc 15 tỉnh trong cấu hình | `main` trong `spark/bronze_to_silver.py` |
| Y7 | Trước Silver | Chuẩn cấu trúc | Đọc JSON theo schema khai báo, chuyển mảng thành 1 dòng cho mỗi tỉnh, giờ, model. Bỏ hậu tố model khỏi tên trường | `parse_hourly`, `MAPPINGS`; 2 bảng Silver đều 12 cột, không null ở cột khoá |
| Y8 | Trước Silver | Chuẩn thời gian | `timestamp` theo UTC, `timestamp_local` theo UTC+7, `date_partition` là ngày theo giờ Việt Nam | `standardize`; 0 giá trị null ở cột thời gian |
| Y9 | Trước Silver | Chuẩn giá trị | Làm tròn 2 chữ số thập phân, `us_aqi` đổi sang số nguyên | `standardize` |
| Y10 | Trước Silver | Loại dòng không có dữ liệu | Loại dòng có mọi biến đo đều null (ERA5 chưa công bố, model chưa ra đời) hoặc không có thời gian | `classify_drops`; 0 giá trị null ở cột khoá |
| Y11 | Trước Silver | Loại giá trị tương lai | Loại dòng có `timestamp` > `fetched_at` | `classify_drops` |
| Y12 | Trước Silver | Xử lý thiếu một phần | Giữ dòng và giữ null, không nội suy và không điền giá trị | `standardize`; 18.525 ô null (0,193%) được giữ nguyên |
| Y13 | Trước Silver | Kiểm tra ngưỡng vật lý | Nhiệt độ −20…50 °C, độ ẩm 0…100%, mưa 0…300 mm/giờ, PM2.5 0…1000, PM10 0…2000, AQI 0…1000. Vi phạm thì giữ dòng, gắn `is_valid = false`, không đưa vào Gold | `RULES`, `changed_rows`; 0 dòng vi phạm ngưỡng vật lý |
| Y14 | Trước Silver | Loại trùng trong lô | Theo khoá tỉnh, giờ, model, giữ bản có `fetched_at` mới nhất | `latest_rows`; 0 trùng khoá ở mọi nguồn |
| Y15 | Vào Silver | Cập nhật không sinh trùng, bản mới thắng | `MERGE` theo khoá, chỉ cập nhật khi `fetched_at` của bản đưa vào không cũ hơn | `merge_into`; 0 trùng khoá sau nhiều lượt lấy lại |

### 2.2. Các yêu cầu kiểm tra chất lượng bổ sung

Cả 5 yêu cầu đã hoàn thành. Số liệu minh chứng lấy từ lần xử lý lại toàn bộ Bronze ngày 09/11/2026.

| Mã | Chặng | Yêu cầu | Quy tắc xử lý | Minh chứng | Trạng thái |
| --- | --- | --- | --- | --- | --- |
| C1 | Trước Bronze | Kiểm tra cấu trúc payload | Kiểm tra có `hourly.time`, đủ biến, các mảng đúng số giờ đã gọi, `hourly_units` đúng. Không đạt thì dừng, không ghi Bronze | `validate_payload` trong `ingest/common.py`; 3 kiểu lỗi cố ý (đổi tên trường, mảng ngắn, sai đơn vị) đều bị chặn ở cả 3 nguồn; các lượt lấy dữ liệu thật ngày 09/11/2026 đều qua được | Hoàn thành |
| C2 | Trước Silver | Kiểm tra quan hệ giữa các biến | Luật `pm2_5 <= pm10`. Vi phạm thì gắn `is_valid = false` | `pm25_not_above_pm10`; 55 dòng CAMS bị gắn không hợp lệ, lý do `pm2_5_above_pm10` | Hoàn thành |
| C3 | Trước Silver | Kiểm tra đột biến theo thời gian | So với giờ liền trước theo tỉnh và model, chênh quá 8 °C thì gắn không hợp lệ. Giờ đầu của lô lấy giờ trước đó từ Silver | `no_temperature_jump`, `add_previous_temperature`; 50 dòng GFS bị gắn không hợp lệ, ERA5 và ECMWF 0 dòng | Hoàn thành |
| C4 | Sau Silver | Tự động kiểm tra đủ giờ | Sau mỗi lượt, so số dòng thực tế với số giờ kỳ vọng × 15 tỉnh. Lệch thì ghi cảnh báo vào log và `silver.dq_run_log` | `check_completeness`; cả 4 model đủ 100% số giờ khi xử lý lại toàn bộ Bronze | Hoàn thành |
| C5 | Trước Silver | Ghi nhận dữ liệu bị loại | Cột `invalid_reason` ghi luật bị vi phạm. Bảng `silver.dq_run_log` ghi số dòng đọc, loại, ghi theo từng model và lượt chạy | `process`, `merge_into`; ví dụ ERA5 đọc 1.630.440 dòng, loại 38.160 dòng không có dữ liệu và 37.080 dòng trùng, ghi 1.555.200 dòng | Hoàn thành |
