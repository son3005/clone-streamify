# STREAMIFY LOCAL OFFLINE - ROADMAP & CHECKLIST

> **Vai trò trong dự án:**
> - **Bạn (Học viên / Kỹ sư chính):** Tự tay viết code, sửa cấu hình, chạy lệnh và gỡ lỗi (debug).
> - **Antigravity (Mentor / Người hướng dẫn):** Đưa ra yêu cầu, giải thích bản chất kiến trúc, gợi ý hướng giải quyết, rà soát (code review) và đánh giá kết quả của bạn.

---

## Kiến trúc hệ thống Local (100% Offline - 0đ chi phí)

```
[Eventsim Container]
        │ (Sinh sự kiện nghe nhạc giả lập)
        ▼
[Kafka Broker (Docker Local :9092)]
        │ (Structured Streaming)
        ▼
[PySpark Script (Local)]
        │ (Ghi định dạng Parquet)
        ▼
[Thư mục Local data_lake/] (Thay thế GCS Bucket)
        │ (Airflow định kỳ nạp dữ liệu)
        ▼
[PostgreSQL Database (Docker :5432)] (Thay thế BigQuery)
        │ (dbt transform sang Data Marts)
        ▼
[Bảng Fact & Dimension]
        │ (Kết nối SQL)
        ▼
[Metabase Dashboard (Docker :3000)] (Thay thế Looker Studio)
```

---

## GIAI ĐOẠN 1: Chuẩn bị Hạ tầng Docker Local
*Mục tiêu: Dựng các container Zookeeper, Kafka, PostgreSQL và Metabase hoạt động trơn tru trên cùng mạng Docker mà không ngốn quá 8GB RAM.*

- [ ] **1.1. Cấu hình WSL2 an toàn RAM**
  - Đã lưu file `C:\Users\ADMIN\.wslconfig` với cấu hình giới hạn 8GB RAM, 4GB Swap, 8 Cores.
  - Đã chạy lệnh `wsl --shutdown` và khởi động lại Docker Desktop.
- [ ] **1.2. Tạo file `docker-compose.local.yml` ở thư mục gốc**
  - Khai báo service `zookeeper` (port 2181, giới hạn RAM heap ~256M-512M).
  - Khai báo service `kafka` (port 9092, cấu hình listener cho localhost, heap ~1G).
  - Khai báo service `postgres` (port 5432, tạo sẵn database tên `streamify`, user/password an toàn, mount volume lưu dữ liệu).
  - Khai báo service `metabase` (port 3000, kết nối được tới postgres).
- [ ] **1.3. Khởi động và kiểm tra dịch vụ**
  - Chạy `docker compose -f docker-compose.local.yml up -d`.
  - Kiểm tra trạng thái các container bằng `docker ps`.
  - Thử kết nối vào Postgres và kiểm tra Kafka port 9092 đang lắng nghe.

---

## GIAI ĐOẠN 2: Sửa Code Spark Streaming & Ghi Local Data Lake
*Mục tiêu: Sửa các lỗi chính tả có sẵn trong code Spark và chuyển đích ghi từ GCS (`gs://`) về thư mục local (`data_lake/`).*

- [ ] **2.1. Rà soát và sửa lỗi trong `spark_streaming/streaming_fuctions.py`**
  - Tìm và sửa lỗi chính tả ở dòng import thư viện (`pysqark` -> `pyspark`, `ufd` -> `udf`).
  - Sửa tham số khởi tạo Spark Session: chuyển `master="yarn"` thành `master="local[*]"`.
  - Sửa lỗi chính tả hàm phân vùng (`.partionBy` -> `.partitionBy`).
- [ ] **2.2. Điều chỉnh đích ghi trong `spark_streaming/streaming_all_events.py`**
  - Tạo thư mục `data_lake/` trong project để chứa dữ liệu.
  - Thay thế biến `GCS_STORAGE_PATH = f"gs://{GCP_GCS_BUCKET}"` bằng đường dẫn thư mục tuyệt đối tới `data_lake/`.
  - Kiểm tra cấu hình `checkpointLocation` trỏ đúng vào thư mục con `data_lake/checkpoint/`.
- [ ] **2.3. Khởi động Eventsim sinh dữ liệu vào Kafka**
  - Build hoặc chạy container Eventsim theo cấu hình trong `scripts/exec_commands.sh`.
  - Xác nhận Eventsim đang bắn message vào topic `listen_events`, `page_view_events`, `auth_events`.
- [ ] **2.4. Chạy PySpark và nghiệm thu dữ liệu đầu ra**
  - Chạy script `streaming_all_events.py` (với kafka package phù hợp).
  - Kiểm tra trong thư mục `data_lake/listen_events/` có sinh ra các thư mục phân vùng `month=.../day=.../hour=.../` chứa các file `.parquet` hay không.

---

## GIAI ĐOẠN 3: Xây dựng Data Warehouse với PostgreSQL & dbt
*Mục tiêu: Thiết lập dbt kết nối tới PostgreSQL thay vì BigQuery, chạy các mô hình biến đổi dữ liệu.*

- [ ] **3.1. Cài đặt adapter `dbt-postgres`**
  - Cài đặt thư viện `dbt-postgres` vào môi trường Python.
- [ ] **3.2. Cập nhật file cấu hình kết nối `dbt/profiles.yml`**
  - Chuyển `type` từ `bigquery` sang `postgres`.
  - Điền thông tin host (`localhost`), port (`5432`), user, password, dbname (`streamify`), schema (`streamify_stg`, `streamify_prod`).
  - Kiểm tra kết nối bằng lệnh `dbt debug --project-dir dbt`.
- [ ] **3.3. Rà soát và điều chỉnh các model SQL trong `dbt/models/`**
  - Kiểm tra các hàm ngày tháng hoặc cú pháp riêng của BigQuery trong các file SQL (ví dụ: `TIMESTAMP_MICROS`, `PARSE_DATETIME`...) và chuyển đổi sang chuẩn cú pháp PostgreSQL nếu cần.
- [ ] **3.4. Chạy và kiểm tra dbt**
  - Chạy `dbt seed` để nạp dữ liệu từ điển mã bang/quốc gia.
  - Chạy `dbt run` để tạo các bảng staging, dim và fact.
  - Chạy `dbt test` để kiểm tra tính toàn vẹn dữ liệu.

---

## GIAI ĐOẠN 4: Tự động hóa Pipeline với Airflow
*Mục tiêu: Thiết lập DAG định kỳ quét dữ liệu Parquet từ `data_lake/`, nạp vào Postgres và trigger dbt run.*

- [ ] **4.1. Khởi động Airflow Local**
  - Cấu hình file `airflow/docker-compose.yml` chạy ở chế độ nhẹ (LocalExecutor).
  - Cài đặt các thư viện cần thiết trong container Airflow (pandas, pyarrow, dbt-postgres...).
- [ ] **4.2. Viết Task nạp dữ liệu Parquet vào PostgreSQL**
  - Tạo hàm hoặc Operator đọc các file Parquet mới sinh ra trong thư mục `data_lake/`.
  - Nạp dữ liệu vào bảng Staging của PostgreSQL.
- [ ] **4.3. Cập nhật `streamify_dag.py`**
  - Xóa các task phụ thuộc vào GCP/BigQuery.
  - Nối luồng: `Task nạp Parquet vào Postgres` -> `Task dbt run` -> `Task dbt test`.
  - Kích hoạt DAG trên giao diện Airflow Webserver (`localhost:8080`) và theo dõi pipeline chạy thành công (màu xanh).

---

## GIAI ĐOẠN 5: Trực quan hóa dữ liệu với Metabase Dashboard
*Mục tiêu: Dựng các biểu đồ phân tích hành vi nghe nhạc tương tự Looker Studio.*

- [ ] **5.1. Thiết lập Metabase ban đầu**
  - Truy cập `http://localhost:3000`, hoàn tất các bước đăng ký admin ban đầu.
  - Thêm nguồn dữ liệu (Database): Chọn PostgreSQL và kết nối tới database `streamify`.
- [ ] **5.2. Xây dựng các biểu đồ phân tích chính**
  - Biểu đồ 1: Top 10 bài hát và ca sĩ được nghe nhiều nhất trong ngày.
  - Biểu đồ 2: Lưu lượng người nghe theo từng khung giờ trong ngày (Line chart).
  - Biểu đồ 3: Phân bố người dùng theo vị trí địa lý hoặc cấp độ tài khoản (Free vs Paid).
- [ ] **5.3. Hoàn thiện Dashboard tổng hợp**
  - Ghép các biểu đồ vào một Dashboard hoàn chỉnh.
  - Chụp ảnh màn hình lưu lại vào repo để làm minh chứng năng lực thực chiến trong CV / Portfolio.
