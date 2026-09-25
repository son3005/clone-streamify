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

## GIAI ĐOẠN 1: Chuẩn bị Hạ tầng Docker Local (HOÀN THÀNH ✅)
*Mục tiêu: Dựng các container Zookeeper, Kafka, PostgreSQL và Metabase hoạt động trơn tru trên cùng mạng Docker mà không ngốn quá 8GB RAM.*

- [x] **1.1. Cấu hình WSL2 an toàn RAM**
  - Đã lưu file `C:\Users\ADMIN\.wslconfig` với cấu hình giới hạn 8GB RAM, 4GB Swap, 8 Cores.
  - Đã chạy lệnh `wsl --shutdown` và khởi động lại Docker Desktop.
- [x] **1.2. Tạo file `docker-compose.yml` ở thư mục gốc**
  - Khai báo service `zookeeper` (port 2181, healthcheck active).
  - Khai báo service `kafka` (port 9092, listeners internal 29092 và external 9092).
  - Khai báo service `postgres` (port 5432, database `streamify`, user/password `streamify`, volume `postgres_data`).
  - Khai báo service `metabase` (port 3000, kết nối được tới postgres).
- [x] **1.3. Khởi động và kiểm tra dịch vụ**
  - Đã chạy `docker compose up -d` và tất cả 4 services hoạt động ổn định.
  - Đã kết nối Metabase thành công tới PostgreSQL container.

---

## GIAI ĐOẠN 2: Sửa Code Spark Streaming & Ghi Local Data Lake (HOÀN THÀNH ✅)
*Mục tiêu: Sửa các lỗi chính tả có sẵn trong code Spark và chuyển đích ghi từ GCS (`gs://`) về thư mục local (`data_lake/`).*

- [x] **2.1. Rà soát và sửa lỗi trong `spark_streaming/streaming_fuctions.py`**
  - Đã sửa lỗi import thư viện (`pysqark` -> `pyspark`, `ufd` -> `udf`).
  - Đã chuyển `master="yarn"` thành `master="local[*]"`.
  - Đã sửa lỗi chính tả hàm phân vùng (`.partionBy` -> `.partitionBy`).
  - Bỏ qua hàm Python UDF `string_decode` để Spark xử lý 100% JVM native, tối ưu tốc độ và tương thích Python 3.12 trên Windows.
- [x] **2.2. Điều chỉnh đích ghi trong `spark_streaming/streaming_all_events.py`**
  - Đã tạo thư mục `data_lake/` trong project để chứa dữ liệu.
  - Đã thay thế `gs://` bằng đường dẫn local `E:/Learn/clone-Streamify/data_lake`.
  - Đã cấu hình `JAVA_HOME` (OpenJDK 17), `HADOOP_HOME` (Winutils 3.3.6), `PYSPARK_PYTHON`.
  - Cấu hình `checkpointLocation` trỏ đúng vào thư mục con `data_lake/checkpoint/`.
- [x] **2.3. Khởi động Eventsim sinh dữ liệu vào Kafka**
  - Build image `events:1.0`, fix lỗi CRLF và hạ heap `-Xmx512m` trong `eventsim.sh`.
  - Container `eventsim` kết nối vào mạng `clone-streamify_streamify-network` và đang đẩy message liên tục vào các topic `listen_events`, `page_view_events`, `auth_events`.
- [x] **2.4. Chạy PySpark và nghiệm thu dữ liệu đầu ra**
  - Chạy `streaming_all_events.py` với connector `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3`.
  - Đã kiểm tra và nghiệm thu: các thư mục phân vùng `month=9/day=25/hour=.../` trong `data_lake/listen_events/`, `auth_events/`, `page_view_events/` đã sinh ra đầy đủ các file `.parquet` chuẩn xác.

---

## GIAI ĐOẠN 3: Xây dựng Data Warehouse với PostgreSQL & dbt (HOÀN THÀNH ✅)
*Mục tiêu: Thiết lập dbt kết nối tới PostgreSQL thay vì BigQuery, chạy các mô hình biến đổi dữ liệu.*

- [x] **3.1. Cài đặt adapter `dbt-postgres`**
  - Đã cài đặt `dbt-postgres` và `dbt-core` 1.12.5.
- [x] **3.2. Cập nhật file cấu hình kết nối `dbt/profiles.yml`**
  - Đã cấu hình adapter `postgres`, đọc credentials an toàn từ `.env` qua `{{ env_var(...) }}`.
  - Đã chạy `dbt debug` thành công: `[OK connection ok]`.
- [x] **3.3. Rà soát và điều chỉnh các model SQL trong `dbt/models/`**
  - Cập nhật `dbt_utils.generate_surrogate_key` thay thế macro cũ đã bị khai tử.
  - Chuyển đổi toàn bộ cú pháp BigQuery sang PostgreSQL: `generate_series`, `EXTRACT(EPOCH FROM ...)`, ngoặc kép định danh case-sensitive, sửa lỗi chính tả alias subqueries.
  - Sửa lỗi thiếu cột `registration` trong `dim_users.sql` của dự án gốc.
- [x] **3.4. Chạy và kiểm tra dbt**
  - Đã chạy `dbt seed` nạp thành công 57 dòng `state_codes` và 10.000 bài hát `songs`.
  - Đã nạp dữ liệu thực tế từ `data_lake/` vào schema `streamify_stg` (`listen_events`, `page_view_events`, `auth_events`).
  - Đã chạy `dbt run` thành công 100% cả 7 models (`PASS=7`, 0 errors, 0 warnings) tạo đầy đủ các bảng Dimension, Fact và View Wide Stream.

---

## GIAI ĐOẠN 4: Tự động hóa Pipeline với Airflow (HOÀN THÀNH ✅)
*Mục tiêu: Thiết lập DAG định kỳ quét dữ liệu Parquet từ `data_lake/`, nạp vào Postgres và trigger dbt run.*

- [x] **4.1. Khởi động Airflow Local**
  - Cấu hình file `airflow/docker-compose.yml` chạy ở chế độ nhẹ (LocalExecutor), tiết kiệm ~3GB RAM.
  - Tích hợp các thư viện cần thiết trong image Airflow: pandas, pyarrow, dbt-postgres, sqlalchemy, psycopg2-binary.
  - Khởi động thành công `airflow-webserver` (:8080), `airflow-scheduler`, `airflow-postgres`.
- [x] **4.2. Viết Task nạp dữ liệu Parquet vào PostgreSQL**
  - Viết PythonOperator đọc toàn bộ file Parquet từ thư mục `/opt/airflow/data_lake` (`listen_events`, `page_view_events`, `auth_events`).
  - Nạp dữ liệu vào schema staging `streamify_stg` trên PostgreSQL data warehouse.
- [x] **4.3. Cập nhật `streamify_dag.py` & Kiểm tra thành công**
  - Thay thế toàn bộ tác vụ GCP/BigQuery bằng Local pipeline: `db_initiate` -> `dbt_streamify_run` -> `dbt_run` -> `dbt_test`.
  - Kích hoạt DAG trên giao diện Airflow Webserver (`localhost:8080`).
  - Toàn bộ 4 task đã chạy thành công rực rỡ (màu xanh lá cây 100%).

---

## GIAI ĐOẠN 5: Trực quan hóa dữ liệu với Metabase Dashboard (HOÀN THÀNH ✅)
*Mục tiêu: Dựng các biểu đồ phân tích hành vi nghe nhạc tương tự Looker Studio.*

- [x] **5.1. Thiết lập Metabase ban đầu**
  - Truy cập `http://localhost:3000`, thiết lập tài khoản admin.
  - Kết nối thành công nguồn dữ liệu PostgreSQL (`streamify`, schema `streamify_prod`, `streamify_stg`).
- [x] **5.2. Xây dựng các biểu đồ phân tích chính**
  - Đã truy vấn thành công bảng Data Mart `streamify_prod.wide_stream` (>3.400 dòng stream thực tế).
  - Biểu đồ Top 10 bài hát nghe nhiều nhất (Bar chart).
  - Phân bố tài khoản Free vs Paid (Pie/Donut chart).
  - Xu hướng nghe nhạc theo thời gian (Line chart).
  - Top 10 nghệ sĩ phổ biến nhất & Thẻ chỉ số tổng quan (KPI Cards).
- [x] **5.3. Hoàn thiện Dashboard tổng hợp**
  - Ghép các biểu đồ vào Dashboard tổng thể `Streamify Analytics Dashboard`.
  - Thiết lập làm mới dữ liệu và nghiệm thu toàn bộ hệ sinh thái Data Engineering End-to-End hoạt động mượt mà 100% Offline.
