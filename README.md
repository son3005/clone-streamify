# 🎵 Streamify: End-to-End Real-Time & Batch Data Engineering Pipeline (100% Local Setup)

[![Python](https://img.shields.io/badge/Python-3.10%20%7C%203.12-blue?logo=python)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-2.13-red?logo=apachekafka)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5.3-orange?logo=apachespark)](https://spark.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)](https://www.docker.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql)](https://www.postgresql.org/)
[![dbt](https://img.shields.io/badge/dbt-1.12.5-FF694B?logo=dbt)](https://www.getdbt.com/)
[![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-2.8.1-017CEE?logo=apacheairflow)](https://airflow.apache.org/)
[![Metabase](https://img.shields.io/badge/Metabase-BI_Dashboard-509EE3?logo=metabase)](https://www.metabase.com/)

---

## 📖 Introduction & Project Overview

**Streamify** is an enterprise-grade, end-to-end data platform that simulates real-time music streaming event ingestion, processing, warehousing, and analytics (inspired by Spotify).

### 🌟 100% Local & Zero-Cost Architecture
The original project was architected for Google Cloud Platform (GCS, Dataproc, BigQuery, and Looker Studio). This repository is a **complete re-engineering** into a **100% Local, Offline Data Lakehouse & Warehouse** running smoothly on a standard personal workstation (Core i5, 16GB RAM) at **zero cloud cost**:
- **GCS Bucket** ➡️ **Local Partitioned Parquet Data Lake (`data_lake/`)**
- **Dataproc (Cloud Spark)** ➡️ **PySpark Structured Streaming (Local Engine)**
- **Google BigQuery** ➡️ **PostgreSQL 15 (Docker Data Warehouse)**
- **Looker Studio** ➡️ **Metabase Local Analytics Platform**
- **Cloud Composer** ➡️ **Lightweight Apache Airflow 2.8 (`LocalExecutor`)**

---

## 🏛️ System Architecture

```
                                      [Eventsim (Docker)]
                                               │
                                 (Simulated Music Events: Listen,
                                    Page Views, Auth Events)
                                               ▼
                                 [Apache Kafka Broker (:9092)]
                                               │
                                    (Real-Time Streaming)
                                               ▼
                             [PySpark Structured Streaming (3.5.3)]
                                               │
                                 (Micro-batch Partitioning)
                                               ▼
                       ┌───────────────────────────────────────────────┐
                       │   Local Data Lake: data_lake/                 │
                       │   └── listen_events/month=*/day=*/hour=*/*.parquet
                       └───────────────────────┬───────────────────────┘
                                               │
                               (Scheduled Hourly Trigger)
                                               ▼
                               [Apache Airflow 2.8 (:8080)]
                               (LocalExecutor - Orchestrator)
                                               │
                       ┌───────────────────────┴───────────────────────┐
                       │                                               │
                 [Task 1: Ingest]                                [Task 2: dbt]
            Read Parquet -> Staging Schema               dbt seed -> dbt run -> dbt test
                       │                                               │
                       └───────────────────────┬───────────────────────┘
                                               │
                                               ▼
                                [PostgreSQL 15 Container (:5432)]
                                ├── streamify_stg  (Staging Tables)
                                └── streamify_prod (Star Schema Warehouse)
                                               │
                                               ▼
                                 [Metabase BI Dashboard (:3000)]
                              (Interactive Analytics & KPI Metrics)
```

---

## 🗄️ Data Modeling (Star Schema & Data Marts)

The Data Warehouse is modeled following **Kimball's Dimensional Modeling** approach:

```
                            ┌────────────────────────┐
                            │      dim_datetime      │
                            └───────────┬────────────┘
                                        │
┌────────────────────────┐              │              ┌────────────────────────┐
│       dim_users        ├──────────────┼──────────────┤       dim_songs        │
└────────────────────────┘              │              └────────────────────────┘
                                        ▼
                            ┌────────────────────────┐
                            │      fact_streams      │
                            └───────────┬────────────┘
                                        │
┌────────────────────────┐              │              ┌────────────────────────┐
│      dim_location      ├──────────────┴──────────────┤      dim_artists       │
└────────────────────────┘                             └────────────────────────┘
                                        │
                                        ▼
                            ┌────────────────────────┐
                            │      wide_stream       │ (Analytical Mart View)
                            └────────────────────────┘
```

- **Fact Table**: `fact_streams` (records every individual song stream event with surrogate foreign keys and timestamps).
- **Dimension Tables**: 
  - `dim_users`: SCD tracking user attributes (`userId`, `firstName`, `lastName`, `gender`, `level`).
  - `dim_songs`: Song metadata (`title`, `duration`, `tempo`).
  - `dim_artists`: Artist details and geographical origins.
  - `dim_datetime`: Granular time intelligence (hour, day, week, month, year).
  - `dim_location`: Geographic location (city, state, coordinates).
- **Data Mart**: `wide_stream` (denormalized, high-performance view ready for BI reporting).

---

## 📁 Repository Structure

```plaintext
clone-Streamify/
├── airflow/
│   ├── dag/
│   │   ├── streamify_dag.py        # Airflow DAG (Local Parquet Ingestion + dbt Run)
│   │   └── dbt_test_dag.py         # Diagnostic DAG
│   ├── docker-compose.yml          # Airflow services (Webserver, Scheduler, Postgres)
│   ├── Dockerfile                  # Custom Airflow image with dbt & data dependencies
│   ├── requirements.txt            # Python packages for Airflow
│   └── .env                        # Airflow environment credentials
├── data_lake/                      # Local Data Lake partitioned by month/day/hour (Parquet)
├── dbt/
│   ├── models/
│   │   ├── core/                   # Dimension & Fact SQL models (PostgreSQL dialect)
│   │   │   ├── dim_artists.sql
│   │   │   ├── dim_datetime.sql
│   │   │   ├── dim_location.sql
│   │   │   ├── dim_songs.sql
│   │   │   ├── dim_users.sql
│   │   │   ├── fact_streams.sql
│   │   │   └── wide_stream.sql
│   ├── seeds/                      # Static reference datasets (state_codes.csv, songs.csv)
│   ├── dbt_project.yml             # dbt project configurations
│   ├── packages.yml                # dbt packages (dbt-utils 1.3.0)
│   └── profiles.yml                # Database connection profiles (reads from .env)
├── eventsim/                       # Synthetic music event generator container
├── scripts/
│   └── load_lake_to_postgres.py    # Standalone ingestion utility
├── spark_streaming/
│   ├── streaming_all_events.py     # Main PySpark Kafka streaming application
│   └── streaming_fuctions.py       # Spark streaming schema definitions and helpers
├── docker-compose.yml              # Core infrastructure: Zookeeper, Kafka, Postgres, Metabase
├── requirements.txt                # Local Python dependencies
├── ROADMAP_LOCAL_STREAMIFY.md      # Detailed progress tracker across 5 phases
└── README.md                       # Project documentation
```

---

## ⚙️ Prerequisites & Environment Setup

- **OS**: Windows 10/11 with WSL2 enabled (Ubuntu)
- **Hardware**: Minimum 16GB RAM recommended.
- **Docker & Docker Compose**: Docker Desktop with WSL2 backend.
- **Python**: Python 3.10+ (or Conda environment).
- **Java**: OpenJDK 17 (`JAVA_HOME` configured).
- **Hadoop Winutils**: Hadoop 3.3.6 winutils (`HADOOP_HOME` configured).

### 1. Configure WSL2 RAM Safety
Create/edit `C:\Users\<Your_User>\.wslconfig` to prevent WSL2 from exhausting host memory:
```ini
[wsl2]
memory=8GB
processors=8
swap=4GB
```
Apply via PowerShell: `wsl --shutdown`.

### 2. Configure Environment Variables
Create `.env` in the project root:
```env
POSTGRES_USER=streamify_user
POSTGRES_PASSWORD=streamify_password
POSTGRES_DB=streamify
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
```

---

## 🚀 Quickstart Guide (Step-by-Step)

### Step 1: Start Core Infrastructure (Kafka, Postgres, Metabase)
```powershell
docker compose up -d
```
*Verify containers are running:*
- Zookeeper: `localhost:2181`
- Kafka Broker: `localhost:9092`
- PostgreSQL: `localhost:5432`
- Metabase: `http://localhost:3000`

---

### Step 2: Launch Real-Time PySpark Streaming
In Terminal 1, run the streaming job:
```powershell
python spark_streaming/streaming_all_events.py
```
*Spark consumes events from Kafka topics (`listen_events`, `page_view_events`, `auth_events`) and continuously writes partitioned `.parquet` files into `data_lake/`.*

---

### Step 3: Run Eventsim to Stream Music Events
In Terminal 2, start the event simulator:
```powershell
docker run -it --rm --network clone-streamify_streamify-network \
  --name eventsim events:1.0 \
  -c "examples/example-config.json" \
  --start-time "2026-09-25T00:00:00" \
  --end-time "2026-09-25T23:59:59" \
  --npeople 1000 \
  --kafkaBrokerList kafka:29092 \
  --continuous
```

---

### Step 4: Start Apache Airflow Orchestration
In Terminal 3, initialize and launch Airflow:
```powershell
docker compose -f airflow/docker-compose.yml up -d
```
- Access Airflow UI at: **`http://localhost:8080`**
- Credentials: `airflow` / `airflow`

---

### Step 5: Trigger Airflow Pipeline
1. In Airflow UI, find **`streamify_dag`**.
2. Toggle the DAG switch to **On**.
3. Click the **Trigger DAG** (▶️) button.
4. Watch all 4 pipeline stages turn **green (Success)**:
   - `db_initiate`: Reads Parquet files from Local Data Lake into `streamify_stg`.
   - `dbt_streamify_run`: Seeds dimensional lookup data (`state_codes`).
   - `dbt_run`: Executes 7 dbt models transforming raw events into Star Schema Data Warehouse.
   - `dbt_test`: Executes data integrity and schema validation tests.

---

### Step 6: Explore BI Dashboard with Metabase
1. Open **`http://localhost:3000`**.
2. Add PostgreSQL Database:
   - **Host**: `clone-streamify-postgres-1`
   - **Port**: `5432`
   - **Database**: `streamify`
   - **User / Password**: `streamify_user` / `streamify_password`
3. Query the analytical mart: `streamify_prod.wide_stream`.
4. Build interactive charts:
   - **Top 10 Songs Streamed** (Horizontal Bar Chart)
   - **User Level Distribution** (Free vs Paid Subscribers - Donut Chart)
   - **Streaming Volume by Hour** (Line Chart)
   - **Top Artists by Streams** (Bar Chart)
   - **Key Performance Indicators (KPIs)**: Total Streams & Distinct Active Users.

---

## 🛠️ Key Technical Challenges & Engineering Solutions

1. **GCP to Local Dialect Migration**:
   - Replaced BigQuery-specific functions (`FORMAT_DATE`, `GENERATE_ARRAY`, `SAFE_CAST`) with PostgreSQL native equivalents (`generate_series`, `EXTRACT(EPOCH FROM ...)`, explicit typed casts).
   - Replaced deprecated `dbt_utils.surrogate_key` with modern `dbt_utils.generate_surrogate_key`.
2. **Lightweight Orchestration (Resource Optimization)**:
   - Replaced memory-heavy CeleryExecutor (Redis + Flower + Workers) with `LocalExecutor`, saving **>3GB RAM** and eliminating background overhead.
3. **Cross-Container Networking**:
   - Bridged independent Docker Compose stacks (`clone-streamify_streamify-network`) allowing Airflow, Kafka, Postgres, and Metabase to communicate using Docker internal service DNS.
4. **Resilient Data Ingestion**:
   - Implemented an idempotent Python ingestion task in Airflow using `pyarrow` and `sqlalchemy.engine.begin()` to guarantee atomic transactions when loading high-volume Parquet files.

---

## 👤 Author & Acknowledgments

- **Author**: Data Engineering Practitioner
- **Inspiration**: Based on the GCP Streamify architecture by Ankur Chavda. Re-engineered into a 100% Local, Zero-Cost Data Engineering Platform.
