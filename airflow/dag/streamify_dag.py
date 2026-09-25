import os
import glob
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator




EVENTS = ["listen_events", "page_view_events", "auth_events"]
DATA_LAKE_PATH = "/opt/airflow/data_lake"



def ingest_data_lake_to_postgres():
    """
    Đọc tất cả file Parquet từ Local Data Lake và nạp vào staging schema (streamify_stg) trong PostgreSQL.
    """
    pg_user = os.getenv("POSTGRES_USER", "streamify_user")
    pg_pass = os.getenv("POSTGRES_PASSWORD", "streamify_password")
    pg_host = os.getenv("POSTGRES_HOST", "clone-streamify-postgres-1")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    pg_db = os.getenv("POSTGRES_DB", "streamify")
    db_url = f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    engine = create_engine(db_url)
    # Đảm bảo schema staging luôn tồn tại
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS streamify_stg;"))
    total_rows = 0
    for event in EVENTS:
        files = glob.glob(f"{DATA_LAKE_PATH}/{event}/**/*.parquet", recursive=True)
        valid_files = [f for f in files if os.path.getsize(f) > 0]
        if not valid_files:
            print(f"[INFO] Không tìm thấy file Parquet hợp lệ cho: {event}")
            continue
        print(f"[INFO] Tìm thấy {len(valid_files)} file Parquet cho: {event}")
        df_list = [pd.read_parquet(f) for f in valid_files]
        df = pd.concat(df_list, ignore_index=True)
        df.columns = [c.lower() for c in df.columns]
        # Nạp vào bảng staging tương ứng (ghi đè để cập nhật bản mới nhất)
        df.to_sql(name=event, con=engine, schema="streamify_stg", if_exists="replace", index=False)
        print(f"[SUCCESS] Đã nạp {len(df)} dòng vào streamify_stg.{event}")
        total_rows += len(df)
    print(f"[DONE] Hoàn tất nạp Data Lake vào PostgreSQL Staging. Tổng cộng: {total_rows} dòng.")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id = f"streamify_dag",
    default_args = default_args,
    description= "Local pipeline: Data Lake (Parquet) -> Postgres Staging -> dbt Data Warehouse",
    schedule_interval= "5 * * * *",
    start_date= datetime(2024,9,24,18),
    catchup = False,
    max_active_runs=1,
    tags = ["streamify","local"]
) as dag:
    # 1. Import Data from Data Lake to Postgres Staging
    initate_task = PythonOperator(
        task_id = "db_initiate",
        python_callable=ingest_data_lake_to_postgres  
    )

    # 2. Load seed files into Data Warehouse
    execute_dbt_task = BashOperator(
        task_id = "dbt_streamify_run",
        bash_command = "dbt seed --select state_codes --project-dir /dbt --profiles-dir /dbt --target prod"
    )

    # 3. Run 7 models dbt 
    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --project-dir /dbt --profiles-dir /dbt --target prod"
    )

    # 4. Test
    dbt_test_task = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --project-dir /dbt --profiles-dir /dbt --target prod"
    )

    initate_task >> execute_dbt_task >> dbt_run_task >> dbt_test_task