import os
import pyarrow.csv as pv
import pyarrow.parquet as pq
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from google.cloud import storage
from schema import schema

default_args ={
    "owner": "airflow"
}

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
URL = 'https://github.com/ankurchavda/streamify/raw/main/dbt/seeds/songs.csv'
CSV_FILENAME = 'songs.csv'
PARQUET_FILENAME = CSV_FILENAME.replace('csv', 'parquet')
CSV_OUTFILE = f'{AIRFLOW_HOME}/{CSV_FILENAME}'
PARQUET_OUTFILE = f'{AIRFLOW_HOME}/{PARQUET_FILENAME}'
TABLE_NAME = 'songs'
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
GCP_GCS_BUCKET = os.environ.get('GCP_GCS_BUCKET')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'streamify_stg')

def convert_to_parquet(csv_file, parquet_file):
    if not csv_file.endswith('.csv'):
        raise ValueError("csv_file must end with .csv")
    
    table= pv.read_csv(csv_file)
    pq.write_table(table, parquet_file)

def upload_to_gcs(file_path, bucket_name, blob_name):
    """
    Upload file to Google Cloud Storage

    Parameters:
        file_path: str
        bucket_name: str
        blob_name: str
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)

    with DAG(
        dag_id = "load_songs_dag",
        default_args=default_args,
        description= "Execute only once to create songs table in bigquery",
        schedule_interval= "@once",
        start_date= datetime(2024,9,24),
        end_date= datetime(2026,9,24),
        catchup= True,
        tags=["streamify"]) as dag:

        download_songs_file_task = BashOperator(
            task_id="download_songs_csv",
            bash_command=f"curl -o {CSV_OUTFILE} {URL}"
        )

        download_convert_to_parquet = PythonOperator(
            task_id="convert_to_parquet",
            python_callable=convert_to_parquet,
            op_kwargs={"csv_file": CSV_OUTFILE, 
                        "parquet_file": PARQUET_OUTFILE}
        )

        upload_to_gcs_task = PythonOperator(
            task_id="upload_to_gcs",
            python_callable=upload_to_gcs,
            op_kwargs={"file_path": PARQUET_OUTFILE, 
                        "bucket_name": GCP_GCS_BUCKET,
                        "blob_name": f"{TABLE_NAME}/{PARQUET_FILENAME}"}
        )

        remove_files_from_local_task = BashOperator(
            task_id="remove_files_from_local",
            bash_command=f"rm {CSV_OUTFILE} {PARQUET_OUTFILE}"
        )

        create_external_table_task = BigQueryCreateExternalTableOperator(
            task_id="ceate+external_table",
            table_resource = {
                "tableReference": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": TABLE_NAME
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{GCP_GCS_BUCKET}/{TABLE_NAME}/*.parquet"]
                }
            }
        )

        download_songs_file_task >> convert_to_parquet >> upload_to_gcs_task >> remove_files_from_local_task >> create_external_table_task
    