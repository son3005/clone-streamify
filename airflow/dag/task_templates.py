from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
    BigQueryDeleteTableOperator
    )

def create_external_table(event,
                            gcp_project_id,
                            bigquery_dataset,
                            external_table_name,
                            gcp_gcs_bucket,
                            events_path):
    """
    Create an texternal table using the BigQueryCreateExternalTableOperator

    Parameters :
        event: ("listen_events", "page_view_events", "auth_events")
        gcp_project_id: str
        bigquery_dataset: str
        external_table_name: str
        gcp_gcs_bucket: str
        events_path: str

    Returns:
        task: BigQueryCreateExternalTableOperator
    """
    task = BigQueryCreateExternalTableOperator(
        task_id = f"{event}_create_external_table",
        table_resource = {
            "tableReference": {
                "projectId": gcp_project_id,
                "datasetId": bigquery_dataset,
                "tableId": f"{external_table_name}"
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{gcp_gcs_bucket}/{events_path}/*"],
            },
        }
    )
    
    return task

def create_empty_table(event,
                        gcp_project_id,
                        bigquery_dataset,
                        bigquery_table_name,
                        events_schema):
    """
    Create an empty table in BigQuery using BigQueryCreateEmptyTabelOperator

    Paramaters:
        event: ("listen_events", "page_view_events", "auth_events")
        gcp_project_id: str
        bigquery_dataset: str
        bigquery_table_name: str
        events_schema: str
    
    Returns:
        task: BigQueryCreateEmptyTableOperator
    """
    task = BigQueryCreateEmptyTableOperator(
        task_id = f"{event}_create_empty_table",
        project_id = gcp_project_id,
        dataset_id = bigquery_dataset,
        table_id = bigquery_table_name,
        schema_fields = events_schema,
        time_partitioning = {
            "type" : "HOUR",
            "field": "ts"
        },
        exists_ok = True
    )

    return task
   
def insert_job(event,
                insert_query_location,
                bigquery_dataset,
                gcp_project_id,
                timeout=300000):
    """
    Run the insert query using BigQueryInsertJobOperator

    Parameters:
        event: ("listen_events", "page_view_events", "auth_events")
        insert_query_location: str
        bigquery_dataset: str
        gcp_project_id: str
        timeout: int
    
    Returns:
        task: BigQueryInsertJobOperator
    """
    task = BigQueryInsertJobOperator(
        task_id = f"{event}_execute_imsert_query",
        configuration = {
            "query": {
                "query": insert_query_location,
                "useLegacySql": False,
            },
            "timeoutMs": timeout,
            "defaultDataset": {
                "datasetId": bigquery_dataset,
                "projectId": gcp_project_id
            }
        }
    )
    
    return task

def delete_external_table(event,
                            gcp_project_id,
                            bigquery_dataset,
                            external_table_name):
    """
    Delete table from BigQuery using BigQueryDeleteTableOperator

    Parameters:
        gcp_project_id: str
        bigquery_dataset: str
        external_table_name: str

    Returns: 
        task: BigQueryDeleteTableOperator
    """
    task = BigQueryDeleteTableOperator(
        task_id = f"{event}_delete_external_table",
        deletion_dataset_table = f"{gcp_project_id}.{bigquery_dataset}.{external_table_name}",
        ignore_if_missing = True
    )
    
    return task
    
    
    
    