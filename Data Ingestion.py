from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.local_filesystem import (
    LocalFilesystemToGcsOperator,
)
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 1, 1),
    'retries': 0
}

dag = DAG(
    'ingest_local_files_to_bigquery',
    default_args=default_args,
    schedule_interval='@daily'
)

# Define the Google Cloud Storage bucket and dataset
bucket_name = 'your-gcs-bucket-name'
dataset_id = 'your-bigquery-dataset-id'

# Define the local file paths
local_file_paths = [
    '/path/to/local/file1.csv',
    '/path/to/local/file2.csv',
    '/path/to/local/file3.csv',
    '/path/to/local/file4.csv',
    '/path/to/local/file5.csv',
    '/path/to/local/file6.csv',
    '/path/to/local/file7.csv',
    '/path/to/local/file8.csv',
    '/path/to/local/file9.csv',
    '/path/to/local/file10.csv'
]

# Define the GCS file paths
gcs_file_paths = [
    f'gs://{bucket_name}/file1.csv',
    f'gs://{bucket_name}/file2.csv',
    f'gs://{bucket_name}/file3.csv',
    f'gs://{bucket_name}/file4.csv',
    f'gs://{bucket_name}/file5.csv',
    f'gs://{bucket_name}/file6.csv',
    f'gs://{bucket_name}/file7.csv',
    f'gs://{bucket_name}/file8.csv',
    f'gs://{bucket_name}/file9.csv',
    f'gs://{bucket_name}/file10.csv'
]

# Create the GCS bucket if it doesn't exist
create_gcs_bucket = BashOperator(
    task_id='create_gcs_bucket',
    bash_command=f'gsutil mb -c regional -l us-central1 gs://{bucket_name}',
    dag=dag
)

# Copy the local files to GCS
local_files_to_gcs = LocalFilesystemToGcsOperator(
    task_id='local_files_to_gcs',
    src_paths=local_file_paths,
    dst_uris=gcs_file_paths,
    bucket_name=bucket_name,
    dag=dag
)

# Create the BigQuery dataset
create_bigquery_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_bigquery_dataset',
    dataset_id=dataset_id,
    dag=dag
)

# Create the BigQuery table schema
table_schema = [
    {'name': 'column1', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'column2', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'column3', 'type': 'INTEGER', 'mode': 'NULLABLE'}
]

# Create the BigQuery tables for each file
create_bigquery_tables = []
for i, gcs_file_path in enumerate(gcs_file_paths):
    table_id = f'table{i+1}'
    create_bigquery_table = BigQueryCreateEmptyTableOperator(
        task_id=f'create_bigquery_table_{i+1}',
        project_id='your_project_id',
        dataset_id='your_dataset_id',
        table_id='your_table_id',
        skip_leading_rows=1,
        source_format='CSV',
        source_uris=[filename],
        gcp_conn_id='your_gcp_connection_id',
        dag=dag
    )