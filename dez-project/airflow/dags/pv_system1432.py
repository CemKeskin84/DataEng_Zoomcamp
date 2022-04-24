import os
import pyarrow.csv as pv
import pyarrow.parquet as pq
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

def remove(string):
    return string.replace(" ", "")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

#url = 'https://oedi-data-lake.s3.amazonaws.com/pvdaq/csv/pvdata/system_id=1199/year=2011/month=1/day=1/system_1199__date_2011_01_01.csv'


pv_system_ID = '1432' #selected from https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=pvdaq%2Fcsv%2Fpvdata%2F
pv_system_label = 'pvsys'+pv_system_ID



URL_PREFIX='https://oedi-data-lake.s3.amazonaws.com/pvdaq/csv/pvdata/system_id='+pv_system_ID+'/year='+'{{ execution_date.strftime(\'%Y\') }}'+'/month={{ execution_date.strftime(\'%-m\') }}'
URL_TEMPLATE= URL_PREFIX + '/day='+'{{ execution_date.strftime(\'%-e\') }}'+'/system_'+pv_system_ID+'__date_'+'{{ execution_date.strftime(\'%Y\') }}'+'_{{ execution_date.strftime(\'%m\') }}_'+'{{ execution_date.strftime(\'%d\') }}'+'.csv'


OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/pvsys'+pv_system_ID+'data_{{ execution_date.strftime(\'%Y\') }}{{ execution_date.strftime(\'%m\') }}{{ execution_date.strftime(\'%d\') }}.csv'

parquet_file = OUTPUT_FILE_TEMPLATE.replace('.csv', '.parquet')
parquet_file_name = 'pvsys'+pv_system_ID+'data_{{ execution_date.strftime(\'%Y\') }}{{ execution_date.strftime(\'%m\') }}{{ execution_date.strftime(\'%d\') }}.parquet'
                    

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", pv_system_label)

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))



# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)




with DAG(
    dag_id="dag_for_"+pv_system_label+"data",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2015, 12, 31),
    schedule_interval="@daily",
    

) as dag:

    download_task = BashOperator(
        task_id='get_data',
        bash_command=f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'
    )

    convert_task = PythonOperator(
        task_id="convert_csv_to_parquet",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": OUTPUT_FILE_TEMPLATE,
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"{pv_system_label}/{parquet_file_name}",
            "local_file": f"{parquet_file}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": pv_system_label+"_"+"{{execution_date.strftime(\'%d\') }}{{execution_date.strftime(\'%m\') }}{{execution_date.strftime(\'%Y\') }}",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/{pv_system_label}/{parquet_file_name}"],
            },
        },
    )



    download_task >> convert_task >> local_to_gcs_task >> bigquery_external_table_task



