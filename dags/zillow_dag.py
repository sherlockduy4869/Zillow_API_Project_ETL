from airflow import DAG
from datetime import datetime
import os
import sys
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.constants import AWS_ACCESS_KEY_ID, AWS_REGION, AWS_SECRET_ACCESS_KEY, ZILLOW_API_QUERY_STRING, ZILLOW_API_URL
from pipelines.redshift_pipeline import redshift_pipeline
from pipelines.aws_glue_pipeline import glue_pipeline
from pipelines.aws_s3_pipeline import check_file_s3_pipeline, upload_s3_pipeline
from pipelines.zillow_pipeline import zillow_pipeline
import boto3

default_args = {
    'owner' : 'trandinhduy',
    'start_date' : datetime(2025, 10, 18),
}

file_postfix = datetime.now().strftime("%Y%m%d_%H%M%S")

session = boto3.Session(
    aws_access_key_id = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    region_name = AWS_REGION)

dag = DAG(
    dag_id='zillow_analytics_dag',
    default_args = default_args,
    schedule_interval = '@daily',
    catchup = False,
    tags = ['zillow', 'etl']
)

#extracting data from zillow 

extract = PythonOperator(
    task_id='extract_data_from_zillow',
    python_callable = zillow_pipeline,
    op_kwargs = {
        'file_name' : f'zillow_data_{file_postfix}',
        'query_string' : ZILLOW_API_QUERY_STRING,
        'url': ZILLOW_API_URL,
    },
    dag=dag
)

#uploading data to s3 bucket

upload_s3 = PythonOperator(
    task_id = 's3_upload_zillow_data',
    python_callable = upload_s3_pipeline,
    dag = dag
)

#checking availability of file in s3 bucket
is_file_uploaded_to_s3 = PythonOperator(
    task_id = 'is_file_uploaded_to_s3',
    python_callable = check_file_s3_pipeline,
    dag = dag
)

#creating glue crawler to catalog data in s3 bucket
glue_crawler = PythonOperator(
    task_id = 'glue_crawler_zillow_data',
    python_callable = glue_pipeline,
    op_kwargs = {
        'session' : session
    },
    dag = dag
)

#creating redshift cluster to load data from s3 bucket
redshift_cluster = PythonOperator(
    task_id = 'redshift_cluster_zillow_data',
    python_callable = redshift_pipeline,
    op_kwargs = {
        'session' : session
    },
    dag = dag
)

extract >> upload_s3 >> is_file_uploaded_to_s3 >> glue_crawler >> redshift_cluster