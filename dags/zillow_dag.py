from airflow import DAG
from datetime import datetime
import os
import sys
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines.zillow_pipeline import zillow_pipeline

default_args = {
    'owner' : 'trandinhduy',
    'start_date' : datetime(2025, 10, 18),
}

file_postfix = datetime.now().strftime("%Y%m%d_%H%M%S")

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
        'query_string' : {"location":"houston, tx","output":"json","status":"forSale","sortSelection":"priorityscore","listing_type":"by_agent","doz":"any"},
        'url': 'https://zillow56.p.rapidapi.com/search'
    },
    dag=dag
)

#uploading data to s3 bucket
    