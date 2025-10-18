from etls.aws_etl import connect_to_s3, create_bucket_if_not_exists, upload_to_s3
from utils.constants import AWS_BUCKET_NAME
import logging

def upload_s3_pipeline(ti):
    
    file_path = ti.xcom_pull(task_ids='extract_data_from_zillow', key='return_value')
    logging.info(f"File path retrieved from XCom: {file_path}")
    s3 = connect_to_s3()
    create_bucket_if_not_exists(s3, AWS_BUCKET_NAME)
    upload_to_s3(s3, file_path, AWS_BUCKET_NAME, file_path.split('/')[-1])
   