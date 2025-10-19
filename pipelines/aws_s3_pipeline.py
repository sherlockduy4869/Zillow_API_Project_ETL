from etls.aws_s3_etl import check_file_availability, connect_to_s3, create_bucket_if_not_exists, upload_to_s3
from utils.constants import AWS_BUCKET_NAME

def upload_s3_pipeline(ti):
    
    file_path = ti.xcom_pull(task_ids='extract_data_from_zillow', key='return_value')

    #connect to aws s3
    s3 = connect_to_s3()

    #create bucket if not exists in s3
    create_bucket_if_not_exists(s3, AWS_BUCKET_NAME)

    #upload file to s3 bucket
    upload_to_s3(s3, file_path, AWS_BUCKET_NAME, file_path.split('/')[-1])

def check_file_s3_pipeline(ti):

    bucket_transformed_path = f'{AWS_BUCKET_NAME}/transformed/'

    file_path = ti.xcom_pull(task_ids='extract_data_from_zillow', key='return_value')

    check_file_availability(bucket_transformed_path, file_path)



    
   