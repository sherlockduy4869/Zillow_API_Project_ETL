
import s3fs
from utils.constants import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
import logging

def connect_to_s3():
    try:
        s3 = s3fs.S3FileSystem(
            anon = False,
            key = AWS_ACCESS_KEY_ID,
            secret = AWS_SECRET_ACCESS_KEY
        )
        return s3
    except Exception as e:
        logging.error(f"Error connecting to S3: {e}")
        raise e

def create_bucket_if_not_exists(s3, bucket_name):
    try:
        if not s3.exists(bucket_name):
            s3.mkdir(bucket_name)
            logging.info(f"Bucket {bucket_name} created.")
        else:
            logging.info(f"Bucket {bucket_name} already exists.")
    except Exception as e:
        logging.error(f"Error creating bucket {bucket_name}: {e}")
        raise e
    
def upload_to_s3(s3: s3fs.S3FileSystem, file_path: str, bucket:str, s3_file_name: str):
    try:
        s3.put(file_path, bucket+'/transformed/'+ s3_file_name)
        print('File uploaded to s3')
    except FileNotFoundError:
        print('The file was not found')