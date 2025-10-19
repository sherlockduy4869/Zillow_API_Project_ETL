
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
        logging.info('File uploaded to s3')
    except FileNotFoundError:
        logging.error('The file was not found')

def check_file_availability(bucket_path: str, file_name: str):
    try:
        s3 = connect_to_s3()
        full_path = bucket_path + file_name.split('/')[-1]
        if s3.exists(full_path):
            logging.info(f"File {full_path} exists in S3.")
            return True
        else:
            logging.warning(f"File {full_path} does not exist in S3.")
            return False
    except Exception as e:
        logging.error(f"Error checking file availability in S3: {e}")
        raise e