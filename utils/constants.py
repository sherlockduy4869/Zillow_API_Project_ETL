import configparser
import os

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), '../config/config.conf'))

X_RAPIDAPI_KEY = parser.get('api_keys', 'x_rapidapi_key')
X_RAPIDAPI_HOST = parser.get('api_keys', 'x_rapidapi_host')

DATABASE_HOST =  parser.get('database', 'database_host')
DATABASE_NAME =  parser.get('database', 'database_name')
DATABASE_PORT =  parser.get('database', 'database_port')
DATABASE_USER =  parser.get('database', 'database_username')
DATABASE_PASSWORD =  parser.get('database', 'database_password')

#AWS
AWS_ACCESS_KEY_ID = parser.get('aws', 'aws_access_key_id')
AWS_SECRET_ACCESS_KEY = parser.get('aws', 'aws_secret_access_key')
AWS_REGION = parser.get('aws', 'aws_region')
AWS_BUCKET_NAME = parser.get('aws', 'aws_bucket_name')

INPUT_PATH = parser.get('file_paths', 'input_path')
OUTPUT_PATH = parser.get('file_paths', 'output_path')


#GLUE
GLUE_CRAWLER_NAME = parser.get('glue', 'crawler_name')
GLUE_DATABASE_NAME = parser.get('glue', 'database_name')
GLUE_ROLE_ARN = parser.get('glue', 'role_arn')