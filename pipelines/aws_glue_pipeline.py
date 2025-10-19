from etls.aws_glue_etl import create_glue_crawler, run_glue_crawler
import boto3
from utils.constants import AWS_ACCESS_KEY_ID, AWS_BUCKET_NAME, AWS_SECRET_ACCESS_KEY, AWS_REGION, GLUE_CRAWLER_NAME, GLUE_DATABASE_NAME, GLUE_ROLE_ARN

def glue_pipeline():

    session = boto3.Session(
    aws_access_key_id = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    region_name = AWS_REGION)

    glue = session.client("glue", region_name = AWS_REGION)

    crawler_name = GLUE_CRAWLER_NAME
    role_arn = GLUE_ROLE_ARN
    database_name = GLUE_DATABASE_NAME
    s3_path = f'{AWS_BUCKET_NAME}/transformed'

    create_glue_crawler(glue, crawler_name, database_name, role_arn, s3_path)

    run_glue_crawler(glue, crawler_name)