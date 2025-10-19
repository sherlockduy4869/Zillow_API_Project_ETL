from etls.aws_glue_etl import create_glue_crawler, run_glue_crawler
from utils.constants import AWS_BUCKET_NAME, GLUE_CRAWLER_NAME, GLUE_DATABASE_NAME, GLUE_ROLE_ARN

def glue_pipeline(session):

    glue = session.client("glue")

    crawler_name = GLUE_CRAWLER_NAME
    role_arn = GLUE_ROLE_ARN
    database_name = GLUE_DATABASE_NAME
    s3_path = f'{AWS_BUCKET_NAME}/transformed'

    create_glue_crawler(glue, crawler_name, database_name, role_arn, s3_path)

    run_glue_crawler(glue, crawler_name)