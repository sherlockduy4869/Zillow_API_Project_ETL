import boto3
import logging
from botocore.exceptions import ClientError
import time

def create_glue_crawler(
    glue: boto3.client,
    crawler_name: str,
    database_name: str,
    role_arn: str,
    s3_path: str,
):
    
    try:
        glue.get_crawler(Name=crawler_name)
        logging.info(f"Crawler '{crawler_name}' already exists.")
    except glue.exceptions.EntityNotFoundException:
        logging.info(f"Creating crawler '{crawler_name}'...")
        glue.create_crawler(
            Name=crawler_name,
            Role=role_arn,
            DatabaseName=database_name,
            Targets={"S3Targets": [{"Path": s3_path}]},
            RecrawlPolicy={"RecrawlBehavior": "CRAWL_EVERYTHING"},
        )
        logging.info("Crawler created successfully.")

def run_glue_crawler(glue: boto3.client, crawler_name: str):
    try:
        logging.info(f"Starting crawler '{crawler_name}'...")
        glue.start_crawler(Name=crawler_name)
    except ClientError as e:
        if "CrawlerRunningException" in str(e):
            logging.info("Crawler is already running, waiting for it to finish...")
        else:
            raise

    # Wait until crawler finishes
    while True:
        response = glue.get_crawler(Name=crawler_name)
        state = response["Crawler"]["State"]
        print(f"Current state: {state}")
        if state == "READY":
            print("Crawler has completed successfully ✅")
            break
        time.sleep(30)