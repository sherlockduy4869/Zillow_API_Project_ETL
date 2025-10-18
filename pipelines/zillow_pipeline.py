from etls.zillow_etl import extract_zillow_data, load_data_to_csv, transform_zillow_data
from utils.constants import OUTPUT_PATH, X_RAPIDAPI_KEY, X_RAPIDAPI_HOST
import pandas as pd
import logging

def zillow_pipeline(file_name: str, query_string: str, url:str):
    #extract

    zillow_data = extract_zillow_data(query_string, url, X_RAPIDAPI_KEY, X_RAPIDAPI_HOST)

    #transform
    zillow_data_transformed_df = transform_zillow_data(zillow_data)


    #loading to csv
    file_path = f'{OUTPUT_PATH}/{file_name}.csv'
    load_data_to_csv(zillow_data_transformed_df, file_path)

