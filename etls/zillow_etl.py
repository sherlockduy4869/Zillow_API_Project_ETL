import requests

import pandas as pd

def extract_zillow_data(query_string, url, x_rapidapi_key: str, x_rapidapi_host: str):

    headers = {
        "x-rapidapi-key": x_rapidapi_key,
        "x-rapidapi-host": x_rapidapi_host
    }

    response = requests.get(url, headers=headers, params=query_string)
    response_data = response.json()

    return response_data

def transform_zillow_data(zillow_data: dict):

    zillow_data_f = []
    for i in zillow_data['results']:
        zillow_data_f.append(i)

    zillow_data_df = pd.DataFrame(zillow_data_f)

    selected_columns = ['bathrooms', 'bedrooms', 'city', 'homeStatus', 
                        'homeType','livingArea','price', 'rentZestimate','zipcode']
    zillow_data_transformed_df = zillow_data_df[selected_columns]

    return zillow_data_transformed_df

def load_data_to_csv(zillow_data_transformed_df: pd.DataFrame, file_path: str):
    zillow_data_transformed_df.to_csv(file_path, index=False)
    