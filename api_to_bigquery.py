# A program that requests stock data from Marketstack REST API and uploads to BigQuery
import requests
from google.cloud import bigquery
from configure import marketstack_api_key, project_id, dataset_name, table_name


# Initialize BigQuery client
client = bigquery.Client()

# Google Cloud project ID, dataset, table name
table_id = f"{project_id}.{dataset_name}.{table_name}"

# Marketstack API key
api_key = marketstack_api_key
# Stock symbol
symbol = 'ORCL'
# marketstack api endpoint for end-of-day stock data
url = f'http://api.marketstack.com/v1/eod?access_key={api_key}&smybols={symbol}'

try:
    # API request
    response = requests.get(url)
    # Raise exception for HTTP error
    response.raise_for_status()  

    # check for successful request
    if response.status_code == 200:
        stock_data = response.json()

        # save fetched data
        rows_to_insert = [
            {
                'Date': row['date'],
                'Open': row['open'],
                'High': row['high'],
                'Low': row['low'],
                'Close': row['close'],
                'Volume': row['volume']
            }
            for row in stock_data['data']
        ]

        errors = client.insert_rows_json(table_id, rows_to_insert)
        if errors == []:
            print(f"Successfully added {len(rows_to_insert)} rows into {table_id}.")
        else:
            print('Error during BigQuery insertion', errors) 
    else:
        print('Failed to retrieve data:', response.status_code)
except requests.RequestException as e:
    print("Error during API request", e)