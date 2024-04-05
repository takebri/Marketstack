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
url = f'http://api.marketstack.com/v1/eod?access_key={api_key}&symbols={symbol}'

try:
    # API request
    response = requests.get(url)
    # Raise exception for HTTP error
    response.raise_for_status()

    # check for successful request
    if response.ok:
        stock_data = response.json()['data']

        if stock_data:
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
                for row in stock_data
            ]

        # insert data into bigquery
        errors = client.insert_rows_json(table_id, rows_to_insert)
        if not errors:
            print(f"Successfully added {len(rows_to_insert)} rows into {table_id}.")
        else:
            print('Error during BigQuery insertion', errors)
    else:
        print('Failed to retrieve data:', response.status_code)
except requests.RequestException as e:
    print("Error during API request", e)
except Exception as e:
    print("An unexpected error occurred:", e)
