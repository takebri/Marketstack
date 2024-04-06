# Script requests stock data from Marketstack REST API and uploads to BigQuery
import requests
from google.cloud import bigquery
from configure import api_key, project_id, dataset_name, table_name


# Initialize BigQuery client
client = bigquery.Client()

# Google Cloud project ID, dataset, table name
table_id = f"{project_id}.{dataset_name}.{table_name}"

# Parameters
params = {
    'access_key': api_key,
    'symbols': 'ORCL'
}

# API endpoint for stock data
url = 'http://api.marketstack.com/v1/eod'

try:
    # API request
    response = requests.get(url, params=params)
    # Raise exception for HTTP error
    response.raise_for_status()

    # check for successful request
    if response.ok:
        stock_data = response.json()['data']

        # Check for data
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

        # Insert data into bigquery
        errors = client.insert_rows_json(table_id, rows_to_insert)
        # No error inserting into BigQuery
        if not errors:
            print(f"Added {len(rows_to_insert)} rows into {table_id}.")
        # Error inserting into BigQuery
        else:
            print('Error during BigQuery insertion', errors)
    # Error retrieving data
    else:
        print('Failed to retrieve data:', response.status_code)
# API errors
except requests.RequestException as e:
    print("Error during API request", e)
# general errors
except Exception as e:
    print("An unexpected error occurred:", e)
