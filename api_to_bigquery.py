# Script requests stock data from Marketstack REST API and uploads to BigQuery
import requests
import logging
from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
from configure import api_key, project_id, dataset_name, table_name

# Logging setup
logging.basicConfig(level=logging.INFO)


def fetch_stock_data(api_key, symbol):
    """Fetches stock data from Marketstack REST API"""
    url = 'http://api.marketstack.com/v1/eod'
    params = {'access_key': api_key, 'symbols': symbol}
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()    # Raise exception for HTTP error
        if response.ok:
            return response.json()['data']
        else:
            logging.error((
                f"Failed to retrieve data. HTTP Status Code: "
                f"{response.status_code}"
            ))
            return None
    except requests.RequestException as e:
        logging.error(f"Error during API request: {e}")
        return None


def transform_stock_data(stock_data):
    """Transforms raw stock data into format for BigQuery insertion."""
    return [
        {
            'Date': datetime.fromisoformat(row['date']).date()
                            .strftime('%Y-%m-%d'),
            'Open': row['open'],
            'High': row['high'],
            'Close': row['close'],
            'Volume': row['volume']
        }
        for row in stock_data
    ]


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
                    'Date': datetime.fromisoformat(row['date']).date()
                                    .strftime('%Y-%m-%d'),
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
            logging.info(f"Added {len(rows_to_insert)} rows into {table_id}.")
        # Error inserting into BigQuery
        else:
            logging.error(f"Error during BigQuery insertion: {errors}")
    # Error retrieving data
    else:
        print('Failed to retrieve data. HTTP Status Code:',
              response.status_code)

# API errors
except requests.RequestException as e:
    logging.error(f"Error during API request: {e}")

# BigQuery Error
except GoogleAPIError as e:
    logging.error(f"Error inserting data into BigQuery: {e}")

# general errors
except Exception as e:
    logging.error(f"An unexpected error occurred: {e}")
